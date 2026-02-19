"""Rule base class and rule engine.

Every deterministic RCA rule inherits from Rule. The RuleEngine manages
rule registration, priority ordering, and bounded evaluation with the
4-band matching strategy described in the Technical Design Spec §4.4.
"""

from __future__ import annotations

import builtins
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from kuberca.models.analysis import CorrelationResult, EvaluationMeta, RuleResult
from kuberca.models.events import EventRecord
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.observability.logging import get_logger

_logger = get_logger("rule_engine")

# Safety bounds (§4.6)
_MAX_OBJECTS: int = 50
_CORRELATE_TIMEOUT_MS: float = 500.0
_FALLBACK_TIMEOUT_MS: float = 250.0


@runtime_checkable
class ResourceCache(Protocol):
    """Minimal interface the rule engine needs from the resource cache."""

    def get(self, kind: str, namespace: str, name: str) -> CachedResourceView | None: ...

    def list(self, kind: str, namespace: str = "") -> builtins.list[CachedResourceView]: ...

    def list_by_label(
        self, kind: str, labels: dict[str, str], namespace: str = ""
    ) -> builtins.list[CachedResourceView]: ...

    def list_truncated(self, kind: str) -> bool: ...


@runtime_checkable
class ChangeLedger(Protocol):
    """Minimal interface the rule engine needs from the change ledger."""

    def diff(
        self,
        kind: str,
        namespace: str,
        name: str,
        since_hours: float = 2.0,
    ) -> list[FieldChange]: ...

    def latest(self, kind: str, namespace: str, name: str) -> object | None: ...


class Rule(ABC):
    """Abstract base class for all deterministic RCA rules.

    Subclasses MUST define class-level attributes:
        rule_id               -- e.g. "R01_oom_killed"
        display_name          -- e.g. "OOMKilled"
        priority              -- lower is evaluated first (1–100)
        base_confidence       -- starting confidence, typically 0.50–0.60
        resource_dependencies -- resource kinds needed: ["Deployment", "Pod"]
        relevant_field_paths  -- JSONPath prefixes for causal diffs

    The three-phase pipeline:
        match()     -- O(1), pure, no side effects
        correlate() -- bounded: max 50 objects, 500 ms wall-clock
        explain()   -- converts correlation into a structured RuleResult
    """

    rule_id: str
    display_name: str
    priority: int
    base_confidence: float
    resource_dependencies: list[str]
    relevant_field_paths: list[str]

    @abstractmethod
    def match(self, event: EventRecord) -> bool:
        """Phase 1: Does this event trigger this rule?

        MUST be pure. No side effects. No API calls. O(1) complexity.
        """

    @abstractmethod
    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Phase 2: Fetch related changes from cache and ledger.

        MUST read only from cache/ledger. No direct K8s API calls.
        MUST complete within 500 ms. Max 50 cached objects queried.
        Returns CorrelationResult; objects_queried and duration_ms are
        populated by the rule itself.
        """

    @abstractmethod
    def explain(
        self,
        event: EventRecord,
        correlation: CorrelationResult,
    ) -> RuleResult:
        """Phase 3: Produce structured RCA output from the correlation."""


@dataclass
class _Candidate:
    """Internal evaluation candidate tracked during rule scanning."""

    result: RuleResult
    rule: Rule


class RuleEngine:
    """Evaluates events against all registered rules.

    Matching strategy (§4.4):
      1. Evaluate rules in priority order (lowest value first).
      2. confidence >= 0.85  → return immediately (short-circuit).
      3. 0.70 <= confidence < 0.85 → record candidate; continue only rules
         that share a resource_dependency with the matched rule.
      4. confidence < 0.70  → record candidate; continue all rules.
      5. Return highest-confidence candidate after all applicable rules.

    Safety bounds are enforced, not advisory: if correlate() exceeds
    500 ms or 50 objects, the rule is skipped and a warning is recorded
    in EvaluationMeta.warnings.
    """

    def __init__(self, cache: ResourceCache, ledger: ChangeLedger) -> None:
        self._cache = cache
        self._ledger = ledger
        self._rules: list[Rule] = []

    def register(self, rule: Rule) -> None:
        """Add a rule and maintain the priority-sorted order.

        Tie-breaker: equal-priority rules are sorted by rule_id
        lexicographically (ascending) for deterministic evaluation order.
        """
        self._rules.append(rule)
        self._rules.sort(key=lambda r: (r.priority, r.rule_id))

    def evaluate(self, event: EventRecord) -> tuple[RuleResult | None, EvaluationMeta]:
        """Evaluate event against all rules. Returns (best result | None, meta).

        Enforces the 4-band confidence strategy and all safety bounds.
        """
        meta = EvaluationMeta(
            rules_evaluated=0,
            rules_matched=0,
            duration_ms=0.0,
            warnings=[],
        )
        wall_start = time.monotonic()

        # Best candidate found so far
        best: _Candidate | None = None
        # When in "competing rules only" mode: set of resource_dependency kinds
        # shared with the first medium-confidence match.
        competing_deps: frozenset[str] | None = None

        for rule in self._rules:
            # In competing-only mode, skip rules that don't share a dependency.
            if competing_deps is not None:
                rule_deps = frozenset(rule.resource_dependencies)
                if rule_deps.isdisjoint(competing_deps):
                    continue

            # Phase 1: match — O(1), pure
            try:
                matched = rule.match(event)
            except Exception as exc:
                _logger.error(
                    "rule_match_exception",
                    rule_id=rule.rule_id,
                    error=str(exc),
                )
                meta.warnings.append(f"{rule.rule_id} raised exception during match: {exc}")
                continue

            meta.rules_evaluated += 1

            if not matched:
                continue

            # Phase 2: correlate — bounded
            corr, timed_out, obj_cap_hit = self._run_correlate(rule, event)

            if timed_out:
                meta.warnings.append(f"{rule.rule_id} timed out after {_CORRELATE_TIMEOUT_MS:.0f}ms")
                _logger.warning(
                    "rule_correlate_timeout",
                    rule_id=rule.rule_id,
                    timeout_ms=_CORRELATE_TIMEOUT_MS,
                )
                continue

            if obj_cap_hit:
                meta.warnings.append(f"{rule.rule_id} hit object cap ({corr.objects_queried}/{_MAX_OBJECTS})")

            # Phase 3: explain
            try:
                result = rule.explain(event, corr)
            except Exception as exc:
                _logger.error(
                    "rule_explain_exception",
                    rule_id=rule.rule_id,
                    error=str(exc),
                )
                meta.warnings.append(f"{rule.rule_id} raised exception during explain: {exc}")
                continue

            meta.rules_matched += 1
            _logger.info(
                "rule_matched",
                rule_id=rule.rule_id,
                confidence=result.confidence,
                event_reason=event.reason,
                resource=f"{event.resource_kind}/{event.namespace}/{event.resource_name}",
            )

            confidence = result.confidence

            # Short-circuit band: >= 0.85
            if confidence >= 0.85:
                meta.duration_ms = (time.monotonic() - wall_start) * 1000.0
                return result, meta

            # Medium confidence band: 0.70 – 0.84
            # Record candidate and switch to competing-rules-only mode.
            if confidence >= 0.70:
                if best is None or confidence > best.result.confidence:
                    best = _Candidate(result=result, rule=rule)
                    competing_deps = frozenset(rule.resource_dependencies)
                continue

            # Low confidence band: < 0.70 — record candidate, continue all.
            if best is None or confidence > best.result.confidence:
                best = _Candidate(result=result, rule=rule)
                # Do NOT enter competing-only mode for low-confidence matches.

        meta.duration_ms = (time.monotonic() - wall_start) * 1000.0

        if best is not None:
            _logger.info(
                "rule_engine_result",
                rule_id=best.result.rule_id,
                confidence=best.result.confidence,
                rules_evaluated=meta.rules_evaluated,
                rules_matched=meta.rules_matched,
                duration_ms=meta.duration_ms,
            )
            return best.result, meta

        _logger.info(
            "rule_engine_no_match",
            rules_evaluated=meta.rules_evaluated,
            duration_ms=meta.duration_ms,
            event_reason=event.reason,
        )
        return None, meta

    def _run_correlate(
        self,
        rule: Rule,
        event: EventRecord,
    ) -> tuple[CorrelationResult, bool, bool]:
        """Run rule.correlate() in a thread with a hard wall-clock timeout.

        Returns (CorrelationResult, timed_out, obj_cap_hit).
        timed_out=True when the correlate() wall time exceeds _CORRELATE_TIMEOUT_MS.
        obj_cap_hit=True when CorrelationResult.objects_queried > _MAX_OBJECTS.

        Uses a daemon thread with threading.Event for timeout enforcement.
        The thread is allowed to complete naturally; we do not kill it.
        The result container and exception holder are shared via mutable list.
        """
        result_holder: list[CorrelationResult | None] = [None]
        exc_holder: list[BaseException | None] = [None]
        done = threading.Event()

        def _target() -> None:
            try:
                result_holder[0] = rule.correlate(event, self._cache, self._ledger)
            except Exception as exc:
                exc_holder[0] = exc
            finally:
                done.set()

        thread = threading.Thread(target=_target, daemon=True, name=f"correlate-{rule.rule_id}")
        thread.start()

        timeout_seconds = _CORRELATE_TIMEOUT_MS / 1000.0
        finished = done.wait(timeout=timeout_seconds)

        if not finished:
            # Thread is still running — treat as timeout, return empty result.
            empty = CorrelationResult(objects_queried=0, duration_ms=_CORRELATE_TIMEOUT_MS)
            return empty, True, False

        if exc_holder[0] is not None:
            _logger.error(
                "rule_correlate_exception",
                rule_id=rule.rule_id,
                error=str(exc_holder[0]),
            )
            empty = CorrelationResult(objects_queried=0, duration_ms=0.0)
            return empty, False, False

        corr = result_holder[0]
        if corr is None:
            empty = CorrelationResult(objects_queried=0, duration_ms=0.0)
            return empty, False, False

        obj_cap_hit = corr.objects_queried > _MAX_OBJECTS
        return corr, False, obj_cap_hit
