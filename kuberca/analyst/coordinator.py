"""Analyst Coordinator — orchestrates rule engine + LLM for RCA analysis.

Receives analysis requests from MCP/REST/CLI, checks cache readiness,
invokes the rule engine, optionally escalates to the LLM, and assembles
the final RCAResponse with full _meta populated.
"""

from __future__ import annotations

import re
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol

import structlog

from kuberca.llm.evidence import EvidencePackage
from kuberca.models.analysis import (
    AffectedResource,
    EvaluationMeta,
    EvidenceItem,
    RCAResponse,
    ResponseMeta,
    RuleResult,
    StateContextEntry,
)
from kuberca.models.events import DiagnosisSource, EventRecord, EventSource, EvidenceType, Severity
from kuberca.models.resources import CachedResourceView, CacheReadiness, FieldChange
from kuberca.observability.metrics import (
    llm_quality_check_failures_total,
    rca_duration_seconds,
    rca_requests_total,
)

if TYPE_CHECKING:
    from kuberca.graph.dependency_graph import DependencyGraph
    from kuberca.llm.analyzer import LLMAnalyzer, LLMResult


class _CacheProto(Protocol):
    """Minimal cache interface required by AnalystCoordinator."""

    def get(self, kind: str, namespace: str, name: str) -> CachedResourceView | None: ...

    def readiness(self) -> CacheReadiness: ...


class _LedgerProto(Protocol):
    """Minimal ledger interface required by AnalystCoordinator."""

    def diff(
        self,
        kind: str,
        namespace: str,
        name: str,
        since: timedelta | None = ...,
        since_hours: float | None = ...,
    ) -> list[FieldChange]: ...


class _EventBufferProto(Protocol):
    """Minimal event buffer interface required by AnalystCoordinator."""

    def get_events(
        self,
        resource_kind: str,
        namespace: str,
        name: str,
    ) -> list[EventRecord]: ...


class _RuleEngineProto(Protocol):
    """Minimal rule engine interface required by AnalystCoordinator."""

    def evaluate(self, event: EventRecord) -> tuple[RuleResult | None, EvaluationMeta]: ...


_logger = structlog.get_logger(component="analyst_coordinator")

# Canonical kind map per Section 10.4
KIND_ALIASES: dict[str, str] = {
    "pod": "Pod",
    "pods": "Pod",
    "po": "Pod",
    "deployment": "Deployment",
    "deployments": "Deployment",
    "deploy": "Deployment",
    "statefulset": "StatefulSet",
    "statefulsets": "StatefulSet",
    "sts": "StatefulSet",
    "daemonset": "DaemonSet",
    "daemonsets": "DaemonSet",
    "ds": "DaemonSet",
    "service": "Service",
    "services": "Service",
    "svc": "Service",
    "node": "Node",
    "nodes": "Node",
    "job": "Job",
    "jobs": "Job",
    "cronjob": "CronJob",
    "cronjobs": "CronJob",
    "ingress": "Ingress",
    "ingresses": "Ingress",
    "ing": "Ingress",
    "hpa": "HorizontalPodAutoscaler",
    "horizontalpodautoscaler": "HorizontalPodAutoscaler",
    "horizontalpodautoscalers": "HorizontalPodAutoscaler",
    "replicaset": "ReplicaSet",
    "replicasets": "ReplicaSet",
    "rs": "ReplicaSet",
    "persistentvolumeclaim": "PersistentVolumeClaim",
    "persistentvolumeclaims": "PersistentVolumeClaim",
    "pvc": "PersistentVolumeClaim",
    "namespace": "Namespace",
    "namespaces": "Namespace",
    "ns": "Namespace",
    "configmap": "ConfigMap",
    "configmaps": "ConfigMap",
    "cm": "ConfigMap",
    "secret": "Secret",
    "secrets": "Secret",
    "networkpolicy": "NetworkPolicy",
    "networkpolicies": "NetworkPolicy",
    "netpol": "NetworkPolicy",
    "persistentvolume": "PersistentVolume",
    "persistentvolumes": "PersistentVolume",
    "pv": "PersistentVolume",
    "resourcequota": "ResourceQuota",
    "resourcequotas": "ResourceQuota",
    "quota": "ResourceQuota",
}

_TIME_WINDOW_RE = re.compile(r"^([0-9]+)(m|h|d)$")
_DNS_SUBDOMAIN_RE = re.compile(r"^[a-z0-9]([a-z0-9\-\.]{0,251}[a-z0-9])?$")

_KUBERCA_VERSION = "0.1.0"
_SCHEMA_VERSION = "1"


class ResourceFormatError(Exception):
    """Raised when the resource string is malformed or uses an unknown kind."""

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self.code = code


class TimeWindowFormatError(Exception):
    """Raised when the time_window string is not parseable."""


def _utcnow_iso() -> str:
    """Return the current UTC time as an ISO-8601 string with milliseconds."""
    now = datetime.now(tz=UTC)
    return now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _parse_resource(resource: str) -> tuple[str, str, str]:
    """Parse and normalize a resource string into (kind, namespace, name).

    Raises ResourceFormatError on malformed input or unknown kind.
    """
    parts = resource.split("/")
    if len(parts) != 3:
        raise ResourceFormatError(
            f"Resource must be in kind/namespace/name format, got: {resource!r}",
            code="INVALID_RESOURCE_FORMAT",
        )
    raw_kind, namespace, name = parts
    kind = KIND_ALIASES.get(raw_kind.lower())
    if kind is None:
        accepted = sorted(set(KIND_ALIASES.values()))
        raise ResourceFormatError(
            f"Unknown resource kind: {raw_kind!r}. Accepted kinds: {accepted}",
            code="UNKNOWN_RESOURCE_KIND",
        )
    if not namespace or not name:
        raise ResourceFormatError(
            "namespace and name must not be empty",
            code="INVALID_RESOURCE_FORMAT",
        )
    if len(namespace) > 253 or len(name) > 253:
        raise ResourceFormatError(
            "namespace and name must be ≤253 characters",
            code="INVALID_RESOURCE_FORMAT",
        )
    return kind, namespace, name


def _validate_time_window(time_window: str) -> None:
    """Raise TimeWindowFormatError if time_window is not valid."""
    m = _TIME_WINDOW_RE.match(time_window)
    if not m:
        raise TimeWindowFormatError(f"Invalid time_window format: {time_window!r}. Must match [0-9]+(m|h|d).")
    value = int(m.group(1))
    unit = m.group(2)
    max_hours = 24
    hours = value if unit == "h" else (value / 60 if unit == "m" else value * 24)
    if hours > max_hours:
        raise TimeWindowFormatError(f"time_window {time_window!r} exceeds maximum of 24h.")


def _dedup_affected_resources(resources: list[AffectedResource]) -> list[AffectedResource]:
    """Deduplicate affected resources by (kind, namespace, name)."""
    seen: set[tuple[str, str, str]] = set()
    result: list[AffectedResource] = []
    for r in resources:
        key = (r.kind, r.namespace, r.name)
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def _rule_result_to_rca_response(
    rule_result: RuleResult,
    cache_readiness: CacheReadiness,
    eval_meta: EvaluationMeta,
    cluster_id: str,
    start_ms: int,
) -> RCAResponse:
    """Convert a RuleResult to RCAResponse per Section 8.3."""
    confidence = rule_result.confidence

    # Apply cache-state confidence penalties per spec
    if cache_readiness == CacheReadiness.DEGRADED:
        confidence -= 0.10
    elif cache_readiness == CacheReadiness.PARTIALLY_READY:
        confidence -= 0.15

    confidence = max(0.0, min(confidence, 0.95))

    # Sort evidence items by timestamp descending (most recent first)
    sorted_evidence = sorted(
        rule_result.evidence,
        key=lambda e: e.timestamp,
        reverse=True,
    )

    deduped_resources = _dedup_affected_resources(rule_result.affected_resources)

    warnings: list[str] = []
    if cache_readiness == CacheReadiness.DEGRADED:
        warnings.append("Cache degraded: data may be stale")
    elif cache_readiness == CacheReadiness.PARTIALLY_READY:
        warnings.append("Cache partially ready: controller hierarchy may be stale")
    warnings.extend(eval_meta.warnings)

    response_time_ms = (time.monotonic_ns() // 1_000_000) - start_ms

    return RCAResponse(
        root_cause=rule_result.root_cause,
        confidence=confidence,
        diagnosed_by=DiagnosisSource.RULE_ENGINE,
        rule_id=rule_result.rule_id,
        evidence=sorted_evidence,
        affected_resources=deduped_resources,
        suggested_remediation=rule_result.suggested_remediation,
        _meta=ResponseMeta(
            kuberca_version=_KUBERCA_VERSION,
            schema_version=_SCHEMA_VERSION,
            cluster_id=cluster_id,
            timestamp=_utcnow_iso(),
            response_time_ms=response_time_ms,
            cache_state=cache_readiness.value,
            warnings=warnings,
        ),
    )


def _llm_result_to_rca_response(
    llm_result: LLMResult,
    cache_readiness: CacheReadiness,
    eval_meta: EvaluationMeta,
    cluster_id: str,
    start_ms: int,
) -> RCAResponse:
    """Convert an LLMResult to RCAResponse per Section 8.4."""
    confidence = llm_result.confidence

    if cache_readiness == CacheReadiness.DEGRADED:
        confidence -= 0.10

    confidence = max(0.0, min(confidence, 0.70))

    # Build EvidenceItems from LLM citations
    evidence_items: list[EvidenceItem] = []
    for citation in llm_result.evidence_citations:
        # Citations are formatted as "timestamp + summary" by the LLM
        # Extract timestamp if present (first token), remainder is summary
        parts = citation.split(" ", 1)
        timestamp = parts[0] if parts else _utcnow_iso()
        summary = parts[1] if len(parts) > 1 else citation
        evidence_items.append(
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=timestamp,
                summary=summary,
                debug_context=None,
            )
        )

    warnings: list[str] = []
    if cache_readiness == CacheReadiness.DEGRADED:
        warnings.append("Cache degraded: data may be stale")
    elif cache_readiness == CacheReadiness.PARTIALLY_READY:
        warnings.append("Cache partially ready: controller hierarchy may be stale")
    warnings.extend(eval_meta.warnings)
    if llm_result.retries_used > 0:
        warnings.append(f"LLM retries used: {llm_result.retries_used}")

    response_time_ms = (time.monotonic_ns() // 1_000_000) - start_ms

    return RCAResponse(
        root_cause=llm_result.root_cause,
        confidence=confidence,
        diagnosed_by=DiagnosisSource.LLM,
        rule_id=None,
        evidence=evidence_items,
        affected_resources=_dedup_affected_resources(llm_result.affected_resources),
        suggested_remediation=llm_result.suggested_remediation,
        _meta=ResponseMeta(
            kuberca_version=_KUBERCA_VERSION,
            schema_version=_SCHEMA_VERSION,
            cluster_id=cluster_id,
            timestamp=_utcnow_iso(),
            response_time_ms=response_time_ms,
            cache_state=cache_readiness.value,
            warnings=warnings,
        ),
    )


def _inconclusive_response(
    root_cause: str,
    cache_readiness: CacheReadiness,
    warnings: list[str],
    cluster_id: str,
    start_ms: int,
) -> RCAResponse:
    """Build an INCONCLUSIVE RCAResponse."""
    response_time_ms = (time.monotonic_ns() // 1_000_000) - start_ms
    return RCAResponse(
        root_cause=root_cause,
        confidence=0.0,
        diagnosed_by=DiagnosisSource.INCONCLUSIVE,
        rule_id=None,
        evidence=[],
        affected_resources=[],
        suggested_remediation="",
        _meta=ResponseMeta(
            kuberca_version=_KUBERCA_VERSION,
            schema_version=_SCHEMA_VERSION,
            cluster_id=cluster_id,
            timestamp=_utcnow_iso(),
            response_time_ms=response_time_ms,
            cache_state=cache_readiness.value,
            warnings=warnings,
        ),
    )


class AnalystCoordinator:
    """Orchestrates the full RCA analysis pipeline.

    Decision flow per Section 8.2:
      1. Validate resource string and time_window.
      2. Check cache readiness — WARMING returns immediately.
      3. Fetch events from event buffer.
      4. Invoke rule engine on most recent event.
      5. If rule matched: convert to RCAResponse.
      6. If DEGRADED: skip LLM, return INCONCLUSIVE.
      7. If LLM available and not DEGRADED: invoke LLM.
      8. Convert LLMResult or fall through to INCONCLUSIVE.
    """

    def __init__(
        self,
        rule_engine: _RuleEngineProto,
        llm_analyzer: LLMAnalyzer | None,
        cache: _CacheProto,
        ledger: _LedgerProto,
        event_buffer: _EventBufferProto,
        config: Any,
        dependency_graph: DependencyGraph | None = None,
    ) -> None:
        self._rule_engine = rule_engine
        self._llm_analyzer = llm_analyzer
        self._cache = cache
        self._ledger = ledger
        self._event_buffer = event_buffer
        self._config = config
        self._dependency_graph = dependency_graph

    async def analyze(
        self,
        resource: str,
        time_window: str = "2h",
    ) -> RCAResponse:
        """Full analysis pipeline. Always returns an RCAResponse, never raises.

        Arg resource: "kind/namespace/name" format (case-insensitive kind).
        Arg time_window: lookback window, e.g. "2h", "30m". Max "24h".
        """
        start_ms = time.monotonic_ns() // 1_000_000
        cluster_id: str = getattr(self._config, "cluster_id", "")

        # Step 1: validate inputs
        try:
            kind, namespace, name = _parse_resource(resource)
        except ResourceFormatError as exc:
            _logger.warning("invalid_resource_format", resource=resource, error=str(exc))
            cache_readiness = self._get_cache_readiness()
            return _inconclusive_response(
                root_cause=str(exc),
                cache_readiness=cache_readiness,
                warnings=[str(exc)],
                cluster_id=cluster_id,
                start_ms=start_ms,
            )

        try:
            _validate_time_window(time_window)
        except TimeWindowFormatError as exc:
            _logger.warning("invalid_time_window", time_window=time_window, error=str(exc))
            cache_readiness = self._get_cache_readiness()
            return _inconclusive_response(
                root_cause=str(exc),
                cache_readiness=cache_readiness,
                warnings=[str(exc)],
                cluster_id=cluster_id,
                start_ms=start_ms,
            )

        canonical_resource = f"{kind}/{namespace}/{name}"

        # Step 2: check cache readiness
        cache_readiness = self._get_cache_readiness()

        if cache_readiness == CacheReadiness.WARMING:
            warming_warnings = self._warming_warnings()
            _logger.info(
                "analysis_blocked_warming",
                resource=canonical_resource,
                warnings=warming_warnings,
            )
            return _inconclusive_response(
                root_cause="KubeRCA is still initializing. Retry in 30 seconds.",
                cache_readiness=cache_readiness,
                warnings=warming_warnings,
                cluster_id=cluster_id,
                start_ms=start_ms,
            )

        # Step 3: fetch events from buffer + synthesise from pod status
        events = self._get_events(kind, namespace, name)
        synthetic = self._synthesise_status_events(kind, namespace, name)
        all_events = synthetic + events  # synthetic first for priority

        if not all_events:
            _logger.info("no_events_found", resource=canonical_resource)
            rca_requests_total.labels(diagnosed_by=DiagnosisSource.INCONCLUSIVE.value).inc()
            return _inconclusive_response(
                root_cause=f"No recent events found for {canonical_resource}",
                cache_readiness=cache_readiness,
                warnings=[],
                cluster_id=cluster_id,
                start_ms=start_ms,
            )

        # Step 4: invoke rule engine — try each event until a rule matches
        rule_result: RuleResult | None = None
        eval_meta = EvaluationMeta(rules_evaluated=0, rules_matched=0, duration_ms=0.0)
        incident_event = all_events[0]

        for candidate_event in all_events:
            rule_result, eval_meta = self._evaluate_rules(candidate_event)
            if rule_result is not None:
                incident_event = candidate_event
                break

        # Step 5: rule matched
        if rule_result is not None:
            _logger.info(
                "rule_matched",
                rule_id=rule_result.rule_id,
                confidence=rule_result.confidence,
                resource=canonical_resource,
            )
            rca_requests_total.labels(diagnosed_by=DiagnosisSource.RULE_ENGINE.value).inc()
            response = _rule_result_to_rca_response(
                rule_result=rule_result,
                cache_readiness=cache_readiness,
                eval_meta=eval_meta,
                cluster_id=cluster_id,
                start_ms=start_ms,
            )
            # Populate blast_radius from dependency graph
            response.blast_radius = self._compute_blast_radius(kind, namespace, name)
            diagnosed_by = "rule_engine"
            rule_id = rule_result.rule_id
            _record_duration(start_ms, diagnosed_by, rule_id)
            return response

        # Step 6: DEGRADED — skip LLM
        if cache_readiness == CacheReadiness.DEGRADED:
            suppression_msg = (
                "LLM escalation suppressed: cache degraded, evidence package integrity cannot be guaranteed."
            )
            degraded_warnings = self._degraded_warnings()
            _logger.warning(
                "llm_suppressed_degraded",
                resource=canonical_resource,
            )
            rca_requests_total.labels(diagnosed_by=DiagnosisSource.INCONCLUSIVE.value).inc()
            return _inconclusive_response(
                root_cause="Unable to diagnose: cache degraded.",
                cache_readiness=cache_readiness,
                warnings=degraded_warnings + [suppression_msg] + eval_meta.warnings,
                cluster_id=cluster_id,
                start_ms=start_ms,
            )

        # Step 7: LLM escalation (if available)
        if self._llm_analyzer is not None and self._llm_analyzer.available:
            llm_warning: list[str] = []
            if cache_readiness == CacheReadiness.PARTIALLY_READY:
                llm_warning = [
                    "Cache partially ready: last full relist may be stale. "
                    "LLM analysis proceeds with available evidence."
                ]

            # State validation gate: gather verified state context from graph
            state_context = self._build_state_context(kind, namespace, name)

            evidence_pkg = self._assemble_evidence(
                incident_event=incident_event,
                all_events=events,
                time_window=time_window,
                state_context=state_context,
            )

            llm_result = await self._llm_analyzer.analyze(
                event=incident_event,
                evidence=evidence_pkg,
            )

            if llm_result.success:
                # Quality check loop: retry LLM up to 2 times on quality failure
                max_quality_retries = 2
                quality_retries = 0

                while quality_retries < max_quality_retries:
                    quality = self._llm_analyzer.quality_check(
                        result=llm_result,
                        event=incident_event,
                        state_context=state_context,
                    )
                    if quality.passed:
                        break

                    for failure in quality.failures:
                        llm_quality_check_failures_total.labels(failure_type=failure).inc()
                    _logger.warning(
                        "llm_quality_check_failed",
                        failures=quality.failures,
                        retry=quality_retries + 1,
                        resource=canonical_resource,
                    )

                    # Retry with QUALITY_RETRY_PROMPT
                    quality_retries += 1
                    retry_result = await self._llm_analyzer.analyze_with_quality_retry(
                        event=incident_event,
                        evidence=evidence_pkg,
                        quality_failures=quality.failures,
                    )
                    if retry_result.success:
                        retry_result.retries_used = llm_result.retries_used + quality_retries
                        llm_result = retry_result
                    else:
                        _logger.warning(
                            "llm_quality_retry_failed",
                            retry=quality_retries,
                            resource=canonical_resource,
                        )
                        break

                # Final quality check after retries exhausted
                if not quality.passed:
                    llm_result.confidence = min(llm_result.confidence, 0.35)
                    llm_warning.append(
                        f"LLM quality check failed after {quality_retries} retries: {', '.join(quality.failures)}"
                    )

                _logger.info(
                    "llm_diagnosis",
                    confidence=llm_result.confidence,
                    quality_retries=quality_retries,
                    resource=canonical_resource,
                )
                rca_requests_total.labels(diagnosed_by=DiagnosisSource.LLM.value).inc()
                eval_meta.warnings = llm_warning + eval_meta.warnings
                response = _llm_result_to_rca_response(
                    llm_result=llm_result,
                    cache_readiness=cache_readiness,
                    eval_meta=eval_meta,
                    cluster_id=cluster_id,
                    start_ms=start_ms,
                )
                # Populate blast_radius from dependency graph
                response.blast_radius = self._compute_blast_radius(kind, namespace, name)
                _record_duration(start_ms, "llm", None)
                return response

            _logger.warning(
                "llm_failed",
                reason=llm_result.root_cause,
                resource=canonical_resource,
            )

        # Step 8: INCONCLUSIVE
        _logger.info("inconclusive", resource=canonical_resource)
        rca_requests_total.labels(diagnosed_by=DiagnosisSource.INCONCLUSIVE.value).inc()
        inconclusive_warnings = eval_meta.warnings[:]
        if self._llm_analyzer is not None and not self._llm_analyzer.available:
            ollama_endpoint = getattr(getattr(self._config, "ollama", None), "endpoint", "unknown")
            inconclusive_warnings.append(f"LLM unavailable: Ollama not reachable at {ollama_endpoint}")
        _record_duration(start_ms, "inconclusive", None)
        return _inconclusive_response(
            root_cause=f"No diagnosis determined for {canonical_resource}",
            cache_readiness=cache_readiness,
            warnings=inconclusive_warnings,
            cluster_id=cluster_id,
            start_ms=start_ms,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _synthesise_status_events(
        self,
        kind: str,
        namespace: str,
        name: str,
    ) -> list[EventRecord]:
        """Generate synthetic EventRecords from pod container status.

        Kubernetes v1.Events use generic reasons like "Failed" and "BackOff",
        but the rule engine matches on specific reasons like "OOMKilled",
        "ImagePullBackOff", and "CrashLoopBackOff" that appear in the pod
        container status.  This method bridges the gap.
        """
        if kind != "Pod":
            return []
        try:
            cached = self._cache.get(kind, namespace, name)
            if cached is None:
                return []
        except Exception:
            return []

        now = datetime.now(tz=UTC)
        results: list[EventRecord] = []
        _raw_cs = cached.status.get("containerStatuses")
        container_statuses: list[object] = _raw_cs if isinstance(_raw_cs, list) else []
        for cs in container_statuses:
            if not isinstance(cs, dict):
                continue

            # Terminated container (state.terminated or lastState.terminated)
            state = cs.get("state", {})
            last_state = cs.get("lastState") or cs.get("last_state", {})
            terminated = (state.get("terminated") if isinstance(state, dict) else None) or (
                last_state.get("terminated") if isinstance(last_state, dict) else None
            )
            if isinstance(terminated, dict):
                reason = terminated.get("reason", "")
                if reason in ("OOMKilled", "OOMKilling"):
                    results.append(
                        EventRecord(
                            source=EventSource.POD_PHASE,
                            severity=Severity.CRITICAL,
                            reason=reason,
                            message=terminated.get("message", f"Container terminated: {reason}"),
                            namespace=namespace,
                            resource_kind="Pod",
                            resource_name=name,
                            first_seen=now,
                            last_seen=now,
                            cluster_id=getattr(self._config, "cluster_id", ""),
                        )
                    )

            # Waiting container (state.waiting)
            waiting = state.get("waiting") if isinstance(state, dict) else None
            if isinstance(waiting, dict):
                reason = waiting.get("reason", "")
                if reason in ("CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"):
                    results.append(
                        EventRecord(
                            source=EventSource.CORE_EVENT,
                            severity=Severity.ERROR,
                            reason=reason,
                            message=waiting.get("message", f"Container waiting: {reason}"),
                            namespace=namespace,
                            resource_kind="Pod",
                            resource_name=name,
                            first_seen=now,
                            last_seen=now,
                            cluster_id=getattr(self._config, "cluster_id", ""),
                        )
                    )

        # Also check init container statuses
        _raw_init = cached.status.get("initContainerStatuses")
        init_statuses: list[object] = _raw_init if isinstance(_raw_init, list) else []
        for cs in init_statuses:
            if not isinstance(cs, dict):
                continue
            state = cs.get("state", {})
            waiting = state.get("waiting") if isinstance(state, dict) else None
            if isinstance(waiting, dict):
                reason = waiting.get("reason", "")
                if reason in ("CrashLoopBackOff", "ImagePullBackOff", "ErrImagePull"):
                    results.append(
                        EventRecord(
                            source=EventSource.CORE_EVENT,
                            severity=Severity.ERROR,
                            reason=reason,
                            message=waiting.get("message", f"Init container waiting: {reason}"),
                            namespace=namespace,
                            resource_kind="Pod",
                            resource_name=name,
                            first_seen=now,
                            last_seen=now,
                            cluster_id=getattr(self._config, "cluster_id", ""),
                        )
                    )

        return results

    def _get_cache_readiness(self) -> CacheReadiness:
        """Read the current cache readiness state, defaulting to READY on error."""
        try:
            return self._cache.readiness()
        except Exception as exc:
            _logger.error("cache_readiness_error", error=str(exc))
            return CacheReadiness.READY

    def _warming_warnings(self) -> list[str]:
        """Collect per-kind warming warnings from the cache."""
        try:
            # If the cache exposes a warming_kinds() method, use it
            warming_kinds_fn = getattr(self._cache, "warming_kinds", None)
            if warming_kinds_fn is None:
                raise AttributeError("warming_kinds not available")
            kinds: list[str] = warming_kinds_fn()
            warnings: list[str] = [f"Waiting for initial list: {', '.join(kinds)}"] if kinds else []
            return warnings
        except AttributeError:
            return ["Cache is warming — initial list calls in progress."]
        except Exception as exc:
            _logger.warning("warming_warnings_error", error=str(exc))
            return ["Cache is warming."]

    def _degraded_warnings(self) -> list[str]:
        """Collect per-kind staleness warnings from the cache."""
        try:
            staleness_warnings_fn = getattr(self._cache, "staleness_warnings", None)
            if staleness_warnings_fn is None:
                raise AttributeError("staleness_warnings not available")
            staleness = staleness_warnings_fn()
            return staleness if isinstance(staleness, list) else []
        except AttributeError:
            return ["Cache degraded: one or more resource kinds unavailable."]
        except Exception as exc:
            _logger.warning("degraded_warnings_error", error=str(exc))
            return ["Cache degraded."]

    def _get_events(
        self,
        kind: str,
        namespace: str,
        name: str,
    ) -> list[EventRecord]:
        """Fetch events from the event buffer for the given resource.

        Returns events sorted by last_seen descending (most recent first).
        """
        try:
            events: list[EventRecord] = self._event_buffer.get_events(
                resource_kind=kind,
                namespace=namespace,
                name=name,
            )
            return sorted(events, key=lambda e: e.last_seen, reverse=True)
        except Exception as exc:
            _logger.error("event_buffer_error", error=str(exc))
            return []

    def _evaluate_rules(
        self,
        event: EventRecord,
    ) -> tuple[RuleResult | None, EvaluationMeta]:
        """Invoke the rule engine and return (result, meta)."""
        try:
            result, meta = self._rule_engine.evaluate(event)
            return result, meta
        except Exception as exc:
            _logger.error("rule_engine_error", error=str(exc))
            return None, EvaluationMeta(rules_evaluated=0, rules_matched=0, duration_ms=0.0)

    def _assemble_evidence(
        self,
        incident_event: EventRecord,
        all_events: list[EventRecord],
        time_window: str,
        state_context: list[StateContextEntry] | None = None,
    ) -> EvidencePackage:
        """Assemble an EvidencePackage for LLM consumption."""
        related = [e for e in all_events if e.event_id != incident_event.event_id]

        # Fetch field changes from the ledger
        changes = self._get_ledger_changes(
            kind=incident_event.resource_kind,
            namespace=incident_event.namespace,
            name=incident_event.resource_name,
            time_window=time_window,
        )

        # Fetch container statuses and resource specs from cache
        container_statuses = self._get_container_statuses(
            namespace=incident_event.namespace,
            name=incident_event.resource_name,
        )
        resource_specs = self._get_resource_specs(
            kind=incident_event.resource_kind,
            namespace=incident_event.namespace,
            name=incident_event.resource_name,
        )

        return EvidencePackage(
            incident_event=incident_event,
            related_events=related,
            changes=changes,
            container_statuses=container_statuses,
            resource_specs=resource_specs,
            time_window=time_window,
            state_context=state_context or [],
        )

    def _get_ledger_changes(
        self,
        kind: str,
        namespace: str,
        name: str,
        time_window: str,
    ) -> list[FieldChange]:
        """Query the change ledger for recent diffs."""
        try:
            m = _TIME_WINDOW_RE.match(time_window)
            if m is None:
                return []
            value = int(m.group(1))
            unit = m.group(2)
            if unit == "m":
                delta = timedelta(minutes=value)
            elif unit == "h":
                delta = timedelta(hours=value)
            else:
                delta = timedelta(days=value)

            return self._ledger.diff(kind, namespace, name, since=delta)
        except Exception as exc:
            _logger.warning("ledger_diff_error", error=str(exc))
            return []

    def _get_container_statuses(
        self,
        namespace: str,
        name: str,
    ) -> list[dict[str, Any]]:
        """Fetch container statuses from the cache for the incident pod."""
        try:
            resource = self._cache.get("Pod", namespace, name)
            if resource is None:
                return []
            status = resource.status
            containers = status.get("containerStatuses", [])
            if isinstance(containers, list):
                return [c for c in containers if isinstance(c, dict)]
            return []
        except Exception as exc:
            _logger.warning("container_status_error", error=str(exc))
            return []

    def _get_resource_specs(
        self,
        kind: str,
        namespace: str,
        name: str,
    ) -> list[dict[str, Any]]:
        """Fetch the incident resource spec from cache as a redacted view."""
        try:
            resource = self._cache.get(kind, namespace, name)
            if resource is None:
                return []
            return [
                {
                    "kind": resource.kind,
                    "namespace": resource.namespace,
                    "name": resource.name,
                    "spec": resource.spec,
                    "status": resource.status,
                }
            ]
        except Exception as exc:
            _logger.warning("resource_spec_error", error=str(exc))
            return []

    def _build_state_context(
        self,
        kind: str,
        namespace: str,
        name: str,
    ) -> list[StateContextEntry]:
        """Build verified state context from the dependency graph.

        Queries upstream and downstream resources, checks their existence
        and current status in the cache, and returns StateContextEntry items
        for injection into the LLM evidence package.
        """
        if self._dependency_graph is None:
            return []

        entries: list[StateContextEntry] = []
        try:
            # Get downstream dependencies (what this resource depends on)
            downstream = self._dependency_graph.downstream(kind, namespace, name, max_depth=2)
            for res in downstream.resources:
                if res.kind == kind and res.namespace == namespace and res.name == name:
                    continue  # skip self
                cached = self._cache.get(res.kind, res.namespace, res.name)
                exists = cached is not None
                status_summary = "<not found>"
                if cached is not None:
                    phase = cached.status.get("phase", "")
                    status_summary = str(phase) if phase else "Active"
                # Derive relationship from edges
                relationship = self._edge_relationship(downstream.edges, kind, namespace, name, res)
                entries.append(
                    StateContextEntry(
                        kind=res.kind,
                        namespace=res.namespace,
                        name=res.name,
                        exists=exists,
                        status_summary=status_summary,
                        relationship=relationship,
                    )
                )

            # Get upstream dependents (who depends on this resource)
            upstream = self._dependency_graph.upstream(kind, namespace, name, max_depth=1)
            for res in upstream.resources:
                if res.kind == kind and res.namespace == namespace and res.name == name:
                    continue
                # Avoid duplicates
                if any(e.kind == res.kind and e.namespace == res.namespace and e.name == res.name for e in entries):
                    continue
                cached = self._cache.get(res.kind, res.namespace, res.name)
                exists = cached is not None
                status_summary = "<not found>"
                if cached is not None:
                    phase = cached.status.get("phase", "")
                    status_summary = str(phase) if phase else "Active"
                relationship = self._edge_relationship(upstream.edges, kind, namespace, name, res)
                entries.append(
                    StateContextEntry(
                        kind=res.kind,
                        namespace=res.namespace,
                        name=res.name,
                        exists=exists,
                        status_summary=status_summary,
                        relationship=relationship if relationship else f"{res.kind} depends on {kind}/{name}",
                    )
                )
        except Exception as exc:
            _logger.warning("state_context_build_error", error=str(exc))

        return entries[:10]  # Cap at 10 entries

    @staticmethod
    def _edge_relationship(
        edges: list[Any],
        src_kind: str,
        src_ns: str,
        src_name: str,
        target: object,
    ) -> str:
        """Derive a human-readable relationship string from graph edges."""
        t_kind = getattr(target, "kind", "")
        t_name = getattr(target, "name", "")
        for edge in edges:
            if (
                edge.source.kind == src_kind
                and edge.source.name == src_name
                and edge.target.kind == t_kind
                and edge.target.name == t_name
            ) or (
                edge.target.kind == src_kind
                and edge.target.name == src_name
                and edge.source.kind == t_kind
                and edge.source.name == t_name
            ):
                return f"{edge.edge_type.value} link between {src_kind}/{src_name} and {t_kind}/{t_name}"
        return ""

    def _compute_blast_radius(
        self,
        kind: str,
        namespace: str,
        name: str,
    ) -> list[AffectedResource] | None:
        """Compute blast radius from the dependency graph.

        Returns upstream resources that depend on the incident resource,
        indicating which resources would be affected if this resource fails.
        """
        if self._dependency_graph is None:
            return None

        try:
            upstream = self._dependency_graph.upstream(kind, namespace, name, max_depth=3)
            resources: list[AffectedResource] = []
            for res in upstream.resources:
                if res.kind == kind and res.namespace == namespace and res.name == name:
                    continue
                resources.append(
                    AffectedResource(
                        kind=res.kind,
                        namespace=res.namespace,
                        name=res.name,
                    )
                )
            return resources if resources else None
        except Exception as exc:
            _logger.warning("blast_radius_error", error=str(exc))
            return None


def _record_duration(start_ms: int, diagnosed_by: str, rule_id: str | None) -> None:
    """Record the RCA duration histogram metric."""
    duration_s = ((time.monotonic_ns() // 1_000_000) - start_ms) / 1000.0
    rca_duration_seconds.labels(
        diagnosed_by=diagnosed_by,
        rule_id=rule_id or "",
    ).observe(duration_s)
