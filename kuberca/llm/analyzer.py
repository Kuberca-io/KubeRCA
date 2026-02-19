"""LLM Analyzer — Ollama OpenAI-compatible client for KubeRCA.

Wraps Ollama's /v1/chat/completions endpoint, handles JSON validation,
retry logic, confidence computation, and health-check tracking.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

import httpx
import structlog

from kuberca.llm.evidence import EvidencePackage
from kuberca.llm.prompts import QUALITY_RETRY_PROMPT, RETRY_PROMPT, SYSTEM_PROMPT
from kuberca.models.analysis import AffectedResource, QualityCheckResult, StateContextEntry
from kuberca.models.config import OllamaConfig
from kuberca.models.events import EventRecord
from kuberca.observability.metrics import llm_available, llm_requests_total

_logger = structlog.get_logger(component="llm_analyzer")

_RAW_RESPONSE_MAX_BYTES: int = 8_192
_REQUIRED_SCHEMA_KEYS: frozenset[str] = frozenset(
    {"root_cause", "evidence_citations", "affected_resources", "suggested_remediation", "causal_chain"}
)


@dataclass
class LLMResult:
    """Result of an LLM analysis call.

    confidence is always system-computed — never self-reported by the model.
    raw_response is only populated at log.level=debug and is never forwarded
    to any consumer outside of internal debugging.
    """

    success: bool
    root_cause: str
    confidence: float
    evidence_citations: list[str] = field(default_factory=list)
    affected_resources: list[AffectedResource] = field(default_factory=list)
    suggested_remediation: str = ""
    causal_chain: str = ""
    raw_response: str = ""  # empty unless log.level=debug; max 8 KB
    retries_used: int = 0
    latency_ms: int = 0


def _compute_confidence(
    citations: list[str],
    causal_chain: str,
    has_ledger_diff_support: bool,
) -> float:
    """System-computed LLM confidence per Section 7.5 formula.

    Citations count | Range
    0               | 0.30–0.40
    1–2             | 0.40–0.50
    3+ coherent     | 0.50–0.60
    3+ with diff    | 0.60–0.70
    Hard cap        | 0.70
    """
    count = len(citations)

    if count == 0:
        base = 0.30
        # Slightly higher if causal_chain cites evidence even without formal citations
        if causal_chain:
            base = 0.35
        return base

    if count <= 2:
        # 0.40–0.50: scale within the band by citation count
        base = 0.40 + min(count - 1, 1) * 0.05
        return min(base, 0.50)

    # 3+ citations
    if has_ledger_diff_support:
        # 0.60–0.70: more citations = higher within band, capped at 0.70
        base = 0.60 + min((count - 3) * 0.02, 0.09)
        return min(base, 0.70)

    # Coherent causal chain without diff support: 0.50–0.60
    base = 0.50 + min((count - 3) * 0.02, 0.09)
    return min(base, 0.60)


def _parse_affected_resources(raw: object) -> list[AffectedResource]:
    """Parse affected_resources from LLM JSON output."""
    if not isinstance(raw, list):
        return []
    result: list[AffectedResource] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        kind = item.get("kind", "")
        namespace = item.get("namespace", "")
        name = item.get("name", "")
        if isinstance(kind, str) and kind and isinstance(namespace, str) and isinstance(name, str) and name:
            result.append(AffectedResource(kind=kind, namespace=namespace, name=name))
    return result


def _cap_raw_response(text: str) -> str:
    """Cap raw LLM response to 8 KB with a truncation marker."""
    encoded = text.encode("utf-8")
    if len(encoded) <= _RAW_RESPONSE_MAX_BYTES:
        return text
    return encoded[:_RAW_RESPONSE_MAX_BYTES].decode("utf-8", errors="replace") + " [TRUNCATED]"


class LLMAnalyzer:
    """Wraps Ollama's OpenAI-compatible API for structured RCA reasoning.

    Uses a persistent httpx.AsyncClient connection pool. The caller is
    responsible for calling aclose() during shutdown.
    """

    def __init__(self, config: OllamaConfig) -> None:
        self._config = config
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(float(config.timeout_seconds)),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
        )
        self._available: bool = False

    @property
    def available(self) -> bool:
        """Whether the LLM is currently reachable and the model is loaded."""
        return self._available

    async def health_check(self) -> bool:
        """GET /api/tags to verify Ollama is reachable and the model is loaded.

        Updates the internal _available flag and the prometheus gauge.
        Returns True on success, False on any failure.
        """
        try:
            response = await self._client.get(
                f"{self._config.endpoint}/api/tags",
                timeout=5.0,
            )
            response.raise_for_status()
            body = response.json()
            models = body.get("models", [])
            model_names: list[str] = [
                m.get("name", "") if isinstance(m, dict) else "" for m in (models if isinstance(models, list) else [])
            ]
            # Check if configured model is present (may be with or without tag suffix)
            configured = self._config.model
            loaded = any(name == configured or name.startswith(configured.split(":")[0]) for name in model_names)
            self._available = loaded
            llm_available.set(1.0 if loaded else 0.0)
            if not loaded:
                _logger.warning(
                    "ollama_model_not_loaded",
                    model=configured,
                    available_models=model_names,
                )
            return loaded
        except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
            self._available = False
            llm_available.set(0.0)
            _logger.warning("ollama_health_check_failed", error=str(exc))
            return False

    async def analyze(
        self,
        event: EventRecord,
        evidence: EvidencePackage,
    ) -> LLMResult:
        """Assemble prompt, call Ollama, parse and validate response.

        Retries up to config.max_retries times (default 3) on malformed JSON.
        Always returns an LLMResult — never raises.
        """
        start_ms = time.monotonic_ns() // 1_000_000
        user_prompt = self._build_user_prompt(event, evidence)

        messages: list[dict[str, str]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ]

        retries_used = 0
        last_parse_error = ""
        debug_mode: bool = logging.getLogger().isEnabledFor(logging.DEBUG)

        for attempt in range(self._config.max_retries + 1):
            retries_used = attempt
            try:
                raw_text, http_error = await self._call_ollama(messages)
            except _OllamaUnavailableError as exc:
                latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
                llm_requests_total.labels(success="false", retries=str(retries_used)).inc()
                return LLMResult(
                    success=False,
                    root_cause=f"LLM unavailable: {exc}",
                    confidence=0.0,
                    retries_used=retries_used,
                    latency_ms=latency_ms,
                )
            except _OllamaTimeoutError:
                latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
                _logger.warning("llm_timeout", timeout_s=self._config.timeout_seconds)
                llm_requests_total.labels(success="false", retries=str(retries_used)).inc()
                return LLMResult(
                    success=False,
                    root_cause="LLM timeout",
                    confidence=0.0,
                    retries_used=retries_used,
                    latency_ms=latency_ms,
                )

            if http_error:
                latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
                llm_requests_total.labels(success="false", retries=str(retries_used)).inc()
                return LLMResult(
                    success=False,
                    root_cause=f"LLM unavailable: {http_error}",
                    confidence=0.0,
                    retries_used=retries_used,
                    latency_ms=latency_ms,
                )

            capped_raw = _cap_raw_response(raw_text)

            parsed, parse_error = self._parse_response(raw_text)
            if parsed is not None:
                latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
                result = self._build_result(
                    parsed=parsed,
                    evidence=evidence,
                    raw_response=capped_raw if debug_mode else "",
                    retries_used=retries_used,
                    latency_ms=latency_ms,
                )
                llm_requests_total.labels(success="true", retries=str(retries_used)).inc()
                _logger.info(
                    "llm_analysis_complete",
                    confidence=result.confidence,
                    citations=len(result.evidence_citations),
                    retries=retries_used,
                    latency_ms=latency_ms,
                )
                return result

            # JSON parse failed
            last_parse_error = parse_error
            _logger.warning(
                "llm_invalid_json",
                attempt=attempt + 1,
                max_retries=self._config.max_retries,
                error=parse_error,
            )

            if attempt < self._config.max_retries:
                # Append correction prompt as a new user message
                messages = messages + [
                    {"role": "assistant", "content": raw_text},
                    {
                        "role": "user",
                        "content": RETRY_PROMPT.format(parse_error=parse_error),
                    },
                ]

        # All retries exhausted
        latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
        _logger.error(
            "llm_output_unparseable",
            retries=retries_used,
            last_error=last_parse_error,
        )
        llm_requests_total.labels(success="false", retries=str(retries_used)).inc()
        return LLMResult(
            success=False,
            root_cause="LLM output unparseable",
            confidence=0.0,
            retries_used=retries_used,
            latency_ms=latency_ms,
        )

    def _build_user_prompt(self, event: EventRecord, evidence: EvidencePackage) -> str:
        """Build the user prompt using the evidence package's formatted sections."""
        ctx = evidence.to_prompt_context()
        # The context already contains all sections; wrap with the inciting question
        return f"{ctx}\n\nProvide your diagnosis as JSON."

    async def _call_ollama(
        self,
        messages: list[dict[str, str]],
    ) -> tuple[str, str]:
        """POST to Ollama /v1/chat/completions.

        Returns (raw_text, error_message). error_message is empty on success.
        Raises _OllamaUnavailableError or _OllamaTimeoutError on network issues.
        """
        payload = {
            "model": self._config.model,
            "messages": messages,
            "temperature": self._config.temperature,
            "max_tokens": self._config.max_tokens,
            "stream": False,
            "format": "json",
        }

        try:
            response = await self._client.post(
                f"{self._config.endpoint}/v1/chat/completions",
                json=payload,
            )
        except httpx.ConnectError as exc:
            self._available = False
            llm_available.set(0.0)
            raise _OllamaUnavailableError(str(exc)) from exc
        except (httpx.ReadTimeout, httpx.WriteTimeout, httpx.ConnectTimeout, httpx.PoolTimeout) as exc:
            raise _OllamaTimeoutError(str(exc)) from exc
        except httpx.TimeoutException as exc:
            raise _OllamaTimeoutError(str(exc)) from exc

        try:
            response.raise_for_status()
        except httpx.HTTPStatusError as exc:
            return "", f"HTTP {exc.response.status_code}"

        try:
            body = response.json()
        except Exception as exc:
            return "", f"Response body not JSON: {exc}"

        try:
            content = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            return "", f"Unexpected response structure: {exc}"

        if not isinstance(content, str):
            return "", "Response content is not a string"

        return content, ""

    def _parse_response(self, raw: str) -> tuple[dict | None, str]:  # type: ignore[type-arg]
        """Parse and minimally validate the LLM JSON response.

        Returns (parsed_dict, error_message). On success, error_message is "".
        On failure, parsed_dict is None.

        If 'root_cause' is present but other keys are missing, returns the
        partial dict so the caller can apply a low-confidence result (Section 7.6).
        """
        stripped = raw.strip()
        # Strip markdown code fences if the model added them despite instructions
        if stripped.startswith("```"):
            lines = stripped.split("\n")
            # Remove first and last fence lines
            inner = lines[1:-1] if lines[-1].strip().startswith("```") else lines[1:]
            stripped = "\n".join(inner)

        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            return None, str(exc)

        if not isinstance(parsed, dict):
            return None, f"Expected JSON object, got {type(parsed).__name__}"

        if "root_cause" not in parsed:
            return None, "Missing required key: root_cause"

        # Partial parse is acceptable per Section 7.6
        return parsed, ""

    def _build_result(
        self,
        parsed: dict,  # type: ignore[type-arg]
        evidence: EvidencePackage,
        raw_response: str,
        retries_used: int,
        latency_ms: int,
    ) -> LLMResult:
        """Build an LLMResult from a validated (possibly partial) JSON dict."""
        root_cause = str(parsed.get("root_cause", ""))
        citations: list[str] = []
        raw_citations = parsed.get("evidence_citations", [])
        if isinstance(raw_citations, list):
            citations = [str(c) for c in raw_citations if isinstance(c, str)]

        affected_resources = _parse_affected_resources(parsed.get("affected_resources", []))
        suggested_remediation = str(parsed.get("suggested_remediation", ""))
        causal_chain = str(parsed.get("causal_chain", ""))

        # Determine if a change ledger diff supports the diagnosis
        has_diff_support = bool(evidence.changes)

        # Detect partial parse (missing keys): apply floor confidence
        is_partial = not _REQUIRED_SCHEMA_KEYS.issubset(parsed.keys())
        if is_partial:
            confidence = 0.30
        else:
            confidence = _compute_confidence(
                citations=citations,
                causal_chain=causal_chain,
                has_ledger_diff_support=has_diff_support,
            )

        return LLMResult(
            success=True,
            root_cause=root_cause,
            confidence=confidence,
            evidence_citations=citations,
            affected_resources=affected_resources,
            suggested_remediation=suggested_remediation,
            causal_chain=causal_chain,
            raw_response=raw_response,
            retries_used=retries_used,
            latency_ms=latency_ms,
        )

    def quality_check(
        self,
        result: LLMResult,
        event: EventRecord,
        state_context: list[StateContextEntry] | None = None,
    ) -> QualityCheckResult:
        """Deterministic quality check on LLM output. No LLM call.

        Checks:
        1. evidence_citations >= 2
        2. root_cause contains event.reason
        3. root_cause does not contradict state_context facts
        """
        failures: list[str] = []

        # Check 1: Evidence citation count
        if len(result.evidence_citations) < 2:
            failures.append("zero_citations")

        # Check 2: Reason alignment
        if event.reason and event.reason.lower() not in result.root_cause.lower():
            failures.append("reason_mismatch")

        # Check 3: State consistency
        if state_context:
            for entry in state_context:
                if not entry.exists and entry.name.lower() in result.root_cause.lower():
                    # LLM references a non-existent resource as if it exists
                    rc_lower = result.root_cause.lower()
                    if "not found" not in rc_lower and "missing" not in rc_lower and "does not exist" not in rc_lower:
                        failures.append("state_contradiction")
                        break

        return QualityCheckResult(
            passed=len(failures) == 0,
            failures=failures,
        )

    async def analyze_with_quality_retry(
        self,
        event: EventRecord,
        evidence: EvidencePackage,
        quality_failures: list[str],
    ) -> LLMResult:
        """Re-invoke the LLM with QUALITY_RETRY_PROMPT after a quality check failure.

        Builds a new conversation with the original system prompt, the evidence
        package, and a quality retry prompt that describes the failures. Returns
        an LLMResult — never raises.
        """
        start_ms = time.monotonic_ns() // 1_000_000
        user_prompt = self._build_user_prompt(event, evidence)

        quality_prompt = QUALITY_RETRY_PROMPT.format(
            quality_failures=", ".join(quality_failures),
            event_reason=event.reason,
        )

        messages: list[dict[str, str]] = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
            {"role": "user", "content": quality_prompt},
        ]

        debug_mode: bool = logging.getLogger().isEnabledFor(logging.DEBUG)

        try:
            raw_text, http_error = await self._call_ollama(messages)
        except (_OllamaUnavailableError, _OllamaTimeoutError) as exc:
            latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
            return LLMResult(
                success=False,
                root_cause=f"LLM quality retry failed: {exc}",
                confidence=0.0,
                latency_ms=latency_ms,
            )

        if http_error:
            latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
            return LLMResult(
                success=False,
                root_cause=f"LLM quality retry failed: {http_error}",
                confidence=0.0,
                latency_ms=latency_ms,
            )

        parsed, parse_error = self._parse_response(raw_text)
        if parsed is None:
            latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
            _logger.warning("llm_quality_retry_parse_failed", error=parse_error)
            return LLMResult(
                success=False,
                root_cause="LLM quality retry output unparseable",
                confidence=0.0,
                latency_ms=latency_ms,
            )

        latency_ms = (time.monotonic_ns() // 1_000_000) - start_ms
        capped_raw = _cap_raw_response(raw_text)
        result = self._build_result(
            parsed=parsed,
            evidence=evidence,
            raw_response=capped_raw if debug_mode else "",
            retries_used=0,
            latency_ms=latency_ms,
        )
        _logger.info(
            "llm_quality_retry_complete",
            confidence=result.confidence,
            citations=len(result.evidence_citations),
        )
        return result

    async def aclose(self) -> None:
        """Close the underlying HTTP client and release connections."""
        await self._client.aclose()


class _OllamaUnavailableError(Exception):
    """Raised when Ollama is unreachable (connection refused, DNS failure)."""


class _OllamaTimeoutError(Exception):
    """Raised when the Ollama request exceeds the configured timeout."""
