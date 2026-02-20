"""Tests for the 3 remaining SynergyRCA addendum gaps.

Gap 1: top_rules_matched — ordered rule_id list in status endpoint
Gap 2: Quality retry loop — LLM re-invocation with QUALITY_RETRY_PROMPT
Gap 3: PV/ResourceQuota/Secret in cache watched kinds
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from kuberca.api.routes import _get_top_rules_matched
from kuberca.api.schemas import RuleEngineCoverage
from kuberca.cache.resource_cache import _default_kinds, _get_list_func
from kuberca.llm.analyzer import LLMAnalyzer, LLMResult
from kuberca.models.analysis import (
    AffectedResource,
    EvaluationMeta,
    QualityCheckResult,
    RuleResult,
)
from kuberca.models.config import OllamaConfig
from kuberca.models.events import DiagnosisSource, EventRecord, EventSource, Severity
from kuberca.models.resources import CacheReadiness

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_event(
    reason: str = "OOMKilled",
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
) -> EventRecord:
    ts = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test",
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message="test event",
        namespace=namespace,
        resource_kind=kind,
        resource_name=name,
        first_seen=ts,
        last_seen=ts,
        count=1,
    )


def _make_llm_result(
    root_cause: str = "Pod was OOMKilled due to memory pressure",
    citations: list[str] | None = None,
    confidence: float = 0.55,
) -> LLMResult:
    if citations is None:
        citations = [
            "2026-02-19T12:00:00.000Z OOMKilled event",
            "2026-02-19T11:50:00.000Z memory limit changed",
        ]
    return LLMResult(
        success=True,
        root_cause=root_cause,
        confidence=confidence,
        evidence_citations=citations,
        affected_resources=[AffectedResource("Pod", "default", "my-pod")],
        suggested_remediation="Increase memory limit",
        causal_chain="Memory pressure caused OOM...",
    )


def _make_coordinator(
    cache_readiness: CacheReadiness = CacheReadiness.READY,
    events: list[EventRecord] | None = None,
    rule_result: RuleResult | None = None,
    llm_analyzer: object | None = None,
):
    from kuberca.analyst.coordinator import AnalystCoordinator

    cache = MagicMock()
    cache.readiness.return_value = cache_readiness
    cache.get.return_value = None

    ledger = MagicMock()
    ledger.diff.return_value = []

    if events is None:
        events = [_make_event()]
    event_buffer = MagicMock()
    event_buffer.get_events.return_value = events

    eval_meta = EvaluationMeta(rules_evaluated=1, rules_matched=1 if rule_result else 0)
    rule_engine = MagicMock()
    rule_engine.evaluate.return_value = (rule_result, eval_meta)

    config = MagicMock()
    config.cluster_id = "test-cluster"
    config.ollama = MagicMock()
    config.ollama.endpoint = "http://localhost:11434"

    return AnalystCoordinator(
        rule_engine=rule_engine,
        llm_analyzer=llm_analyzer,
        cache=cache,
        ledger=ledger,
        event_buffer=event_buffer,
        config=config,
    )


# ===========================================================================
# Gap 1: top_rules_matched
# ===========================================================================


class TestTopRulesMatched:
    def test_schema_has_top_rules_matched_field(self) -> None:
        """RuleEngineCoverage schema includes top_rules_matched."""
        coverage = RuleEngineCoverage()
        assert hasattr(coverage, "top_rules_matched")
        assert coverage.top_rules_matched == []

    def test_schema_accepts_top_rules_matched_list(self) -> None:
        """RuleEngineCoverage can accept a populated list."""
        coverage = RuleEngineCoverage(
            top_rules_matched=["R01_oom_killed", "R02_crash_loop", "R03_failed_scheduling"],
        )
        assert len(coverage.top_rules_matched) == 3
        assert coverage.top_rules_matched[0] == "R01_oom_killed"

    def test_get_top_rules_matched_returns_list(self) -> None:
        """_get_top_rules_matched returns a list (may be empty with no data)."""
        result = _get_top_rules_matched()
        assert isinstance(result, list)

    def test_get_top_rules_matched_respects_limit(self) -> None:
        """_get_top_rules_matched limits to the requested number."""
        result = _get_top_rules_matched(limit=3)
        assert len(result) <= 3

    def test_get_top_rules_matched_with_counter_data(self) -> None:
        """When rule_matches_total has data, top rules are returned sorted."""
        from kuberca.observability.metrics import rule_matches_total

        # Increment counters for specific rules
        rule_matches_total.labels(rule_id="R01_oom_killed").inc(10)
        rule_matches_total.labels(rule_id="R02_crash_loop").inc(5)
        rule_matches_total.labels(rule_id="R03_failed_scheduling").inc(8)

        result = _get_top_rules_matched(limit=5)
        assert isinstance(result, list)
        # R01 should be first (highest count), R03 second, R02 third
        assert len(result) >= 3
        r01_idx = result.index("R01_oom_killed")
        r03_idx = result.index("R03_failed_scheduling")
        r02_idx = result.index("R02_crash_loop")
        assert r01_idx < r03_idx < r02_idx


# ===========================================================================
# Gap 2: Quality retry loop
# ===========================================================================


class TestQualityRetryLoop:
    @pytest.mark.asyncio
    async def test_quality_passes_first_time_no_retry(self) -> None:
        """When quality_check passes on first try, no retry is invoked."""
        llm_result = _make_llm_result()
        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=llm_result)
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=True, failures=[]),
        )
        llm_analyzer.analyze_with_quality_retry = AsyncMock()

        coordinator = _make_coordinator(rule_result=None, llm_analyzer=llm_analyzer)
        response = await coordinator.analyze("pod/default/my-pod")

        assert response.diagnosed_by == DiagnosisSource.LLM
        llm_analyzer.analyze_with_quality_retry.assert_not_called()

    @pytest.mark.asyncio
    async def test_quality_fails_triggers_retry(self) -> None:
        """When quality_check fails, analyze_with_quality_retry is invoked."""
        initial_result = _make_llm_result(
            root_cause="Network issue caused failure",
            citations=[],
            confidence=0.40,
        )
        retry_result = _make_llm_result(
            root_cause="Pod was OOMKilled due to memory pressure",
            citations=["ts1 event1", "ts2 event2"],
            confidence=0.55,
        )

        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=initial_result)
        llm_analyzer.analyze_with_quality_retry = AsyncMock(return_value=retry_result)
        # First call fails quality, second (retry result) passes
        llm_analyzer.quality_check = MagicMock(
            side_effect=[
                QualityCheckResult(passed=False, failures=["zero_citations", "reason_mismatch"]),
                QualityCheckResult(passed=True, failures=[]),
            ],
        )

        coordinator = _make_coordinator(rule_result=None, llm_analyzer=llm_analyzer)
        response = await coordinator.analyze("pod/default/my-pod")

        assert response.diagnosed_by == DiagnosisSource.LLM
        llm_analyzer.analyze_with_quality_retry.assert_called_once()
        # Confidence should NOT be capped since retry passed
        assert response.confidence > 0.35

    @pytest.mark.asyncio
    async def test_quality_fails_twice_caps_confidence(self) -> None:
        """When quality_check fails on both retries, confidence is capped."""
        initial_result = _make_llm_result(
            root_cause="Network issue caused failure",
            citations=[],
            confidence=0.50,
        )
        retry_result = _make_llm_result(
            root_cause="Still wrong diagnosis",
            citations=[],
            confidence=0.45,
        )

        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=initial_result)
        llm_analyzer.analyze_with_quality_retry = AsyncMock(return_value=retry_result)
        # All quality checks fail
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=False, failures=["reason_mismatch"]),
        )

        coordinator = _make_coordinator(rule_result=None, llm_analyzer=llm_analyzer)
        response = await coordinator.analyze("pod/default/my-pod")

        assert response.diagnosed_by == DiagnosisSource.LLM
        # Should have retried exactly 2 times
        assert llm_analyzer.analyze_with_quality_retry.call_count == 2
        # Confidence capped at 0.35
        assert response.confidence <= 0.35

    @pytest.mark.asyncio
    async def test_quality_retry_failure_falls_back_gracefully(self) -> None:
        """When the retry LLM call itself fails, falls back to capped confidence."""
        initial_result = _make_llm_result(
            root_cause="Network issue caused failure",
            citations=[],
            confidence=0.50,
        )
        failed_retry = LLMResult(
            success=False,
            root_cause="LLM quality retry failed",
            confidence=0.0,
        )

        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=initial_result)
        llm_analyzer.analyze_with_quality_retry = AsyncMock(return_value=failed_retry)
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=False, failures=["zero_citations"]),
        )

        coordinator = _make_coordinator(rule_result=None, llm_analyzer=llm_analyzer)
        response = await coordinator.analyze("pod/default/my-pod")

        assert response.diagnosed_by == DiagnosisSource.LLM
        # Retry LLM failed, so only 1 retry attempted before break
        assert llm_analyzer.analyze_with_quality_retry.call_count == 1
        assert response.confidence <= 0.35

    @pytest.mark.asyncio
    async def test_quality_retry_warnings_include_retry_count(self) -> None:
        """Warnings include information about quality retry failures."""
        initial_result = _make_llm_result(
            root_cause="Wrong diagnosis",
            citations=[],
            confidence=0.50,
        )
        retry_result = _make_llm_result(
            root_cause="Still wrong",
            citations=[],
            confidence=0.45,
        )

        llm_analyzer = AsyncMock()
        llm_analyzer.available = True
        llm_analyzer.analyze = AsyncMock(return_value=initial_result)
        llm_analyzer.analyze_with_quality_retry = AsyncMock(return_value=retry_result)
        llm_analyzer.quality_check = MagicMock(
            return_value=QualityCheckResult(passed=False, failures=["reason_mismatch"]),
        )

        coordinator = _make_coordinator(rule_result=None, llm_analyzer=llm_analyzer)
        response = await coordinator.analyze("pod/default/my-pod")

        assert response._meta is not None
        warnings_text = " ".join(response._meta.warnings)
        assert "quality check failed" in warnings_text.lower()
        assert "retries" in warnings_text.lower()


class TestAnalyzeWithQualityRetry:
    """Tests for LLMAnalyzer.analyze_with_quality_retry method."""

    def _make_analyzer(self) -> LLMAnalyzer:
        config = OllamaConfig(
            endpoint="http://localhost:11434",
            model="qwen2.5:14b",
        )
        return LLMAnalyzer(config)

    @pytest.mark.asyncio
    async def test_builds_quality_retry_prompt(self) -> None:
        """analyze_with_quality_retry includes QUALITY_RETRY_PROMPT in messages."""
        analyzer = self._make_analyzer()
        event = _make_event(reason="OOMKilled")

        from kuberca.llm.evidence import EvidencePackage

        evidence = MagicMock(spec=EvidencePackage)
        evidence.to_prompt_context.return_value = "test context"
        evidence.changes = []

        good_result = {
            "root_cause": "Pod OOMKilled due to memory",
            "evidence_citations": ["ts1 event1", "ts2 event2"],
            "affected_resources": [],
            "suggested_remediation": "Increase memory",
            "causal_chain": "Memory pressure...",
        }

        with patch.object(analyzer, "_call_ollama", new_callable=AsyncMock) as mock_call:
            import json

            mock_call.return_value = (json.dumps(good_result), "")
            result = await analyzer.analyze_with_quality_retry(
                event=event,
                evidence=evidence,
                quality_failures=["zero_citations", "reason_mismatch"],
            )

        assert result.success is True
        assert result.root_cause == "Pod OOMKilled due to memory"
        # Verify messages include quality retry prompt
        call_args = mock_call.call_args[0][0]
        assert len(call_args) == 3  # system + user + quality retry
        assert "quality checks" in call_args[2]["content"].lower()
        assert "OOMKilled" in call_args[2]["content"]

    @pytest.mark.asyncio
    async def test_returns_failure_on_parse_error(self) -> None:
        """When the retry response is unparseable, returns a failed LLMResult."""
        analyzer = self._make_analyzer()
        event = _make_event()

        from kuberca.llm.evidence import EvidencePackage

        evidence = MagicMock(spec=EvidencePackage)
        evidence.to_prompt_context.return_value = "test context"
        evidence.changes = []

        with patch.object(analyzer, "_call_ollama", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = ("not valid json", "")
            result = await analyzer.analyze_with_quality_retry(
                event=event,
                evidence=evidence,
                quality_failures=["zero_citations"],
            )

        assert result.success is False
        assert "unparseable" in result.root_cause.lower()

    @pytest.mark.asyncio
    async def test_returns_failure_on_http_error(self) -> None:
        """When the retry HTTP call fails, returns a failed LLMResult."""
        analyzer = self._make_analyzer()
        event = _make_event()

        from kuberca.llm.evidence import EvidencePackage

        evidence = MagicMock(spec=EvidencePackage)
        evidence.to_prompt_context.return_value = "test context"
        evidence.changes = []

        with patch.object(analyzer, "_call_ollama", new_callable=AsyncMock) as mock_call:
            mock_call.return_value = ("", "HTTP 500")
            result = await analyzer.analyze_with_quality_retry(
                event=event,
                evidence=evidence,
                quality_failures=["reason_mismatch"],
            )

        assert result.success is False


# ===========================================================================
# Gap 3: PV / ResourceQuota / Secret in cache watched kinds
# ===========================================================================


class TestCacheWatchedKinds:
    def test_persistent_volume_in_default_kinds(self) -> None:
        """PersistentVolume is included in the default watched kinds."""
        kinds = _default_kinds()
        assert "PersistentVolume" in kinds

    def test_resource_quota_in_default_kinds(self) -> None:
        """ResourceQuota is included in the default watched kinds."""
        kinds = _default_kinds()
        assert "ResourceQuota" in kinds

    def test_secret_in_default_kinds(self) -> None:
        """Secret is included in the default watched kinds."""
        kinds = _default_kinds()
        assert "Secret" in kinds

    def test_pvc_still_in_default_kinds(self) -> None:
        """PersistentVolumeClaim is still in the default watched kinds."""
        kinds = _default_kinds()
        assert "PersistentVolumeClaim" in kinds

    def test_default_kinds_count(self) -> None:
        """Default kinds list has the expected count after additions."""
        kinds = _default_kinds()
        assert len(kinds) == 18

    def test_persistent_volume_has_list_func(self) -> None:
        """PersistentVolume maps to the correct API list method."""
        api = MagicMock()
        api.list_persistent_volume = MagicMock()
        func = _get_list_func(api, "PersistentVolume")
        assert func is not None

    def test_resource_quota_has_list_func(self) -> None:
        """ResourceQuota maps to the correct API list method."""
        api = MagicMock()
        api.list_resource_quota_for_all_namespaces = MagicMock()
        func = _get_list_func(api, "ResourceQuota")
        assert func is not None

    def test_secret_has_list_func(self) -> None:
        """Secret maps to the correct API list method."""
        api = MagicMock()
        api.list_secret_for_all_namespaces = MagicMock()
        func = _get_list_func(api, "Secret")
        assert func is not None

    def test_no_duplicates_in_default_kinds(self) -> None:
        """No duplicate entries in the default kinds list."""
        kinds = _default_kinds()
        assert len(kinds) == len(set(kinds))

    def test_all_default_kinds_have_list_func(self) -> None:
        """Every kind in _default_kinds has a mapping in _get_list_func."""
        api = MagicMock()
        # Create all expected methods on the mock
        for _kind in _default_kinds():
            # getattr on a MagicMock auto-creates attributes
            pass

        for kind in _default_kinds():
            func = _get_list_func(api, kind)
            assert func is not None, f"Kind {kind} has no list function mapping"
