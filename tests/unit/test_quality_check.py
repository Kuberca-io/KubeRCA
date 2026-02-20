"""Tests for LLMAnalyzer.quality_check — deterministic quality gate."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from kuberca.llm.analyzer import LLMAnalyzer, LLMResult
from kuberca.models.analysis import StateContextEntry
from kuberca.models.config import OllamaConfig
from kuberca.models.events import EventRecord, EventSource, Severity


def _make_event(
    reason: str = "OOMKilled",
    message: str = "Container was OOM killed",
) -> EventRecord:
    ts = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test",
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message=message,
        namespace="default",
        resource_kind="Pod",
        resource_name="my-pod",
        first_seen=ts,
        last_seen=ts,
        count=3,
    )


def _make_llm_result(
    root_cause: str = "Pod was OOMKilled due to insufficient memory",
    citations: list[str] | None = None,
) -> LLMResult:
    if citations is None:
        citations = [
            "2026-02-18T12:00:00.000Z OOMKilled event",
            "2026-02-18T11:50:00.000Z memory limit changed",
        ]
    return LLMResult(
        success=True,
        root_cause=root_cause,
        confidence=0.55,
        evidence_citations=citations,
    )


def _make_analyzer() -> LLMAnalyzer:
    config = OllamaConfig(
        endpoint="http://localhost:11434",
        model="qwen2.5:14b",
    )
    return LLMAnalyzer(config)


class TestQualityCheckCitationCount:
    def test_passes_with_two_citations(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(citations=["a", "b"])
        event = _make_event()
        qc = analyzer.quality_check(result, event)
        assert "zero_citations" not in qc.failures

    def test_fails_with_one_citation(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(citations=["a"])
        event = _make_event()
        qc = analyzer.quality_check(result, event)
        assert "zero_citations" in qc.failures

    def test_fails_with_zero_citations(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(citations=[])
        event = _make_event()
        qc = analyzer.quality_check(result, event)
        assert "zero_citations" in qc.failures


class TestQualityCheckReasonAlignment:
    def test_passes_when_reason_in_root_cause(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(root_cause="Pod was OOMKilled due to memory pressure")
        event = _make_event(reason="OOMKilled")
        qc = analyzer.quality_check(result, event)
        assert "reason_mismatch" not in qc.failures

    def test_fails_when_reason_missing_from_root_cause(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(root_cause="Pod crashed due to network failure")
        event = _make_event(reason="OOMKilled")
        qc = analyzer.quality_check(result, event)
        assert "reason_mismatch" in qc.failures

    def test_case_insensitive_match(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(root_cause="Pod was oomkilled")
        event = _make_event(reason="OOMKilled")
        qc = analyzer.quality_check(result, event)
        assert "reason_mismatch" not in qc.failures


class TestQualityCheckStateContradiction:
    def test_no_contradiction_with_existing_resource(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(root_cause="ConfigMap app-config caused the failure")
        event = _make_event(reason="FailedMount")
        state_context = [
            StateContextEntry(
                kind="ConfigMap",
                namespace="default",
                name="app-config",
                exists=True,
                status_summary="Active",
                relationship="volume mount",
            ),
        ]
        qc = analyzer.quality_check(result, event, state_context)
        assert "state_contradiction" not in qc.failures

    def test_contradiction_when_referencing_nonexistent_resource(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(
            root_cause="The app-config ConfigMap has incorrect values causing the crash",
        )
        event = _make_event(reason="FailedMount")
        state_context = [
            StateContextEntry(
                kind="ConfigMap",
                namespace="default",
                name="app-config",
                exists=False,
                status_summary="<not found>",
                relationship="volume mount",
            ),
        ]
        qc = analyzer.quality_check(result, event, state_context)
        assert "state_contradiction" in qc.failures

    def test_no_contradiction_if_root_cause_says_not_found(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(
            root_cause="The app-config ConfigMap was not found, causing mount failure",
        )
        event = _make_event(reason="FailedMount")
        state_context = [
            StateContextEntry(
                kind="ConfigMap",
                namespace="default",
                name="app-config",
                exists=False,
                status_summary="<not found>",
                relationship="volume mount",
            ),
        ]
        qc = analyzer.quality_check(result, event, state_context)
        assert "state_contradiction" not in qc.failures

    def test_no_contradiction_if_root_cause_says_missing(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(
            root_cause="The app-config is missing from the namespace",
        )
        event = _make_event(reason="FailedMount")
        state_context = [
            StateContextEntry(
                kind="ConfigMap",
                namespace="default",
                name="app-config",
                exists=False,
                status_summary="<not found>",
                relationship="volume mount",
            ),
        ]
        qc = analyzer.quality_check(result, event, state_context)
        assert "state_contradiction" not in qc.failures

    def test_no_state_context_passes(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result()
        event = _make_event()
        qc = analyzer.quality_check(result, event, state_context=None)
        assert "state_contradiction" not in qc.failures


class TestQualityCheckOverall:
    def test_passes_when_all_checks_pass(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(
            root_cause="Pod was OOMKilled due to insufficient memory limit",
            citations=["a", "b"],
        )
        event = _make_event(reason="OOMKilled")
        qc = analyzer.quality_check(result, event)
        assert qc.passed is True
        assert qc.failures == []

    def test_fails_with_multiple_issues(self) -> None:
        analyzer = _make_analyzer()
        result = _make_llm_result(
            root_cause="Network timeout caused the failure",
            citations=[],
        )
        event = _make_event(reason="OOMKilled")
        qc = analyzer.quality_check(result, event)
        assert qc.passed is False
        assert "zero_citations" in qc.failures
        assert "reason_mismatch" in qc.failures
