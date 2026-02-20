"""Tests for kuberca.llm.analyzer — LLMAnalyzer and confidence computation."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest

from kuberca.llm.analyzer import (
    LLMAnalyzer,
    _cap_raw_response,
    _compute_confidence,
    _parse_affected_resources,
)
from kuberca.llm.evidence import EvidencePackage
from kuberca.models.config import OllamaConfig
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import FieldChange


def _make_event() -> EventRecord:
    ts = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test",
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason="OOMKilled",
        message="Container was OOM killed",
        namespace="default",
        resource_kind="Pod",
        resource_name="my-pod-abc",
        first_seen=ts,
        last_seen=ts,
        count=3,
    )


def _make_evidence(with_changes: bool = False) -> EvidencePackage:
    incident = _make_event()
    changes = []
    if with_changes:
        changes = [
            FieldChange(
                field_path="spec.containers[0].resources.limits.memory",
                old_value="128Mi",
                new_value="64Mi",
                changed_at=datetime(2026, 2, 18, 11, 0, 0, tzinfo=UTC),
            )
        ]
    return EvidencePackage(
        incident_event=incident,
        changes=changes,
    )


def _valid_llm_response(**overrides: object) -> str:
    base = {
        "root_cause": "Pod was OOM killed due to insufficient memory limit",
        "evidence_citations": [
            "2026-02-18T11:00:00.000Z memory limit changed from 128Mi to 64Mi",
            "2026-02-18T12:00:00.000Z OOMKilled event count=3",
            "2026-02-18T11:30:00.000Z container restarted",
        ],
        "affected_resources": [
            {"kind": "Pod", "namespace": "default", "name": "my-pod-abc"},
        ],
        "suggested_remediation": "kubectl set resources deployment/my-app --limits=memory=256Mi",
        "causal_chain": (
            "At 2026-02-18T11:00:00.000Z the memory limit was reduced from 128Mi to 64Mi. "
            "This caused the container to exceed its limit, as seen at 2026-02-18T12:00:00.000Z "
            "when the OOMKilled event fired with count=3."
        ),
    }
    base.update(overrides)
    return json.dumps(base)


class TestComputeConfidence:
    def test_zero_citations_no_chain(self) -> None:
        conf = _compute_confidence([], "", False)
        assert conf == 0.30

    def test_zero_citations_with_chain(self) -> None:
        conf = _compute_confidence([], "Some chain text", False)
        assert conf == 0.35

    def test_one_citation(self) -> None:
        conf = _compute_confidence(["cite1"], "", False)
        assert 0.40 <= conf <= 0.50

    def test_two_citations(self) -> None:
        conf = _compute_confidence(["cite1", "cite2"], "", False)
        assert 0.40 <= conf <= 0.50

    def test_three_citations_no_diff(self) -> None:
        conf = _compute_confidence(["c1", "c2", "c3"], "chain", False)
        assert 0.50 <= conf <= 0.60

    def test_three_citations_with_diff(self) -> None:
        conf = _compute_confidence(["c1", "c2", "c3"], "chain", True)
        assert 0.60 <= conf <= 0.70

    def test_hard_cap_70(self) -> None:
        # Many citations + diff should not exceed 0.70
        citations = [f"cite{i}" for i in range(20)]
        conf = _compute_confidence(citations, "chain", True)
        assert conf <= 0.70

    def test_no_diff_cap_60(self) -> None:
        citations = [f"cite{i}" for i in range(20)]
        conf = _compute_confidence(citations, "chain", False)
        assert conf <= 0.60


class TestParseAffectedResources:
    def test_valid_list(self) -> None:
        raw = [{"kind": "Pod", "namespace": "default", "name": "my-pod"}]
        result = _parse_affected_resources(raw)
        assert len(result) == 1
        assert result[0].kind == "Pod"
        assert result[0].namespace == "default"
        assert result[0].name == "my-pod"

    def test_non_list_returns_empty(self) -> None:
        assert _parse_affected_resources("not a list") == []

    def test_invalid_items_skipped(self) -> None:
        raw = [{"kind": "Pod"}, "not-a-dict", {"kind": "Node", "namespace": "ns", "name": "n1"}]
        result = _parse_affected_resources(raw)
        assert len(result) == 1
        assert result[0].kind == "Node"


class TestCapRawResponse:
    def test_short_response_unchanged(self) -> None:
        text = "short response"
        assert _cap_raw_response(text) == text

    def test_long_response_truncated(self) -> None:
        text = "x" * 10_000
        result = _cap_raw_response(text)
        assert len(result.encode("utf-8")) <= 8_192 + 20  # allow for marker
        assert "[TRUNCATED]" in result


class TestLLMAnalyzerHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_model_present(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b")
        analyzer = LLMAnalyzer(config)

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"models": [{"name": "qwen2.5:7b"}]}

        with patch.object(analyzer._client, "get", new=AsyncMock(return_value=mock_response)):
            result = await analyzer.health_check()

        assert result is True
        assert analyzer.available is True
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_health_check_model_absent(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b")
        analyzer = LLMAnalyzer(config)

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"models": [{"name": "llama2:7b"}]}

        with patch.object(analyzer._client, "get", new=AsyncMock(return_value=mock_response)):
            result = await analyzer.health_check()

        assert result is False
        assert analyzer.available is False
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_health_check_unreachable(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b")
        analyzer = LLMAnalyzer(config)

        with patch.object(
            analyzer._client,
            "get",
            new=AsyncMock(side_effect=httpx.ConnectError("refused")),
        ):
            result = await analyzer.health_check()

        assert result is False
        assert analyzer.available is False
        await analyzer.aclose()


class TestLLMAnalyzerAnalyze:
    @pytest.mark.asyncio
    async def test_successful_analysis(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence(with_changes=True)

        raw_json = _valid_llm_response()
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"choices": [{"message": {"content": raw_json}}]}

        with patch.object(analyzer._client, "post", new=AsyncMock(return_value=mock_response)):
            result = await analyzer.analyze(event, evidence)

        assert result.success is True
        assert "OOM" in result.root_cause
        assert len(result.evidence_citations) == 3
        assert 0.60 <= result.confidence <= 0.70  # 3 citations + diff support
        assert result.retries_used == 0
        assert len(result.affected_resources) == 1
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_invalid_json_retries_and_succeeds(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence()

        bad_json = "not valid json {"
        good_json = _valid_llm_response()

        def make_response(content: str) -> MagicMock:
            r = MagicMock()
            r.raise_for_status = MagicMock()
            r.json.return_value = {"choices": [{"message": {"content": content}}]}
            return r

        responses = [make_response(bad_json), make_response(good_json)]
        response_iter = iter(responses)

        async def mock_post(*args: object, **kwargs: object) -> MagicMock:
            return next(response_iter)

        with patch.object(analyzer._client, "post", new=mock_post):
            result = await analyzer.analyze(event, evidence)

        assert result.success is True
        assert result.retries_used == 1
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_all_retries_exhausted_returns_failure(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence()

        bad_response = MagicMock()
        bad_response.raise_for_status = MagicMock()
        bad_response.json.return_value = {"choices": [{"message": {"content": "bad json {"}}]}

        with patch.object(analyzer._client, "post", new=AsyncMock(return_value=bad_response)):
            result = await analyzer.analyze(event, evidence)

        assert result.success is False
        assert result.root_cause == "LLM output unparseable"
        assert result.retries_used == 3
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_connect_error_returns_unavailable(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence()

        with patch.object(
            analyzer._client,
            "post",
            new=AsyncMock(side_effect=httpx.ConnectError("refused")),
        ):
            result = await analyzer.analyze(event, evidence)

        assert result.success is False
        assert "unavailable" in result.root_cause.lower()
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_timeout_returns_timeout_result(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence()

        with patch.object(
            analyzer._client,
            "post",
            new=AsyncMock(side_effect=httpx.ReadTimeout("timeout")),
        ):
            result = await analyzer.analyze(event, evidence)

        assert result.success is False
        assert "timeout" in result.root_cause.lower()
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_partial_schema_gets_low_confidence(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence()

        # Only root_cause present — partial response per Section 7.6
        partial_json = json.dumps({"root_cause": "Something went wrong"})
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"choices": [{"message": {"content": partial_json}}]}

        with patch.object(analyzer._client, "post", new=AsyncMock(return_value=mock_response)):
            result = await analyzer.analyze(event, evidence)

        assert result.success is True
        assert result.confidence == 0.30
        await analyzer.aclose()

    @pytest.mark.asyncio
    async def test_markdown_fenced_json_is_parsed(self) -> None:
        config = OllamaConfig(enabled=True, model="qwen2.5:7b", max_retries=3)
        analyzer = LLMAnalyzer(config)
        analyzer._available = True

        event = _make_event()
        evidence = _make_evidence()

        fenced = f"```json\n{_valid_llm_response()}\n```"
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"choices": [{"message": {"content": fenced}}]}

        with patch.object(analyzer._client, "post", new=AsyncMock(return_value=mock_response)):
            result = await analyzer.analyze(event, evidence)

        assert result.success is True
        await analyzer.aclose()
