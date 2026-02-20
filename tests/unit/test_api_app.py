"""Unit tests for the FastAPI application (kuberca.api.app + routes)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from kuberca.api.app import create_app
from kuberca.models.analysis import AffectedResource, EvidenceItem, RCAResponse, ResponseMeta
from kuberca.models.events import DiagnosisSource, EvidenceType
from kuberca.models.resources import CacheReadiness

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_rca_response() -> RCAResponse:
    return RCAResponse(
        root_cause="Image pull failure due to invalid tag.",
        confidence=0.91,
        diagnosed_by=DiagnosisSource.RULE_ENGINE,
        rule_id="image-pull-failure",
        evidence=[
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp="2024-01-15T10:30:00Z",
                summary="ErrImagePull: image not found",
            )
        ],
        affected_resources=[AffectedResource(kind="Pod", namespace="default", name="api-pod")],
        suggested_remediation="Fix the image tag in the Deployment spec.",
        _meta=ResponseMeta(
            kuberca_version="0.1.0",
            schema_version="1",
            cluster_id="test-cluster",
            timestamp="2024-01-15T10:30:01Z",
            response_time_ms=42,
            cache_state="ready",
            warnings=[],
        ),
    )


def _make_cache(readiness: CacheReadiness = CacheReadiness.READY) -> MagicMock:
    cache = MagicMock()
    cache.readiness_state = readiness
    cache.get = MagicMock(return_value=MagicMock())  # resource exists by default
    cache.list_pods = MagicMock(return_value=[])
    cache.list_nodes = MagicMock(return_value=[])
    cache.list_events = MagicMock(return_value=[])
    return cache


def _make_coordinator(rca: RCAResponse | None = None) -> MagicMock:
    coordinator = MagicMock()
    coordinator.analyze = AsyncMock(return_value=rca or _make_rca_response())
    return coordinator


def _make_app(
    coordinator: MagicMock | None = None,
    cache: MagicMock | None = None,
) -> TestClient:
    app = create_app(
        coordinator=coordinator or _make_coordinator(),
        cache=cache or _make_cache(),
    )
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_returns_200_with_ok_status(self) -> None:
        client = _make_app()
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert "version" in body
        assert "cache_state" in body

    def test_cache_state_reflects_readiness(self) -> None:
        cache = _make_cache(CacheReadiness.WARMING)
        client = _make_app(cache=cache)
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json()["cache_state"] == "warming"


# ---------------------------------------------------------------------------
# GET /api/v1/status
# ---------------------------------------------------------------------------


class TestStatusEndpoint:
    def test_returns_200_with_expected_fields(self) -> None:
        client = _make_app()
        resp = client.get("/api/v1/status")
        assert resp.status_code == 200
        body = resp.json()
        assert "pod_counts" in body
        assert "node_conditions" in body
        assert "active_anomalies" in body
        assert "recent_events" in body
        assert "cache_state" in body

    def test_namespace_query_param_is_accepted(self) -> None:
        client = _make_app()
        resp = client.get("/api/v1/status?namespace=production")
        assert resp.status_code == 200

    def test_pod_counts_populated_from_cache(self) -> None:
        cache = _make_cache()
        mock_pod = MagicMock()
        mock_pod.status = {"phase": "Running"}
        cache.list_pods = MagicMock(return_value=[mock_pod, mock_pod])
        client = _make_app(cache=cache)
        resp = client.get("/api/v1/status")
        assert resp.status_code == 200
        body = resp.json()
        assert body["pod_counts"].get("Running") == 2


# ---------------------------------------------------------------------------
# POST /api/v1/analyze
# ---------------------------------------------------------------------------


class TestAnalyzeEndpoint:
    def test_returns_200_with_rca_response(self) -> None:
        client = _make_app()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/api-pod"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["root_cause"] == "Image pull failure due to invalid tag."
        assert body["confidence"] == pytest.approx(0.91)
        assert body["diagnosed_by"] == "rule_engine"
        assert body["rule_id"] == "image-pull-failure"

    def test_evidence_and_affected_resources_serialised(self) -> None:
        client = _make_app()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/api-pod"},
        )
        body = resp.json()
        assert len(body["evidence"]) == 1
        assert body["evidence"][0]["type"] == "event"
        assert len(body["affected_resources"]) == 1
        assert body["affected_resources"][0]["kind"] == "Pod"

    def test_time_window_accepted(self) -> None:
        client = _make_app()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Deployment/prod/api", "time_window": "30m"},
        )
        assert resp.status_code == 200

    def test_invalid_resource_format_returns_400(self) -> None:
        client = _make_app()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default"},  # missing name
        )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "INVALID_RESOURCE_FORMAT"

    def test_invalid_time_window_returns_400(self) -> None:
        client = _make_app()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/x", "time_window": "2hours"},
        )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "INVALID_TIME_WINDOW"

    def test_resource_not_found_returns_404(self) -> None:
        cache = _make_cache()
        cache.get = MagicMock(return_value=None)  # resource absent
        client = _make_app(cache=cache)
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/missing"},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "RESOURCE_NOT_FOUND"

    def test_queue_full_returns_429(self) -> None:
        coordinator = _make_coordinator()
        cache = _make_cache()
        queue = MagicMock()
        queue.is_full = MagicMock(return_value=True)
        app = create_app(
            coordinator=coordinator,
            cache=cache,
            analysis_queue=queue,
        )
        client = TestClient(app, raise_server_exceptions=False)
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/x"},
        )
        assert resp.status_code == 429
        body = resp.json()
        assert body["error"] == "QUEUE_FULL"

    def test_coordinator_exception_returns_500(self) -> None:
        coordinator = MagicMock()
        coordinator.analyze = AsyncMock(side_effect=RuntimeError("coordinator broke"))
        client = _make_app(coordinator=coordinator)
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/x"},
        )
        assert resp.status_code == 500
        body = resp.json()
        assert body["error"] == "INTERNAL_ERROR"


# ---------------------------------------------------------------------------
# OpenAPI spec
# ---------------------------------------------------------------------------


class TestOpenAPISpec:
    def test_openapi_json_is_accessible(self) -> None:
        client = _make_app()
        resp = client.get("/api/v1/openapi.json")
        assert resp.status_code == 200
        spec = resp.json()
        assert "openapi" in spec
        assert "/api/v1/analyze" in spec["paths"]
        assert "/api/v1/status" in spec["paths"]
        assert "/api/v1/health" in spec["paths"]
