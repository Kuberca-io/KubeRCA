"""Integration tests for the FastAPI REST API.

Uses TestClient to exercise the full HTTP request/response cycle including
validation, error handling, and response serialisation.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi.testclient import TestClient

from kuberca.api.app import create_app
from kuberca.cache.resource_cache import ResourceCache
from kuberca.models.analysis import (
    AffectedResource,
    EvidenceItem,
    RCAResponse,
    ResponseMeta,
)
from kuberca.models.config import KubeRCAConfig
from kuberca.models.events import DiagnosisSource, EvidenceType
from kuberca.models.resources import CacheReadiness

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_rca_response(**overrides) -> RCAResponse:
    """Build a valid RCAResponse with sensible defaults for API tests."""
    defaults = {
        "root_cause": "Pod OOM-killed due to memory limit.",
        "confidence": 0.85,
        "diagnosed_by": DiagnosisSource.RULE_ENGINE,
        "rule_id": "R01_oom_killed",
        "evidence": [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp="2024-01-15T10:30:00Z",
                summary="OOMKilled event on pod my-app-7b4f8c6d-x2kj",
            )
        ],
        "affected_resources": [
            AffectedResource(kind="Pod", namespace="default", name="my-app-7b4f8c6d-x2kj"),
        ],
        "suggested_remediation": "Increase memory limit for the container.",
        "_meta": ResponseMeta(
            kuberca_version="0.1.2",
            schema_version="1",
            cluster_id="test-cluster",
            timestamp="2024-01-15T10:30:01Z",
            response_time_ms=42,
            cache_state="ready",
            warnings=[],
        ),
    }
    defaults.update(overrides)
    return RCAResponse(**defaults)


def _build_integration_client(
    cache: ResourceCache | None = None,
    rca_response: RCAResponse | None = None,
    queue_full: bool = False,
) -> TestClient:
    """Build a TestClient with a real coordinator or mocked components."""
    if cache is None:
        cache = MagicMock()
        cache.readiness_state = CacheReadiness.READY
        cache.get = MagicMock(return_value=MagicMock())

    coordinator = MagicMock()
    coordinator.analyze = AsyncMock(return_value=rca_response or _make_rca_response())

    queue = None
    if queue_full:
        queue = MagicMock()
        queue.is_full = MagicMock(return_value=True)

    app = create_app(
        coordinator=coordinator,
        cache=cache,
        config=KubeRCAConfig(cluster_id="test-cluster"),
        analysis_queue=queue,
    )
    return TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# GET /api/v1/health
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_health_returns_200(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_health_body_has_required_fields(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/health")
        body = resp.json()
        assert body["status"] == "ok"
        assert "version" in body
        assert "cache_state" in body

    def test_health_content_type_is_json(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/health")
        assert resp.headers["content-type"].startswith("application/json")


# ---------------------------------------------------------------------------
# GET /api/v1/status
# ---------------------------------------------------------------------------


class TestStatusEndpoint:
    def test_status_returns_200(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/status")
        assert resp.status_code == 200

    def test_status_body_has_required_fields(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/status")
        body = resp.json()
        assert "pod_counts" in body
        assert "node_conditions" in body
        assert "active_anomalies" in body
        assert "recent_events" in body
        assert "cache_state" in body

    def test_status_content_type_is_json(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/status")
        assert resp.headers["content-type"].startswith("application/json")


# ---------------------------------------------------------------------------
# POST /api/v1/analyze — success
# ---------------------------------------------------------------------------


class TestAnalyzeEndpointSuccess:
    def test_analyze_valid_resource_returns_200(self) -> None:
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/my-app-7b4f8c6d-x2kj"})
        assert resp.status_code == 200

    def test_analyze_returns_rca_response_structure(self) -> None:
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/my-app-7b4f8c6d-x2kj"})
        body = resp.json()
        assert "root_cause" in body
        assert "confidence" in body
        assert "diagnosed_by" in body
        assert "evidence" in body
        assert "affected_resources" in body
        assert "suggested_remediation" in body
        assert isinstance(body["confidence"], float)
        assert body["diagnosed_by"] in ("rule_engine", "llm", "inconclusive")

    def test_analyze_with_time_window(self) -> None:
        client = _build_integration_client()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Deployment/prod/api-server", "time_window": "30m"},
        )
        assert resp.status_code == 200

    def test_analyze_evidence_items_have_correct_schema(self) -> None:
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/my-app-7b4f8c6d-x2kj"})
        body = resp.json()
        for item in body["evidence"]:
            assert "type" in item
            assert "timestamp" in item
            assert "summary" in item

    def test_analyze_affected_resources_have_correct_schema(self) -> None:
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/my-app-7b4f8c6d-x2kj"})
        body = resp.json()
        for resource in body["affected_resources"]:
            assert "kind" in resource
            assert "namespace" in resource
            assert "name" in resource

    def test_analyze_content_type_is_json(self) -> None:
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default/my-app-7b4f8c6d-x2kj"})
        assert resp.headers["content-type"].startswith("application/json")


# ---------------------------------------------------------------------------
# POST /api/v1/analyze — validation errors
# ---------------------------------------------------------------------------


class TestAnalyzeEndpointValidation:
    def test_invalid_resource_format_returns_400(self) -> None:
        """Resource with only 2 parts should return 400 INVALID_RESOURCE_FORMAT."""
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": "Pod/default"})
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "INVALID_RESOURCE_FORMAT"
        assert "detail" in body

    def test_empty_resource_returns_400(self) -> None:
        client = _build_integration_client()
        resp = client.post("/api/v1/analyze", json={"resource": ""})
        assert resp.status_code == 400

    def test_invalid_time_window_format_returns_400(self) -> None:
        """Time window not matching d+(m|h|d) should return 400 INVALID_TIME_WINDOW."""
        client = _build_integration_client()
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/x", "time_window": "2hours"},
        )
        assert resp.status_code == 400
        body = resp.json()
        assert body["error"] == "INVALID_TIME_WINDOW"

    def test_resource_not_found_returns_404(self) -> None:
        """When cache lookup returns None, should return 404 RESOURCE_NOT_FOUND."""
        cache = MagicMock()
        cache.readiness_state = CacheReadiness.READY
        cache.get = MagicMock(return_value=None)  # resource absent

        client = _build_integration_client(cache=cache)
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/nonexistent-pod"},
        )
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "RESOURCE_NOT_FOUND"


# ---------------------------------------------------------------------------
# POST /api/v1/analyze — queue full
# ---------------------------------------------------------------------------


class TestAnalyzeQueueFull:
    def test_queue_full_returns_429(self) -> None:
        client = _build_integration_client(queue_full=True)
        resp = client.post(
            "/api/v1/analyze",
            json={"resource": "Pod/default/x"},
        )
        assert resp.status_code == 429
        body = resp.json()
        assert body["error"] == "QUEUE_FULL"


# ---------------------------------------------------------------------------
# GET /api/v1/openapi.json
# ---------------------------------------------------------------------------


class TestOpenAPISpec:
    def test_openapi_json_returns_200(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/openapi.json")
        assert resp.status_code == 200

    def test_openapi_json_is_valid_schema(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/openapi.json")
        spec = resp.json()
        assert "openapi" in spec
        assert spec["openapi"].startswith("3.")
        assert "paths" in spec
        assert "/api/v1/analyze" in spec["paths"]
        assert "/api/v1/health" in spec["paths"]
        assert "/api/v1/status" in spec["paths"]

    def test_openapi_info_section_populated(self) -> None:
        client = _build_integration_client()
        resp = client.get("/api/v1/openapi.json")
        spec = resp.json()
        assert spec["info"]["title"] == "KubeRCA"
        assert "version" in spec["info"]
