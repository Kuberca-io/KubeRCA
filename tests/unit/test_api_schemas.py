"""Unit tests for kuberca.api.schemas."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from kuberca.api.schemas import (
    AnalyzeRequest,
    ClusterStatus,
    ErrorResponse,
    HealthStatus,
)


class TestAnalyzeRequest:
    def test_valid_resource_parses(self) -> None:
        req = AnalyzeRequest(resource="Pod/default/my-pod")
        assert req.resource == "Pod/default/my-pod"

    def test_valid_resource_with_time_window(self) -> None:
        req = AnalyzeRequest(resource="Deployment/prod/api", time_window="2h")
        assert req.time_window == "2h"

    def test_valid_time_window_minutes(self) -> None:
        req = AnalyzeRequest(resource="Pod/default/x", time_window="30m")
        assert req.time_window == "30m"

    def test_valid_time_window_days(self) -> None:
        req = AnalyzeRequest(resource="Pod/default/x", time_window="1d")
        assert req.time_window == "1d"

    def test_none_time_window_is_allowed(self) -> None:
        req = AnalyzeRequest(resource="Pod/default/x", time_window=None)
        assert req.time_window is None

    def test_invalid_resource_too_few_parts(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AnalyzeRequest(resource="Pod/default")
        errors = exc_info.value.errors()
        assert any("resource" in str(e["loc"]) for e in errors)

    def test_invalid_resource_empty_segment(self) -> None:
        with pytest.raises(ValidationError):
            AnalyzeRequest(resource="Pod//my-pod")

    def test_invalid_resource_too_many_parts(self) -> None:
        with pytest.raises(ValidationError):
            AnalyzeRequest(resource="Pod/default/my-pod/extra")

    def test_invalid_time_window_format(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            AnalyzeRequest(resource="Pod/default/x", time_window="2hours")
        errors = exc_info.value.errors()
        assert any("time_window" in str(e["loc"]) for e in errors)

    def test_invalid_time_window_missing_unit(self) -> None:
        with pytest.raises(ValidationError):
            AnalyzeRequest(resource="Pod/default/x", time_window="120")

    def test_invalid_time_window_bad_unit(self) -> None:
        with pytest.raises(ValidationError):
            AnalyzeRequest(resource="Pod/default/x", time_window="2s")


class TestClusterStatus:
    def test_default_values(self) -> None:
        status = ClusterStatus()
        assert status.pod_counts == {}
        assert status.node_conditions == []
        assert status.active_anomalies == []
        assert status.recent_events == []
        assert status.cache_state == "ready"

    def test_populated_values_are_stored(self) -> None:
        status = ClusterStatus(
            pod_counts={"Running": 10, "Pending": 2},
            cache_state="warming",
        )
        assert status.pod_counts["Running"] == 10
        assert status.cache_state == "warming"


class TestHealthStatus:
    def test_fields_are_present(self) -> None:
        hs = HealthStatus(status="ok", version="0.1.0", cache_state="ready")
        assert hs.status == "ok"
        assert hs.version == "0.1.0"
        assert hs.cache_state == "ready"


class TestErrorResponse:
    def test_serialises_to_dict(self) -> None:
        err = ErrorResponse(error="INVALID_RESOURCE_FORMAT", detail="bad input")
        d = err.model_dump()
        assert d["error"] == "INVALID_RESOURCE_FORMAT"
        assert d["detail"] == "bad input"
