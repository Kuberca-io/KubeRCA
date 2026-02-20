"""Tests for KubeRCA core data models.

Verifies the contracts defined in Technical Design Spec Section 3:
  - EventRecord: immutability, field presence, default values
  - ResourceSnapshot: immutability, field types
  - FieldChange: immutability, field types
  - RCAResponse: construction, default values, blast_radius contract
  - AnomalyAlert: immutability, auto-generated fields
  - CachedResource: redacted_view() delegation, view caching, invalidation
  - Enumerations: string values
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

import pytest

from kuberca.models.alerts import AnomalyAlert
from kuberca.models.analysis import (
    AffectedResource,
    EvidenceItem,
    RCAResponse,
    ResponseMeta,
)
from kuberca.models.events import (
    DiagnosisSource,
    EventRecord,
    EventSource,
    EvidenceType,
    Severity,
)
from kuberca.models.resources import (
    CachedResource,
    CachedResourceView,
    FieldChange,
    ResourceSnapshot,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)


def _make_event(**overrides: object) -> EventRecord:
    defaults: dict[str, object] = {
        "source": EventSource.CORE_EVENT,
        "severity": Severity.ERROR,
        "reason": "OOMKilled",
        "message": "Container killed",
        "namespace": "default",
        "resource_kind": "Pod",
        "resource_name": "my-pod-abc",
        "first_seen": _NOW,
        "last_seen": _NOW,
        "count": 1,
    }
    defaults.update(overrides)
    return EventRecord(**defaults)  # type: ignore[arg-type]


def _make_snapshot(**overrides: object) -> ResourceSnapshot:
    defaults: dict[str, object] = {
        "kind": "Deployment",
        "namespace": "default",
        "name": "my-app",
        "spec_hash": "abc123",
        "spec": {"replicas": 1},
        "captured_at": _NOW,
        "resource_version": "1",
    }
    defaults.update(overrides)
    return ResourceSnapshot(**defaults)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Enumeration string values
# ---------------------------------------------------------------------------


class TestEnumerations:
    def test_event_source_values(self) -> None:
        assert EventSource.CORE_EVENT == "core_event"
        assert EventSource.POD_PHASE == "pod_phase"
        assert EventSource.NODE_CONDITION == "node_condition"

    def test_severity_values(self) -> None:
        assert Severity.INFO == "info"
        assert Severity.WARNING == "warning"
        assert Severity.ERROR == "error"
        assert Severity.CRITICAL == "critical"

    def test_diagnosis_source_values(self) -> None:
        assert DiagnosisSource.RULE_ENGINE == "rule_engine"
        assert DiagnosisSource.LLM == "llm"
        assert DiagnosisSource.INCONCLUSIVE == "inconclusive"

    def test_evidence_type_values(self) -> None:
        assert EvidenceType.EVENT == "event"
        assert EvidenceType.CHANGE == "change"

    def test_enums_are_str_subclasses(self) -> None:
        # All enumerations inherit from str for JSON serialisation compatibility
        assert isinstance(EventSource.CORE_EVENT, str)
        assert isinstance(Severity.INFO, str)
        assert isinstance(DiagnosisSource.RULE_ENGINE, str)
        assert isinstance(EvidenceType.EVENT, str)


# ---------------------------------------------------------------------------
# EventRecord
# ---------------------------------------------------------------------------


class TestEventRecord:
    def test_construction_with_required_fields(self) -> None:
        event = _make_event()
        assert event.reason == "OOMKilled"
        assert event.namespace == "default"
        assert event.resource_kind == "Pod"

    def test_event_id_auto_generated(self) -> None:
        event = _make_event()
        # Should be a valid UUID v4
        uuid = UUID(event.event_id)
        assert uuid.version == 4

    def test_event_id_unique_per_instance(self) -> None:
        e1 = _make_event()
        e2 = _make_event()
        assert e1.event_id != e2.event_id

    def test_count_defaults_to_1(self) -> None:
        event = _make_event()
        assert event.count == 1

    def test_cluster_id_defaults_to_empty_string(self) -> None:
        event = _make_event()
        assert event.cluster_id == ""

    def test_labels_default_to_empty_dict(self) -> None:
        event = _make_event()
        assert event.labels == {}

    def test_raw_object_defaults_to_none(self) -> None:
        event = _make_event()
        assert event.raw_object is None

    def test_immutability(self) -> None:
        event = _make_event()
        with pytest.raises((AttributeError, TypeError)):
            event.reason = "BackOff"  # type: ignore[misc]

    def test_explicit_count(self) -> None:
        event = _make_event(count=5)
        assert event.count == 5

    def test_explicit_labels(self) -> None:
        event = _make_event(labels={"app": "my-app"})
        assert event.labels == {"app": "my-app"}

    def test_explicit_cluster_id(self) -> None:
        event = _make_event(cluster_id="prod")
        assert event.cluster_id == "prod"

    def test_explicit_event_id(self) -> None:
        event = _make_event(event_id="fixed-id")
        assert event.event_id == "fixed-id"


# ---------------------------------------------------------------------------
# ResourceSnapshot
# ---------------------------------------------------------------------------


class TestResourceSnapshot:
    def test_construction(self) -> None:
        snap = _make_snapshot()
        assert snap.kind == "Deployment"
        assert snap.namespace == "default"
        assert snap.name == "my-app"
        assert snap.spec == {"replicas": 1}

    def test_immutability(self) -> None:
        snap = _make_snapshot()
        with pytest.raises((AttributeError, TypeError)):
            snap.kind = "StatefulSet"  # type: ignore[misc]

    def test_spec_hash_stored(self) -> None:
        snap = _make_snapshot(spec_hash="deadbeef")
        assert snap.spec_hash == "deadbeef"

    def test_resource_version_stored(self) -> None:
        snap = _make_snapshot(resource_version="42")
        assert snap.resource_version == "42"

    def test_captured_at_stored(self) -> None:
        snap = _make_snapshot(captured_at=_NOW)
        assert snap.captured_at == _NOW


# ---------------------------------------------------------------------------
# FieldChange
# ---------------------------------------------------------------------------


class TestFieldChange:
    def test_construction(self) -> None:
        change = FieldChange(
            field_path="spec.replicas",
            old_value="1",
            new_value="3",
            changed_at=_NOW,
        )
        assert change.field_path == "spec.replicas"
        assert change.old_value == "1"
        assert change.new_value == "3"

    def test_immutability(self) -> None:
        change = FieldChange(
            field_path="spec.replicas",
            old_value="1",
            new_value="3",
            changed_at=_NOW,
        )
        with pytest.raises((AttributeError, TypeError)):
            change.field_path = "spec.image"  # type: ignore[misc]

    def test_old_value_can_be_none(self) -> None:
        change = FieldChange(
            field_path="spec.new_field",
            old_value=None,
            new_value="added",
            changed_at=_NOW,
        )
        assert change.old_value is None

    def test_new_value_can_be_none(self) -> None:
        change = FieldChange(
            field_path="spec.deleted_field",
            old_value="was_here",
            new_value=None,
            changed_at=_NOW,
        )
        assert change.new_value is None


# ---------------------------------------------------------------------------
# RCAResponse
# ---------------------------------------------------------------------------


class TestRCAResponse:
    def test_construction(self) -> None:
        response = RCAResponse(
            root_cause="Pod OOMKilled due to memory limit",
            confidence=0.80,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
        )
        assert response.root_cause == "Pod OOMKilled due to memory limit"
        assert response.confidence == 0.80
        assert response.diagnosed_by == DiagnosisSource.RULE_ENGINE

    def test_blast_radius_always_none(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.INCONCLUSIVE,
        )
        assert response.blast_radius is None

    def test_rule_id_defaults_to_none(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.LLM,
        )
        assert response.rule_id is None

    def test_evidence_defaults_to_empty_list(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
        )
        assert response.evidence == []

    def test_affected_resources_defaults_to_empty_list(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
        )
        assert response.affected_resources == []

    def test_suggested_remediation_defaults_to_empty_string(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
        )
        assert response.suggested_remediation == ""

    def test_meta_defaults_to_none(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.5,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
        )
        assert response._meta is None

    def test_with_rule_id(self) -> None:
        response = RCAResponse(
            root_cause="test",
            confidence=0.80,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            rule_id="R01_oom_killed",
        )
        assert response.rule_id == "R01_oom_killed"

    def test_with_evidence_items(self) -> None:
        evidence = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp="2026-02-19T12:00:00.000Z",
                summary="Pod OOMKilled",
            )
        ]
        response = RCAResponse(
            root_cause="test",
            confidence=0.8,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            evidence=evidence,
        )
        assert len(response.evidence) == 1
        assert response.evidence[0].summary == "Pod OOMKilled"

    def test_with_affected_resources(self) -> None:
        affected = [AffectedResource(kind="Pod", namespace="default", name="my-pod")]
        response = RCAResponse(
            root_cause="test",
            confidence=0.8,
            diagnosed_by=DiagnosisSource.RULE_ENGINE,
            affected_resources=affected,
        )
        assert len(response.affected_resources) == 1
        assert response.affected_resources[0].kind == "Pod"


# ---------------------------------------------------------------------------
# ResponseMeta
# ---------------------------------------------------------------------------


class TestResponseMeta:
    def test_construction(self) -> None:
        meta = ResponseMeta(kuberca_version="0.1.0")
        assert meta.kuberca_version == "0.1.0"

    def test_schema_version_defaults_to_1(self) -> None:
        meta = ResponseMeta(kuberca_version="0.1.0")
        assert meta.schema_version == "1"

    def test_warnings_defaults_to_empty_list(self) -> None:
        meta = ResponseMeta(kuberca_version="0.1.0")
        assert meta.warnings == []

    def test_cache_state_defaults_to_ready(self) -> None:
        meta = ResponseMeta(kuberca_version="0.1.0")
        assert meta.cache_state == "ready"

    def test_cluster_id_defaults_to_empty_string(self) -> None:
        meta = ResponseMeta(kuberca_version="0.1.0")
        assert meta.cluster_id == ""


# ---------------------------------------------------------------------------
# EvidenceItem
# ---------------------------------------------------------------------------


class TestEvidenceItem:
    def test_construction(self) -> None:
        item = EvidenceItem(
            type=EvidenceType.CHANGE,
            timestamp="2026-02-19T12:00:00.000Z",
            summary="Memory limit changed from 256Mi to 128Mi",
        )
        assert item.type == EvidenceType.CHANGE
        assert item.timestamp == "2026-02-19T12:00:00.000Z"

    def test_debug_context_defaults_to_none(self) -> None:
        item = EvidenceItem(
            type=EvidenceType.EVENT,
            timestamp="2026-02-19T12:00:00.000Z",
            summary="test",
        )
        assert item.debug_context is None

    def test_debug_context_can_be_set(self) -> None:
        item = EvidenceItem(
            type=EvidenceType.EVENT,
            timestamp="2026-02-19T12:00:00.000Z",
            summary="test",
            debug_context={"event_reason": "OOMKilled"},
        )
        assert item.debug_context == {"event_reason": "OOMKilled"}


# ---------------------------------------------------------------------------
# AnomalyAlert
# ---------------------------------------------------------------------------


class TestAnomalyAlert:
    def test_construction(self) -> None:
        alert = AnomalyAlert(
            severity=Severity.WARNING,
            resource_kind="Pod",
            resource_name="my-pod",
            namespace="default",
            reason="OOMKilled",
            summary="Pod OOM killed repeatedly",
            detected_at=_NOW,
        )
        assert alert.severity == Severity.WARNING
        assert alert.resource_kind == "Pod"

    def test_alert_id_auto_generated(self) -> None:
        alert = AnomalyAlert(
            severity=Severity.WARNING,
            resource_kind="Pod",
            resource_name="my-pod",
            namespace="default",
            reason="OOMKilled",
            summary="test",
            detected_at=_NOW,
        )
        uuid = UUID(alert.alert_id)
        assert uuid.version == 4

    def test_event_count_defaults_to_1(self) -> None:
        alert = AnomalyAlert(
            severity=Severity.WARNING,
            resource_kind="Pod",
            resource_name="my-pod",
            namespace="default",
            reason="BackOff",
            summary="test",
            detected_at=_NOW,
        )
        assert alert.event_count == 1

    def test_source_events_defaults_to_empty_list(self) -> None:
        alert = AnomalyAlert(
            severity=Severity.ERROR,
            resource_kind="Pod",
            resource_name="my-pod",
            namespace="default",
            reason="OOMKilled",
            summary="test",
            detected_at=_NOW,
        )
        assert alert.source_events == []

    def test_immutability(self) -> None:
        alert = AnomalyAlert(
            severity=Severity.WARNING,
            resource_kind="Pod",
            resource_name="my-pod",
            namespace="default",
            reason="OOMKilled",
            summary="test",
            detected_at=_NOW,
        )
        with pytest.raises((AttributeError, TypeError)):
            alert.severity = Severity.CRITICAL  # type: ignore[misc]


# ---------------------------------------------------------------------------
# CachedResource and CachedResourceView
# ---------------------------------------------------------------------------


class TestCachedResource:
    def _make_cached_resource(self, **overrides: object) -> CachedResource:
        defaults: dict[str, object] = {
            "kind": "Deployment",
            "namespace": "default",
            "name": "my-app",
            "resource_version": "42",
            "labels": {"app": "my-app"},
            "annotations": {"app.kubernetes.io/name": "my-app", "corp.io/secret": "value"},
            "spec": {"replicas": 3, "containers": [{"env": [{"name": "DB_PASS", "value": "s3cret"}]}]},
            "status": {"readyReplicas": 3},
            "last_updated": _NOW,
        }
        defaults.update(overrides)
        return CachedResource(**defaults)  # type: ignore[arg-type]

    def test_redacted_view_returns_cached_resource_view(self) -> None:
        resource = self._make_cached_resource()
        view = resource.redacted_view()
        assert isinstance(view, CachedResourceView)

    def test_redacted_view_preserves_identity_fields(self) -> None:
        resource = self._make_cached_resource()
        view = resource.redacted_view()
        assert view.kind == "Deployment"
        assert view.namespace == "default"
        assert view.name == "my-app"
        assert view.resource_version == "42"

    def test_redacted_view_safe_annotations_pass_through(self) -> None:
        resource = self._make_cached_resource()
        view = resource.redacted_view()
        assert view.annotations.get("app.kubernetes.io/name") == "my-app"

    def test_redacted_view_unsafe_annotations_redacted(self) -> None:
        resource = self._make_cached_resource()
        view = resource.redacted_view()
        assert view.annotations.get("corp.io/secret") == "[REDACTED]"

    def test_redacted_view_is_cached(self) -> None:
        resource = self._make_cached_resource()
        view1 = resource.redacted_view()
        view2 = resource.redacted_view()
        assert view1 is view2  # same object returned from cache

    def test_invalidate_view_clears_cache(self) -> None:
        resource = self._make_cached_resource()
        view1 = resource.redacted_view()
        resource.invalidate_view()
        view2 = resource.redacted_view()
        assert view1 is not view2  # new object after invalidation

    def test_cached_resource_view_is_immutable(self) -> None:
        resource = self._make_cached_resource()
        view = resource.redacted_view()
        with pytest.raises((AttributeError, TypeError)):
            view.name = "other"  # type: ignore[misc]

    def test_labels_preserved_without_redaction(self) -> None:
        resource = self._make_cached_resource(labels={"env": "prod", "app": "api"})
        view = resource.redacted_view()
        assert view.labels == {"env": "prod", "app": "api"}
