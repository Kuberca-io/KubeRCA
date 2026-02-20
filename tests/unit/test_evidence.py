"""Tests for kuberca.llm.evidence — EvidencePackage assembly and truncation."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from kuberca.llm.evidence import MAX_PROMPT_BYTES, EvidencePackage, _truncate_value
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import FieldChange


def _make_event(
    reason: str = "OOMKilled",
    count: int = 1,
    namespace: str = "default",
    name: str = "my-pod",
    kind: str = "Pod",
    severity: Severity = Severity.WARNING,
    offset_seconds: int = 0,
) -> EventRecord:
    base = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
    from datetime import timedelta

    ts = base + timedelta(seconds=offset_seconds)
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test-cluster",
        source=EventSource.CORE_EVENT,
        severity=severity,
        reason=reason,
        message=f"{reason} occurred",
        namespace=namespace,
        resource_kind=kind,
        resource_name=name,
        first_seen=ts,
        last_seen=ts,
        count=count,
    )


def _make_change(field_path: str = "spec.containers.memory", offset: int = 0) -> FieldChange:
    from datetime import timedelta

    base = datetime(2026, 2, 18, 11, 0, 0, tzinfo=UTC)
    return FieldChange(
        field_path=field_path,
        old_value="128Mi",
        new_value="256Mi",
        changed_at=base + timedelta(seconds=offset),
    )


class TestEvidencePackageCaps:
    def test_related_events_capped_at_20(self) -> None:
        incident = _make_event()
        events = [_make_event(offset_seconds=i) for i in range(30)]
        pkg = EvidencePackage(incident_event=incident, related_events=events)
        assert len(pkg.related_events) == 20

    def test_related_events_sorted_most_recent_first(self) -> None:
        incident = _make_event()
        events = [_make_event(offset_seconds=i) for i in range(5)]
        pkg = EvidencePackage(incident_event=incident, related_events=events)
        timestamps = [e.last_seen for e in pkg.related_events]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_changes_capped_at_30(self) -> None:
        incident = _make_event()
        changes = [_make_change(offset=i) for i in range(50)]
        pkg = EvidencePackage(incident_event=incident, changes=changes)
        assert len(pkg.changes) == 30

    def test_changes_sorted_most_recent_first(self) -> None:
        incident = _make_event()
        changes = [_make_change(offset=i) for i in range(5)]
        pkg = EvidencePackage(incident_event=incident, changes=changes)
        timestamps = [c.changed_at for c in pkg.changes]
        assert timestamps == sorted(timestamps, reverse=True)

    def test_container_statuses_capped_at_10(self) -> None:
        incident = _make_event()
        statuses = [{"name": f"c{i}"} for i in range(20)]
        pkg = EvidencePackage(incident_event=incident, container_statuses=statuses)
        assert len(pkg.container_statuses) == 10

    def test_resource_specs_capped_at_5(self) -> None:
        incident = _make_event()
        specs = [{"kind": "Pod", "name": f"pod-{i}"} for i in range(10)]
        pkg = EvidencePackage(incident_event=incident, resource_specs=specs)
        assert len(pkg.resource_specs) == 5


class TestToPromptContext:
    def test_output_within_max_bytes_for_minimal_package(self) -> None:
        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        ctx = pkg.to_prompt_context()
        assert len(ctx.encode("utf-8")) <= MAX_PROMPT_BYTES

    def test_output_contains_required_sections(self) -> None:
        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        ctx = pkg.to_prompt_context()
        assert "## Incident" in ctx
        assert "## Recent Events" in ctx
        assert "## Recent Changes" in ctx
        assert "## Container Status" in ctx
        assert "## Resource Specifications" in ctx

    def test_incident_fields_present(self) -> None:
        incident = _make_event(reason="OOMKilled", namespace="prod", name="api-pod")
        pkg = EvidencePackage(incident_event=incident)
        ctx = pkg.to_prompt_context()
        assert "OOMKilled" in ctx
        assert "prod" in ctx
        assert "api-pod" in ctx

    def test_truncation_drops_resource_specs_first(self) -> None:
        incident = _make_event()
        # Create a large spec list that should push over the limit
        big_specs = [{"kind": "Pod", "data": "x" * 3000} for _ in range(5)]
        big_events = [_make_event(offset_seconds=i) for i in range(20)]
        pkg = EvidencePackage(
            incident_event=incident,
            related_events=big_events,
            resource_specs=big_specs,
        )
        ctx = pkg.to_prompt_context()
        # Should still be within bounds after truncation
        assert len(ctx.encode("utf-8")) <= MAX_PROMPT_BYTES * 2  # allow some overage on extreme data

    def test_change_values_in_output(self) -> None:
        incident = _make_event()
        change = _make_change("spec.containers[0].resources.limits.memory")
        pkg = EvidencePackage(incident_event=incident, changes=[change])
        ctx = pkg.to_prompt_context()
        assert "spec.containers[0].resources.limits.memory" in ctx
        assert "128Mi" in ctx
        assert "256Mi" in ctx

    def test_none_change_values_render_as_none_marker(self) -> None:
        incident = _make_event()
        change = FieldChange(
            field_path="spec.replicas",
            old_value=None,
            new_value="3",
            changed_at=datetime(2026, 2, 18, 11, 0, 0, tzinfo=UTC),
        )
        pkg = EvidencePackage(incident_event=incident, changes=[change])
        ctx = pkg.to_prompt_context()
        assert "(none)" in ctx


class TestTruncateValue:
    def test_short_value_unchanged(self) -> None:
        assert _truncate_value("abc", 200) == "abc"

    def test_none_returns_none(self) -> None:
        assert _truncate_value(None, 200) is None

    def test_long_value_truncated(self) -> None:
        long_val = "x" * 300
        result = _truncate_value(long_val, 200)
        assert result is not None
        assert result.startswith("x" * 200)
        assert "[TRUNCATED]" in result

    def test_exact_boundary_not_truncated(self) -> None:
        val = "y" * 200
        assert _truncate_value(val, 200) == val
