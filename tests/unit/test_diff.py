"""Tests for kuberca.ledger.diff."""

from __future__ import annotations

from datetime import datetime

from kuberca.ledger.diff import compute_diff
from kuberca.models.resources import FieldChange


def _paths(changes: list[FieldChange]) -> list[str]:
    return [c.field_path for c in changes]


def _change_map(changes: list[FieldChange]) -> dict[str, tuple[str | None, str | None]]:
    return {c.field_path: (c.old_value, c.new_value) for c in changes}


# ---------------------------------------------------------------------------
# Basic leaf changes
# ---------------------------------------------------------------------------


class TestLeafChanges:
    def test_no_changes_returns_empty(self) -> None:
        spec = {"replicas": 3, "image": "nginx:1.25"}
        assert compute_diff(spec, spec.copy()) == []

    def test_single_leaf_change(self) -> None:
        old = {"replicas": 3}
        new = {"replicas": 5}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "replicas"
        assert changes[0].old_value == "3"
        assert changes[0].new_value == "5"

    def test_string_leaf_change(self) -> None:
        old = {"image": "nginx:1.24"}
        new = {"image": "nginx:1.25"}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "image"
        assert changes[0].old_value == '"nginx:1.24"'
        assert changes[0].new_value == '"nginx:1.25"'

    def test_added_field(self) -> None:
        old: dict[str, object] = {"replicas": 1}
        new: dict[str, object] = {"replicas": 1, "minReadySeconds": 10}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "minReadySeconds"
        assert changes[0].old_value is None
        assert changes[0].new_value == "10"

    def test_removed_field(self) -> None:
        old: dict[str, object] = {"replicas": 1, "minReadySeconds": 10}
        new: dict[str, object] = {"replicas": 1}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "minReadySeconds"
        assert changes[0].old_value == "10"
        assert changes[0].new_value is None

    def test_bool_change(self) -> None:
        old = {"paused": False}
        new = {"paused": True}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].old_value == "false"
        assert changes[0].new_value == "true"

    def test_null_to_value(self) -> None:
        old: dict[str, object] = {"value": None}
        new: dict[str, object] = {"value": "present"}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].old_value is None
        assert changes[0].new_value == '"present"'

    def test_value_to_null(self) -> None:
        old: dict[str, object] = {"value": "present"}
        new: dict[str, object] = {"value": None}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].old_value == '"present"'
        assert changes[0].new_value is None


# ---------------------------------------------------------------------------
# Nested dict paths
# ---------------------------------------------------------------------------


class TestNestedPaths:
    def test_nested_change_uses_dotted_path(self) -> None:
        old: dict[str, object] = {"spec": {"replicas": 1}}
        new: dict[str, object] = {"spec": {"replicas": 3}}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "spec.replicas"

    def test_deeply_nested_path(self) -> None:
        old: dict[str, object] = {"spec": {"template": {"spec": {"terminationGracePeriodSeconds": 30}}}}
        new: dict[str, object] = {"spec": {"template": {"spec": {"terminationGracePeriodSeconds": 60}}}}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "spec.template.spec.terminationGracePeriodSeconds"

    def test_multiple_nested_changes(self) -> None:
        old: dict[str, object] = {"spec": {"replicas": 1, "paused": False}}
        new: dict[str, object] = {"spec": {"replicas": 3, "paused": True}}
        changes = compute_diff(old, new)
        paths = _paths(changes)
        assert "spec.replicas" in paths
        assert "spec.paused" in paths


# ---------------------------------------------------------------------------
# Array diffs with indexed paths
# ---------------------------------------------------------------------------


class TestArrayPaths:
    def test_list_element_change_uses_indexed_path(self) -> None:
        old: dict[str, object] = {
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx:1.24"},
                ]
            }
        }
        new: dict[str, object] = {
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx:1.25"},
                ]
            }
        }
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "spec.containers[0].image"

    def test_multiple_container_changes(self) -> None:
        old: dict[str, object] = {
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx:1.24"},
                    {"name": "sidecar", "image": "envoy:1.0"},
                ]
            }
        }
        new: dict[str, object] = {
            "spec": {
                "containers": [
                    {"name": "app", "image": "nginx:1.25"},
                    {"name": "sidecar", "image": "envoy:1.1"},
                ]
            }
        }
        changes = compute_diff(old, new)
        cm = _change_map(changes)
        assert "spec.containers[0].image" in cm
        assert "spec.containers[1].image" in cm

    def test_added_container(self) -> None:
        old: dict[str, object] = {"containers": [{"name": "app"}]}
        new: dict[str, object] = {"containers": [{"name": "app"}, {"name": "sidecar"}]}
        changes = compute_diff(old, new)
        # New container at index 1 — name field added
        paths = _paths(changes)
        assert any("[1]" in p for p in paths)

    def test_removed_container(self) -> None:
        old: dict[str, object] = {"containers": [{"name": "app"}, {"name": "sidecar"}]}
        new: dict[str, object] = {"containers": [{"name": "app"}]}
        changes = compute_diff(old, new)
        paths = _paths(changes)
        assert any("[1]" in p for p in paths)

    def test_empty_to_empty_list_no_change(self) -> None:
        assert compute_diff({"items": []}, {"items": []}) == []

    def test_primitive_list_change(self) -> None:
        old: dict[str, object] = {"ports": [8080, 8443]}
        new: dict[str, object] = {"ports": [8080, 9443]}
        changes = compute_diff(old, new)
        assert len(changes) == 1
        assert changes[0].field_path == "ports[1]"
        assert changes[0].old_value == "8443"
        assert changes[0].new_value == "9443"


# ---------------------------------------------------------------------------
# Type changes (structural)
# ---------------------------------------------------------------------------


class TestStructuralChanges:
    def test_scalar_to_dict_emits_change(self) -> None:
        old: dict[str, object] = {"config": "simple"}
        new: dict[str, object] = {"config": {"key": "value"}}
        changes = compute_diff(old, new)
        assert len(changes) >= 1
        # The top-level field must appear in the changes
        paths = _paths(changes)
        assert any("config" in p for p in paths)


# ---------------------------------------------------------------------------
# Timestamp propagation
# ---------------------------------------------------------------------------


class TestTimestamp:
    def test_all_changes_share_same_timestamp(self) -> None:
        old: dict[str, object] = {"a": 1, "b": 2, "c": 3}
        new: dict[str, object] = {"a": 9, "b": 8, "c": 7}
        changes = compute_diff(old, new)
        assert len(changes) == 3
        timestamps = {c.changed_at for c in changes}
        assert len(timestamps) == 1

    def test_changed_at_is_recent(self) -> None:
        before = datetime.utcnow()
        changes = compute_diff({"x": 1}, {"x": 2})
        after = datetime.utcnow()
        assert len(changes) == 1
        assert before <= changes[0].changed_at <= after


# ---------------------------------------------------------------------------
# Sort order
# ---------------------------------------------------------------------------


class TestSortOrder:
    def test_changes_sorted_by_field_path(self) -> None:
        old: dict[str, object] = {"z": 1, "a": 1, "m": 1}
        new: dict[str, object] = {"z": 2, "a": 2, "m": 2}
        changes = compute_diff(old, new)
        paths = _paths(changes)
        assert paths == sorted(paths)
