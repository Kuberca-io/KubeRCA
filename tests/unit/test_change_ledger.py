"""Tests for kuberca.ledger.change_ledger."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta

import pytest

from kuberca.ledger.change_ledger import (
    ChangeLedger,
    LedgerStats,
    _estimate_snapshot_size,
)
from kuberca.models.resources import ResourceSnapshot

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _snap(
    kind: str = "Deployment",
    namespace: str = "default",
    name: str = "my-app",
    spec: dict[str, object] | None = None,
    resource_version: str = "1",
    captured_at: datetime | None = None,
) -> ResourceSnapshot:
    if spec is None:
        spec = {"replicas": 1}
    if captured_at is None:
        captured_at = datetime.utcnow()
    spec_hash = hashlib.sha256(str(spec).encode()).hexdigest()
    return ResourceSnapshot(
        kind=kind,
        namespace=namespace,
        name=name,
        spec_hash=spec_hash,
        spec=spec,
        captured_at=captured_at,
        resource_version=resource_version,
    )


# ---------------------------------------------------------------------------
# record() and latest()
# ---------------------------------------------------------------------------


class TestRecord:
    def test_record_single_snapshot(self) -> None:
        ledger = ChangeLedger()
        s = _snap()
        ledger.record(s)
        assert ledger.latest("Deployment", "default", "my-app") is s

    def test_latest_returns_none_for_unknown_resource(self) -> None:
        ledger = ChangeLedger()
        assert ledger.latest("Deployment", "default", "missing") is None

    def test_latest_returns_most_recent(self) -> None:
        ledger = ChangeLedger(max_versions=5)
        s1 = _snap(spec={"replicas": 1}, resource_version="1")
        s2 = _snap(spec={"replicas": 2}, resource_version="2")
        ledger.record(s1)
        ledger.record(s2)
        assert ledger.latest("Deployment", "default", "my-app") is s2

    def test_ring_buffer_drops_oldest_at_max_versions(self) -> None:
        ledger = ChangeLedger(max_versions=3)
        snaps = [_snap(spec={"replicas": i}, resource_version=str(i)) for i in range(5)]
        for s in snaps:
            ledger.record(s)
        # Only the most recent 3 should be retained
        stats = ledger.stats()
        assert stats.snapshot_count == 3

    def test_record_different_resources_isolated(self) -> None:
        ledger = ChangeLedger()
        s_app = _snap(name="app")
        s_db = _snap(name="db")
        ledger.record(s_app)
        ledger.record(s_db)
        assert ledger.latest("Deployment", "default", "app") is s_app
        assert ledger.latest("Deployment", "default", "db") is s_db


# ---------------------------------------------------------------------------
# diff()
# ---------------------------------------------------------------------------


class TestDiff:
    def test_diff_returns_empty_with_single_snapshot(self) -> None:
        ledger = ChangeLedger()
        ledger.record(_snap(spec={"replicas": 1}))
        changes = ledger.diff("Deployment", "default", "my-app")
        assert changes == []

    def test_diff_returns_empty_for_unknown_resource(self) -> None:
        ledger = ChangeLedger()
        changes = ledger.diff("Deployment", "default", "ghost")
        assert changes == []

    def test_diff_detects_replica_change(self) -> None:
        ledger = ChangeLedger()
        ledger.record(_snap(spec={"replicas": 1}))
        ledger.record(_snap(spec={"replicas": 3}))
        changes = ledger.diff("Deployment", "default", "my-app")
        assert len(changes) == 1
        assert changes[0].field_path == "replicas"
        assert changes[0].old_value == "1"
        assert changes[0].new_value == "3"

    def test_diff_uses_since_window(self) -> None:
        ledger = ChangeLedger()
        old_time = datetime.utcnow() - timedelta(hours=3)
        recent_time = datetime.utcnow() - timedelta(minutes=10)
        # Old snapshot is outside the default 2h window
        ledger.record(_snap(spec={"replicas": 1}, resource_version="1", captured_at=old_time))
        ledger.record(_snap(spec={"replicas": 2}, resource_version="2", captured_at=recent_time))
        ledger.record(_snap(spec={"replicas": 3}, resource_version="3"))

        # With default 2h window only the last two snapshots are in range
        changes = ledger.diff("Deployment", "default", "my-app", since=timedelta(hours=2))
        paths = [c.field_path for c in changes]
        assert "replicas" in paths
        # Verify old snapshot not contributing: old value should be 2 (not 1)
        replica_change = next(c for c in changes if c.field_path == "replicas")
        assert replica_change.old_value == "2"
        assert replica_change.new_value == "3"

    def test_diff_with_custom_since(self) -> None:
        ledger = ChangeLedger()
        t0 = datetime.utcnow() - timedelta(minutes=5)
        ledger.record(_snap(spec={"replicas": 1}, resource_version="1", captured_at=t0))
        ledger.record(_snap(spec={"replicas": 2}, resource_version="2"))
        # 1-minute window excludes snapshot at t0
        changes = ledger.diff("Deployment", "default", "my-app", since=timedelta(minutes=1))
        assert changes == []

    def test_diff_concatenates_multiple_pairs(self) -> None:
        ledger = ChangeLedger(max_versions=5)
        t = datetime.utcnow()
        ledger.record(_snap(spec={"replicas": 1, "paused": False}, resource_version="1", captured_at=t))
        ledger.record(
            _snap(spec={"replicas": 2, "paused": False}, resource_version="2", captured_at=t + timedelta(seconds=1))
        )
        ledger.record(
            _snap(spec={"replicas": 2, "paused": True}, resource_version="3", captured_at=t + timedelta(seconds=2))
        )
        changes = ledger.diff("Deployment", "default", "my-app", since=timedelta(hours=1))
        paths = [c.field_path for c in changes]
        assert "replicas" in paths
        assert "paused" in paths


# ---------------------------------------------------------------------------
# stats()
# ---------------------------------------------------------------------------


class TestStats:
    def test_stats_initial_empty(self) -> None:
        ledger = ChangeLedger()
        stats = ledger.stats()
        assert stats.resource_count == 0
        assert stats.snapshot_count == 0
        assert stats.memory_bytes >= 0

    def test_stats_after_records(self) -> None:
        ledger = ChangeLedger()
        ledger.record(_snap(name="app1"))
        ledger.record(_snap(name="app1", spec={"replicas": 2}, resource_version="2"))
        ledger.record(_snap(name="app2"))
        stats = ledger.stats()
        assert stats.resource_count == 2
        assert stats.snapshot_count == 3

    def test_stats_returns_ledger_stats_type(self) -> None:
        ledger = ChangeLedger()
        stats = ledger.stats()
        assert isinstance(stats, LedgerStats)


# ---------------------------------------------------------------------------
# Retention / expiry
# ---------------------------------------------------------------------------


class TestRetention:
    def test_old_snapshots_expired_on_record(self) -> None:
        # Use a 1-hour retention ledger
        ledger = ChangeLedger(max_versions=10, retention_hours=1)
        ancient = datetime.utcnow() - timedelta(hours=8)
        ledger.record(_snap(spec={"replicas": 0}, resource_version="0", captured_at=ancient))
        # Adding a fresh snapshot should trigger expiry of the ancient one
        ledger.record(_snap(spec={"replicas": 1}, resource_version="1"))
        stats = ledger.stats()
        # Only the fresh snapshot should remain
        assert stats.snapshot_count == 1


# ---------------------------------------------------------------------------
# Memory estimation
# ---------------------------------------------------------------------------


class TestMemoryEstimation:
    def test_estimate_snapshot_size_positive(self) -> None:
        s = _snap()
        size = _estimate_snapshot_size(s)
        assert size > 0

    def test_stats_memory_increases_with_more_snapshots(self) -> None:
        ledger = ChangeLedger(max_versions=100)
        baseline = ledger.stats().memory_bytes
        for i in range(20):
            ledger.record(_snap(name=f"app{i}"))
        assert ledger.stats().memory_bytes > baseline


# ---------------------------------------------------------------------------
# LedgerStats dataclass
# ---------------------------------------------------------------------------


class TestLedgerStats:
    def test_fields_accessible(self) -> None:
        stats = LedgerStats(memory_bytes=1024, resource_count=5, snapshot_count=25)
        assert stats.memory_bytes == 1024
        assert stats.resource_count == 5
        assert stats.snapshot_count == 25

    def test_frozen(self) -> None:
        stats = LedgerStats(memory_bytes=100, resource_count=1, snapshot_count=1)
        # LedgerStats is a frozen dataclass
        with pytest.raises((AttributeError, TypeError)):
            stats.memory_bytes = 999  # type: ignore[misc]
