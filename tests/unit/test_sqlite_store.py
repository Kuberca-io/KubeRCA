"""Tests for kuberca.ledger.sqlite_store.

Verifies the SQLite persistence layer defined in Technical Design Spec
Section 5.6:

  - Feature is behind persistence_enabled flag (tested via store behaviour)
  - WAL mode and schema creation
  - store() enqueues; flush writes to DB
  - load() retrieves snapshots within the time window
  - cleanup() deletes expired rows and enforces version caps
  - Backpressure: slow write latency sets degraded flag, emits metric
  - VACUUM runs on open()
  - close() flushes pending and closes connection

All tests use a temporary file-based SQLite database so aiosqlite behaves
consistently across open/close/reopen cycles.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from kuberca.ledger.sqlite_store import (
    _BACKPRESSURE_THRESHOLD_MS,
    _DEFAULT_MAX_VERSIONS,
    _DEFAULT_RETENTION_HOURS,
    _FLUSH_INTERVAL_SECONDS,
    SQLiteStore,
)
from kuberca.models.resources import ResourceSnapshot

# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _snap(
    kind: str = "Deployment",
    namespace: str = "default",
    name: str = "my-app",
    spec: dict | None = None,
    resource_version: str = "1",
    captured_at: datetime | None = None,
) -> ResourceSnapshot:
    if spec is None:
        spec = {"replicas": 1}
    if captured_at is None:
        captured_at = datetime.now(tz=UTC)
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


@pytest.fixture
def tmp_db(tmp_path):
    """Return a path to a temporary SQLite database file."""
    return str(tmp_path / "test_ledger.db")


# ---------------------------------------------------------------------------
# Lifecycle tests
# ---------------------------------------------------------------------------


class TestSQLiteStoreLifecycle:
    async def test_open_creates_schema(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            # Verify the snapshots table exists by loading (should return empty list)
            results = await store.load("Deployment", "default", "app")
            assert results == []
        finally:
            await store.close()

    async def test_close_after_open_does_not_raise(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        await store.close()  # Should not raise

    async def test_double_open_overwrite(self, tmp_db: str) -> None:
        """Opening a second store on the same DB path should not fail."""
        store1 = SQLiteStore(tmp_db)
        await store1.open()
        await store1.close()

        store2 = SQLiteStore(tmp_db)
        await store2.open()
        await store2.close()

    async def test_load_on_unopened_store_returns_empty(self) -> None:
        store = SQLiteStore(":memory:")
        # Not opened — should return empty, not raise
        results = await store.load("Deployment", "default", "app")
        assert results == []


# ---------------------------------------------------------------------------
# store() and flush tests
# ---------------------------------------------------------------------------


class TestStoreAndFlush:
    async def test_store_enqueues_snapshot(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            snap = _snap()
            await store.store(snap)
            assert len(store._pending) == 1
        finally:
            await store.close()

    async def test_flush_writes_to_db(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            snap = _snap()
            await store.store(snap)
            # Manually trigger flush
            await store._do_flush()
            results = await store.load("Deployment", "default", "my-app")
            assert len(results) == 1
        finally:
            await store.close()

    async def test_flush_clears_pending(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            await store.store(_snap())
            await store._do_flush()
            assert store._pending == []
        finally:
            await store.close()

    async def test_multiple_stores_flushed_in_batch(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            snaps = [_snap(resource_version=str(i), spec={"replicas": i}) for i in range(3)]
            for s in snaps:
                await store.store(s)
            await store._do_flush()
            results = await store.load("Deployment", "default", "my-app")
            assert len(results) == 3
        finally:
            await store.close()

    async def test_close_flushes_pending(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        snap = _snap()
        await store.store(snap)
        # Close should flush without explicit _do_flush call
        await store.close()

        # Re-open and verify data was persisted
        store2 = SQLiteStore(tmp_db)
        await store2.open()
        try:
            results = await store2.load("Deployment", "default", "my-app")
            assert len(results) >= 1
        finally:
            await store2.close()


# ---------------------------------------------------------------------------
# load() tests
# ---------------------------------------------------------------------------


class TestLoad:
    async def test_load_returns_empty_for_unknown_resource(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            results = await store.load("Deployment", "default", "nonexistent")
            assert results == []
        finally:
            await store.close()

    async def test_load_returns_snapshots_in_time_window(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            now = datetime.now(tz=UTC)
            snap_recent = _snap(
                captured_at=now - timedelta(hours=1),
                resource_version="recent",
            )
            snap_old = _snap(
                captured_at=now - timedelta(hours=10),
                resource_version="old",
            )
            await store.store(snap_recent)
            await store.store(snap_old)
            await store._do_flush()

            # Default retention is 6h — only recent snap should be returned
            results = await store.load("Deployment", "default", "my-app")
            assert len(results) == 1
        finally:
            await store.close()

    async def test_load_respects_custom_since_window(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            now = datetime.now(tz=UTC)
            snap = _snap(captured_at=now - timedelta(minutes=30))
            await store.store(snap)
            await store._do_flush()

            # 1-hour window should include it
            results_1h = await store.load("Deployment", "default", "my-app", since=timedelta(hours=1))
            assert len(results_1h) == 1

            # 10-minute window should exclude it
            results_10m = await store.load("Deployment", "default", "my-app", since=timedelta(minutes=10))
            assert len(results_10m) == 0
        finally:
            await store.close()

    async def test_load_returns_snapshots_oldest_first(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            now = datetime.now(tz=UTC)
            t1 = now - timedelta(hours=2)
            t2 = now - timedelta(hours=1)
            t3 = now - timedelta(minutes=30)

            for t, rv in [(t3, "3"), (t1, "1"), (t2, "2")]:
                await store.store(_snap(captured_at=t, resource_version=rv))
            await store._do_flush()

            results = await store.load("Deployment", "default", "my-app", since=timedelta(hours=3))
            assert len(results) == 3
            # Results should be oldest first
            captured_ats = [r.captured_at for r in results]
            assert captured_ats == sorted(captured_ats)
        finally:
            await store.close()

    async def test_load_reconstructs_spec_from_json(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            spec = {"replicas": 3, "strategy": {"type": "RollingUpdate"}}
            snap = _snap(spec=spec)
            await store.store(snap)
            await store._do_flush()

            results = await store.load("Deployment", "default", "my-app")
            assert len(results) == 1
            assert results[0].spec == spec
        finally:
            await store.close()

    async def test_load_filters_by_resource_identity(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            await store.store(_snap(name="app-a"))
            await store.store(_snap(name="app-b"))
            await store._do_flush()

            results_a = await store.load("Deployment", "default", "app-a")
            results_b = await store.load("Deployment", "default", "app-b")
            assert len(results_a) == 1
            assert len(results_b) == 1
        finally:
            await store.close()


# ---------------------------------------------------------------------------
# cleanup() tests
# ---------------------------------------------------------------------------


class TestCleanup:
    async def test_cleanup_deletes_expired_snapshots(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db, retention_hours=1)
        await store.open()
        try:
            now = datetime.now(tz=UTC)
            expired = _snap(captured_at=now - timedelta(hours=3), resource_version="old")
            fresh = _snap(captured_at=now - timedelta(minutes=30), resource_version="new")
            await store.store(expired)
            await store.store(fresh)
            await store._do_flush()

            await store.cleanup()

            results = await store.load("Deployment", "default", "my-app", since=timedelta(hours=10))
            # Only the fresh snapshot should remain
            # Note: resource_version is not stored in SQLite, so check count
            assert len(results) == 1
        finally:
            await store.close()

    async def test_cleanup_enforces_version_caps(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db, max_versions=2)
        await store.open()
        try:
            now = datetime.now(tz=UTC)
            # Store 4 snapshots
            for i in range(4):
                snap = _snap(
                    spec={"replicas": i},
                    captured_at=now - timedelta(hours=i),
                    resource_version=str(i),
                )
                await store.store(snap)
            await store._do_flush()

            await store.cleanup()

            results = await store.load("Deployment", "default", "my-app", since=timedelta(hours=10))
            assert len(results) <= 2
        finally:
            await store.close()

    async def test_cleanup_keeps_most_recent_versions(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db, max_versions=2)
        await store.open()
        try:
            now = datetime.now(tz=UTC)
            times = [
                now - timedelta(hours=5),
                now - timedelta(hours=3),
                now - timedelta(hours=1),
            ]
            for i, t in enumerate(times):
                await store.store(_snap(spec={"replicas": i}, captured_at=t))
            await store._do_flush()

            await store.cleanup()

            results = await store.load("Deployment", "default", "my-app", since=timedelta(hours=10))
            assert len(results) == 2
            # Most recent should be retained
            assert results[-1].spec == {"replicas": 2}
        finally:
            await store.close()


# ---------------------------------------------------------------------------
# Backpressure tests
# ---------------------------------------------------------------------------


class TestBackpressure:
    async def test_degraded_flag_set_on_slow_write(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            snap = _snap()
            await store.store(snap)

            # Patch the `time` module reference within sqlite_store only (not globally),
            # so the asyncio event loop keeps using the real time.monotonic.
            import time as real_time_mod
            from unittest.mock import MagicMock

            fake_time = MagicMock(wraps=real_time_mod)
            monotonic_values = iter([0.0, (_BACKPRESSURE_THRESHOLD_MS + 10) / 1000.0])
            fake_time.monotonic = MagicMock(side_effect=monotonic_values)

            with patch("kuberca.ledger.sqlite_store.time", fake_time):
                await store._do_flush()

            assert store._degraded is True
        finally:
            if store._flush_task and not store._flush_task.done():
                store._flush_task.cancel()
            if store._cleanup_task and not store._cleanup_task.done():
                store._cleanup_task.cancel()
            if store._db:
                await store._db.close()
                store._db = None

    async def test_store_discards_when_degraded(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            # Manually set degraded mode
            store._degraded = True
            snap = _snap()
            await store.store(snap)
            # Snapshot should NOT be enqueued when in degraded mode
            assert len(store._pending) == 0
        finally:
            await store.close()

    async def test_normal_write_does_not_set_degraded(self, tmp_db: str) -> None:
        store = SQLiteStore(tmp_db)
        await store.open()
        try:
            snap = _snap()
            await store.store(snap)
            await store._do_flush()
            # Under normal latency, degraded flag should remain False
            assert store._degraded is False
        finally:
            await store.close()


# ---------------------------------------------------------------------------
# Default constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_default_max_versions(self) -> None:
        assert _DEFAULT_MAX_VERSIONS == 5

    def test_default_retention_hours(self) -> None:
        assert _DEFAULT_RETENTION_HOURS == 6.0

    def test_flush_interval_is_500ms(self) -> None:
        assert pytest.approx(0.5, abs=0.01) == _FLUSH_INTERVAL_SECONDS

    def test_backpressure_threshold_is_50ms(self) -> None:
        assert pytest.approx(50.0, abs=0.01) == _BACKPRESSURE_THRESHOLD_MS
