"""SQLite persistence layer for the Change Ledger.

Feature-flagged: only active when ``ledger.persistence.enabled=true``
(i.e. ``ChangeLedgerConfig.persistence_enabled=True``).

Write model:
- WAL mode + single writer (serialised via asyncio).
- Snapshots are batched and flushed every 500 ms via a background task.
- If SQLite write latency exceeds 50 ms sustained over a flush cycle,
  the store falls back to no-op mode (in-memory only) and emits the
  ``kuberca_sqlite_backpressure_total`` counter.

Retention:
- Snapshots older than 6 hours are pruned on every cleanup pass.
- At most ``max_versions`` snapshots are retained per (kind, namespace, name).
- Cleanup runs every 5 minutes.
- ``VACUUM`` is executed once on startup to reclaim disk space.

Schema::

    CREATE TABLE snapshots (
        id          INTEGER PRIMARY KEY,
        kind        TEXT,
        namespace   TEXT,
        name        TEXT,
        spec_hash   TEXT,
        spec_json   TEXT,
        captured_at DATETIME
    );
    CREATE INDEX idx_snapshots_resource ON snapshots (kind, namespace, name);
    CREATE INDEX idx_snapshots_time     ON snapshots (captured_at);
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from datetime import UTC, datetime, timedelta
from typing import Final

import aiosqlite

from kuberca.models.resources import ResourceSnapshot
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import sqlite_backpressure_total

_logger = get_logger("sqlite_store")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SCHEMA_DDL: Final[str] = """
CREATE TABLE IF NOT EXISTS snapshots (
    id          INTEGER PRIMARY KEY,
    kind        TEXT    NOT NULL,
    namespace   TEXT    NOT NULL,
    name        TEXT    NOT NULL,
    spec_hash   TEXT    NOT NULL,
    spec_json   TEXT    NOT NULL,
    captured_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snapshots_resource
    ON snapshots (kind, namespace, name);

CREATE INDEX IF NOT EXISTS idx_snapshots_time
    ON snapshots (captured_at);
"""

_FLUSH_INTERVAL_SECONDS: Final[float] = 0.5  # 500 ms batch flush
_CLEANUP_INTERVAL_SECONDS: Final[float] = 300.0  # 5 minutes
_BACKPRESSURE_THRESHOLD_MS: Final[float] = 50.0  # 50 ms write latency

_DEFAULT_MAX_VERSIONS: Final[int] = 5
_DEFAULT_RETENTION_HOURS: Final[float] = 6.0


# ---------------------------------------------------------------------------
# SQLiteStore
# ---------------------------------------------------------------------------


class SQLiteStore:
    """Async SQLite persistence layer for Change Ledger snapshots.

    Designed to run alongside an in-memory ``ChangeLedger``.  Persistence
    is best-effort: if the write path becomes a bottleneck the store
    degrades to a no-op so analysis is never blocked.

    Args:
        db_path:        Path to the SQLite database file.
        max_versions:   Maximum snapshots to retain per resource.
        retention_hours: Maximum snapshot age in hours.
    """

    def __init__(
        self,
        db_path: str,
        max_versions: int = _DEFAULT_MAX_VERSIONS,
        retention_hours: float = _DEFAULT_RETENTION_HOURS,
    ) -> None:
        self._db_path = db_path
        self._max_versions = max(1, max_versions)
        self._retention = timedelta(hours=retention_hours)

        self._db: aiosqlite.Connection | None = None
        self._pending: list[ResourceSnapshot] = []
        self._flush_task: asyncio.Task[None] | None = None
        self._cleanup_task: asyncio.Task[None] | None = None
        self._degraded: bool = False  # True when backpressure threshold exceeded

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """Open the database, apply schema, VACUUM, and start background tasks."""
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(_SCHEMA_DDL)
        await self._db.commit()

        # VACUUM on startup to reclaim fragmented space
        await self._db.execute("VACUUM")
        await self._db.commit()

        _logger.info("sqlite_store_opened", db_path=self._db_path)

        self._flush_task = asyncio.create_task(self._flush_loop(), name="sqlite_flush")
        self._cleanup_task = asyncio.create_task(self._cleanup_loop(), name="sqlite_cleanup")

    async def close(self) -> None:
        """Flush pending writes, cancel background tasks, and close the database."""
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._flush_task

        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._cleanup_task

        # Final flush of any remaining pending writes
        if self._pending and self._db and not self._degraded:
            await self._do_flush()

        if self._db:
            await self._db.close()
            self._db = None

        _logger.info("sqlite_store_closed", db_path=self._db_path)

    # ------------------------------------------------------------------
    # Public write interface
    # ------------------------------------------------------------------

    async def store(self, snapshot: ResourceSnapshot) -> None:
        """Enqueue *snapshot* for the next batch flush.

        If the store is in degraded mode (backpressure), the snapshot is
        silently dropped — the in-memory ledger remains authoritative.
        """
        if self._degraded:
            return
        self._pending.append(snapshot)

    # ------------------------------------------------------------------
    # Public read interface
    # ------------------------------------------------------------------

    async def load(
        self,
        kind: str,
        namespace: str,
        name: str,
        since: timedelta | None = None,
    ) -> list[ResourceSnapshot]:
        """Return stored snapshots for a resource within the retention window.

        Args:
            kind, namespace, name: Resource identity.
            since: Time window; defaults to the store's configured retention.

        Returns:
            List of :class:`ResourceSnapshot` objects, oldest first.
        """
        if self._db is None:
            return []

        window = since if since is not None else self._retention
        cutoff = (datetime.now(tz=UTC) - window).isoformat()

        rows = await self._db.execute_fetchall(
            """
            SELECT kind, namespace, name, spec_hash, spec_json, captured_at
            FROM snapshots
            WHERE kind = ? AND namespace = ? AND name = ? AND captured_at >= ?
            ORDER BY captured_at ASC
            """,
            (kind, namespace, name, cutoff),
        )

        results: list[ResourceSnapshot] = []
        for row in rows:
            try:
                spec = json.loads(row[4])
                captured_at = datetime.fromisoformat(row[5])
                if captured_at.tzinfo is None:
                    captured_at = captured_at.replace(tzinfo=UTC)
                results.append(
                    ResourceSnapshot(
                        kind=row[0],
                        namespace=row[1],
                        name=row[2],
                        spec_hash=row[3],
                        spec=spec,
                        captured_at=captured_at,
                        resource_version="",  # Not stored in SQLite
                    )
                )
            except (json.JSONDecodeError, ValueError) as exc:
                _logger.error(
                    "sqlite_load_parse_error",
                    error=str(exc),
                    kind=kind,
                    namespace=namespace,
                    name=name,
                )

        return results

    # ------------------------------------------------------------------
    # Explicit cleanup
    # ------------------------------------------------------------------

    async def cleanup(self) -> None:
        """Delete expired snapshots and enforce per-resource version caps.

        Called by the background cleanup loop and exposed for testing.
        """
        if self._db is None:
            return

        await self._delete_expired()
        await self._enforce_version_caps()
        await self._db.commit()

    # ------------------------------------------------------------------
    # Internal: flush loop
    # ------------------------------------------------------------------

    async def _flush_loop(self) -> None:
        """Background task: flush pending snapshots every 500 ms."""
        while True:
            await asyncio.sleep(_FLUSH_INTERVAL_SECONDS)
            if self._pending and not self._degraded:
                await self._do_flush()

    async def _do_flush(self) -> None:
        """Write all pending snapshots to SQLite in a single transaction."""
        if not self._pending or self._db is None:
            return

        batch = self._pending[:]
        self._pending.clear()

        t_start = time.monotonic()
        try:
            await self._db.executemany(
                """
                INSERT INTO snapshots (kind, namespace, name, spec_hash, spec_json, captured_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        snap.kind,
                        snap.namespace,
                        snap.name,
                        snap.spec_hash,
                        json.dumps(snap.spec, default=str),
                        snap.captured_at.isoformat(),
                    )
                    for snap in batch
                ],
            )
            await self._db.commit()
        except Exception as exc:
            _logger.error("sqlite_flush_error", error=str(exc), batch_size=len(batch))
            return

        elapsed_ms = (time.monotonic() - t_start) * 1000.0

        if elapsed_ms > _BACKPRESSURE_THRESHOLD_MS:
            sqlite_backpressure_total.inc()
            _logger.warning(
                "sqlite_backpressure",
                write_latency_ms=elapsed_ms,
                threshold_ms=_BACKPRESSURE_THRESHOLD_MS,
            )
            # Switch to degraded / no-op mode
            self._degraded = True

    # ------------------------------------------------------------------
    # Internal: cleanup loop
    # ------------------------------------------------------------------

    async def _cleanup_loop(self) -> None:
        """Background task: prune expired rows and enforce version caps every 5 min."""
        while True:
            await asyncio.sleep(_CLEANUP_INTERVAL_SECONDS)
            try:
                await self.cleanup()
            except Exception as exc:
                _logger.error("sqlite_cleanup_error", error=str(exc))

    async def _delete_expired(self) -> None:
        """Delete all rows older than the retention window."""
        if self._db is None:
            return
        cutoff = (datetime.now(tz=UTC) - self._retention).isoformat()
        await self._db.execute(
            "DELETE FROM snapshots WHERE captured_at < ?",
            (cutoff,),
        )

    async def _enforce_version_caps(self) -> None:
        """Keep only the most recent *max_versions* rows per resource."""
        if self._db is None:
            return

        # Identify resources with more than max_versions snapshots
        rows = await self._db.execute_fetchall(
            """
            SELECT kind, namespace, name, COUNT(*) as cnt
            FROM snapshots
            GROUP BY kind, namespace, name
            HAVING cnt > ?
            """,
            (self._max_versions,),
        )

        for kind, namespace, name, count in rows:
            excess = count - self._max_versions
            await self._db.execute(
                """
                DELETE FROM snapshots
                WHERE id IN (
                    SELECT id FROM snapshots
                    WHERE kind = ? AND namespace = ? AND name = ?
                    ORDER BY captured_at ASC
                    LIMIT ?
                )
                """,
                (kind, namespace, name, excess),
            )
