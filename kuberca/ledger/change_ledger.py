"""In-memory Change Ledger with tiered memory enforcement.

Maintains a ring buffer of :class:`~kuberca.models.resources.ResourceSnapshot`
objects per ``(kind, namespace, name)`` resource key.  Callers can query the
diff between any two points in time using :meth:`ChangeLedger.diff`.

Memory enforcement tiers
------------------------
Soft  (150 MB) -- adaptive trim: reduce each buffer to ``max(1, len//2)``.
Hard  (200 MB) -- stop accepting non-incident snapshots (backpressure).
Fail-safe (300 MB) -- emergency eviction: drop snapshots older than 1 hour
                      from the largest buffers first until below fail-safe.

All thresholds are conservative estimates based on ``sys.getsizeof`` sampling.
The real heap footprint is larger; that is intentional — we measure cheaply
and err on the side of eviction.
"""

from __future__ import annotations

import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Final

from kuberca.ledger.diff import compute_diff
from kuberca.models.resources import FieldChange, ResourceSnapshot
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import (
    ledger_memory_bytes,
    ledger_memory_pressure_total,
    ledger_snapshots,
    ledger_trim_events_total,
)

_log = get_logger("change_ledger")

# ---------------------------------------------------------------------------
# Memory tier constants (bytes)
# ---------------------------------------------------------------------------

_SOFT_LIMIT_BYTES: Final[int] = 150 * 1024 * 1024  # 150 MB
_HARD_LIMIT_BYTES: Final[int] = 200 * 1024 * 1024  # 200 MB
_FAILSAFE_LIMIT_BYTES: Final[int] = 300 * 1024 * 1024  # 300 MB

_FAILSAFE_AGE_CUTOFF: Final[timedelta] = timedelta(hours=1)

# Default configuration
_DEFAULT_MAX_VERSIONS: Final[int] = 5
_DEFAULT_RETENTION_HOURS: Final[int] = 6

# Type alias for the resource key
_ResourceKey = tuple[str, str, str]  # (kind, namespace, name)


# ---------------------------------------------------------------------------
# Public data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LedgerStats:
    """Snapshot of Change Ledger runtime metrics."""

    memory_bytes: int
    resource_count: int
    snapshot_count: int


# ---------------------------------------------------------------------------
# Internal buffer type alias
# ---------------------------------------------------------------------------

_SnapshotBuffer = deque[ResourceSnapshot]


# ---------------------------------------------------------------------------
# Change Ledger
# ---------------------------------------------------------------------------


class ChangeLedger:
    """In-memory ring buffer ledger for Kubernetes resource spec snapshots.

    Thread-safety: this class is NOT thread-safe.  All callers must be on
    the same asyncio event loop or protect access with an external lock.

    Args:
        max_versions:    Maximum snapshots retained per resource (ring depth).
        retention_hours: Maximum age of retained snapshots in hours.
    """

    def __init__(
        self,
        max_versions: int = _DEFAULT_MAX_VERSIONS,
        retention_hours: int = _DEFAULT_RETENTION_HOURS,
    ) -> None:
        self._max_versions: int = max(1, max_versions)
        self._retention: timedelta = timedelta(hours=retention_hours)
        # Primary storage: resource key → bounded deque of snapshots
        self._buffers: dict[_ResourceKey, _SnapshotBuffer] = {}
        # Cached total memory estimate (invalidated on every mutation)
        self._cached_memory_bytes: int | None = None

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    def record(self, snapshot: ResourceSnapshot) -> None:
        """Store *snapshot* in the ring buffer for its resource key.

        The snapshot is silently dropped if the ledger is at hard-limit
        capacity (200 MB) to prevent memory runaway.  An emergency eviction
        pass is triggered first; if memory remains above the hard limit after
        eviction the snapshot is discarded and the drop is logged.

        Args:
            snapshot: The :class:`ResourceSnapshot` to record.
        """
        self._invalidate_memory_cache()

        # Enforce tiered memory limits before accepting new data
        current_bytes = self._estimate_memory_bytes()
        if current_bytes >= _FAILSAFE_LIMIT_BYTES:
            self._failsafe_evict()
            current_bytes = self._estimate_memory_bytes()

        if current_bytes >= _HARD_LIMIT_BYTES:
            ledger_memory_pressure_total.inc()
            _log.warning(
                "ledger_hard_limit_reached",
                memory_bytes=current_bytes,
                kind=snapshot.kind,
                namespace=snapshot.namespace,
                name=snapshot.name,
            )
            return

        if current_bytes >= _SOFT_LIMIT_BYTES:
            self._soft_trim()

        key = self._key(snapshot)
        if key not in self._buffers:
            self._buffers[key] = deque(maxlen=self._max_versions)

        buf = self._buffers[key]

        # Expire old snapshots before appending
        self._expire_buffer(buf)

        buf.append(snapshot)
        self._invalidate_memory_cache()

        # Update Prometheus metrics
        ledger_snapshots.labels(kind=snapshot.kind).inc()
        ledger_memory_bytes.set(self._estimate_memory_bytes())

        _log.debug(
            "ledger_snapshot_recorded",
            kind=snapshot.kind,
            namespace=snapshot.namespace,
            name=snapshot.name,
            resource_version=snapshot.resource_version,
            buffer_depth=len(buf),
        )

    def diff(
        self,
        kind: str,
        namespace: str,
        name: str,
        since: timedelta | None = None,
        since_hours: float | None = None,
    ) -> list[FieldChange]:
        """Return the cumulative diff for a resource since *since*.

        Diffs consecutive snapshot pairs in chronological order and
        concatenates all :class:`FieldChange` objects.  Duplicates are
        not deduplicated — callers see the complete change history.

        Args:
            kind:        Kubernetes resource kind (case-sensitive).
            namespace:   Namespace, or ``""`` for cluster-scoped resources.
            name:        Resource name.
            since:       Only include snapshots captured within this timedelta
                         relative to ``now()``.  Defaults to 2 hours.
            since_hours: Convenience float alias for *since* expressed in hours.
                         When both are provided, *since* takes precedence.

        Returns:
            All field changes in the requested window, oldest first.
            Returns an empty list when no snapshots exist or there is
            only one snapshot (no pair to diff).
        """
        if since is None:
            since = timedelta(hours=since_hours) if since_hours is not None else timedelta(hours=2)

        key = (kind, namespace, name)
        buf = self._buffers.get(key)
        if buf is None or len(buf) < 2:
            return []

        cutoff = datetime.utcnow() - since
        # Collect snapshots within the window, preserving chronological order
        window = [snap for snap in buf if snap.captured_at >= cutoff]

        if len(window) < 2:
            return []

        all_changes: list[FieldChange] = []
        for i in range(1, len(window)):
            pair_changes = compute_diff(window[i - 1].spec, window[i].spec)
            all_changes.extend(pair_changes)

        return all_changes

    def latest(self, kind: str, namespace: str, name: str) -> ResourceSnapshot | None:
        """Return the most recent snapshot for the given resource, or ``None``.

        Args:
            kind:      Kubernetes resource kind.
            namespace: Namespace, or ``""`` for cluster-scoped resources.
            name:      Resource name.

        Returns:
            The most recently recorded :class:`ResourceSnapshot`, or ``None``
            if no snapshots exist for this resource.
        """
        key = (kind, namespace, name)
        buf = self._buffers.get(key)
        if not buf:
            return None
        return buf[-1]

    def stats(self) -> LedgerStats:
        """Return a point-in-time summary of ledger memory and counts.

        Returns:
            A :class:`LedgerStats` instance with current memory usage,
            the number of tracked resources, and the total snapshot count.
        """
        memory = self._estimate_memory_bytes()
        resource_count = len(self._buffers)
        snapshot_count = sum(len(buf) for buf in self._buffers.values())
        return LedgerStats(
            memory_bytes=memory,
            resource_count=resource_count,
            snapshot_count=snapshot_count,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _key(snapshot: ResourceSnapshot) -> _ResourceKey:
        """Derive the storage key from a snapshot."""
        return (snapshot.kind, snapshot.namespace, snapshot.name)

    def _invalidate_memory_cache(self) -> None:
        """Mark the cached memory estimate as stale."""
        self._cached_memory_bytes = None

    def _estimate_memory_bytes(self) -> int:
        """Estimate total heap usage of the ledger using ``sys.getsizeof``.

        Uses a shallow walk over the primary dict and each buffer to avoid
        O(n) deep recursion on every call — this is called before every
        ``record()`` and must be cheap.
        """
        if self._cached_memory_bytes is not None:
            return self._cached_memory_bytes

        total = sys.getsizeof(self._buffers)
        for key, buf in self._buffers.items():
            total += sys.getsizeof(key)
            total += sys.getsizeof(buf)
            for snap in buf:
                total += _estimate_snapshot_size(snap)

        self._cached_memory_bytes = total
        return total

    def _expire_buffer(self, buf: _SnapshotBuffer) -> None:
        """Remove snapshots from the left of *buf* that exceed the retention window.

        Modifies *buf* in-place.  Uses the instance retention configured at
        construction time (defaults to 6 hours).
        """
        cutoff = datetime.utcnow() - self._retention
        while buf and buf[0].captured_at < cutoff:
            buf.popleft()

    def _soft_trim(self) -> None:
        """Adaptive trim: halve every buffer when at soft-limit.

        Retains at least one snapshot per resource so that ``latest()``
        continues to work.  This is a best-effort pass — if the halved
        buffers still exceed the soft limit the hard-limit guard will fire
        on the next ``record()`` call.
        """
        ledger_trim_events_total.inc()
        ledger_memory_pressure_total.inc()
        _log.warning(
            "ledger_soft_limit_trim",
            memory_bytes=self._estimate_memory_bytes(),
        )
        for buf in self._buffers.values():
            target = max(1, len(buf) // 2)
            while len(buf) > target:
                buf.popleft()
        self._invalidate_memory_cache()

    def _failsafe_evict(self) -> None:
        """Emergency eviction: drop snapshots older than 1 hour from the largest buffers.

        Iterates buffers from largest to smallest.  For each buffer, removes
        all entries older than ``_FAILSAFE_AGE_CUTOFF``.  Stops as soon as
        the estimated memory drops below ``_FAILSAFE_LIMIT_BYTES``.
        """
        ledger_trim_events_total.inc()
        ledger_memory_pressure_total.inc()
        cutoff = datetime.utcnow() - _FAILSAFE_AGE_CUTOFF
        _log.error(
            "ledger_failsafe_eviction",
            memory_bytes=self._estimate_memory_bytes(),
            buffer_count=len(self._buffers),
        )

        # Sort descending by buffer size to evict the most expensive first
        sorted_keys = sorted(
            self._buffers.keys(),
            key=lambda k: len(self._buffers[k]),
            reverse=True,
        )

        for key in sorted_keys:
            buf = self._buffers[key]
            before = len(buf)
            while buf and buf[0].captured_at < cutoff:
                buf.popleft()
            evicted = before - len(buf)
            if evicted:
                self._invalidate_memory_cache()
                _log.debug(
                    "ledger_failsafe_evicted_buffer",
                    kind=key[0],
                    namespace=key[1],
                    name=key[2],
                    evicted=evicted,
                )
            if self._estimate_memory_bytes() < _FAILSAFE_LIMIT_BYTES:
                break

        # Remove empty buffers to free the dict entries
        empty_keys = [k for k, buf in self._buffers.items() if not buf]
        for k in empty_keys:
            del self._buffers[k]

        self._invalidate_memory_cache()
        ledger_memory_bytes.set(self._estimate_memory_bytes())


# ---------------------------------------------------------------------------
# Module-level size estimation helper
# ---------------------------------------------------------------------------


def _estimate_snapshot_size(snap: ResourceSnapshot) -> int:
    """Cheaply estimate the byte size of a single :class:`ResourceSnapshot`.

    Uses ``sys.getsizeof`` on the object itself plus the spec dict.  Does
    NOT recurse into nested dicts — this keeps the call O(1) per snapshot
    while still being directionally correct for memory pressure decisions.
    """
    size = sys.getsizeof(snap)
    size += sys.getsizeof(snap.spec)
    size += sys.getsizeof(snap.spec_hash)
    size += sys.getsizeof(snap.kind)
    size += sys.getsizeof(snap.namespace)
    size += sys.getsizeof(snap.name)
    size += sys.getsizeof(snap.resource_version)
    return size
