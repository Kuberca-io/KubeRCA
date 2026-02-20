"""Core event watcher for Kubernetes v1.Event resources.

Extends BaseWatcher to watch all namespaced Events cluster-wide,
convert them to EventRecord, and maintain a bounded in-memory buffer
for downstream query.

Buffer contract:
    - Time window: events seen within the last 2 hours are retained.
    - Capacity cap: at most 500 events (oldest dropped when full).
"""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime, timedelta
from typing import Any

from kuberca.collector.watcher import BaseWatcher
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import events_total

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BUFFER_MAX_EVENTS: int = 500
_BUFFER_WINDOW_H: int = 2
_BUFFER_WINDOW: timedelta = timedelta(hours=_BUFFER_WINDOW_H)

# Kubernetes event type to our Severity mapping
_KUBE_TYPE_TO_SEVERITY: dict[str, Severity] = {
    "Normal": Severity.INFO,
    "Warning": Severity.WARNING,
}

# Reason strings that we treat as ERROR / CRITICAL
_ERROR_REASONS: frozenset[str] = frozenset(
    {
        "Failed",
        "FailedCreate",
        "FailedMount",
        "FailedScheduling",
        "FailedKillPod",
        "BackOff",
        "CrashLoopBackOff",
        "OOMKilling",
        "Unhealthy",
    }
)
_CRITICAL_REASONS: frozenset[str] = frozenset({"OOMKilling", "NodeNotReady", "Evicted"})

# Type alias for the optional callback
EventCallback = Callable[[EventRecord], Coroutine[Any, Any, None]]


class EventWatcher(BaseWatcher):
    """Watches Kubernetes v1.Events cluster-wide and buffers them.

    Usage::

        v1 = kubernetes_asyncio.client.CoreV1Api()
        watcher = EventWatcher(v1, cluster_id="prod")
        watcher.add_callback(my_handler)
        await watcher.start()
        ...
        records = watcher.get_events("Pod", "default", "my-pod")
    """

    def __init__(self, api: Any, cluster_id: str = "") -> None:
        """Initialise the event watcher.

        Args:
            api: A ``CoreV1Api`` instance.
            cluster_id: Cluster identifier forwarded to ``EventRecord.cluster_id``.
        """
        super().__init__(api, cluster_id=cluster_id, name="event")
        self._log = get_logger("watcher.event")
        self._buffer: deque[EventRecord] = deque(maxlen=_BUFFER_MAX_EVENTS)
        self._callbacks: list[EventCallback] = []

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def add_callback(self, cb: EventCallback) -> None:
        """Register an async callback invoked for every new EventRecord.

        Args:
            cb: ``async def cb(record: EventRecord) -> None``
        """
        self._callbacks.append(cb)

    def get_events(
        self,
        resource_kind: str,
        namespace: str,
        name: str,
        since: datetime | None = None,
    ) -> list[EventRecord]:
        """Return buffered events matching the given resource.

        Applies the 2-hour window filter, the 500-event cap (already enforced
        by the deque), and an optional ``since`` lower-bound.

        Args:
            resource_kind: e.g. ``"Pod"``.
            namespace: Resource namespace; empty string matches all.
            name: Resource name.
            since: Optional datetime lower-bound (UTC). Events older than
                   this are excluded even if within the 2 h window.

        Returns:
            Matching EventRecord list, oldest first.
        """
        cutoff = datetime.now(tz=UTC) - _BUFFER_WINDOW
        results: list[EventRecord] = []

        for record in self._buffer:
            if record.last_seen < cutoff:
                continue
            if since is not None and record.last_seen < since:
                continue
            if record.resource_kind != resource_kind:
                continue
            if namespace and record.namespace != namespace:
                continue
            if record.resource_name != name:
                continue
            results.append(record)

        return results

    # ------------------------------------------------------------------
    # BaseWatcher implementation
    # ------------------------------------------------------------------

    def _list_func(self) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Use the cluster-wide event list endpoint."""
        return self._api.list_event_for_all_namespaces  # type: ignore[no-any-return]

    async def _handle_event(self, event_type: str, obj: Any, raw: dict[str, Any]) -> None:
        """Convert a watch event to an EventRecord and buffer it."""
        if event_type == "DELETED":
            # Deleted events are not diagnostically useful
            return

        record = _convert_event(obj, raw, self._cluster_id)
        if record is None:
            return

        # Evict events outside the 2-hour window from the front of the deque
        # (the deque handles the 500-cap automatically via maxlen).
        self._evict_stale()
        self._buffer.append(record)

        events_total.labels(source=record.source.value, severity=record.severity.value).inc()

        # Fire callbacks concurrently
        if self._callbacks:
            await asyncio.gather(*(cb(record) for cb in self._callbacks), return_exceptions=True)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _evict_stale(self) -> None:
        """Remove events older than the 2-hour window from the left of the deque."""
        cutoff = datetime.now(tz=UTC) - _BUFFER_WINDOW
        while self._buffer and self._buffer[0].last_seen < cutoff:
            self._buffer.popleft()


# ---------------------------------------------------------------------------
# Conversion helper
# ---------------------------------------------------------------------------


def _convert_event(obj: Any, raw: dict[str, Any], cluster_id: str) -> EventRecord | None:
    """Convert a kubernetes_asyncio V1Event object to an EventRecord.

    Returns None if the event lacks the minimum required fields.
    """
    # Prefer the deserialized object; fall back to raw dict
    if obj is not None and hasattr(obj, "metadata"):
        return _from_v1_event(obj, cluster_id)

    return _from_raw_dict(raw, cluster_id)


def _from_v1_event(obj: Any, cluster_id: str) -> EventRecord | None:
    """Build an EventRecord from a deserialized V1Event."""
    metadata = obj.metadata
    if metadata is None:
        return None

    namespace: str = getattr(metadata, "namespace", "") or ""
    involved = obj.involved_object
    resource_kind: str = getattr(involved, "kind", "Unknown") or "Unknown"
    resource_name: str = getattr(involved, "name", "") or ""

    reason: str = getattr(obj, "reason", "") or ""
    message: str = getattr(obj, "message", "") or ""
    kube_type: str = getattr(obj, "type", "Normal") or "Normal"
    count: int = getattr(obj, "count", 1) or 1

    labels: dict[str, str] = {}
    raw_labels = getattr(metadata, "labels", None)
    if isinstance(raw_labels, dict):
        labels = {str(k): str(v) for k, v in raw_labels.items()}

    first_seen = _coerce_dt(getattr(obj, "first_timestamp", None))
    last_seen = _coerce_dt(getattr(obj, "last_timestamp", None))
    if last_seen is None:
        last_seen = first_seen or datetime.now(tz=UTC)
    if first_seen is None:
        first_seen = last_seen

    severity = _classify_severity(kube_type, reason)

    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=severity,
        reason=reason,
        message=message,
        namespace=namespace,
        resource_kind=resource_kind,
        resource_name=resource_name,
        first_seen=first_seen,
        last_seen=last_seen,
        cluster_id=cluster_id,
        labels=labels,
        count=count,
    )


def _from_raw_dict(raw: dict[str, Any], cluster_id: str) -> EventRecord | None:
    """Build an EventRecord from a raw event dict when deserialization failed."""
    metadata = raw.get("metadata", {})
    if not isinstance(metadata, dict):
        return None

    namespace: str = str(metadata.get("namespace", ""))
    involved = raw.get("involvedObject", {})
    if not isinstance(involved, dict):
        involved = {}

    resource_kind: str = str(involved.get("kind", "Unknown"))
    resource_name: str = str(involved.get("name", ""))
    reason: str = str(raw.get("reason", ""))
    message: str = str(raw.get("message", ""))
    kube_type: str = str(raw.get("type", "Normal"))
    count_raw = raw.get("count", 1)
    count: int = int(count_raw) if isinstance(count_raw, int | str) else 1

    labels_raw = metadata.get("labels", {})
    labels: dict[str, str] = {str(k): str(v) for k, v in labels_raw.items()} if isinstance(labels_raw, dict) else {}

    first_seen = _parse_dt_str(raw.get("firstTimestamp"))
    last_seen = _parse_dt_str(raw.get("lastTimestamp"))
    if last_seen is None:
        last_seen = first_seen or datetime.now(tz=UTC)
    if first_seen is None:
        first_seen = last_seen

    severity = _classify_severity(kube_type, reason)

    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=severity,
        reason=reason,
        message=message,
        namespace=namespace,
        resource_kind=resource_kind,
        resource_name=resource_name,
        first_seen=first_seen,
        last_seen=last_seen,
        cluster_id=cluster_id,
        labels=labels,
        count=count,
    )


def _classify_severity(kube_type: str, reason: str) -> Severity:
    """Map a Kubernetes event type and reason to a Severity level."""
    if reason in _CRITICAL_REASONS:
        return Severity.CRITICAL
    if reason in _ERROR_REASONS:
        return Severity.ERROR
    if kube_type == "Warning":
        return Severity.WARNING
    return Severity.INFO


def _coerce_dt(value: Any) -> datetime | None:
    """Coerce a kubernetes_asyncio datetime field (already a datetime) or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
    return None


def _parse_dt_str(value: Any) -> datetime | None:
    """Parse an ISO-8601 UTC datetime string from a raw dict field."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return _coerce_dt(value)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            return None
    return None
