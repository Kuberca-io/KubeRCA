"""Pod phase watcher.

Watches all pods cluster-wide, emits EventRecord(source=POD_PHASE) on phase
transitions, and feeds ResourceSnapshot to the ChangeLedger on spec changes.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

from kuberca.collector.watcher import BaseWatcher
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import ResourceSnapshot
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import events_total

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

EventHandler = Callable[[EventRecord], Coroutine[Any, Any, None]]
ResourceHandler = Callable[[ResourceSnapshot], Coroutine[Any, Any, None]]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Pod phases that warrant WARNING or higher severity
_PHASE_SEVERITY: dict[str, Severity] = {
    "Pending": Severity.INFO,
    "Running": Severity.INFO,
    "Succeeded": Severity.INFO,
    "Failed": Severity.ERROR,
    "Unknown": Severity.WARNING,
}

# Phases for which we emit EventRecord on entry
_NOTABLE_PHASES: frozenset[str] = frozenset({"Failed", "Unknown", "Succeeded", "Running", "Pending"})


class PodWatcher(BaseWatcher):
    """Watches Pod phase transitions across all namespaces.

    For each Pod ADDED/MODIFIED event the watcher:
    1.  Detects a phase change vs. the previously seen phase.
    2.  Emits an ``EventRecord(source=POD_PHASE)`` via ``event_handler`` callbacks.
    3.  Feeds a ``ResourceSnapshot`` to ``resource_handler`` callbacks whenever
        the spec hash differs (indicating a spec change).

    Usage::

        v1 = kubernetes_asyncio.client.CoreV1Api()
        watcher = PodWatcher(v1, cluster_id="prod")
        watcher.add_event_handler(my_event_cb)
        watcher.add_resource_handler(my_snapshot_cb)
        await watcher.start()
    """

    def __init__(self, api: Any, cluster_id: str = "") -> None:
        """Initialise the pod watcher.

        Args:
            api: A ``CoreV1Api`` instance.
            cluster_id: Forwarded to emitted ``EventRecord.cluster_id``.
        """
        super().__init__(api, cluster_id=cluster_id, name="pod")
        self._log = get_logger("watcher.pod")
        self._event_handlers: list[EventHandler] = []
        self._resource_handlers: list[ResourceHandler] = []

        # Track last known phase per pod: key = (namespace, name)
        self._pod_phases: dict[tuple[str, str], str] = {}
        # Track last known spec hash per pod
        self._pod_spec_hashes: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def add_event_handler(self, handler: EventHandler) -> None:
        """Register an async callback for phase-change EventRecords.

        Args:
            handler: ``async def handler(record: EventRecord) -> None``
        """
        self._event_handlers.append(handler)

    def add_resource_handler(self, handler: ResourceHandler) -> None:
        """Register an async callback for ResourceSnapshot deliveries.

        Args:
            handler: ``async def handler(snapshot: ResourceSnapshot) -> None``
        """
        self._resource_handlers.append(handler)

    # ------------------------------------------------------------------
    # BaseWatcher implementation
    # ------------------------------------------------------------------

    def _list_func(self) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Use the cluster-wide pod list endpoint."""
        return self._api.list_pod_for_all_namespaces  # type: ignore[no-any-return]

    async def _handle_event(self, event_type: str, obj: Any, raw: dict[str, Any]) -> None:
        """Process a pod watch event for phase transitions and spec changes."""
        if event_type == "DELETED":
            key = _pod_key(obj, raw)
            if key:
                self._pod_phases.pop(key, None)
                self._pod_spec_hashes.pop(key, None)
            return

        key = _pod_key(obj, raw)
        if key is None:
            return

        namespace, name = key
        now = datetime.now(tz=UTC)

        phase = _extract_phase(obj, raw)
        prev_phase = self._pod_phases.get(key)

        if phase and phase != prev_phase:
            self._pod_phases[key] = phase
            record = _build_phase_event(namespace, name, phase, prev_phase, now, self._cluster_id)
            events_total.labels(source=record.source.value, severity=record.severity.value).inc()
            if self._event_handlers:
                await asyncio.gather(
                    *(h(record) for h in self._event_handlers),
                    return_exceptions=True,
                )

        # Check spec changes and emit ResourceSnapshot
        spec = _extract_spec(obj, raw)
        if spec:
            spec_hash = _hash_spec(spec)
            prev_hash = self._pod_spec_hashes.get(key)
            resource_version = _extract_resource_version(obj, raw)

            if spec_hash != prev_hash:
                self._pod_spec_hashes[key] = spec_hash
                snapshot = ResourceSnapshot(
                    kind="Pod",
                    namespace=namespace,
                    name=name,
                    spec_hash=spec_hash,
                    spec=spec,
                    captured_at=now,
                    resource_version=resource_version,
                )
                if self._resource_handlers:
                    await asyncio.gather(
                        *(h(snapshot) for h in self._resource_handlers),
                        return_exceptions=True,
                    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pod_key(obj: Any, raw: dict[str, Any]) -> tuple[str, str] | None:
    """Extract (namespace, name) from a Pod object or raw dict."""
    if obj is not None and hasattr(obj, "metadata") and obj.metadata is not None:
        ns = getattr(obj.metadata, "namespace", None) or ""
        name = getattr(obj.metadata, "name", None) or ""
        if name:
            return (ns, name)

    metadata = raw.get("metadata", {})
    if isinstance(metadata, dict):
        ns = str(metadata.get("namespace", ""))
        name = str(metadata.get("name", ""))
        if name:
            return (ns, name)
    return None


def _extract_phase(obj: Any, raw: dict[str, Any]) -> str:
    """Return the current pod phase string or empty string."""
    if obj is not None and hasattr(obj, "status") and obj.status is not None:
        phase = getattr(obj.status, "phase", None)
        if phase:
            return str(phase)

    status = raw.get("status", {})
    if isinstance(status, dict):
        return str(status.get("phase", ""))
    return ""


def _extract_spec(obj: Any, raw: dict[str, Any]) -> dict[str, Any] | None:
    """Return the pod spec as a dict, or None if unavailable."""
    if obj is not None and hasattr(obj, "spec") and obj.spec is not None:
        # Use the raw spec dict for hashing accuracy
        raw_spec = raw.get("spec")
        if isinstance(raw_spec, dict):
            return raw_spec
        # Last resort: serialise the spec object via its to_dict if available
        if hasattr(obj.spec, "to_dict"):
            return obj.spec.to_dict()  # type: ignore[no-any-return]

    raw_spec = raw.get("spec")
    return raw_spec if isinstance(raw_spec, dict) else None


def _extract_resource_version(obj: Any, raw: dict[str, Any]) -> str:
    """Return the resource version string."""
    if obj is not None and hasattr(obj, "metadata") and obj.metadata is not None:
        rv = getattr(obj.metadata, "resource_version", None)
        if rv:
            return str(rv)
    metadata = raw.get("metadata", {})
    if isinstance(metadata, dict):
        return str(metadata.get("resourceVersion", ""))
    return ""


def _hash_spec(spec: dict[str, Any]) -> str:
    """Return a stable SHA-256 hex digest of the spec dict."""
    serialised = json.dumps(spec, sort_keys=True, default=str)
    return hashlib.sha256(serialised.encode()).hexdigest()


def _build_phase_event(
    namespace: str,
    name: str,
    phase: str,
    prev_phase: str | None,
    at: datetime,
    cluster_id: str,
) -> EventRecord:
    """Build an EventRecord representing a pod phase transition."""
    severity = _PHASE_SEVERITY.get(phase, Severity.WARNING)
    prev_str = prev_phase or "None"
    return EventRecord(
        source=EventSource.POD_PHASE,
        severity=severity,
        reason="PhaseTransition",
        message=f"Pod {namespace}/{name} transitioned from {prev_str} to {phase}",
        namespace=namespace,
        resource_kind="Pod",
        resource_name=name,
        first_seen=at,
        last_seen=at,
        cluster_id=cluster_id,
    )
