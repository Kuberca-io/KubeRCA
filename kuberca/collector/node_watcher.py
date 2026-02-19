"""Node condition watcher.

Watches all Nodes cluster-wide and emits EventRecord(source=NODE_CONDITION)
whenever a tracked condition transitions to a True/Unknown state.

Tracked conditions:
    - NotReady  (Ready == False or Unknown)
    - MemoryPressure
    - DiskPressure
    - PIDPressure
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from datetime import UTC, datetime
from typing import Any

from kuberca.collector.watcher import BaseWatcher
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import events_total

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

EventHandler = Callable[[EventRecord], Coroutine[Any, Any, None]]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Condition types we monitor.  For "Ready" we invert the polarity: a Ready
# condition with status!=True means the node is NotReady.
_WATCHED_CONDITIONS: frozenset[str] = frozenset({"Ready", "MemoryPressure", "DiskPressure", "PIDPressure"})

_CONDITION_SEVERITY: dict[str, Severity] = {
    "NotReady": Severity.CRITICAL,
    "MemoryPressure": Severity.WARNING,
    "DiskPressure": Severity.WARNING,
    "PIDPressure": Severity.WARNING,
}


class NodeWatcher(BaseWatcher):
    """Watches Node conditions and emits EventRecords on adverse transitions.

    Usage::

        v1 = kubernetes_asyncio.client.CoreV1Api()
        watcher = NodeWatcher(v1, cluster_id="prod")
        watcher.add_event_handler(my_handler)
        await watcher.start()
    """

    def __init__(self, api: Any, cluster_id: str = "") -> None:
        """Initialise the node watcher.

        Args:
            api: A ``CoreV1Api`` instance.
            cluster_id: Forwarded to emitted ``EventRecord.cluster_id``.
        """
        super().__init__(api, cluster_id=cluster_id, name="node")
        self._log = get_logger("watcher.node")
        self._event_handlers: list[EventHandler] = []

        # Track last known condition state per node:
        # key = (node_name, condition_reason) -> last emitted condition status string
        self._condition_states: dict[tuple[str, str], str] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def add_event_handler(self, handler: EventHandler) -> None:
        """Register an async callback for condition-change EventRecords.

        Args:
            handler: ``async def handler(record: EventRecord) -> None``
        """
        self._event_handlers.append(handler)

    # ------------------------------------------------------------------
    # BaseWatcher implementation
    # ------------------------------------------------------------------

    def _list_func(self) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Use the node list endpoint (nodes are cluster-scoped)."""
        return self._api.list_node  # type: ignore[no-any-return]

    async def _handle_event(self, event_type: str, obj: Any, raw: dict[str, Any]) -> None:
        """Process a node watch event for condition changes."""
        if event_type == "DELETED":
            node_name = _extract_node_name(obj, raw)
            if node_name:
                # Clean up tracked states for this node
                stale = [k for k in self._condition_states if k[0] == node_name]
                for k in stale:
                    del self._condition_states[k]
            return

        node_name = _extract_node_name(obj, raw)
        if not node_name:
            return

        now = datetime.now(tz=UTC)
        conditions = _extract_conditions(obj, raw)

        records: list[EventRecord] = []
        for cond in conditions:
            cond_type: str = cond.get("type", "")
            cond_status: str = cond.get("status", "")
            if cond_type not in _WATCHED_CONDITIONS:
                continue

            reason_key, is_adverse = _classify_condition(cond_type, cond_status)
            state_key = (node_name, cond_type)
            prev_status = self._condition_states.get(state_key)

            if cond_status != prev_status:
                self._condition_states[state_key] = cond_status
                if is_adverse:
                    record = _build_condition_event(
                        node_name=node_name,
                        condition_type=cond_type,
                        reason_key=reason_key,
                        message=cond.get("message", ""),
                        at=now,
                        cluster_id=self._cluster_id,
                    )
                    records.append(record)

        if records and self._event_handlers:
            for record in records:
                events_total.labels(source=record.source.value, severity=record.severity.value).inc()
            await asyncio.gather(
                *(h(r) for h in self._event_handlers for r in records),
                return_exceptions=True,
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_node_name(obj: Any, raw: dict[str, Any]) -> str:
    """Return the node name from a deserialized object or raw dict."""
    if obj is not None and hasattr(obj, "metadata") and obj.metadata is not None:
        name = getattr(obj.metadata, "name", None)
        if name:
            return str(name)
    metadata = raw.get("metadata", {})
    if isinstance(metadata, dict):
        return str(metadata.get("name", ""))
    return ""


def _extract_conditions(obj: Any, raw: dict[str, Any]) -> list[dict[str, str]]:
    """Return a list of condition dicts with keys type, status, reason, message."""
    conditions: list[dict[str, str]] = []

    if obj is not None and hasattr(obj, "status") and obj.status is not None:
        raw_conds = getattr(obj.status, "conditions", None)
        if raw_conds:
            for c in raw_conds:
                cond_type = getattr(c, "type", None) or ""
                cond_status = getattr(c, "status", None) or ""
                reason = getattr(c, "reason", None) or ""
                message = getattr(c, "message", None) or ""
                conditions.append(
                    {
                        "type": str(cond_type),
                        "status": str(cond_status),
                        "reason": str(reason),
                        "message": str(message),
                    }
                )
            return conditions

    # Fall back to raw dict
    status = raw.get("status", {})
    if not isinstance(status, dict):
        return conditions
    raw_conds = status.get("conditions", [])
    if not isinstance(raw_conds, list):
        return conditions
    for c in raw_conds:
        if not isinstance(c, dict):
            continue
        conditions.append(
            {
                "type": str(c.get("type", "")),
                "status": str(c.get("status", "")),
                "reason": str(c.get("reason", "")),
                "message": str(c.get("message", "")),
            }
        )
    return conditions


def _classify_condition(cond_type: str, cond_status: str) -> tuple[str, bool]:
    """Return (reason_key, is_adverse) for a condition type + status pair.

    For the Ready condition, adverse means status is NOT "True".
    For pressure conditions, adverse means status IS "True".
    """
    if cond_type == "Ready":
        is_adverse = cond_status != "True"
        return "NotReady", is_adverse
    is_adverse = cond_status == "True"
    return cond_type, is_adverse


def _build_condition_event(
    node_name: str,
    condition_type: str,
    reason_key: str,
    message: str,
    at: datetime,
    cluster_id: str,
) -> EventRecord:
    """Construct an EventRecord for a node condition adverse transition."""
    severity = _CONDITION_SEVERITY.get(reason_key, Severity.WARNING)
    display_message = message or f"Node {node_name} condition {reason_key} is active"
    return EventRecord(
        source=EventSource.NODE_CONDITION,
        severity=severity,
        reason=reason_key,
        message=display_message,
        namespace="",
        resource_kind="Node",
        resource_name=node_name,
        first_seen=at,
        last_seen=at,
        cluster_id=cluster_id,
    )
