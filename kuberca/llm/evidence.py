"""Evidence package assembly for LLM prompt construction.

Assembles pre-filtered, in-cluster data into a structured evidence package
that the LLM analyzer consumes. Enforces hard size caps and truncation order.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime

from kuberca.models.analysis import StateContextEntry
from kuberca.models.events import EventRecord
from kuberca.models.resources import FieldChange

MAX_PROMPT_BYTES: int = 12_000
_MAX_RELATED_EVENTS: int = 20
_MAX_CHANGES: int = 30
_MAX_CONTAINER_STATUSES: int = 10
_MAX_RESOURCE_SPECS: int = 5
_CHANGE_VALUE_TRUNCATE_LEN: int = 200


def _iso(dt: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with milliseconds."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _truncate_value(value: str | None, max_len: int) -> str | None:
    """Truncate a string value to max_len characters with an ellipsis marker."""
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    return value[:max_len] + "...[TRUNCATED]"


@dataclass
class EvidencePackage:
    """Pre-filtered evidence assembled for LLM consumption.

    All data must already be redacted per the Section 5.2 redaction policy
    before being placed in this package. This class enforces size caps and
    formats data as structured text for the LLM prompt.
    """

    incident_event: EventRecord
    related_events: list[EventRecord] = field(default_factory=list)
    changes: list[FieldChange] = field(default_factory=list)
    container_statuses: list[dict] = field(default_factory=list)  # type: ignore[type-arg]
    resource_specs: list[dict] = field(default_factory=list)  # type: ignore[type-arg]
    time_window: str = "2h"
    state_context: list[StateContextEntry] = field(default_factory=list)

    def __post_init__(self) -> None:
        """Enforce hard limits on list sizes by trimming excess entries."""
        # Sort related events by last_seen descending (most recent first) then cap
        self.related_events = sorted(
            self.related_events,
            key=lambda e: e.last_seen,
            reverse=True,
        )[:_MAX_RELATED_EVENTS]

        # Sort changes by changed_at descending then cap
        self.changes = sorted(
            self.changes,
            key=lambda c: c.changed_at,
            reverse=True,
        )[:_MAX_CHANGES]

        # Cap container statuses and resource specs
        self.container_statuses = self.container_statuses[:_MAX_CONTAINER_STATUSES]
        self.resource_specs = self.resource_specs[:_MAX_RESOURCE_SPECS]

    # ------------------------------------------------------------------
    # Section formatting helpers
    # ------------------------------------------------------------------

    def _format_incident(self) -> str:
        ev = self.incident_event
        lines = [
            f"Resource: {ev.resource_kind}/{ev.namespace}/{ev.resource_name}",
            f"Reason: {ev.reason}",
            f"Severity: {ev.severity.value}",
            f"First seen: {_iso(ev.first_seen)}",
            f"Last seen: {_iso(ev.last_seen)}",
            f"Occurrences: {ev.count}",
            f"Message: {ev.message}",
        ]
        return "\n".join(lines)

    def _format_events(self, events: list[EventRecord]) -> str:
        if not events:
            return "(none)"
        lines: list[str] = []
        for ev in events:
            lines.append(
                f"- [{_iso(ev.last_seen)}] {ev.severity.value.upper()} "
                f"{ev.resource_kind}/{ev.namespace}/{ev.resource_name} "
                f"reason={ev.reason} count={ev.count}: {ev.message}"
            )
        return "\n".join(lines)

    def _format_changes(self, changes: list[FieldChange], truncate_values: bool) -> str:
        if not changes:
            return "(none)"
        lines: list[str] = []
        for ch in changes:
            old_val = ch.old_value
            new_val = ch.new_value
            if truncate_values:
                old_val = _truncate_value(old_val, _CHANGE_VALUE_TRUNCATE_LEN)
                new_val = _truncate_value(new_val, _CHANGE_VALUE_TRUNCATE_LEN)
            old_str = old_val if old_val is not None else "(none)"
            new_str = new_val if new_val is not None else "(none)"
            lines.append(f"- [{_iso(ch.changed_at)}] {ch.field_path}: {old_str} -> {new_str}")
        return "\n".join(lines)

    def _format_container_statuses(self, statuses: list[dict]) -> str:  # type: ignore[type-arg]
        if not statuses:
            return "(none)"
        parts: list[str] = []
        for s in statuses:
            try:
                parts.append(json.dumps(s, default=str, indent=None, separators=(", ", ": ")))
            except (TypeError, ValueError):
                parts.append(str(s))
        return "\n".join(parts)

    def _format_resource_specs(self, specs: list[dict]) -> str:  # type: ignore[type-arg]
        if not specs:
            return "(none)"
        parts: list[str] = []
        for spec in specs:
            try:
                parts.append(json.dumps(spec, default=str, indent=2))
            except (TypeError, ValueError):
                parts.append(str(spec))
        return "\n---\n".join(parts)

    def _format_state_context(self, entries: list[StateContextEntry]) -> str:
        """Format state context entries for LLM consumption."""
        if not entries:
            return "(none)"
        lines: list[str] = []
        for entry in entries[:10]:  # Max 10 entries
            status = entry.status_summary if entry.exists else "<not found>"
            lines.append(
                f"- {entry.kind}/{entry.namespace}/{entry.name}: status={status}, relationship={entry.relationship}"
            )
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Main assembly method
    # ------------------------------------------------------------------

    def to_prompt_context(self) -> str:
        """Format evidence as structured text for LLM consumption.

        Enforces MAX_PROMPT_BYTES = 12,000 bytes. Applies truncation in this
        order when the limit is exceeded:
          1. Drop resource specs beyond the incident resource itself.
          2. Drop oldest related events.
          3. Truncate container statuses to current state only (drop previousState).
          4. Truncate change field values to 200 chars each.

        Events and changes are never fully dropped — they are the primary signal.
        """
        result = self._assemble(
            events=self.related_events,
            changes=self.changes,
            container_statuses=self.container_statuses,
            resource_specs=self.resource_specs,
            truncate_change_values=False,
        )
        if len(result.encode("utf-8")) <= MAX_PROMPT_BYTES:
            return result

        # Step 1: drop resource specs beyond the first (incident resource)
        reduced_specs = self.resource_specs[:1]
        result = self._assemble(
            events=self.related_events,
            changes=self.changes,
            container_statuses=self.container_statuses,
            resource_specs=reduced_specs,
            truncate_change_values=False,
        )
        if len(result.encode("utf-8")) <= MAX_PROMPT_BYTES:
            return result

        # Step 2: drop oldest related events progressively
        events = list(self.related_events)
        while events and len(result.encode("utf-8")) > MAX_PROMPT_BYTES:
            events = events[:-1]
            result = self._assemble(
                events=events,
                changes=self.changes,
                container_statuses=self.container_statuses,
                resource_specs=reduced_specs,
                truncate_change_values=False,
            )

        if len(result.encode("utf-8")) <= MAX_PROMPT_BYTES:
            return result

        # Step 3: truncate container statuses to current state only
        stripped_statuses = [{k: v for k, v in s.items() if k != "previousState"} for s in self.container_statuses]
        result = self._assemble(
            events=events,
            changes=self.changes,
            container_statuses=stripped_statuses,
            resource_specs=reduced_specs,
            truncate_change_values=False,
        )
        if len(result.encode("utf-8")) <= MAX_PROMPT_BYTES:
            return result

        # Step 4: truncate change field values to 200 chars each
        result = self._assemble(
            events=events,
            changes=self.changes,
            container_statuses=stripped_statuses,
            resource_specs=reduced_specs,
            truncate_change_values=True,
        )
        # Return best effort even if still over limit — this is the final step
        return result

    def _assemble(
        self,
        events: list[EventRecord],
        changes: list[FieldChange],
        container_statuses: list[dict],  # type: ignore[type-arg]
        resource_specs: list[dict],  # type: ignore[type-arg]
        truncate_change_values: bool,
    ) -> str:
        """Assemble the full prompt context string from given inputs."""
        sections = [
            "## Incident",
            self._format_incident(),
            "",
            f"## Recent Events (last {self.time_window})",
            self._format_events(events),
            "",
            "## Recent Changes",
            self._format_changes(changes, truncate_values=truncate_change_values),
            "",
            "## Container Status",
            self._format_container_statuses(container_statuses),
            "",
            "## Resource Specifications",
            self._format_resource_specs(resource_specs),
        ]
        if self.state_context:
            sections.extend(
                [
                    "",
                    "## State Context",
                    self._format_state_context(self.state_context),
                ]
            )
        return "\n".join(sections)
