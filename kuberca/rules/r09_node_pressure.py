"""R09 Node Pressure — Tier 2 rule.

Matches NodeCondition events indicating MemoryPressure or DiskPressure
and correlates pod resource requests on the affected node.
"""

from __future__ import annotations

import time
from datetime import UTC

from kuberca.models.analysis import (
    AffectedResource,
    CorrelationResult,
    EvidenceItem,
    RuleResult,
)
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r09_node_pressure")

_PRESSURE_REASONS = frozenset(
    {
        "NodeHasInsufficientMemory",
        "NodeHasInsufficientDisk",
        "NodeHasDiskPressure",
        "NodeHasMemoryPressure",
        "MemoryPressure",
        "DiskPressure",
        "PIDPressure",
    }
)


def _is_pressure_relevant(fc: FieldChange) -> bool:
    """Return True for resource request/limit changes on pods or nodes."""
    path = fc.field_path.lower()
    return any(kw in path for kw in ("resources.requests", "resources.limits", "conditions"))


def _pressure_type(reason: str) -> str:
    """Return 'memory' or 'disk' based on the event reason."""
    r = reason.lower()
    if "memory" in r:
        return "memory"
    if "disk" in r:
        return "disk"
    if "pid" in r:
        return "PID"
    return "resource"


class NodePressureRule(Rule):
    """Matches node pressure conditions and correlates resource consumption."""

    rule_id = "R09_node_pressure"
    display_name = "Node Pressure"
    priority = 15
    base_confidence = 0.55
    resource_dependencies = ["Node", "Pod"]
    relevant_field_paths = ["spec.template.spec.containers"]

    def match(self, event: EventRecord) -> bool:
        """Return True for MemoryPressure or DiskPressure node condition events."""
        return event.reason in _PRESSURE_REASONS

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find the affected node and diff its condition changes."""
        t_start = time.monotonic()
        objects_queried = 0

        # The involved object is typically the Node.
        node = cache.get("Node", "", event.resource_name)
        objects_queried += 1

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        if node is not None:
            related_resources.append(node)
            raw = ledger.diff("Node", "", event.resource_name, since_hours=2.0)
            objects_queried += 1
            for fc in raw:
                if _is_pressure_relevant(fc):
                    relevant_changes.append(fc)

        # List pods on this node as context.
        all_pods = cache.list("Pod", event.namespace)
        objects_queried += len(all_pods)
        node_pods = [p for p in all_pods if p.spec.get("nodeName") == event.resource_name]
        related_resources.extend(node_pods[:10])

        duration_ms = (time.monotonic() - t_start) * 1000.0
        return CorrelationResult(
            changes=relevant_changes,
            related_events=[],
            related_resources=related_resources,
            objects_queried=objects_queried,
            duration_ms=duration_ms,
        )

    def explain(
        self,
        event: EventRecord,
        correlation: CorrelationResult,
    ) -> RuleResult:
        """Produce the RuleResult for this node pressure event."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)
        pressure = _pressure_type(event.reason)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(
                    f"Node {event.resource_name} has {pressure} pressure (count={event.count}): {event.message[:300]}"
                ),
            )
        ]
        affected: list[AffectedResource] = [
            AffectedResource(
                kind="Node",
                namespace="",
                name=event.resource_name,
            )
        ]

        pod_count = sum(1 for r in correlation.related_resources if r.kind == "Pod")

        if correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Node condition changed: '{latest.field_path}' {latest.old_value!r} → {latest.new_value!r}"
                    ),
                )
            )
            root_cause = (
                f"Node {event.resource_name} is under {pressure} pressure. "
                f"Condition changed at {latest.changed_at.isoformat()}. "
                f"{pod_count} pod(s) are running on this node."
            )
        else:
            root_cause = (
                f"Node {event.resource_name} is under {pressure} pressure. "
                f"{pod_count} pod(s) are running on this node and may be consuming "
                f"excessive {pressure}."
            )

        remediation = (
            f"Check node capacity: `kubectl describe node {event.resource_name}`. "
            f"Identify high-{pressure} pods: `kubectl top pods --sort-by={pressure}`. "
            f"Consider evicting pods or adding node capacity."
        )

        _logger.info(
            "r09_match",
            node=event.resource_name,
            pressure_type=pressure,
            confidence=confidence,
            pod_count=pod_count,
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
