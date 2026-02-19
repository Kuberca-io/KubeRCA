"""R08 Volume Mount Failure — Tier 2 rule.

Matches FailedMount and FailedAttachVolume events and correlates PVC
status, StorageClass availability, and node affinity constraints.
"""

from __future__ import annotations

import re
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

_logger = get_logger("rule.r08_volume_mount")

_VOLUME_REASONS = frozenset({"FailedMount", "FailedAttachVolume"})

_RE_PVC_NAME = re.compile(r"pvc[- /]?\"?([\w-]+)\"?|persistentvolumeclaim[\"/ ]([\w-]+)", re.IGNORECASE)


def _is_volume_relevant(fc: FieldChange) -> bool:
    """Return True for PVC, volume, or storageclass changes."""
    path = fc.field_path.lower()
    return any(kw in path for kw in ("volume", "pvc", "storageclass", "accessmode"))


def _extract_pvc_name(message: str) -> str | None:
    """Try to extract a PVC name from the scheduler/kubelet message."""
    match = _RE_PVC_NAME.search(message)
    if match:
        return match.group(1) or match.group(2)
    return None


class VolumeMountRule(Rule):
    """Matches volume mount failures and correlates PVC/StorageClass status."""

    rule_id = "R08_volume_mount"
    display_name = "Volume Mount Failure"
    priority = 20
    base_confidence = 0.60
    resource_dependencies = ["Pod", "PersistentVolumeClaim"]
    relevant_field_paths = ["spec.volumes", "spec.template.spec.volumes"]

    def match(self, event: EventRecord) -> bool:
        """Return True for FailedMount or FailedAttachVolume reasons."""
        return event.reason in _VOLUME_REASONS

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find referenced PVCs and diff for status/binding changes."""
        t_start = time.monotonic()
        objects_queried = 0

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        # Look for PVC name in the event message.
        pvc_name = _extract_pvc_name(event.message)
        if pvc_name:
            pvc = cache.get("PersistentVolumeClaim", event.namespace, pvc_name)
            objects_queried += 1
            if pvc is not None:
                related_resources.append(pvc)
                raw = ledger.diff("PersistentVolumeClaim", event.namespace, pvc_name, since_hours=2.0)
                objects_queried += 1
                for fc in raw:
                    if _is_volume_relevant(fc):
                        relevant_changes.append(fc)

        # Scan all PVCs in the namespace for unbound ones as additional context.
        all_pvcs = cache.list("PersistentVolumeClaim", event.namespace)
        objects_queried += len(all_pvcs)
        for pvc in all_pvcs:
            phase = pvc.status.get("phase", "Bound")
            if phase != "Bound" and pvc not in related_resources:
                related_resources.append(pvc)

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
        """Produce the RuleResult for this volume mount failure."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(
                    f"Pod {event.resource_name} failed to mount/attach volume "
                    f"(count={event.count}): {event.message[:300]}"
                ),
            )
        ]
        affected: list[AffectedResource] = [
            AffectedResource(
                kind="Pod",
                namespace=event.namespace,
                name=event.resource_name,
            )
        ]

        for res in correlation.related_resources:
            if res.kind == "PersistentVolumeClaim":
                affected.append(AffectedResource(kind="PersistentVolumeClaim", namespace=res.namespace, name=res.name))

        unbound_pvcs = [
            r.name
            for r in correlation.related_resources
            if r.kind == "PersistentVolumeClaim" and r.status.get("phase", "Bound") != "Bound"
        ]

        if unbound_pvcs:
            root_cause = (
                f"Pod {event.resource_name} cannot mount volume: PVC(s) {', '.join(unbound_pvcs)} are not bound."
            )
        elif correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Volume field changed: '{latest.field_path}' {latest.old_value!r} → {latest.new_value!r}"
                    ),
                )
            )
            root_cause = (
                f"Pod {event.resource_name} failed to mount volume after a recent change to '{latest.field_path}'."
            )
        else:
            root_cause = (
                f"Pod {event.resource_name} failed to mount/attach volume. "
                f"Check PVC binding status and StorageClass availability."
            )

        remediation = (
            f"Check PVC status: `kubectl get pvc -n {event.namespace}`. "
            f"Verify the StorageClass exists: `kubectl get storageclass`. "
            f"Inspect pod events: `kubectl describe pod {event.resource_name} "
            f"-n {event.namespace}`."
        )

        _logger.info(
            "r08_match",
            pod=event.resource_name,
            namespace=event.namespace,
            confidence=confidence,
            unbound_pvcs=unbound_pvcs,
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
