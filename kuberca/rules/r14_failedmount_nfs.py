"""R14 FailedMount-NFS/StaleNFS -- Tier 2 rule.

Matches FailedMount events related to NFS mount failures or stale NFS handles.
"""

from __future__ import annotations

import time
from datetime import UTC

from kuberca.models.analysis import AffectedResource, CorrelationResult, EvidenceItem, RuleResult
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r14_failedmount_nfs")

_NFS_KEYWORDS = frozenset({"nfs", "stale file handle", "mount.nfs", "nfs: server", "stale nfs"})


class FailedMountNFSRule(Rule):
    """Matches FailedMount events caused by NFS issues."""

    rule_id = "R14_failedmount_nfs"
    display_name = "FailedMount -- NFS/Stale NFS"
    priority = 22
    base_confidence = 0.55
    resource_dependencies = ["Pod", "PersistentVolumeClaim", "PersistentVolume"]
    relevant_field_paths = ["spec.volumes"]

    def match(self, event: EventRecord) -> bool:
        if event.reason != "FailedMount":
            return False
        msg = event.message.lower()
        return any(kw in msg for kw in _NFS_KEYWORDS)

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources = []

        # Check PVCs and PVs in namespace for NFS-backed volumes
        pvcs = cache.list("PersistentVolumeClaim", event.namespace)
        objects_queried += len(pvcs)
        for pvc in pvcs:
            related_resources.append(pvc)

        duration_ms = (time.monotonic() - t_start) * 1000.0
        return CorrelationResult(
            changes=[],
            related_resources=related_resources,
            objects_queried=objects_queried,
            duration_ms=duration_ms,
        )

    def explain(self, event: EventRecord, correlation: CorrelationResult) -> RuleResult:
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=f"Pod {event.resource_name} NFS mount failure (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)]

        is_stale = "stale" in event.message.lower()
        if is_stale:
            root_cause = f"Pod {event.resource_name} has a stale NFS file handle. The NFS server may be unreachable or the export path invalid."
        else:
            root_cause = (
                f"Pod {event.resource_name} failed to mount NFS volume. Check NFS server accessibility and export path."
            )

        remediation = (
            f"Check NFS server connectivity from the node. Verify the NFS export path exists. "
            f"Inspect PV/PVC: `kubectl describe pod {event.resource_name} -n {event.namespace}`."
        )

        _logger.info("r14_match", pod=event.resource_name, namespace=event.namespace, confidence=confidence)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
