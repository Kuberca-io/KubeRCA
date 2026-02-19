"""R18 ClaimLost-PVLost -- Tier 2 rule.

Matches events where a PersistentVolumeClaim is lost because the
backing PersistentVolume has been deleted or entered a Lost phase.
"""

from __future__ import annotations

import re
import time
from datetime import UTC

from kuberca.models.analysis import AffectedResource, CorrelationResult, EvidenceItem, RuleResult
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.models.resources import CachedResourceView
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r18_claim_lost")

_LOST_REASONS = frozenset({"ClaimLost", "VolumeFailedRecycle", "VolumeFailedDelete"})
_RE_PHASE_LOST = re.compile(r"phase.*lost", re.IGNORECASE)


class ClaimLostRule(Rule):
    """Matches PV/PVC lost events."""

    rule_id = "R18_claim_lost"
    display_name = "ClaimLost -- PV Lost"
    priority = 22
    base_confidence = 0.55
    resource_dependencies = ["PersistentVolumeClaim", "PersistentVolume"]
    relevant_field_paths = ["status.phase"]

    def match(self, event: EventRecord) -> bool:
        if event.reason in _LOST_REASONS:
            return True
        return bool(_RE_PHASE_LOST.search(event.message))

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources: list[CachedResourceView] = []

        # Try to get the PVC
        pvc = cache.get("PersistentVolumeClaim", event.namespace, event.resource_name)
        objects_queried += 1
        if pvc is not None:
            related_resources.append(pvc)

        # Check PVs for Lost phase
        pvs = cache.list("PersistentVolume", "")
        objects_queried += len(pvs)
        for pv in pvs:
            phase = pv.status.get("phase", "")
            if phase == "Lost" or phase == "Failed":
                related_resources.append(pv)

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
                summary=f"PV/PVC lost event (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind=event.resource_kind, namespace=event.namespace, name=event.resource_name)]

        lost_pvs = [
            r.name
            for r in correlation.related_resources
            if r.kind == "PersistentVolume" and r.status.get("phase") in ("Lost", "Failed")
        ]
        for pv_name in lost_pvs:
            affected.append(AffectedResource(kind="PersistentVolume", namespace="", name=pv_name))

        if lost_pvs:
            root_cause = f"PersistentVolume(s) {', '.join(lost_pvs)} entered Lost/Failed phase. Bound PVCs will lose access to storage."
        else:
            root_cause = f"PVC {event.resource_name} lost its backing PersistentVolume. The storage may have been deleted or become unavailable."

        remediation = (
            f"Check PV status: `kubectl get pv`. "
            f"Check PVC: `kubectl get pvc {event.resource_name} -n {event.namespace}`. "
            f"Recover or recreate the backing storage."
        )

        _logger.info("r18_match", resource=event.resource_name, namespace=event.namespace, confidence=confidence)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
