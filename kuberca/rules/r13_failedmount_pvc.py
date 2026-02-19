"""R13 FailedMount-PVCNotBound -- Tier 2 rule.

Matches FailedMount events where a PVC referenced by a Pod is not bound.
Uses the dependency graph for PVC->PV traversal when available.
"""

from __future__ import annotations

import re
import time
from datetime import UTC

from kuberca.models.analysis import AffectedResource, CorrelationResult, EvidenceItem, RuleResult
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r13_failedmount_pvc")

_RE_PVC_NAME = re.compile(r'pvc[- /]?"?([\w-]+)"?|persistentvolumeclaim["\/ ]([\w-]+)', re.IGNORECASE)
_MOUNT_REASONS = frozenset({"FailedMount", "FailedAttachVolume"})


def _extract_pvc_name(message: str) -> str | None:
    match = _RE_PVC_NAME.search(message)
    if match:
        return match.group(1) or match.group(2)
    return None


class FailedMountPVCRule(Rule):
    """Matches FailedMount events caused by unbound PVCs."""

    rule_id = "R13_failedmount_pvc"
    display_name = "FailedMount -- PVC Not Bound"
    priority = 20
    base_confidence = 0.60
    resource_dependencies = ["Pod", "PersistentVolumeClaim", "PersistentVolume"]
    relevant_field_paths = ["spec.volumes"]

    def match(self, event: EventRecord) -> bool:
        if event.reason not in _MOUNT_REASONS:
            return False
        msg = event.message.lower()
        return "pvc" in msg or "persistentvolumeclaim" in msg or "not bound" in msg

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources = []

        pvc_name = _extract_pvc_name(event.message)
        if pvc_name:
            pvc = cache.get("PersistentVolumeClaim", event.namespace, pvc_name)
            objects_queried += 1
            if pvc is not None:
                related_resources.append(pvc)

        # Check all PVCs in namespace for unbound ones
        all_pvcs = cache.list("PersistentVolumeClaim", event.namespace)
        objects_queried += len(all_pvcs)
        for pvc in all_pvcs:
            phase = pvc.status.get("phase", "Bound")
            if phase != "Bound" and pvc not in related_resources:
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
                summary=f"Pod {event.resource_name} FailedMount PVC (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)]

        unbound = [
            r.name
            for r in correlation.related_resources
            if r.kind == "PersistentVolumeClaim" and r.status.get("phase", "Bound") != "Bound"
        ]

        if unbound:
            root_cause = f"Pod {event.resource_name} cannot mount volume: PVC(s) {', '.join(unbound)} are not bound."
            for name in unbound:
                affected.append(AffectedResource(kind="PersistentVolumeClaim", namespace=event.namespace, name=name))
        else:
            root_cause = f"Pod {event.resource_name} failed to mount PVC volume. Check PVC binding status."

        remediation = (
            f"Check PVC status: `kubectl get pvc -n {event.namespace}`. Check PV availability: `kubectl get pv`."
        )

        _logger.info("r13_match", pod=event.resource_name, namespace=event.namespace, confidence=confidence)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
