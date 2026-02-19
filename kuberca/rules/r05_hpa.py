"""R05 HPA Misbehavior — Tier 2 rule.

Matches HorizontalPodAutoscaler scaling-failure events and correlates
HPA target metric and min/max replica changes.
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

_logger = get_logger("rule.r05_hpa")

# HPA scaling-failure event reasons emitted by the HPA controller.
_HPA_FAILURE_REASONS = frozenset(
    {
        "FailedScale",
        "DesiredReplicasComputed",
        "FailedGetScale",
        "FailedComputeMetricsReplicas",
        "FailedGetResourceMetric",
        "FailedGetExternalMetric",
        "FailedGetPodsMetric",
        "FailedGetContainerResource",
        "FailedGetObjectMetric",
        "BackoffLimitExceeded",
    }
)

_RE_HPA_FAIL = re.compile(
    r"failed.*(scale|metric|replicas|compute)",
    re.IGNORECASE,
)


def _is_hpa_relevant(fc: FieldChange) -> bool:
    """Return True for HPA spec changes (metrics, min/max replicas)."""
    path = fc.field_path.lower()
    return any(kw in path for kw in ("minreplicas", "maxreplicas", "metrics", "scaledup"))


class HPARule(Rule):
    """Matches HPA scaling failures and correlates HPA spec changes."""

    rule_id = "R05_hpa_misbehavior"
    display_name = "HPA Misbehavior"
    priority = 30
    base_confidence = 0.50
    resource_dependencies = ["HorizontalPodAutoscaler", "Deployment"]
    relevant_field_paths = ["spec"]

    def match(self, event: EventRecord) -> bool:
        """Return True for HPA scaling failure events."""
        if event.reason in _HPA_FAILURE_REASONS:
            return True
        if event.resource_kind == "HorizontalPodAutoscaler":
            return bool(_RE_HPA_FAIL.search(event.message))
        return False

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find the HPA object and diff target metric / replica range changes."""
        t_start = time.monotonic()
        objects_queried = 0

        # HPA may be the involved object itself or referenced by name in the message.
        hpa_name = event.resource_name
        hpa = cache.get("HorizontalPodAutoscaler", event.namespace, hpa_name)
        objects_queried += 1

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        if hpa is not None:
            related_resources.append(hpa)
            raw = ledger.diff("HorizontalPodAutoscaler", event.namespace, hpa_name, since_hours=2.0)
            objects_queried += 1
            for fc in raw:
                if _is_hpa_relevant(fc):
                    relevant_changes.append(fc)

            # Also diff the scaled Deployment if identifiable.
            scale_target = hpa.spec.get("scaleTargetRef", {}) if isinstance(hpa.spec, dict) else {}
            if isinstance(scale_target, dict):
                target_name = scale_target.get("name")
                target_kind = scale_target.get("kind", "Deployment")
                if isinstance(target_name, str) and target_name:
                    deploy = cache.get(str(target_kind), event.namespace, target_name)
                    objects_queried += 1
                    if deploy is not None:
                        related_resources.append(deploy)

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
        """Produce the RuleResult for this HPA scaling failure."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(f"HPA {event.resource_name} scaling failure (count={event.count}): {event.message[:300]}"),
            )
        ]
        affected: list[AffectedResource] = [
            AffectedResource(
                kind=event.resource_kind,
                namespace=event.namespace,
                name=event.resource_name,
            )
        ]

        for res in correlation.related_resources:
            if res.kind != event.resource_kind or res.name != event.resource_name:
                affected.append(AffectedResource(kind=res.kind, namespace=res.namespace, name=res.name))

        if correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(f"HPA spec changed: '{latest.field_path}' {latest.old_value!r} → {latest.new_value!r}"),
                )
            )
            root_cause = (
                f"HPA {event.resource_name} is failing to scale. "
                f"A recent HPA configuration change at '{latest.field_path}' "
                f"may be contributing to the issue."
            )
        else:
            root_cause = (
                f"HPA {event.resource_name} is failing to scale. "
                f"Check that the target metric is available and the HPA "
                f"min/max replica bounds are correct."
            )

        remediation = (
            f"Inspect the HPA status: "
            f"`kubectl describe hpa {event.resource_name} -n {event.namespace}`. "
            f"Verify the metrics server is available and the scale target exists."
        )

        _logger.info(
            "r05_match",
            hpa=event.resource_name,
            namespace=event.namespace,
            confidence=confidence,
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
