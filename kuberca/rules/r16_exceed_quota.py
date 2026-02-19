"""R16 ExceedQuota -- Tier 2 rule.

Matches events where resource quota is exceeded, and correlates
ResourceQuota spec.hard vs status.used in the namespace.
"""

from __future__ import annotations

import time
from datetime import UTC

from kuberca.models.analysis import AffectedResource, CorrelationResult, EvidenceItem, RuleResult
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.models.resources import CachedResourceView
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r16_exceed_quota")


class ExceedQuotaRule(Rule):
    """Matches events caused by resource quota exhaustion."""

    rule_id = "R16_exceed_quota"
    display_name = "Exceed Quota"
    priority = 15
    base_confidence = 0.65
    resource_dependencies = ["Pod", "ResourceQuota"]
    relevant_field_paths = ["spec.hard", "status.used"]

    def match(self, event: EventRecord) -> bool:
        msg = event.message.lower()
        if "exceeded quota" in msg:
            return True
        return event.reason == "FailedCreate" and "quota" in msg

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources: list[CachedResourceView] = []

        quotas = cache.list("ResourceQuota", event.namespace)
        objects_queried += len(quotas)
        for quota in quotas:
            related_resources.append(quota)

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
                summary=f"Quota exceeded (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)]

        quota_names = [r.name for r in correlation.related_resources if r.kind == "ResourceQuota"]
        for qn in quota_names:
            affected.append(AffectedResource(kind="ResourceQuota", namespace=event.namespace, name=qn))

        if quota_names:
            root_cause = f"Resource quota exceeded in namespace '{event.namespace}'. Quotas: {', '.join(quota_names)}."
        else:
            root_cause = f"Resource quota exceeded in namespace '{event.namespace}'."

        remediation = (
            f"Check quota usage: `kubectl describe resourcequota -n {event.namespace}`. "
            f"Request quota increase or reduce resource requests."
        )

        _logger.info("r16_match", namespace=event.namespace, confidence=confidence, quotas=quota_names)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
