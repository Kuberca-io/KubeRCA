"""R12 FailedMount-SecretNotFound -- Tier 2 rule.

Matches FailedMount events where a Secret referenced by a Pod volume
could not be found.
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

_logger = get_logger("rule.r12_failedmount_secret")

_RE_SECRET_NAME = re.compile(
    r'secret[s]?\s+["\']?([\w.-]+)["\']?|secret\s+"?([\w.-]+)"?\s+not found',
    re.IGNORECASE,
)


def _extract_secret_name(message: str) -> str | None:
    match = _RE_SECRET_NAME.search(message)
    if match:
        return match.group(1) or match.group(2)
    return None


class FailedMountSecretRule(Rule):
    """Matches FailedMount events caused by missing Secrets."""

    rule_id = "R12_failedmount_secret"
    display_name = "FailedMount -- Secret Not Found"
    priority = 20
    base_confidence = 0.60
    resource_dependencies = ["Pod", "Secret"]
    relevant_field_paths = ["spec.volumes", "spec.template.spec.volumes"]

    def match(self, event: EventRecord) -> bool:
        if event.reason != "FailedMount":
            return False
        return "secret" in event.message.lower()

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources = []

        secret_name = _extract_secret_name(event.message)
        if secret_name:
            secret = cache.get("Secret", event.namespace, secret_name)
            objects_queried += 1
            if secret is not None:
                related_resources.append(secret)

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

        secret_name = _extract_secret_name(event.message)
        secret_found = any(r.kind == "Secret" for r in correlation.related_resources)

        evidence = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=f"Pod {event.resource_name} failed to mount Secret (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)]

        if secret_name and not secret_found:
            root_cause = f"Pod {event.resource_name} cannot start: Secret '{secret_name}' not found in namespace '{event.namespace}'."
        else:
            root_cause = f"Pod {event.resource_name} failed to mount a Secret volume."

        remediation = (
            f"Verify the Secret exists: `kubectl get secret {secret_name or '<name>'} -n {event.namespace}`. "
            f"Inspect pod events: `kubectl describe pod {event.resource_name} -n {event.namespace}`."
        )

        _logger.info("r12_match", pod=event.resource_name, namespace=event.namespace, confidence=confidence)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
