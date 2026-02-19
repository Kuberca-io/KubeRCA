"""R06 Service Unreachable — Tier 2 rule.

Matches endpoint-not-ready and connection-failure events, and correlates
Service selector diffs and NetworkPolicy changes.
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

_logger = get_logger("rule.r06_service_unreachable")

_UNREACHABLE_REASONS = frozenset(
    {
        "FailedSync",
        "NetworkNotReady",
        "NotReady",
    }
)

_RE_UNREACHABLE = re.compile(
    r"(connection refused|endpoint not ready|no endpoints|"
    r"service unavailable|dial tcp.*i/o timeout)",
    re.IGNORECASE,
)


def _is_service_relevant(fc: FieldChange) -> bool:
    """Return True for Service selector or NetworkPolicy changes."""
    path = fc.field_path.lower()
    return any(kw in path for kw in ("selector", "ports", "networkpolicy", "ingress", "egress"))


class ServiceUnreachableRule(Rule):
    """Matches service-unreachable patterns and correlates selector/policy changes."""

    rule_id = "R06_service_unreachable"
    display_name = "Service Unreachable"
    priority = 25
    base_confidence = 0.50
    resource_dependencies = ["Service", "Pod", "Endpoints"]
    relevant_field_paths = ["spec.selector", "spec.ports"]

    def match(self, event: EventRecord) -> bool:
        """Return True for endpoint-not-ready or connection-failure events."""
        if event.reason in _UNREACHABLE_REASONS:
            return True
        return bool(_RE_UNREACHABLE.search(event.message))

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find matching Service and diff selector/port/NetworkPolicy changes."""
        t_start = time.monotonic()
        objects_queried = 0

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        # Try to find the Service matching the involved resource name.
        service = cache.get("Service", event.namespace, event.resource_name)
        objects_queried += 1
        if service is None:
            # Fallback: list all services and look for name prefix match.
            services = cache.list("Service", event.namespace)
            objects_queried += len(services)
            for svc in services:
                if event.resource_name.startswith(svc.name):
                    service = svc
                    break

        if service is not None:
            related_resources.append(service)
            raw = ledger.diff("Service", event.namespace, service.name, since_hours=2.0)
            objects_queried += 1
            for fc in raw:
                if _is_service_relevant(fc):
                    relevant_changes.append(fc)

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
        """Produce the RuleResult for this service-unreachable match."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(
                    f"Service/endpoint unreachable for {event.resource_name} "
                    f"(count={event.count}): {event.message[:300]}"
                ),
            )
        ]
        affected: list[AffectedResource] = [
            AffectedResource(
                kind=event.resource_kind,
                namespace=event.namespace,
                name=event.resource_name,
            )
        ]

        service: CachedResourceView | None = correlation.related_resources[0] if correlation.related_resources else None
        if service and service.name != event.resource_name:
            affected.append(AffectedResource(kind="Service", namespace=service.namespace, name=service.name))

        if correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Service field changed: '{latest.field_path}' {latest.old_value!r} → {latest.new_value!r}"
                    ),
                )
            )
            root_cause = (
                f"Service {service.name if service else event.resource_name} is unreachable. "
                f"A recent change to '{latest.field_path}' may have broken endpoint routing."
            )
        else:
            root_cause = (
                f"Service {event.resource_name} is unreachable. "
                f"No recent selector or port changes detected. "
                f"Check that backing pods are ready and NetworkPolicies permit traffic."
            )

        remediation = (
            f"Check endpoint readiness: `kubectl get endpoints -n {event.namespace}`. "
            f"Verify the Service selector matches pod labels: "
            f"`kubectl describe service {service.name if service else event.resource_name} "
            f"-n {event.namespace}`."
        )

        _logger.info(
            "r06_match",
            resource=event.resource_name,
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
