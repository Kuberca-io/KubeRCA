"""R17 Evicted-ResourcePressure -- Tier 2 rule.

Matches pod eviction events caused by node resource pressure
(DiskPressure, MemoryPressure).
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

_logger = get_logger("rule.r17_evicted")


class EvictedRule(Rule):
    """Matches pod eviction events due to node resource pressure."""

    rule_id = "R17_evicted"
    display_name = "Evicted -- Resource Pressure"
    priority = 18
    base_confidence = 0.60
    resource_dependencies = ["Pod", "Node"]
    relevant_field_paths = ["status.conditions"]

    def match(self, event: EventRecord) -> bool:
        if event.reason == "Evicted":
            return True
        return event.reason in ("Killing", "Preempting") and "evict" in event.message.lower()

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources: list[CachedResourceView] = []

        # Get the pod to find its node
        pod = cache.get("Pod", event.namespace, event.resource_name)
        objects_queried += 1
        node_name = ""
        if pod is not None:
            related_resources.append(pod)
            node_name = str(pod.spec.get("nodeName", ""))

        if node_name:
            node = cache.get("Node", "", node_name)
            objects_queried += 1
            if node is not None:
                related_resources.append(node)

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
                summary=f"Pod {event.resource_name} evicted (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)]

        # Check node conditions for pressure
        node = next((r for r in correlation.related_resources if r.kind == "Node"), None)
        pressure_type = "resource"
        if node is not None:
            conditions = node.status.get("conditions", [])
            if isinstance(conditions, list):
                for cond in conditions:
                    if isinstance(cond, dict) and cond.get("status") == "True":
                        ctype = cond.get("type", "")
                        if "MemoryPressure" in str(ctype):
                            pressure_type = "memory"
                        elif "DiskPressure" in str(ctype):
                            pressure_type = "disk"
            affected.append(AffectedResource(kind="Node", namespace="", name=node.name))

        msg = event.message.lower()
        if "memory" in msg or pressure_type == "memory":
            root_cause = f"Pod {event.resource_name} was evicted due to node memory pressure."
        elif "disk" in msg or "ephemeral" in msg or pressure_type == "disk":
            root_cause = f"Pod {event.resource_name} was evicted due to node disk pressure."
        else:
            root_cause = f"Pod {event.resource_name} was evicted due to node resource pressure."

        remediation = (
            f"Check node conditions: `kubectl describe node`. "
            f"Check pod resource usage: `kubectl top pod {event.resource_name} -n {event.namespace}`. "
            f"Add resource limits or provision more capacity."
        )

        _logger.info("r17_match", pod=event.resource_name, namespace=event.namespace, confidence=confidence)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
