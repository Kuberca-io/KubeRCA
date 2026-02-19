"""R15 FailedScheduling-NodeNotAvailable -- Tier 2 rule.

Matches FailedScheduling events where no node can satisfy constraints
(taints, affinity, resource limits).
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

_logger = get_logger("rule.r15_failedscheduling_node")

_NODE_KEYWORDS = frozenset({"nodes are available", "didn't match", "taints", "toleration", "affinity"})


class FailedSchedulingNodeRule(Rule):
    """Matches FailedScheduling events due to node constraints."""

    rule_id = "R15_failedscheduling_node"
    display_name = "FailedScheduling -- Node Not Available"
    priority = 18
    base_confidence = 0.55
    resource_dependencies = ["Pod", "Node"]
    relevant_field_paths = ["spec.template.spec.nodeSelector", "spec.template.spec.tolerations"]

    def match(self, event: EventRecord) -> bool:
        if event.reason != "FailedScheduling":
            return False
        msg = event.message.lower()
        return any(kw in msg for kw in _NODE_KEYWORDS)

    def correlate(self, event: EventRecord, cache: ResourceCache, ledger: ChangeLedger) -> CorrelationResult:
        t_start = time.monotonic()
        objects_queried = 0
        related_resources: list[CachedResourceView] = []

        nodes = cache.list("Node", "")
        objects_queried += len(nodes)
        for node in nodes[:10]:
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
        node_count = len(correlation.related_resources)

        evidence = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=f"Pod {event.resource_name} FailedScheduling (count={event.count}): {event.message[:300]}",
            )
        ]
        affected = [AffectedResource(kind="Pod", namespace=event.namespace, name=event.resource_name)]

        msg = event.message.lower()
        if "taint" in msg:
            root_cause = f"Pod {event.resource_name} cannot be scheduled: no nodes match required tolerations. {node_count} nodes available."
        elif "affinity" in msg:
            root_cause = f"Pod {event.resource_name} cannot be scheduled: node affinity rules cannot be satisfied. {node_count} nodes available."
        else:
            root_cause = f"Pod {event.resource_name} cannot be scheduled: no available nodes meet constraints. {node_count} nodes in cluster."

        remediation = (
            f"Check node taints: `kubectl describe nodes | grep -A5 Taints`. "
            f"Check pod tolerations: `kubectl describe pod {event.resource_name} -n {event.namespace}`. "
            f"Consider adding nodes or adjusting scheduling constraints."
        )

        _logger.info("r15_match", pod=event.resource_name, namespace=event.namespace, confidence=confidence)

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
