"""R03 FailedScheduling — Tier 1 rule.

Matches FailedScheduling events and correlates unmet scheduling constraints
by parsing the scheduler message against four recognized patterns:
  1. Insufficient resources (CPU/memory/ephemeral-storage)
  2. Node selector / affinity mismatch
  3. Taint / toleration mismatch
  4. PVC binding failure

Unrecognized scheduler messages result in an empty correlation (no diff
bonus) and a warning appended to the EvaluationMeta by the caller.
"""

from __future__ import annotations

import re
import time
from datetime import UTC
from enum import StrEnum

from kuberca.models.analysis import (
    AffectedResource,
    CorrelationResult,
    EvidenceItem,
    RuleResult,
)
from kuberca.models.events import EventRecord, EvidenceType
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import rule_matches_total, scheduler_unknown_pattern_total
from kuberca.rules.base import ChangeLedger, ResourceCache, Rule
from kuberca.rules.confidence import compute_confidence

_logger = get_logger("rule.r03_failed_scheduling")


class _SchedulerPattern(StrEnum):
    INSUFFICIENT_RESOURCES = "insufficient_resources"
    NODE_SELECTOR_MISMATCH = "node_selector_mismatch"
    TAINT_TOLERATION = "taint_toleration"
    PVC_BINDING = "pvc_binding"
    UNKNOWN = "unknown"


# Compiled patterns for scheduler message classification (§4.5).
_RE_INSUFFICIENT = re.compile(r"Insufficient\s+(cpu|memory|ephemeral-storage)", re.IGNORECASE)
_RE_NODE_SELECTOR = re.compile(
    r"didn't match Pod's node affinity/selector|"
    r"node\(s\) didn't match node selector|"
    r"didn't match node selector",
    re.IGNORECASE,
)
_RE_TAINT = re.compile(
    r"had taint.+that the pod didn't tolerate",
    re.IGNORECASE | re.DOTALL,
)
_RE_PVC = re.compile(
    r"persistentvolumeclaim.+(not found|unbound)",
    re.IGNORECASE,
)


def _classify_scheduler_message(message: str) -> _SchedulerPattern:
    """Return the recognized scheduler pattern or UNKNOWN."""
    if _RE_INSUFFICIENT.search(message):
        return _SchedulerPattern.INSUFFICIENT_RESOURCES
    if _RE_NODE_SELECTOR.search(message):
        return _SchedulerPattern.NODE_SELECTOR_MISMATCH
    if _RE_TAINT.search(message):
        return _SchedulerPattern.TAINT_TOLERATION
    if _RE_PVC.search(message):
        return _SchedulerPattern.PVC_BINDING
    return _SchedulerPattern.UNKNOWN


def _find_owning_deployment(
    pod_name: str,
    namespace: str,
    cache: ResourceCache,
) -> CachedResourceView | None:
    """Resolve Pod → Deployment via name-prefix heuristic."""
    parts = pod_name.rsplit("-", 2)
    rs_name_prefix = parts[0] if len(parts) >= 3 else pod_name

    replicasets = cache.list("ReplicaSet", namespace)
    owning_rs: CachedResourceView | None = None
    for rs in replicasets:
        if rs.name == rs_name_prefix or pod_name.startswith(rs.name + "-"):
            owning_rs = rs
            break

    if owning_rs is None:
        return None

    rs_parts = owning_rs.name.rsplit("-", 1)
    deploy_prefix = rs_parts[0] if len(rs_parts) == 2 else owning_rs.name  # noqa: SIM210

    deployments = cache.list("Deployment", namespace)
    for deploy in deployments:
        if deploy.name == deploy_prefix or owning_rs.name.startswith(deploy.name + "-"):
            return deploy

    return None


def _is_scheduling_relevant(fc: FieldChange) -> bool:
    """Return True if the change is causally relevant to a scheduling failure."""
    path = fc.field_path.lower()
    keywords = (
        "nodeselector",
        "affinity",
        "tolerations",
        "resources.requests",
        "resources.limits",
    )
    return any(kw in path for kw in keywords)


class FailedSchedulingRule(Rule):
    """Matches FailedScheduling events and diagnoses unmet scheduling constraints."""

    rule_id = "R03_failed_scheduling"
    display_name = "FailedScheduling"
    priority = 20
    base_confidence = 0.60
    resource_dependencies = ["Pod", "Node", "Deployment"]
    relevant_field_paths = ["spec.template.spec"]

    def match(self, event: EventRecord) -> bool:
        """Return True for FailedScheduling events."""
        return event.reason == "FailedScheduling"

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Parse scheduler message, check node cache, diff Deployment for constraint changes."""
        t_start = time.monotonic()
        objects_queried = 0

        pattern = _classify_scheduler_message(event.message)

        if pattern == _SchedulerPattern.UNKNOWN:
            scheduler_unknown_pattern_total.inc()
            _logger.warning(
                "r03_unknown_scheduler_pattern",
                message=event.message[:200],
                pod=event.resource_name,
                namespace=event.namespace,
            )
            duration_ms = (time.monotonic() - t_start) * 1000.0
            return CorrelationResult(
                changes=[],
                related_events=[],
                related_resources=[],
                objects_queried=0,
                duration_ms=duration_ms,
            )

        # Gather node information for constraint analysis.
        nodes = cache.list("Node")
        objects_queried += len(nodes)

        related_resources: list[CachedResourceView] = []
        all_changes: list[FieldChange] = []

        # Include a sample of matching / non-matching nodes.
        related_resources.extend(nodes[:5])

        # Find owning Deployment and diff for nodeSelector / affinity changes.
        deployment = _find_owning_deployment(event.resource_name, event.namespace, cache)
        objects_queried += len(cache.list("ReplicaSet", event.namespace))
        objects_queried += len(cache.list("Deployment", event.namespace))

        if deployment is not None:
            related_resources.append(deployment)
            raw_changes = ledger.diff(
                "Deployment",
                event.namespace,
                deployment.name,
                since_hours=2.0,
            )
            objects_queried += 1
            for fc in raw_changes:
                if _is_scheduling_relevant(fc):
                    all_changes.append(fc)

        # For PVC pattern, check PVC status in cache.
        if pattern == _SchedulerPattern.PVC_BINDING:
            pvcs = cache.list("PersistentVolumeClaim", event.namespace)
            objects_queried += len(pvcs)
            # Include unbound PVCs as related resources.
            for pvc in pvcs:
                phase = pvc.status.get("phase", "")
                if phase != "Bound":
                    related_resources.append(pvc)

        duration_ms = (time.monotonic() - t_start) * 1000.0
        return CorrelationResult(
            changes=all_changes,
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
        """Produce the RuleResult for this FailedScheduling match."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()

        confidence = compute_confidence(self, correlation, event)
        pattern = _classify_scheduler_message(event.message)

        evidence: list[EvidenceItem] = []
        affected: list[AffectedResource] = []

        evidence.append(
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(f"Pod {event.resource_name} failed scheduling (count={event.count}): {event.message[:300]}"),
            )
        )
        affected.append(
            AffectedResource(
                kind="Pod",
                namespace=event.namespace,
                name=event.resource_name,
            )
        )

        # Include Deployment in affected resources if found.
        deploy: CachedResourceView | None = None
        for res in correlation.related_resources:
            if res.kind == "Deployment":
                deploy = res
                affected.append(
                    AffectedResource(
                        kind="Deployment",
                        namespace=res.namespace,
                        name=res.name,
                    )
                )
                break

        if correlation.changes:
            latest_change: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest_change.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Scheduling-related field changed: "
                        f"'{latest_change.field_path}' "
                        f"{latest_change.old_value!r} → {latest_change.new_value!r} "
                        f"at {latest_change.changed_at.isoformat()}"
                    ),
                )
            )

        node_count = sum(1 for r in correlation.related_resources if r.kind == "Node")
        node_truncated = len(correlation.related_resources) > 0 and cache_truncated_note(
            correlation, cache_truncated=False
        )

        root_cause, remediation = _build_diagnosis(
            event=event,
            pattern=pattern,
            node_count=node_count,
            correlation=correlation,
            deploy=deploy,
            node_truncated=node_truncated,
        )

        # Unknown pattern → confidence stays at base, no diff bonus applies.
        if pattern == _SchedulerPattern.UNKNOWN:
            confidence = self.base_confidence

        _logger.info(
            "r03_match",
            pod=event.resource_name,
            namespace=event.namespace,
            pattern=pattern.value,
            confidence=confidence,
            node_count=node_count,
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )


def cache_truncated_note(correlation: CorrelationResult, *, cache_truncated: bool) -> bool:
    """Return True if node data was truncated during correlation."""
    return cache_truncated


def _build_diagnosis(
    event: EventRecord,
    pattern: _SchedulerPattern,
    node_count: int,
    correlation: CorrelationResult,
    deploy: CachedResourceView | None,
    node_truncated: bool,
) -> tuple[str, str]:
    """Return (root_cause, remediation) strings for the given scheduler pattern."""
    pod = event.resource_name
    ns = event.namespace
    deploy_name = deploy.name if deploy else pod

    if pattern == _SchedulerPattern.INSUFFICIENT_RESOURCES:
        resource_match = _RE_INSUFFICIENT.search(event.message)
        resource_type = resource_match.group(1) if resource_match else "resource"
        root_cause = (
            f"Pod {pod} could not be scheduled: insufficient {resource_type} "
            f"on available nodes. "
            f"Checked {node_count} node(s)."
        )
        if node_truncated:
            root_cause += f" (node analysis based on {node_count} of available nodes)"
        remediation = (
            f"Reduce resource requests for pod {pod} in deployment {deploy_name}, "
            f"or scale up the node pool with sufficient {resource_type}. "
            f"Check current usage: `kubectl top nodes`."
        )

    elif pattern == _SchedulerPattern.NODE_SELECTOR_MISMATCH:
        root_cause = (
            f"Pod {pod} could not be scheduled: no node satisfies the pod's "
            f"nodeSelector or affinity rules. "
            f"Checked {node_count} node(s)."
        )
        if node_truncated:
            root_cause += f" (node analysis based on {node_count} of available nodes)"
        remediation = (
            f"Review the nodeSelector and affinity rules for deployment {deploy_name}. "
            f"Verify that matching nodes exist: `kubectl get nodes --show-labels`."
        )

    elif pattern == _SchedulerPattern.TAINT_TOLERATION:
        root_cause = (
            f"Pod {pod} could not be scheduled: node(s) have taints that "
            f"the pod does not tolerate. "
            f"Checked {node_count} node(s)."
        )
        if node_truncated:
            root_cause += f" (node analysis based on {node_count} of available nodes)"
        remediation = (
            f"Add the required toleration to deployment {deploy_name}, "
            f"or remove/update the taint from the target node(s): "
            f"`kubectl describe nodes | grep Taints`."
        )

    elif pattern == _SchedulerPattern.PVC_BINDING:
        unbound_pvcs = [r.name for r in correlation.related_resources if r.kind == "PersistentVolumeClaim"]
        pvc_list = ", ".join(unbound_pvcs) if unbound_pvcs else "referenced PVC"
        root_cause = f"Pod {pod} could not be scheduled: PersistentVolumeClaim {pvc_list} is not bound or not found."
        remediation = (
            f"Check PVC status: `kubectl get pvc -n {ns}`. Ensure the StorageClass exists and PV is available."
        )

    else:
        # UNKNOWN pattern
        root_cause = (
            f"Pod {pod} failed scheduling. Scheduler message not in the recognized "
            f"pattern set — diagnosis may be incomplete. "
            f"Message: {event.message[:200]}"
        )
        remediation = f"Inspect the scheduler events for pod {pod}: `kubectl describe pod {pod} -n {ns}`."

    return root_cause, remediation
