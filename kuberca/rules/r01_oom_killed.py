"""R01 OOMKilled — Tier 1 rule.

Matches pods killed by the Linux OOM killer (reason OOMKilled / OOMKilling).
Correlates via the ReplicaSet → Deployment ownership chain and diffs
memory-limit field changes in the Deployment spec.
"""

from __future__ import annotations

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

_logger = get_logger("rule.r01_oom_killed")

# Field paths that are causally relevant to OOMKilled (§4.3).
# Only changes under these prefixes earn the +0.15 diff bonus.
_MEMORY_FIELD_PREFIX = "spec.template.spec.containers"


def _is_memory_change(fc: FieldChange) -> bool:
    """Return True if the FieldChange is a memory-limit change."""
    return fc.field_path.startswith(_MEMORY_FIELD_PREFIX) and "resources" in fc.field_path and "memory" in fc.field_path


def _find_owning_deployment(
    pod_name: str,
    namespace: str,
    cache: ResourceCache,
) -> CachedResourceView | None:
    """Walk Pod → ReplicaSet → Deployment using label matching.

    Strategy:
      1. List ReplicaSets in the namespace; find one whose pod-template
         labels are a subset of the pod's labels (if pod_name is known).
         Fall back to name-prefix matching (pod names have the RS name as prefix).
      2. From the RS, find the owning Deployment by name prefix.

    Returns the Deployment CachedResourceView or None.
    """
    # ReplicaSet name is the pod name up to the last two dash-separated segments
    # e.g. "my-app-7b4f8c6d-x2kj" → RS = "my-app-7b4f8c6d"
    parts = pod_name.rsplit("-", 2)
    rs_name_prefix = parts[0] if len(parts) >= 3 else pod_name

    # Try to find matching ReplicaSet by name prefix
    replicasets = cache.list("ReplicaSet", namespace)
    owning_rs: CachedResourceView | None = None
    for rs in replicasets:
        if rs.name == rs_name_prefix or pod_name.startswith(rs.name + "-"):
            owning_rs = rs
            break

    if owning_rs is None:
        return None

    # Deployment name is typically the RS name without the last hash segment
    rs_parts = owning_rs.name.rsplit("-", 1)
    deploy_name_prefix = rs_parts[0] if len(rs_parts) == 2 else owning_rs.name  # noqa: SIM210

    deployments = cache.list("Deployment", namespace)
    for deploy in deployments:
        if deploy.name == deploy_name_prefix or owning_rs.name.startswith(deploy.name + "-"):
            return deploy

    return None


def _current_memory_limit(deployment: CachedResourceView) -> str:
    """Extract the memory limit string from the first container in the spec.

    Returns "<unknown>" if the spec structure is not as expected.
    """
    try:
        raw_template = deployment.spec.get("template", {})
        template: dict[str, object] = raw_template if isinstance(raw_template, dict) else {}
        raw_spec = template.get("spec", {})
        spec_dict: dict[str, object] = raw_spec if isinstance(raw_spec, dict) else {}
        containers = spec_dict.get("containers", [])
        if containers and isinstance(containers, list):
            raw_first = containers[0]
            if not isinstance(raw_first, dict):
                return "<unknown>"
            first: dict[str, object] = raw_first
            resources = first.get("resources")
            if not isinstance(resources, dict):
                return "<unknown>"
            limits = resources.get("limits")
            if not isinstance(limits, dict):
                return "<unknown>"
            limit = limits.get("memory")
            if limit:
                return str(limit)
    except (AttributeError, TypeError, IndexError):
        pass
    return "<unknown>"


class OOMKilledRule(Rule):
    """Matches OOMKilled / OOMKilling events and correlates memory-limit changes."""

    rule_id = "R01_oom_killed"
    display_name = "OOMKilled"
    priority = 10
    base_confidence = 0.55
    resource_dependencies = ["Pod", "Deployment", "ReplicaSet"]
    relevant_field_paths = ["spec.template.spec.containers"]

    def match(self, event: EventRecord) -> bool:
        """Return True for OOMKilled or OOMKilling reasons."""
        return event.reason in ("OOMKilled", "OOMKilling")

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find the owning Deployment and diff memory limit changes."""
        t_start = time.monotonic()
        objects_queried = 0

        deployment = _find_owning_deployment(event.resource_name, event.namespace, cache)
        # Count: all ReplicaSets + all Deployments listed
        objects_queried += len(cache.list("ReplicaSet", event.namespace))
        objects_queried += len(cache.list("Deployment", event.namespace))

        related_resources: list[CachedResourceView] = []
        raw_changes: list[FieldChange] = []

        if deployment is not None:
            related_resources.append(deployment)
            raw_changes = ledger.diff(
                "Deployment",
                event.namespace,
                deployment.name,
                since_hours=2.0,
            )
            objects_queried += 1  # ledger diff counts as one logical query

        # Filter to memory-limit-only changes (causally relevant per §4.3).
        memory_changes: list[FieldChange] = [fc for fc in raw_changes if _is_memory_change(fc)]

        duration_ms = (time.monotonic() - t_start) * 1000.0
        return CorrelationResult(
            changes=memory_changes,
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
        """Produce the RuleResult for this OOMKilled match."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()

        confidence = compute_confidence(self, correlation, event)
        evidence: list[EvidenceItem] = []
        affected: list[AffectedResource] = []

        # Always include the triggering event as evidence.
        evidence.append(
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(f"Pod {event.resource_name} received {event.reason} (count={event.count}): {event.message}"),
            )
        )
        affected.append(
            AffectedResource(
                kind="Pod",
                namespace=event.namespace,
                name=event.resource_name,
            )
        )

        deploy: CachedResourceView | None = correlation.related_resources[0] if correlation.related_resources else None

        if deploy:
            affected.append(
                AffectedResource(
                    kind="Deployment",
                    namespace=deploy.namespace,
                    name=deploy.name,
                )
            )

        # Build root cause and remediation strings.
        if correlation.changes:
            # Use the most-recent memory change as the primary evidence item.
            latest_change: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest_change.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Memory limit changed on {deploy.name if deploy else 'deployment'} "
                        f"from {latest_change.old_value!r} to {latest_change.new_value!r} "
                        f"at {latest_change.changed_at.isoformat()}"
                    ),
                )
            )
            root_cause = (
                f"Pod {event.resource_name} was OOM-killed. "
                f"Memory limit was changed from {latest_change.old_value} to "
                f"{latest_change.new_value} in deployment "
                f"{deploy.name if deploy else '<unknown>'} "
                f"at {latest_change.changed_at.isoformat()}."
            )
            container_hint = _container_name_from_path(latest_change.field_path)
            remediation = (
                f"Increase memory limit for container {container_hint} in deployment "
                f"{deploy.name if deploy else event.resource_name}. "
                f"New limit {latest_change.new_value} may be insufficient. "
                f"Consider profiling memory usage with `kubectl top pod "
                f"{event.resource_name} -n {event.namespace}`."
            )
        else:
            current_limit: str = _current_memory_limit(deploy) if deploy else "<unknown>"
            root_cause = (
                f"Pod {event.resource_name} was OOM-killed. "
                f"No recent memory limit change detected; "
                f"current limit is {current_limit}."
            )
            remediation = (
                f"Increase memory limit for containers in deployment "
                f"{deploy.name if deploy else event.resource_name}. "
                f"Current limit: {current_limit}. "
                f"Consider profiling memory usage with "
                f"`kubectl top pod {event.resource_name} -n {event.namespace}`."
            )

        _logger.info(
            "r01_match",
            pod=event.resource_name,
            namespace=event.namespace,
            deploy=deploy.name if deploy else None,
            confidence=confidence,
            memory_changes=len(correlation.changes),
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )


def _container_name_from_path(field_path: str) -> str:
    """Extract a human-readable container reference from a JSONPath-style field_path.

    e.g. "spec.template.spec.containers[0].resources.limits.memory" → "containers[0]"
    Falls back to "the affected container" if parsing fails.
    """
    try:
        start = field_path.index("containers")
        segment = field_path[start:].split(".")[0]
        return segment
    except (ValueError, IndexError):
        return "the affected container"
