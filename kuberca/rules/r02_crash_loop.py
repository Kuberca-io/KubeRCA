"""R02 CrashLoopBackOff — Tier 1 rule.

Matches CrashLoop events (reason BackOff, count >= 3) and correlates
image changes, ConfigMap key-set changes, and env-key changes on the
owning Deployment.

ConfigMap values are NOT stored per the redaction policy (§5.2).
R02 treats ConfigMap key-set changes and resourceVersion/timestamp changes
as weaker evidence and scores them accordingly.
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

_logger = get_logger("rule.r02_crash_loop")

# Relevant field path prefixes for CrashLoop causality (§4.3).
# Changes outside these paths do NOT earn the +0.15 diff bonus.
_IMAGE_PREFIX = "spec.template.spec.containers"
_ENV_PREFIX = "spec.template.spec.containers"
_CONFIGMAP_PREFIX = "metadata"  # ConfigMap key/version changes appear at metadata level

# Field path fragments that indicate a relevant change.
_RELEVANT_FRAGMENTS = ("image", "env", "configMap", "configmap", "data")


def _is_relevant_change(fc: FieldChange) -> bool:
    """Return True if this FieldChange is causally relevant to a CrashLoop."""
    path = fc.field_path.lower()
    return any(frag in path for frag in _RELEVANT_FRAGMENTS)


def _find_owning_deployment(
    pod_name: str,
    namespace: str,
    cache: ResourceCache,
) -> CachedResourceView | None:
    """Resolve Pod → ReplicaSet → Deployment via name-prefix heuristic."""
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
    deploy_name_prefix = rs_parts[0] if len(rs_parts) == 2 else owning_rs.name  # noqa: SIM210

    deployments = cache.list("Deployment", namespace)
    for deploy in deployments:
        if deploy.name == deploy_name_prefix or owning_rs.name.startswith(deploy.name + "-"):
            return deploy

    return None


def _diff_configmaps(
    deployment: CachedResourceView,
    namespace: str,
    cache: ResourceCache,
    ledger: ChangeLedger,
) -> list[FieldChange]:
    """Find ConfigMap key-set changes referenced by the Deployment.

    ConfigMap values are redacted; we look for key additions/removals
    or resourceVersion changes as a signal that config was modified.
    Returns rule-relevant FieldChange objects for each changed ConfigMap.
    """
    changes: list[FieldChange] = []
    try:
        raw_template = deployment.spec.get("template", {})
        template: dict[str, object] = raw_template if isinstance(raw_template, dict) else {}
        raw_spec = template.get("spec", {})
        spec_dict: dict[str, object] = raw_spec if isinstance(raw_spec, dict) else {}
        containers = spec_dict.get("containers", [])
        if not isinstance(containers, list):
            return changes

        for container in containers:
            if not isinstance(container, dict):
                continue
            env_from = container.get("envFrom", []) or []
            if not isinstance(env_from, list):
                continue
            for env_source in env_from:
                if not isinstance(env_source, dict):
                    continue
                cm_ref = env_source.get("configMapRef", {})
                if not isinstance(cm_ref, dict):
                    continue
                cm_name = cm_ref.get("name")
                if not isinstance(cm_name, str) or not cm_name:
                    continue
                raw = ledger.diff("ConfigMap", namespace, cm_name, since_hours=2.0)
                for fc in raw:
                    changes.append(fc)
    except (AttributeError, TypeError):
        pass
    return changes


class CrashLoopRule(Rule):
    """Matches CrashLoopBackOff and correlates causal changes on the owning Deployment."""

    rule_id = "R02_crash_loop"
    display_name = "CrashLoopBackOff"
    priority = 10
    base_confidence = 0.50
    resource_dependencies = ["Pod", "Deployment", "ReplicaSet", "ConfigMap"]
    relevant_field_paths = ["spec.template.spec.containers"]

    def match(self, event: EventRecord) -> bool:
        """Return True for BackOff events seen at least 3 times."""
        return event.reason == "BackOff" and event.count >= 3

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find the owning Deployment and diff image, env, and ConfigMap changes."""
        t_start = time.monotonic()
        objects_queried = 0

        deployment = _find_owning_deployment(event.resource_name, event.namespace, cache)
        objects_queried += len(cache.list("ReplicaSet", event.namespace))
        objects_queried += len(cache.list("Deployment", event.namespace))

        related_resources: list[CachedResourceView] = []
        all_changes: list[FieldChange] = []

        if deployment is not None:
            related_resources.append(deployment)

            # Diff the Deployment itself for image/env changes.
            raw_deploy_changes = ledger.diff(
                "Deployment",
                event.namespace,
                deployment.name,
                since_hours=2.0,
            )
            objects_queried += 1

            for fc in raw_deploy_changes:
                if _is_relevant_change(fc):
                    all_changes.append(fc)

            # Diff referenced ConfigMaps for key-set / version changes.
            cm_changes = _diff_configmaps(deployment, event.namespace, cache, ledger)
            for fc in cm_changes:
                if _is_relevant_change(fc):
                    all_changes.append(fc)
                    objects_queried += 1

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
        """Produce the RuleResult for this CrashLoopBackOff match."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()

        confidence = compute_confidence(self, correlation, event)
        evidence: list[EvidenceItem] = []
        affected: list[AffectedResource] = []

        evidence.append(
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(f"Pod {event.resource_name} is in CrashLoopBackOff (count={event.count}): {event.message}"),
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

        if correlation.changes:
            # Classify the most recent change for the root-cause message.
            latest_change: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest_change.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=_change_summary(latest_change, deploy),
                )
            )
            change_type = _classify_change(latest_change.field_path)
            root_cause = (
                f"Pod {event.resource_name} is crash-looping "
                f"(restart count: {event.count}). "
                f"Most recent {change_type} on "
                f"{deploy.name if deploy else 'the deployment'} at "
                f"{latest_change.changed_at.isoformat()} may have caused the failure."
            )
        else:
            root_cause = (
                f"Pod {event.resource_name} is crash-looping "
                f"(restart count: {event.count}). "
                f"No recent image, config, or environment changes detected."
            )

        remediation = (
            f"Roll back deployment "
            f"{deploy.name if deploy else event.resource_name} "
            f"to the previous revision: "
            f"`kubectl rollout undo deployment/"
            f"{deploy.name if deploy else event.resource_name} "
            f"-n {event.namespace}`"
        )

        _logger.info(
            "r02_match",
            pod=event.resource_name,
            namespace=event.namespace,
            deploy=deploy.name if deploy else None,
            confidence=confidence,
            relevant_changes=len(correlation.changes),
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )


def _classify_change(field_path: str) -> str:
    """Return a human-readable change category for a field path."""
    path_lower = field_path.lower()
    if "image" in path_lower:
        return "image update"
    if "configmap" in path_lower or "configMap" in path_lower:
        return "ConfigMap change"
    if "env" in path_lower:
        return "environment variable change"
    return "configuration change"


def _change_summary(fc: FieldChange, deploy: CachedResourceView | None) -> str:
    """Build a human-readable evidence summary for a FieldChange."""
    change_type = _classify_change(fc.field_path)
    deploy_name = deploy.name if deploy else "deployment"
    return (
        f"{change_type.capitalize()} detected on {deploy_name} "
        f"at field '{fc.field_path}': {fc.old_value!r} → {fc.new_value!r} "
        f"at {fc.changed_at.isoformat()}"
    )
