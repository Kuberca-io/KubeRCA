"""R07 Config Drift — Tier 2 rule.

Matches pod failures that coincide with a recent ConfigMap or Secret
modification. ConfigMap values are not stored (redaction policy §5.2);
the rule uses modification timestamps and key-set changes as evidence.
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

_logger = get_logger("rule.r07_config_drift")

_CONFIG_DRIFT_REASONS = frozenset(
    {
        "BackOff",
        "Failed",
        "FailedMount",
        "Unhealthy",
    }
)

_RE_CONFIG_MENTION = re.compile(
    r"configmap|config map|secret|configuration",
    re.IGNORECASE,
)


def _is_config_change(fc: FieldChange) -> bool:
    """Return True for ConfigMap/Secret metadata or key changes."""
    path = fc.field_path.lower()
    return "configmap" in path or "secret" in path or "data" in path


def _find_owning_deployment(
    pod_name: str,
    namespace: str,
    cache: ResourceCache,
) -> CachedResourceView | None:
    parts = pod_name.rsplit("-", 2)
    rs_prefix = parts[0] if len(parts) >= 3 else pod_name
    for rs in cache.list("ReplicaSet", namespace):
        if rs.name == rs_prefix or pod_name.startswith(rs.name + "-"):
            rs_parts = rs.name.rsplit("-", 1)
            deploy_prefix = rs_parts[0] if len(rs_parts) == 2 else rs.name  # noqa: SIM210
            for deploy in cache.list("Deployment", namespace):
                if deploy.name == deploy_prefix or rs.name.startswith(deploy.name + "-"):
                    return deploy
    return None


class ConfigDriftRule(Rule):
    """Matches pod failures coinciding with ConfigMap/Secret updates."""

    rule_id = "R07_config_drift"
    display_name = "Config Drift"
    priority = 20
    base_confidence = 0.55
    resource_dependencies = ["Pod", "Deployment", "ConfigMap"]
    relevant_field_paths = ["spec.template.spec.containers"]

    def match(self, event: EventRecord) -> bool:
        """Return True for pod failure events mentioning configuration."""
        if event.reason in _CONFIG_DRIFT_REASONS:
            return bool(_RE_CONFIG_MENTION.search(event.message))
        return False

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find referenced ConfigMaps and diff for key-set changes."""
        t_start = time.monotonic()
        objects_queried = 0

        deployment = _find_owning_deployment(event.resource_name, event.namespace, cache)
        objects_queried += len(cache.list("ReplicaSet", event.namespace))
        objects_queried += len(cache.list("Deployment", event.namespace))

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        if deployment is not None:
            related_resources.append(deployment)
            try:
                raw_template = deployment.spec.get("template", {})
                template: dict[str, object] = raw_template if isinstance(raw_template, dict) else {}
                raw_spec_val = template.get("spec", {})
                spec_inner: dict[str, object] = raw_spec_val if isinstance(raw_spec_val, dict) else {}
                containers = spec_inner.get("containers", [])
                if isinstance(containers, list):
                    for container in containers:
                        if not isinstance(container, dict):
                            continue
                        for env_source in container.get("envFrom") or []:
                            if not isinstance(env_source, dict):
                                continue
                            cm_ref = env_source.get("configMapRef", {})
                            if isinstance(cm_ref, dict):
                                cm_name = cm_ref.get("name")
                                if isinstance(cm_name, str) and cm_name:
                                    cm = cache.get("ConfigMap", event.namespace, cm_name)
                                    objects_queried += 1
                                    if cm is not None:
                                        related_resources.append(cm)
                                    raw = ledger.diff("ConfigMap", event.namespace, cm_name, since_hours=2.0)
                                    objects_queried += 1
                                    for fc in raw:
                                        if _is_config_change(fc):
                                            relevant_changes.append(fc)
            except (AttributeError, TypeError):
                pass

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
        """Produce the RuleResult for this config-drift match."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(
                    f"Pod {event.resource_name} failed with config-related message "
                    f"(count={event.count}): {event.message[:300]}"
                ),
            )
        ]
        affected: list[AffectedResource] = [
            AffectedResource(
                kind="Pod",
                namespace=event.namespace,
                name=event.resource_name,
            )
        ]

        deploy: CachedResourceView | None = None
        for res in correlation.related_resources:
            if res.kind == "Deployment":
                deploy = res
                affected.append(AffectedResource(kind="Deployment", namespace=res.namespace, name=res.name))

        if correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"ConfigMap/Secret change detected: '{latest.field_path}' "
                        f"{latest.old_value!r} → {latest.new_value!r}"
                    ),
                )
            )
            root_cause = (
                f"Pod {event.resource_name} failed after a configuration change. "
                f"Field '{latest.field_path}' was modified at "
                f"{latest.changed_at.isoformat()}."
            )
        else:
            root_cause = (
                f"Pod {event.resource_name} failed with a configuration-related error. "
                f"No recent ConfigMap/Secret changes detected in the 2-hour window."
            )

        remediation = (
            f"Review recent ConfigMap/Secret changes: "
            f"`kubectl get configmap -n {event.namespace}`. "
            f"Roll back deployment "
            f"{deploy.name if deploy else event.resource_name} if a bad config was applied: "
            f"`kubectl rollout undo deployment/"
            f"{deploy.name if deploy else event.resource_name} -n {event.namespace}`."
        )

        _logger.info(
            "r07_match",
            pod=event.resource_name,
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
