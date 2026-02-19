"""R04 ImagePullBackOff — Tier 2 rule.

Matches ErrImagePull and ImagePullBackOff events and correlates image tag
changes and registry Secret reference changes on the owning Deployment.
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

_logger = get_logger("rule.r04_image_pull")

_IMAGE_PULL_REASONS = frozenset({"ErrImagePull", "ImagePullBackOff"})


def _is_image_or_secret_change(fc: FieldChange) -> bool:
    """Return True for image tag or imagePullSecrets field changes."""
    path = fc.field_path.lower()
    return "image" in path or "imagepullsecret" in path


def _find_owning_deployment(
    pod_name: str,
    namespace: str,
    cache: ResourceCache,
) -> CachedResourceView | None:
    """Resolve Pod → Deployment via ReplicaSet name-prefix heuristic."""
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


class ImagePullRule(Rule):
    """Matches image pull failures and correlates image/secret changes."""

    rule_id = "R04_image_pull"
    display_name = "ImagePullBackOff"
    priority = 15
    base_confidence = 0.60
    resource_dependencies = ["Pod", "Deployment", "ReplicaSet"]
    relevant_field_paths = ["spec.template.spec.containers"]

    def match(self, event: EventRecord) -> bool:
        """Return True for ErrImagePull or ImagePullBackOff reasons."""
        return event.reason in _IMAGE_PULL_REASONS

    def correlate(
        self,
        event: EventRecord,
        cache: ResourceCache,
        ledger: ChangeLedger,
    ) -> CorrelationResult:
        """Find owning Deployment and diff image tag / imagePullSecrets changes."""
        t_start = time.monotonic()
        objects_queried = 0

        deployment = _find_owning_deployment(event.resource_name, event.namespace, cache)
        objects_queried += len(cache.list("ReplicaSet", event.namespace))
        objects_queried += len(cache.list("Deployment", event.namespace))

        related_resources: list[CachedResourceView] = []
        relevant_changes: list[FieldChange] = []

        if deployment is not None:
            related_resources.append(deployment)
            raw = ledger.diff("Deployment", event.namespace, deployment.name, since_hours=2.0)
            objects_queried += 1
            for fc in raw:
                if _is_image_or_secret_change(fc):
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
        """Produce the RuleResult for this image pull failure."""
        rule_matches_total.labels(rule_id=self.rule_id).inc()
        confidence = compute_confidence(self, correlation, event)

        evidence: list[EvidenceItem] = [
            EvidenceItem(
                type=EvidenceType.EVENT,
                timestamp=event.last_seen.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                summary=(
                    f"Pod {event.resource_name} failed to pull image (count={event.count}): {event.message[:300]}"
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

        deploy: CachedResourceView | None = correlation.related_resources[0] if correlation.related_resources else None
        if deploy:
            affected.append(AffectedResource(kind="Deployment", namespace=deploy.namespace, name=deploy.name))

        if correlation.changes:
            latest: FieldChange = max(correlation.changes, key=lambda fc: fc.changed_at)
            evidence.append(
                EvidenceItem(
                    type=EvidenceType.CHANGE,
                    timestamp=latest.changed_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                    summary=(
                        f"Image/secret field changed on "
                        f"{deploy.name if deploy else 'deployment'}: "
                        f"'{latest.field_path}' "
                        f"{latest.old_value!r} → {latest.new_value!r}"
                    ),
                )
            )
            root_cause = (
                f"Pod {event.resource_name} cannot pull its image. "
                f"Image was recently changed on "
                f"{deploy.name if deploy else 'the deployment'} "
                f"from {latest.old_value} to {latest.new_value}. "
                f"The new tag may be invalid or the registry may be unreachable."
            )
        else:
            root_cause = (
                f"Pod {event.resource_name} cannot pull its image. "
                f"No recent image change detected. "
                f"Check that the image tag exists and registry credentials are valid."
            )

        deploy_name = deploy.name if deploy else event.resource_name
        remediation = (
            f"Verify the image tag exists in the registry. "
            f"Check imagePullSecrets are correct for deployment {deploy_name}. "
            f"Inspect pod events: `kubectl describe pod {event.resource_name} "
            f"-n {event.namespace}`."
        )

        _logger.info(
            "r04_match",
            pod=event.resource_name,
            namespace=event.namespace,
            confidence=confidence,
            image_changes=len(correlation.changes),
        )

        return RuleResult(
            rule_id=self.rule_id,
            root_cause=root_cause,
            confidence=confidence,
            evidence=evidence,
            affected_resources=affected,
            suggested_remediation=remediation,
        )
