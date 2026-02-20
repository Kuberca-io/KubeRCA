"""Tests for rules R04-R10 — match(), correlate(), and explain() methods."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import CachedResourceView, FieldChange
from kuberca.rules.r04_image_pull import ImagePullRule
from kuberca.rules.r05_hpa import HPARule
from kuberca.rules.r06_service_unreachable import ServiceUnreachableRule
from kuberca.rules.r07_config_drift import ConfigDriftRule
from kuberca.rules.r08_volume_mount import VolumeMountRule
from kuberca.rules.r09_node_pressure import NodePressureRule
from kuberca.rules.r10_readiness_probe import ReadinessProbeRule

_TS = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)


def _make_event(
    reason: str = "FailedMount",
    message: str = "test event",
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
    count: int = 1,
) -> EventRecord:
    return EventRecord(
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message=message,
        namespace=namespace,
        resource_kind=kind,
        resource_name=name,
        first_seen=_TS,
        last_seen=_TS,
        count=count,
    )


def _make_cache(
    get_return: CachedResourceView | None = None,
    list_return: list[CachedResourceView] | None = None,
) -> MagicMock:
    cache = MagicMock()
    cache.get.return_value = get_return
    cache.list.return_value = list_return or []
    return cache


def _make_resource_view(
    kind: str = "ConfigMap",
    namespace: str = "default",
    name: str = "my-cm",
    status: dict | None = None,
    spec: dict | None = None,
) -> CachedResourceView:
    return CachedResourceView(
        kind=kind,
        namespace=namespace,
        name=name,
        resource_version="1",
        labels={},
        annotations={},
        spec=spec or {},
        status=status or {},
        last_updated=_TS,
    )


def _make_ledger() -> MagicMock:
    ledger = MagicMock()
    ledger.diff.return_value = []
    return ledger


def _make_field_change(
    field_path: str = "spec.template.spec.containers[0].image",
    old_value: str = "app:v1",
    new_value: str = "app:v2",
) -> FieldChange:
    return FieldChange(
        field_path=field_path,
        old_value=old_value,
        new_value=new_value,
        changed_at=_TS,
    )


# =====================================================================
# R04 ImagePullRule
# =====================================================================


class TestR04ImagePullRule:
    def test_matches_errimagepull(self) -> None:
        rule = ImagePullRule()
        event = _make_event(reason="ErrImagePull", message="failed to pull image myrepo/app:latest")
        assert rule.match(event) is True

    def test_matches_imagepullbackoff(self) -> None:
        rule = ImagePullRule()
        event = _make_event(reason="ImagePullBackOff", message="Back-off pulling image myrepo/app:v2")
        assert rule.match(event) is True

    def test_no_match_for_other_reason(self) -> None:
        rule = ImagePullRule()
        event = _make_event(reason="OOMKilled", message="container killed")
        assert rule.match(event) is False

    def test_no_match_for_failedmount(self) -> None:
        rule = ImagePullRule()
        event = _make_event(reason="FailedMount", message="failed to mount volume")
        assert rule.match(event) is False

    def test_correlate_finds_owning_deployment_via_replicaset_chain(self) -> None:
        rule = ImagePullRule()
        # pod "my-app-abc12-xyz" → RS "my-app-abc12" → Deploy "my-app"
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        event = _make_event(reason="ErrImagePull", name="my-app-abc12-xyz")
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "Deployment" and r.name == "my-app" for r in corr.related_resources)

    def test_correlate_with_image_change_diff(self) -> None:
        rule = ImagePullRule()
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        fc = _make_field_change(
            field_path="spec.template.spec.containers[0].image",
            old_value="app:v1",
            new_value="app:v2",
        )
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="ErrImagePull", name="my-app-abc12-xyz")
        corr = rule.correlate(event, cache, ledger)
        assert len(corr.changes) == 1
        assert corr.changes[0].field_path == "spec.template.spec.containers[0].image"

    def test_correlate_no_deployment_found(self) -> None:
        rule = ImagePullRule()
        cache = _make_cache(list_return=[])
        event = _make_event(reason="ErrImagePull", name="orphan-pod")
        corr = rule.correlate(event, cache, _make_ledger())
        assert corr.related_resources == []
        assert corr.changes == []

    def test_explain_with_deployment_and_image_change(self) -> None:
        rule = ImagePullRule()
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        fc = _make_field_change(old_value="app:v1", new_value="app:v2")
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="ErrImagePull", name="my-app-abc12-xyz")
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert result.rule_id == "R04_image_pull"
        assert result.confidence > 0
        assert "app:v1" in result.root_cause or "app:v2" in result.root_cause

    def test_explain_without_deployment(self) -> None:
        rule = ImagePullRule()
        cache = _make_cache(list_return=[])
        event = _make_event(reason="ImagePullBackOff", name="orphan-pod")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R04_image_pull"
        assert "registry" in result.root_cause.lower() or "image" in result.root_cause.lower()

    def test_explain_without_changes(self) -> None:
        rule = ImagePullRule()
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        event = _make_event(reason="ErrImagePull", name="my-app-abc12-xyz")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert "No recent image change" in result.root_cause


# =====================================================================
# R05 HPARule
# =====================================================================


class TestR05HPARule:
    def test_matches_failedscale_reason(self) -> None:
        rule = HPARule()
        event = _make_event(reason="FailedScale", message="failed to scale deployment")
        assert rule.match(event) is True

    def test_matches_hpa_resource_kind_with_regex(self) -> None:
        rule = HPARule()
        event = _make_event(
            reason="Warning",
            kind="HorizontalPodAutoscaler",
            message="failed to compute desired replicas",
        )
        assert rule.match(event) is True

    def test_no_match_non_hpa_resource_with_unrelated_message(self) -> None:
        rule = HPARule()
        event = _make_event(reason="Warning", kind="Pod", message="container restarting")
        assert rule.match(event) is False

    def test_no_match_hpa_kind_unrelated_message(self) -> None:
        rule = HPARule()
        event = _make_event(
            reason="Warning",
            kind="HorizontalPodAutoscaler",
            message="reconciled successfully",
        )
        assert rule.match(event) is False

    def test_correlate_with_hpa_and_scale_target(self) -> None:
        rule = HPARule()
        hpa_view = _make_resource_view(
            kind="HorizontalPodAutoscaler",
            name="my-hpa",
            spec={"scaleTargetRef": {"name": "my-deploy", "kind": "Deployment"}},
        )
        deploy_view = _make_resource_view(kind="Deployment", name="my-deploy")
        cache = _make_cache()
        cache.get.side_effect = lambda kind, ns, name: hpa_view if kind == "HorizontalPodAutoscaler" else deploy_view
        event = _make_event(reason="FailedScale", kind="HorizontalPodAutoscaler", name="my-hpa")
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "HorizontalPodAutoscaler" for r in corr.related_resources)
        assert any(r.kind == "Deployment" for r in corr.related_resources)

    def test_correlate_no_hpa_found(self) -> None:
        rule = HPARule()
        cache = _make_cache(get_return=None)
        event = _make_event(reason="FailedScale", name="missing-hpa")
        corr = rule.correlate(event, cache, _make_ledger())
        assert corr.related_resources == []

    def test_explain_with_hpa_changes(self) -> None:
        rule = HPARule()
        hpa_view = _make_resource_view(kind="HorizontalPodAutoscaler", name="my-hpa")
        cache = _make_cache(get_return=hpa_view)
        fc = _make_field_change(field_path="spec.maxReplicas", old_value="10", new_value="2")
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="FailedScale", name="my-hpa")
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert result.rule_id == "R05_hpa_misbehavior"
        assert "maxReplicas" in result.root_cause

    def test_explain_without_hpa(self) -> None:
        rule = HPARule()
        cache = _make_cache(get_return=None)
        event = _make_event(reason="FailedScale", name="my-hpa")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R05_hpa_misbehavior"
        assert "failing to scale" in result.root_cause


# =====================================================================
# R06 ServiceUnreachableRule
# =====================================================================


class TestR06ServiceUnreachableRule:
    def test_matches_failedsync_reason(self) -> None:
        rule = ServiceUnreachableRule()
        event = _make_event(reason="FailedSync", message="sync failed for endpoint")
        assert rule.match(event) is True

    def test_matches_connection_refused_in_message(self) -> None:
        rule = ServiceUnreachableRule()
        event = _make_event(reason="Warning", message="connection refused to 10.0.0.5:8080")
        assert rule.match(event) is True

    def test_matches_no_endpoints_in_message(self) -> None:
        rule = ServiceUnreachableRule()
        event = _make_event(reason="Warning", message="no endpoints available for service")
        assert rule.match(event) is True

    def test_no_match_unrelated_event(self) -> None:
        rule = ServiceUnreachableRule()
        event = _make_event(reason="Pulled", message="Successfully pulled image")
        assert rule.match(event) is False

    def test_correlate_with_service_found(self) -> None:
        rule = ServiceUnreachableRule()
        svc_view = _make_resource_view(kind="Service", name="my-svc")
        cache = _make_cache(get_return=svc_view)
        event = _make_event(reason="FailedSync", name="my-svc")
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "Service" and r.name == "my-svc" for r in corr.related_resources)

    def test_correlate_with_service_selector_changes(self) -> None:
        rule = ServiceUnreachableRule()
        svc_view = _make_resource_view(kind="Service", name="my-svc")
        cache = _make_cache(get_return=svc_view)
        fc = _make_field_change(
            field_path="spec.selector.app",
            old_value="frontend",
            new_value="backend",
        )
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="FailedSync", name="my-svc")
        corr = rule.correlate(event, cache, ledger)
        assert len(corr.changes) == 1
        assert "selector" in corr.changes[0].field_path

    def test_correlate_no_service(self) -> None:
        rule = ServiceUnreachableRule()
        cache = _make_cache(get_return=None, list_return=[])
        event = _make_event(reason="FailedSync", name="missing-svc")
        corr = rule.correlate(event, cache, _make_ledger())
        assert corr.related_resources == []

    def test_explain_with_service_changes(self) -> None:
        rule = ServiceUnreachableRule()
        svc_view = _make_resource_view(kind="Service", name="my-svc")
        cache = _make_cache(get_return=svc_view)
        fc = _make_field_change(field_path="spec.selector.app", old_value="v1", new_value="v2")
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="FailedSync", name="my-svc")
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert result.rule_id == "R06_service_unreachable"
        assert "selector.app" in result.root_cause

    def test_explain_without_service_changes(self) -> None:
        rule = ServiceUnreachableRule()
        cache = _make_cache(get_return=None, list_return=[])
        event = _make_event(reason="FailedSync", name="my-svc")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R06_service_unreachable"
        assert "unreachable" in result.root_cause.lower()


# =====================================================================
# R07 ConfigDriftRule
# =====================================================================


class TestR07ConfigDriftRule:
    def test_matches_backoff_with_configmap_mention(self) -> None:
        rule = ConfigDriftRule()
        event = _make_event(reason="BackOff", message="configmap app-config not found")
        assert rule.match(event) is True

    def test_matches_backoff_with_configuration_mention(self) -> None:
        rule = ConfigDriftRule()
        event = _make_event(reason="BackOff", message="container failed to start due to configuration error")
        assert rule.match(event) is True

    def test_no_match_backoff_without_config(self) -> None:
        rule = ConfigDriftRule()
        event = _make_event(reason="BackOff", message="container restarting frequently")
        assert rule.match(event) is False

    def test_no_match_wrong_reason_with_configmap(self) -> None:
        rule = ConfigDriftRule()
        event = _make_event(reason="Pulled", message="configmap referenced successfully")
        assert rule.match(event) is False

    def test_correlate_with_deployment_and_envfrom_configmap(self) -> None:
        rule = ConfigDriftRule()
        deploy_spec = {"template": {"spec": {"containers": [{"envFrom": [{"configMapRef": {"name": "app-config"}}]}]}}}
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app", spec=deploy_spec)
        cm_view = _make_resource_view(kind="ConfigMap", name="app-config")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        cache.get.return_value = cm_view
        event = _make_event(reason="BackOff", name="my-app-abc12-xyz", message="configmap not found")
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "Deployment" for r in corr.related_resources)
        assert any(r.kind == "ConfigMap" for r in corr.related_resources)

    def test_correlate_no_deployment_found(self) -> None:
        rule = ConfigDriftRule()
        cache = _make_cache(list_return=[])
        event = _make_event(reason="BackOff", name="orphan-pod", message="configmap missing")
        corr = rule.correlate(event, cache, _make_ledger())
        assert corr.related_resources == []

    def test_explain_with_configmap_changes(self) -> None:
        rule = ConfigDriftRule()
        deploy_spec = {"template": {"spec": {"containers": [{"envFrom": [{"configMapRef": {"name": "app-config"}}]}]}}}
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app", spec=deploy_spec)
        cm_view = _make_resource_view(kind="ConfigMap", name="app-config")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        cache.get.return_value = cm_view
        fc = _make_field_change(field_path="data.DATABASE_URL", old_value="old-url", new_value="new-url")
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="BackOff", name="my-app-abc12-xyz", message="configmap issue")
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert result.rule_id == "R07_config_drift"
        assert "data.DATABASE_URL" in result.root_cause

    def test_explain_without_configmap_changes(self) -> None:
        rule = ConfigDriftRule()
        cache = _make_cache(list_return=[])
        event = _make_event(reason="BackOff", name="my-pod", message="configmap missing")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R07_config_drift"
        assert "No recent ConfigMap" in result.root_cause


# =====================================================================
# R08 VolumeMountRule
# =====================================================================


class TestR08VolumeMountRule:
    def test_matches_failedmount(self) -> None:
        rule = VolumeMountRule()
        event = _make_event(reason="FailedMount", message="unable to mount volume data-pvc")
        assert rule.match(event) is True

    def test_matches_failedattachvolume(self) -> None:
        rule = VolumeMountRule()
        event = _make_event(reason="FailedAttachVolume", message="attach of disk failed")
        assert rule.match(event) is True

    def test_no_match_for_other_reason(self) -> None:
        rule = VolumeMountRule()
        event = _make_event(reason="BackOff", message="failed to mount pvc")
        assert rule.match(event) is False

    def test_correlate_extracts_pvc_name_from_message(self) -> None:
        rule = VolumeMountRule()
        pvc_view = _make_resource_view(
            kind="PersistentVolumeClaim",
            name="data-pvc",
            status={"phase": "Bound"},
        )
        cache = _make_cache()
        cache.get.return_value = pvc_view
        cache.list.return_value = []
        event = _make_event(reason="FailedMount", message='pvc "data-pvc" is not ready')
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "PersistentVolumeClaim" and r.name == "data-pvc" for r in corr.related_resources)

    def test_correlate_includes_unbound_pvcs_from_namespace_scan(self) -> None:
        rule = VolumeMountRule()
        unbound_pvc = _make_resource_view(
            kind="PersistentVolumeClaim",
            name="orphan-pvc",
            status={"phase": "Pending"},
        )
        cache = _make_cache()
        cache.get.return_value = None
        cache.list.return_value = [unbound_pvc]
        event = _make_event(reason="FailedMount", message="cannot attach volume")
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.name == "orphan-pvc" for r in corr.related_resources)

    def test_explain_with_unbound_pvcs(self) -> None:
        rule = VolumeMountRule()
        unbound_pvc = _make_resource_view(
            kind="PersistentVolumeClaim",
            name="data-pvc",
            status={"phase": "Pending"},
        )
        cache = _make_cache()
        cache.get.return_value = None
        cache.list.return_value = [unbound_pvc]
        event = _make_event(reason="FailedMount", message="cannot attach volume")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R08_volume_mount"
        assert "data-pvc" in result.root_cause
        assert "not bound" in result.root_cause

    def test_explain_with_volume_changes_no_unbound(self) -> None:
        rule = VolumeMountRule()
        bound_pvc = _make_resource_view(
            kind="PersistentVolumeClaim",
            name="data-pvc",
            status={"phase": "Bound"},
        )
        cache = _make_cache()
        cache.get.return_value = bound_pvc
        cache.list.return_value = []
        fc = _make_field_change(field_path="spec.accessMode", old_value="ReadWriteOnce", new_value="ReadOnlyMany")
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="FailedMount", message='pvc "data-pvc" mount failed')
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert "spec.accessMode" in result.root_cause

    def test_explain_without_unbound_pvcs_or_changes(self) -> None:
        rule = VolumeMountRule()
        cache = _make_cache(get_return=None, list_return=[])
        event = _make_event(reason="FailedMount", message="volume mount failed")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R08_volume_mount"
        assert "PVC" in result.root_cause or "StorageClass" in result.root_cause


# =====================================================================
# R09 NodePressureRule
# =====================================================================


class TestR09NodePressureRule:
    def test_matches_memorypressure(self) -> None:
        rule = NodePressureRule()
        event = _make_event(reason="MemoryPressure", kind="Node", name="node-1", message="node memory pressure")
        assert rule.match(event) is True

    def test_matches_diskpressure(self) -> None:
        rule = NodePressureRule()
        event = _make_event(reason="DiskPressure", kind="Node", name="node-1", message="node disk pressure")
        assert rule.match(event) is True

    def test_matches_pidpressure(self) -> None:
        rule = NodePressureRule()
        event = _make_event(reason="PIDPressure", kind="Node", name="node-1", message="node PID pressure")
        assert rule.match(event) is True

    def test_matches_node_has_insufficient_memory(self) -> None:
        rule = NodePressureRule()
        event = _make_event(reason="NodeHasInsufficientMemory", kind="Node", name="node-1", message="")
        assert rule.match(event) is True

    def test_no_match_unrelated_reason(self) -> None:
        rule = NodePressureRule()
        event = _make_event(reason="OOMKilled", kind="Pod", name="my-pod", message="container killed")
        assert rule.match(event) is False

    def test_correlate_node_lookup_and_pod_listing(self) -> None:
        rule = NodePressureRule()
        node_view = _make_resource_view(
            kind="Node",
            namespace="",
            name="node-1",
            status={"conditions": [{"type": "MemoryPressure", "status": "True"}]},
        )
        pod_view = _make_resource_view(
            kind="Pod",
            namespace="default",
            name="my-pod",
            spec={"nodeName": "node-1"},
        )
        cache = _make_cache(get_return=node_view, list_return=[pod_view])
        event = _make_event(reason="MemoryPressure", kind="Node", name="node-1")
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "Node" for r in corr.related_resources)
        assert any(r.kind == "Pod" and r.name == "my-pod" for r in corr.related_resources)

    def test_correlate_no_node_found(self) -> None:
        rule = NodePressureRule()
        cache = _make_cache(get_return=None, list_return=[])
        event = _make_event(reason="MemoryPressure", kind="Node", name="missing-node")
        corr = rule.correlate(event, cache, _make_ledger())
        assert not any(r.kind == "Node" for r in corr.related_resources)

    def test_explain_with_condition_changes(self) -> None:
        rule = NodePressureRule()
        node_view = _make_resource_view(kind="Node", namespace="", name="node-1")
        cache = _make_cache(get_return=node_view, list_return=[])
        fc = _make_field_change(
            field_path="status.conditions[MemoryPressure]",
            old_value="False",
            new_value="True",
        )
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(reason="MemoryPressure", kind="Node", name="node-1")
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert result.rule_id == "R09_node_pressure"
        assert "memory" in result.root_cause.lower()
        assert "Condition changed" in result.root_cause or "changed at" in result.root_cause

    def test_explain_without_condition_changes(self) -> None:
        rule = NodePressureRule()
        node_view = _make_resource_view(kind="Node", namespace="", name="node-1")
        pod_view = _make_resource_view(kind="Pod", namespace="default", name="my-pod", spec={"nodeName": "node-1"})
        cache = _make_cache(get_return=node_view, list_return=[pod_view])
        event = _make_event(reason="DiskPressure", kind="Node", name="node-1")
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R09_node_pressure"
        assert "disk" in result.root_cause.lower()
        assert "pod(s)" in result.root_cause


# =====================================================================
# R10 ReadinessProbeRule
# =====================================================================


class TestR10ReadinessProbeRule:
    def test_matches_unhealthy_with_readiness_probe_message_and_count_gte_3(self) -> None:
        rule = ReadinessProbeRule()
        event = _make_event(
            reason="Unhealthy",
            message="Readiness probe failed: HTTP probe failed with statuscode: 500",
            count=3,
        )
        assert rule.match(event) is True

    def test_no_match_count_less_than_3(self) -> None:
        rule = ReadinessProbeRule()
        event = _make_event(
            reason="Unhealthy",
            message="Readiness probe failed: connection refused",
            count=2,
        )
        assert rule.match(event) is False

    def test_no_match_unhealthy_without_readiness_probe_message(self) -> None:
        rule = ReadinessProbeRule()
        event = _make_event(reason="Unhealthy", message="liveness probe failed", count=5)
        assert rule.match(event) is False

    def test_no_match_wrong_reason(self) -> None:
        rule = ReadinessProbeRule()
        event = _make_event(reason="BackOff", message="readiness probe failed", count=5)
        assert rule.match(event) is False

    def test_matches_probeerror_reason(self) -> None:
        rule = ReadinessProbeRule()
        event = _make_event(reason="ProbeError", message="readiness probe error: timeout", count=4)
        assert rule.match(event) is True

    def test_correlate_finds_owning_deployment(self) -> None:
        rule = ReadinessProbeRule()
        probe_spec = {
            "template": {"spec": {"containers": [{"readinessProbe": {"httpGet": {"path": "/health", "port": 8080}}}]}}
        }
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app", spec=probe_spec)
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        event = _make_event(
            reason="Unhealthy",
            name="my-app-abc12-xyz",
            message="Readiness probe failed",
            count=3,
        )
        corr = rule.correlate(event, cache, _make_ledger())
        assert any(r.kind == "Deployment" and r.name == "my-app" for r in corr.related_resources)

    def test_correlate_no_deployment(self) -> None:
        rule = ReadinessProbeRule()
        cache = _make_cache(list_return=[])
        event = _make_event(reason="Unhealthy", name="orphan-pod", message="Readiness probe failed", count=5)
        corr = rule.correlate(event, cache, _make_ledger())
        assert corr.related_resources == []

    def test_explain_with_httpget_probe_details(self) -> None:
        rule = ReadinessProbeRule()
        probe_spec = {
            "template": {"spec": {"containers": [{"readinessProbe": {"httpGet": {"path": "/health", "port": 8080}}}]}}
        }
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app", spec=probe_spec)
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        event = _make_event(
            reason="Unhealthy",
            name="my-app-abc12-xyz",
            message="Readiness probe failed",
            count=5,
        )
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R10_readiness_probe"
        assert "httpGet" in result.root_cause

    def test_explain_with_tcpsocket_probe_details(self) -> None:
        """The _probe_details function checks httpGet first via .get("httpGet", {});
        since {} is a dict it always matches the httpGet branch. With only tcpSocket,
        the result still shows httpGet with None values."""
        rule = ReadinessProbeRule()
        probe_spec = {"template": {"spec": {"containers": [{"readinessProbe": {"tcpSocket": {"port": 5432}}}]}}}
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app", spec=probe_spec)
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        event = _make_event(
            reason="Unhealthy",
            name="my-app-abc12-xyz",
            message="Readiness probe failed",
            count=4,
        )
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        # httpGet branch always takes precedence due to .get("httpGet", {}) returning {}
        assert "httpGet" in result.root_cause

    def test_explain_with_exec_probe_details(self) -> None:
        """Same precedence behavior: httpGet branch matches before exec."""
        rule = ReadinessProbeRule()
        probe_spec = {
            "template": {"spec": {"containers": [{"readinessProbe": {"exec": {"command": ["cat", "/tmp/healthy"]}}}]}}
        }
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app", spec=probe_spec)
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        event = _make_event(
            reason="Unhealthy",
            name="my-app-abc12-xyz",
            message="Readiness probe failed",
            count=6,
        )
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        # httpGet branch always takes precedence due to .get("httpGet", {}) returning {}
        assert "httpGet" in result.root_cause

    def test_explain_with_probe_config_change(self) -> None:
        rule = ReadinessProbeRule()
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(kind="Deployment", name="my-app")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        fc = _make_field_change(
            field_path="spec.template.spec.containers[0].readinessProbe.httpGet.path",
            old_value="/health",
            new_value="/ready",
        )
        ledger = _make_ledger()
        ledger.diff.return_value = [fc]
        event = _make_event(
            reason="Unhealthy",
            name="my-app-abc12-xyz",
            message="Readiness probe failed",
            count=3,
        )
        corr = rule.correlate(event, cache, ledger)
        result = rule.explain(event, corr)
        assert result.rule_id == "R10_readiness_probe"
        assert "readinessProbe" in result.root_cause or "recently changed" in result.root_cause


# =====================================================================
# Rule metadata validation
# =====================================================================


class TestRuleMetadataR04R10:
    @pytest.mark.parametrize(
        "rule_cls,expected_id",
        [
            (ImagePullRule, "R04_image_pull"),
            (HPARule, "R05_hpa_misbehavior"),
            (ServiceUnreachableRule, "R06_service_unreachable"),
            (ConfigDriftRule, "R07_config_drift"),
            (VolumeMountRule, "R08_volume_mount"),
            (NodePressureRule, "R09_node_pressure"),
            (ReadinessProbeRule, "R10_readiness_probe"),
        ],
    )
    def test_rule_ids_correct(self, rule_cls: type, expected_id: str) -> None:
        assert rule_cls.rule_id == expected_id

    @pytest.mark.parametrize(
        "rule_cls,expected_priority",
        [
            (ImagePullRule, 15),
            (HPARule, 30),
            (ServiceUnreachableRule, 25),
            (ConfigDriftRule, 20),
            (VolumeMountRule, 20),
            (NodePressureRule, 15),
            (ReadinessProbeRule, 30),
        ],
    )
    def test_rule_priorities_correct(self, rule_cls: type, expected_priority: int) -> None:
        assert rule_cls.priority == expected_priority

    @pytest.mark.parametrize(
        "rule_cls,expected_base_confidence",
        [
            (ImagePullRule, 0.60),
            (HPARule, 0.50),
            (ServiceUnreachableRule, 0.50),
            (ConfigDriftRule, 0.55),
            (VolumeMountRule, 0.60),
            (NodePressureRule, 0.55),
            (ReadinessProbeRule, 0.50),
        ],
    )
    def test_base_confidence_values(self, rule_cls: type, expected_base_confidence: float) -> None:
        assert rule_cls.base_confidence == expected_base_confidence

    @pytest.mark.parametrize(
        "rule_cls",
        [
            ImagePullRule,
            HPARule,
            ServiceUnreachableRule,
            ConfigDriftRule,
            VolumeMountRule,
            NodePressureRule,
            ReadinessProbeRule,
        ],
    )
    def test_all_have_required_attributes(self, rule_cls: type) -> None:
        assert hasattr(rule_cls, "rule_id")
        assert hasattr(rule_cls, "display_name")
        assert hasattr(rule_cls, "priority")
        assert hasattr(rule_cls, "base_confidence")
        assert hasattr(rule_cls, "resource_dependencies")
        assert hasattr(rule_cls, "relevant_field_paths")
        assert isinstance(rule_cls.resource_dependencies, list)
        assert isinstance(rule_cls.relevant_field_paths, list)
        assert len(rule_cls.resource_dependencies) > 0
        assert len(rule_cls.relevant_field_paths) > 0
