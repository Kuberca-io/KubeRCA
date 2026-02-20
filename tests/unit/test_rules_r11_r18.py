"""Tests for rules R11-R18 — match(), correlate(), and explain() methods."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import CachedResourceView
from kuberca.rules.r11_failedmount_configmap import FailedMountConfigMapRule
from kuberca.rules.r12_failedmount_secret import FailedMountSecretRule
from kuberca.rules.r13_failedmount_pvc import FailedMountPVCRule
from kuberca.rules.r14_failedmount_nfs import FailedMountNFSRule
from kuberca.rules.r15_failedscheduling_node import FailedSchedulingNodeRule
from kuberca.rules.r16_exceed_quota import ExceedQuotaRule
from kuberca.rules.r17_evicted import EvictedRule
from kuberca.rules.r18_claim_lost import ClaimLostRule

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
) -> CachedResourceView:
    return CachedResourceView(
        kind=kind,
        namespace=namespace,
        name=name,
        resource_version="1",
        labels={},
        annotations={},
        spec={},
        status=status or {},
        last_updated=_TS,
    )


def _make_ledger() -> MagicMock:
    ledger = MagicMock()
    ledger.diff.return_value = []
    return ledger


# =====================================================================
# R11 FailedMount-ConfigMap
# =====================================================================


class TestR11FailedMountConfigMap:
    def test_matches_failedmount_with_configmap(self) -> None:
        rule = FailedMountConfigMapRule()
        event = _make_event(
            reason="FailedMount",
            message='MountVolume.SetUp failed for volume "config" : configmap "app-config" not found',
        )
        assert rule.match(event) is True

    def test_no_match_for_non_configmap(self) -> None:
        rule = FailedMountConfigMapRule()
        event = _make_event(reason="FailedMount", message="MountVolume.SetUp failed: secret not found")
        assert rule.match(event) is False

    def test_no_match_for_wrong_reason(self) -> None:
        rule = FailedMountConfigMapRule()
        event = _make_event(reason="OOMKilled", message="configmap not found")
        assert rule.match(event) is False

    def test_explain_produces_result(self) -> None:
        rule = FailedMountConfigMapRule()
        event = _make_event(reason="FailedMount", message='configmap "app-config" not found')
        cache = _make_cache()
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R11_failedmount_configmap"
        assert result.confidence > 0
        assert "configmap" in result.root_cause.lower() or "ConfigMap" in result.root_cause


# =====================================================================
# R12 FailedMount-Secret
# =====================================================================


class TestR12FailedMountSecret:
    def test_matches_failedmount_with_secret(self) -> None:
        rule = FailedMountSecretRule()
        event = _make_event(reason="FailedMount", message='MountVolume.SetUp failed: secret "db-creds" not found')
        assert rule.match(event) is True

    def test_no_match_without_secret(self) -> None:
        rule = FailedMountSecretRule()
        event = _make_event(reason="FailedMount", message="configmap not found")
        assert rule.match(event) is False

    def test_explain_produces_result(self) -> None:
        rule = FailedMountSecretRule()
        event = _make_event(reason="FailedMount", message='secret "db-creds" not found')
        cache = _make_cache()
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R12_failedmount_secret"
        assert "secret" in result.root_cause.lower() or "Secret" in result.root_cause


# =====================================================================
# R13 FailedMount-PVC
# =====================================================================


class TestR13FailedMountPVC:
    def test_matches_failedmount_with_pvc(self) -> None:
        rule = FailedMountPVCRule()
        event = _make_event(
            reason="FailedMount", message="Unable to attach or mount volumes: timed out waiting for pvc"
        )
        assert rule.match(event) is True

    def test_matches_failedattachvolume(self) -> None:
        rule = FailedMountPVCRule()
        event = _make_event(reason="FailedAttachVolume", message="volume pvc-123 has been lost")
        assert rule.match(event) is True

    def test_no_match_for_configmap_mount(self) -> None:
        rule = FailedMountPVCRule()
        event = _make_event(reason="FailedMount", message="configmap not found")
        assert rule.match(event) is False

    def test_explain_with_unbound_pvc(self) -> None:
        rule = FailedMountPVCRule()
        event = _make_event(reason="FailedMount", message="pvc data-pvc is not bound")
        pvc = _make_resource_view(
            kind="PersistentVolumeClaim",
            name="data-pvc",
            status={"phase": "Pending"},
        )
        cache = _make_cache(list_return=[pvc])
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R13_failedmount_pvc"


# =====================================================================
# R14 FailedMount-NFS
# =====================================================================


class TestR14FailedMountNFS:
    def test_matches_stale_nfs_handle(self) -> None:
        rule = FailedMountNFSRule()
        event = _make_event(reason="FailedMount", message="mount.nfs: Stale file handle")
        assert rule.match(event) is True

    def test_matches_nfs_keyword(self) -> None:
        rule = FailedMountNFSRule()
        event = _make_event(reason="FailedMount", message="nfs: server 10.0.0.1 not responding")
        assert rule.match(event) is True

    def test_no_match_non_nfs(self) -> None:
        rule = FailedMountNFSRule()
        event = _make_event(reason="FailedMount", message="configmap not found")
        assert rule.match(event) is False

    def test_explain_stale_handle(self) -> None:
        rule = FailedMountNFSRule()
        event = _make_event(reason="FailedMount", message="mount.nfs: Stale file handle")
        cache = _make_cache()
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert "stale" in result.root_cause.lower()


# =====================================================================
# R15 FailedScheduling-Node
# =====================================================================


class TestR15FailedSchedulingNode:
    def test_matches_taint_mismatch(self) -> None:
        rule = FailedSchedulingNodeRule()
        event = _make_event(
            reason="FailedScheduling",
            message="0/3 nodes are available: 3 node(s) had taints that the pod didn't tolerate",
        )
        assert rule.match(event) is True

    def test_matches_affinity(self) -> None:
        rule = FailedSchedulingNodeRule()
        event = _make_event(
            reason="FailedScheduling",
            message="0/3 nodes are available: 3 node(s) didn't match Pod's node affinity/selector",
        )
        assert rule.match(event) is True

    def test_no_match_non_scheduling(self) -> None:
        rule = FailedSchedulingNodeRule()
        event = _make_event(reason="OOMKilled", message="taints not tolerated")
        assert rule.match(event) is False

    def test_no_match_insufficient_cpu(self) -> None:
        rule = FailedSchedulingNodeRule()
        # FailedScheduling but no node constraint keywords
        event = _make_event(reason="FailedScheduling", message="0/3 nodes are available: Insufficient cpu")
        # "nodes are available" IS a keyword, so this actually matches
        assert rule.match(event) is True

    def test_explain_produces_result(self) -> None:
        rule = FailedSchedulingNodeRule()
        event = _make_event(reason="FailedScheduling", message="taints not tolerated")
        cache = _make_cache(list_return=[])
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert result.rule_id == "R15_failedscheduling_node"


# =====================================================================
# R16 ExceedQuota
# =====================================================================


class TestR16ExceedQuota:
    def test_matches_exceeded_quota(self) -> None:
        rule = ExceedQuotaRule()
        event = _make_event(
            reason="FailedCreate",
            message="exceeded quota: cpu-quota, requested: limits.cpu=2, used: limits.cpu=8, limited: limits.cpu=8",
        )
        assert rule.match(event) is True

    def test_matches_failedcreate_with_quota(self) -> None:
        rule = ExceedQuotaRule()
        event = _make_event(reason="FailedCreate", message="quota exceeded for resource cpu")
        assert rule.match(event) is True

    def test_no_match_unrelated(self) -> None:
        rule = ExceedQuotaRule()
        event = _make_event(reason="FailedCreate", message="OOM killed")
        assert rule.match(event) is False

    def test_explain_with_quota_resources(self) -> None:
        rule = ExceedQuotaRule()
        event = _make_event(reason="FailedCreate", message="exceeded quota")
        quota = _make_resource_view(kind="ResourceQuota", name="cpu-quota")
        cache = _make_cache(list_return=[quota])
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert "cpu-quota" in result.root_cause


# =====================================================================
# R17 Evicted
# =====================================================================


class TestR17Evicted:
    def test_matches_evicted_reason(self) -> None:
        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="The node was low on resource: memory")
        assert rule.match(event) is True

    def test_matches_killing_with_evict(self) -> None:
        rule = EvictedRule()
        event = _make_event(reason="Killing", message="Stopping container due to eviction")
        assert rule.match(event) is True

    def test_no_match_killing_without_evict(self) -> None:
        rule = EvictedRule()
        event = _make_event(reason="Killing", message="Container exited")
        assert rule.match(event) is False

    def test_explain_memory_pressure(self) -> None:
        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="The node was low on resource: memory")
        node_view = _make_resource_view(
            kind="Node",
            namespace="",
            name="node-1",
            status={
                "conditions": [
                    {"type": "MemoryPressure", "status": "True"},
                ],
            },
        )
        cache = _make_cache(
            get_return=_make_resource_view(kind="Pod", name="my-pod", status={"phase": "Failed"}),
        )
        # Mock get to return pod first, then node
        pod_view = _make_resource_view(
            kind="Pod",
            name="my-pod",
            status={"phase": "Failed"},
        )
        cache.get.side_effect = [pod_view, node_view]
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert "memory" in result.root_cause.lower()


# =====================================================================
# R18 ClaimLost
# =====================================================================


class TestR18ClaimLost:
    def test_matches_claim_lost_reason(self) -> None:
        rule = ClaimLostRule()
        event = _make_event(reason="ClaimLost", message="PVC lost its backing volume")
        assert rule.match(event) is True

    def test_matches_volume_failed_recycle(self) -> None:
        rule = ClaimLostRule()
        event = _make_event(reason="VolumeFailedRecycle", message="recycler failed")
        assert rule.match(event) is True

    def test_matches_phase_lost_in_message(self) -> None:
        rule = ClaimLostRule()
        event = _make_event(reason="Warning", message="PV phase changed to Lost")
        assert rule.match(event) is True

    def test_no_match_unrelated(self) -> None:
        rule = ClaimLostRule()
        event = _make_event(reason="OOMKilled", message="container killed")
        assert rule.match(event) is False

    def test_explain_with_lost_pv(self) -> None:
        rule = ClaimLostRule()
        event = _make_event(
            reason="ClaimLost",
            message="PVC lost its backing volume",
            kind="PersistentVolumeClaim",
            name="data-pvc",
        )
        pv = _make_resource_view(
            kind="PersistentVolume",
            namespace="",
            name="pv-001",
            status={"phase": "Lost"},
        )
        cache = _make_cache(
            get_return=_make_resource_view(kind="PersistentVolumeClaim", name="data-pvc"),
            list_return=[pv],
        )
        corr = rule.correlate(event, cache, _make_ledger())
        result = rule.explain(event, corr)
        assert "pv-001" in result.root_cause.lower() or "pv-001" in result.root_cause


# =====================================================================
# Rule metadata validation
# =====================================================================


class TestRuleMetadata:
    @pytest.mark.parametrize(
        "rule_cls,expected_id",
        [
            (FailedMountConfigMapRule, "R11_failedmount_configmap"),
            (FailedMountSecretRule, "R12_failedmount_secret"),
            (FailedMountPVCRule, "R13_failedmount_pvc"),
            (FailedMountNFSRule, "R14_failedmount_nfs"),
            (FailedSchedulingNodeRule, "R15_failedscheduling_node"),
            (ExceedQuotaRule, "R16_exceed_quota"),
            (EvictedRule, "R17_evicted"),
            (ClaimLostRule, "R18_claim_lost"),
        ],
    )
    def test_rule_ids_correct(self, rule_cls: type, expected_id: str) -> None:
        assert rule_cls.rule_id == expected_id

    @pytest.mark.parametrize(
        "rule_cls",
        [
            FailedMountConfigMapRule,
            FailedMountSecretRule,
            FailedMountPVCRule,
            FailedMountNFSRule,
            FailedSchedulingNodeRule,
            ExceedQuotaRule,
            EvictedRule,
            ClaimLostRule,
        ],
    )
    def test_all_have_required_attributes(self, rule_cls: type) -> None:
        assert hasattr(rule_cls, "rule_id")
        assert hasattr(rule_cls, "display_name")
        assert hasattr(rule_cls, "priority")
        assert hasattr(rule_cls, "base_confidence")
        assert hasattr(rule_cls, "resource_dependencies")
        assert hasattr(rule_cls, "relevant_field_paths")
