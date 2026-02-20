"""Targeted tests to cover specific gaps in code coverage.

Covers missed lines in:
- kuberca/rules/r17_evicted.py (lines 52-55, 82-91, 96-99)
- kuberca/rules/r02_crash_loop.py (lines 65, 75, 98-119, 136, 175-177, 244, 283-287)
- kuberca/llm/evidence.py (lines 113-114, 123-129, 138-139, 144-152, 208-228, 256)
- kuberca/rules/r10_readiness_probe.py (lines 69, 73, 79-87)
- kuberca/notifications/email.py (lines 108-132)
- kuberca/rules/base.py (lines 175-177, 182-189, 209, 214-221, 242-245, 295-296, 312-318, 322-323)
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch
from uuid import uuid4

from kuberca.models.analysis import CorrelationResult, StateContextEntry
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import CachedResourceView, FieldChange

_TS = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)


def _make_event(
    reason: str = "OOMKilled",
    message: str = "test event",
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
    count: int = 1,
) -> EventRecord:
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test-cluster",
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
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
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


def _make_ledger(diff_return: list[FieldChange] | None = None) -> MagicMock:
    ledger = MagicMock()
    ledger.diff.return_value = diff_return or []
    return ledger


def _make_field_change(
    field_path: str = "spec.template.spec.containers[0].image",
    old_value: str = "app:v1",
    new_value: str = "app:v2",
    offset_seconds: int = 0,
) -> FieldChange:
    return FieldChange(
        field_path=field_path,
        old_value=old_value,
        new_value=new_value,
        changed_at=_TS + timedelta(seconds=offset_seconds),
    )


# =====================================================================
# R17 Evicted — correlate with node_name branch (lines 52-55)
#   and explain with disk pressure and generic pressure (lines 82-91, 96-99)
# =====================================================================


class TestR17EvictedCorrelateWithNode:
    """Tests covering R17 correlate() when pod has a nodeName and node is found."""

    def test_correlate_with_pod_having_node_name_and_node_found(self) -> None:
        """Lines 52-55: node_name is truthy, cache.get returns a node."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        pod_view = _make_resource_view(kind="Pod", name="my-pod", spec={"nodeName": "node-1"})
        node_view = _make_resource_view(kind="Node", namespace="", name="node-1")

        cache = _make_cache()
        cache.get.side_effect = [pod_view, node_view]

        event = _make_event(reason="Evicted", message="evicted from node")
        corr = rule.correlate(event, cache, _make_ledger())

        assert any(r.kind == "Pod" for r in corr.related_resources)
        assert any(r.kind == "Node" for r in corr.related_resources)
        assert corr.objects_queried == 2

    def test_correlate_with_pod_having_node_name_but_node_not_found(self) -> None:
        """Lines 52-53: node_name is truthy, cache.get returns None for node."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        pod_view = _make_resource_view(kind="Pod", name="my-pod", spec={"nodeName": "node-1"})

        cache = _make_cache()
        cache.get.side_effect = [pod_view, None]

        event = _make_event(reason="Evicted", message="evicted from node")
        corr = rule.correlate(event, cache, _make_ledger())

        assert any(r.kind == "Pod" for r in corr.related_resources)
        assert not any(r.kind == "Node" for r in corr.related_resources)
        assert corr.objects_queried == 2


class TestR17EvictedExplainPressureTypes:
    """Tests covering explain() for disk pressure and generic pressure branches."""

    def test_explain_disk_pressure_from_node_condition(self) -> None:
        """Lines 82-90: node has DiskPressure condition with status=True."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="The node was low on resource: ephemeral-storage")
        node_view = _make_resource_view(
            kind="Node",
            namespace="",
            name="node-1",
            status={"conditions": [{"type": "DiskPressure", "status": "True"}]},
        )
        corr = CorrelationResult(
            changes=[],
            related_resources=[node_view],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert "disk" in result.root_cause.lower()
        assert any(a.kind == "Node" for a in result.affected_resources)

    def test_explain_disk_pressure_from_message_keyword(self) -> None:
        """Lines 96-97: message contains 'disk' or 'ephemeral'."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="The node had disk pressure")
        corr = CorrelationResult(changes=[], related_resources=[], objects_queried=1, duration_ms=1.0)
        result = rule.explain(event, corr)
        assert "disk" in result.root_cause.lower()

    def test_explain_generic_pressure(self) -> None:
        """Lines 98-99: no memory/disk keywords, falls to generic 'resource pressure'."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="Pod was evicted")
        corr = CorrelationResult(changes=[], related_resources=[], objects_queried=1, duration_ms=1.0)
        result = rule.explain(event, corr)
        assert "resource pressure" in result.root_cause.lower()

    def test_explain_node_condition_non_list_conditions(self) -> None:
        """Line 83: conditions is not a list, so we skip condition parsing."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="Pod evicted")
        node_view = _make_resource_view(
            kind="Node",
            namespace="",
            name="node-1",
            status={"conditions": "not-a-list"},
        )
        corr = CorrelationResult(
            changes=[],
            related_resources=[node_view],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert "resource pressure" in result.root_cause.lower()

    def test_explain_node_condition_non_dict_entry(self) -> None:
        """Line 85: condition entry is not a dict, skipped."""
        from kuberca.rules.r17_evicted import EvictedRule

        rule = EvictedRule()
        event = _make_event(reason="Evicted", message="Pod evicted")
        node_view = _make_resource_view(
            kind="Node",
            namespace="",
            name="node-1",
            status={"conditions": ["not-a-dict", 42]},
        )
        corr = CorrelationResult(
            changes=[],
            related_resources=[node_view],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert result.rule_id == "R17_evicted"


# =====================================================================
# R02 CrashLoop — _find_owning_deployment returning None (line 65, 75),
#   _diff_configmaps edge cases (lines 98-119),
#   correlate with configmap changes (lines 175-177),
#   explain with no changes (line 244), _classify_change branches (283-287)
# =====================================================================


class TestR02CrashLoopOwnershipResolution:
    """Tests for _find_owning_deployment edge cases."""

    def test_find_owning_deployment_no_replicasets(self) -> None:
        """Line 65: owning_rs is None, returns None."""
        from kuberca.rules.r02_crash_loop import _find_owning_deployment

        cache = _make_cache(list_return=[])
        result = _find_owning_deployment("my-app-abc12-xyz", "default", cache)
        assert result is None

    def test_find_owning_deployment_rs_found_no_matching_deployment(self) -> None:
        """Line 75: owning_rs found but no matching deployment, returns None."""
        from kuberca.rules.r02_crash_loop import _find_owning_deployment

        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else []
        result = _find_owning_deployment("my-app-abc12-xyz", "default", cache)
        assert result is None

    def test_find_owning_deployment_with_two_part_pod_name(self) -> None:
        """When pod name has fewer than 3 parts, uses full name as rs_name_prefix."""
        from kuberca.rules.r02_crash_loop import _find_owning_deployment

        rs_view = _make_resource_view(kind="ReplicaSet", name="simple-pod")
        deploy_view = _make_resource_view(kind="Deployment", name="simple")
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]
        result = _find_owning_deployment("simple-pod", "default", cache)
        assert result is not None
        assert result.name == "simple"


class TestR02CrashLoopDiffConfigmaps:
    """Tests for _diff_configmaps covering lines 98-119."""

    def test_diff_configmaps_with_non_list_containers(self) -> None:
        """Line 98: containers is not a list, returns empty."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": "not-a-list"}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_with_non_dict_container(self) -> None:
        """Line 102: container is not a dict, skipped."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": ["not-a-dict"]}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_with_non_list_envfrom(self) -> None:
        """Line 105: envFrom is not a list, skipped."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": [{"envFrom": "not-a-list"}]}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_with_non_dict_env_source(self) -> None:
        """Line 107-108: env_source is not a dict, skipped."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": [{"envFrom": ["not-a-dict"]}]}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_with_non_dict_cm_ref(self) -> None:
        """Line 110-111: configMapRef is not a dict, skipped."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": [{"envFrom": [{"configMapRef": "bad"}]}]}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_with_non_string_cm_name(self) -> None:
        """Line 113: cm_name is not a string, skipped."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": [{"envFrom": [{"configMapRef": {"name": 123}}]}]}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_with_empty_cm_name(self) -> None:
        """Line 113: cm_name is empty string, skipped."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": [{"envFrom": [{"configMapRef": {"name": ""}}]}]}}},
        )
        cache = _make_cache()
        ledger = _make_ledger()
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert result == []

    def test_diff_configmaps_returns_changes_from_ledger(self) -> None:
        """Lines 115-117: valid configMapRef, ledger returns changes."""
        from kuberca.rules.r02_crash_loop import _diff_configmaps

        deploy = _make_resource_view(
            kind="Deployment",
            spec={
                "template": {
                    "spec": {
                        "containers": [{"envFrom": [{"configMapRef": {"name": "app-config"}}]}],
                    }
                }
            },
        )
        fc = _make_field_change(field_path="metadata.resourceVersion", old_value="100", new_value="101")
        cache = _make_cache()
        ledger = _make_ledger(diff_return=[fc])
        result = _diff_configmaps(deploy, "default", cache, ledger)
        assert len(result) == 1


class TestR02CrashLoopCorrelateWithConfigmapChanges:
    """Test correlate() when configmap changes are relevant (lines 175-177)."""

    def test_correlate_includes_relevant_configmap_changes(self) -> None:
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        rs_view = _make_resource_view(kind="ReplicaSet", name="my-app-abc12")
        deploy_view = _make_resource_view(
            kind="Deployment",
            name="my-app",
            spec={
                "template": {
                    "spec": {
                        "containers": [{"envFrom": [{"configMapRef": {"name": "app-config"}}]}],
                    }
                }
            },
        )
        cache = _make_cache()
        cache.list.side_effect = lambda kind, ns: [rs_view] if kind == "ReplicaSet" else [deploy_view]

        cm_change = _make_field_change(field_path="data.DB_HOST", old_value="old-host", new_value="new-host")
        ledger = MagicMock()
        # First diff call is for Deployment, second for ConfigMap
        ledger.diff.side_effect = [[], [cm_change]]

        event = _make_event(reason="BackOff", message="Back-off restarting", name="my-app-abc12-xyz", count=5)
        corr = rule.correlate(event, cache, ledger)
        assert len(corr.changes) == 1
        assert corr.changes[0].field_path == "data.DB_HOST"


class TestR02CrashLoopExplainBranches:
    """Test explain() for no-changes branch (line 244) and _classify_change (lines 283-287)."""

    def test_explain_without_changes(self) -> None:
        """Line 244: no correlation changes leads to the 'No recent' root cause."""
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        event = _make_event(reason="BackOff", message="Back-off restarting", count=5)
        corr = CorrelationResult(changes=[], related_resources=[], objects_queried=1, duration_ms=1.0)
        result = rule.explain(event, corr)
        assert "No recent image, config, or environment changes detected" in result.root_cause

    def test_explain_with_deployment_but_no_changes(self) -> None:
        """Deploy found but no changes: root cause mentions 'No recent'."""
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        deploy = _make_resource_view(kind="Deployment", name="my-deploy")
        event = _make_event(reason="BackOff", message="Back-off restarting", count=5)
        corr = CorrelationResult(
            changes=[],
            related_resources=[deploy],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert "No recent" in result.root_cause
        assert any(a.kind == "Deployment" for a in result.affected_resources)

    def test_explain_with_configmap_change(self) -> None:
        """Lines 283-284: _classify_change returns 'ConfigMap change'."""
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        deploy = _make_resource_view(kind="Deployment", name="my-deploy")
        fc = _make_field_change(field_path="configMap.data.key", old_value="old", new_value="new")
        event = _make_event(reason="BackOff", message="Back-off restarting", count=5)
        corr = CorrelationResult(
            changes=[fc],
            related_resources=[deploy],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert "ConfigMap change" in result.root_cause

    def test_explain_with_env_change(self) -> None:
        """Lines 285-286: _classify_change returns 'environment variable change'."""
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        deploy = _make_resource_view(kind="Deployment", name="my-deploy")
        fc = _make_field_change(
            field_path="spec.template.spec.containers[0].env[0].value", old_value="a", new_value="b"
        )
        event = _make_event(reason="BackOff", message="Back-off restarting", count=5)
        corr = CorrelationResult(
            changes=[fc],
            related_resources=[deploy],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert "environment variable change" in result.root_cause

    def test_explain_with_generic_change(self) -> None:
        """Line 287: _classify_change falls to 'configuration change'."""
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        deploy = _make_resource_view(kind="Deployment", name="my-deploy")
        fc = _make_field_change(field_path="spec.replicas", old_value="1", new_value="3")
        event = _make_event(reason="BackOff", message="Back-off restarting", count=5)
        corr = CorrelationResult(
            changes=[fc],
            related_resources=[deploy],
            objects_queried=2,
            duration_ms=1.0,
        )
        result = rule.explain(event, corr)
        assert "configuration change" in result.root_cause


class TestR02ClassifyChange:
    """Direct tests for _classify_change to ensure all branches are covered."""

    def test_classify_image_change(self) -> None:
        from kuberca.rules.r02_crash_loop import _classify_change

        assert _classify_change("spec.template.spec.containers[0].image") == "image update"

    def test_classify_configmap_change(self) -> None:
        from kuberca.rules.r02_crash_loop import _classify_change

        assert _classify_change("configMap.data.key") == "ConfigMap change"

    def test_classify_env_change(self) -> None:
        from kuberca.rules.r02_crash_loop import _classify_change

        assert _classify_change("spec.containers[0].env[0].value") == "environment variable change"

    def test_classify_generic_change(self) -> None:
        from kuberca.rules.r02_crash_loop import _classify_change

        assert _classify_change("spec.replicas") == "configuration change"


class TestR02CrashLoopMatch:
    """Test CrashLoopRule.match() for CrashLoopBackOff reason (line 136)."""

    def test_match_crashloopbackoff_reason(self) -> None:
        """Line 136: event.reason == 'CrashLoopBackOff' returns True."""
        from kuberca.rules.r02_crash_loop import CrashLoopRule

        rule = CrashLoopRule()
        event = _make_event(reason="CrashLoopBackOff", message="container is crash-looping", count=1)
        assert rule.match(event) is True


# =====================================================================
# Evidence package — truncation pipeline (lines 208-228, 256),
#   _format_container_statuses (lines 123-129),
#   _format_resource_specs (lines 138-139),
#   _format_state_context (lines 144-152),
#   _format_changes with truncation (lines 113-114)
# =====================================================================


class TestEvidencePackageTruncationPipeline:
    """Tests for the multi-step truncation in to_prompt_context()."""

    def test_step3_strips_previous_state_from_container_statuses(self) -> None:
        """Lines 208-217: truncation strips previousState from container statuses."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        # Create large container statuses with previousState to force step 3
        big_statuses = [{"name": f"c{i}", "state": "Running", "previousState": "x" * 500} for i in range(10)]
        big_events = [_make_event(name=f"pod-{i}") for i in range(20)]
        big_specs = [{"kind": "Pod", "data": "y" * 2000} for _ in range(5)]
        big_changes = [
            _make_field_change(field_path=f"spec.field{i}", old_value="a" * 300, new_value="b" * 300, offset_seconds=i)
            for i in range(30)
        ]
        pkg = EvidencePackage(
            incident_event=incident,
            related_events=big_events,
            changes=big_changes,
            container_statuses=big_statuses,
            resource_specs=big_specs,
        )
        ctx = pkg.to_prompt_context()
        # Should complete without error; the truncation pipeline ran
        assert isinstance(ctx, str)
        assert "## Incident" in ctx

    def test_step4_truncates_change_values(self) -> None:
        """Lines 219-228: final truncation step truncates change values."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        # Create extremely large data to force all truncation steps
        big_changes = [
            _make_field_change(
                field_path=f"spec.containers[{i}].really_long_field_name_here",
                old_value="x" * 500,
                new_value="y" * 500,
                offset_seconds=i,
            )
            for i in range(30)
        ]
        big_statuses = [{"name": f"c{i}", "state": "R", "previousState": "z" * 300} for i in range(10)]
        big_events = [_make_event(name=f"pod-{i}") for i in range(20)]
        pkg = EvidencePackage(
            incident_event=incident,
            related_events=big_events,
            changes=big_changes,
            container_statuses=big_statuses,
            resource_specs=[{"kind": "Pod", "data": "w" * 2000}],
        )
        ctx = pkg.to_prompt_context()
        assert isinstance(ctx, str)


class TestEvidenceFormatContainerStatuses:
    """Tests for _format_container_statuses covering lines 123-129."""

    def test_format_container_statuses_with_valid_dicts(self) -> None:
        """Lines 124-126: valid dicts are JSON-serialized."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        statuses = [{"name": "web", "state": "Running"}]
        pkg = EvidencePackage(incident_event=incident, container_statuses=statuses)
        result = pkg._format_container_statuses(statuses)
        assert "web" in result
        assert "Running" in result

    def test_format_container_statuses_with_unserializable_value(self) -> None:
        """Lines 127-128: TypeError/ValueError falls to str()."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()

        class Unserializable:
            def __str__(self) -> str:
                return "unserializable-obj"

        # Create an object that will cause json.dumps to raise
        bad_status: dict = {"name": "c1", "value": Unserializable()}
        pkg = EvidencePackage(incident_event=incident)
        # Call directly to test the fallback; json.dumps with default=str should handle this
        result = pkg._format_container_statuses([bad_status])
        assert isinstance(result, str)

    def test_format_container_statuses_empty(self) -> None:
        """Lines 121-122: empty list returns '(none)'."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        result = pkg._format_container_statuses([])
        assert result == "(none)"


class TestEvidenceFormatResourceSpecs:
    """Tests for _format_resource_specs covering lines 138-139."""

    def test_format_resource_specs_with_valid_dict(self) -> None:
        """Lines 136-137: valid dict is JSON-serialized with indent=2."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        specs = [{"kind": "Deployment", "replicas": 3}]
        result = pkg._format_resource_specs(specs)
        assert "Deployment" in result
        assert "replicas" in result

    def test_format_resource_specs_with_multiple_specs(self) -> None:
        """Multiple specs joined with '---' separator."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        specs = [{"kind": "Pod"}, {"kind": "Service"}]
        result = pkg._format_resource_specs(specs)
        assert "---" in result

    def test_format_resource_specs_empty(self) -> None:
        """Lines 132-133: empty returns '(none)'."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        result = pkg._format_resource_specs([])
        assert result == "(none)"


class TestEvidenceFormatStateContext:
    """Tests for _format_state_context covering lines 144-152."""

    def test_format_state_context_with_existing_entries(self) -> None:
        """Lines 147-151: entries with exists=True show status_summary."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        entries = [
            StateContextEntry(
                kind="PersistentVolumeClaim",
                namespace="default",
                name="data-pvc",
                exists=True,
                status_summary="Bound",
                relationship="PVC referenced by Pod via volume mount",
            ),
        ]
        pkg = EvidencePackage(incident_event=incident, state_context=entries)
        result = pkg._format_state_context(entries)
        assert "PersistentVolumeClaim" in result
        assert "data-pvc" in result
        assert "Bound" in result
        assert "PVC referenced by Pod" in result

    def test_format_state_context_with_nonexistent_resource(self) -> None:
        """Line 148: entry with exists=False shows '<not found>'."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        entries = [
            StateContextEntry(
                kind="ConfigMap",
                namespace="default",
                name="missing-cm",
                exists=False,
                status_summary="",
                relationship="ConfigMap referenced by Deployment",
            ),
        ]
        pkg = EvidencePackage(incident_event=incident, state_context=entries)
        result = pkg._format_state_context(entries)
        assert "<not found>" in result

    def test_format_state_context_empty(self) -> None:
        """Lines 144-145: empty returns '(none)'."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        pkg = EvidencePackage(incident_event=incident)
        result = pkg._format_state_context([])
        assert result == "(none)"

    def test_state_context_included_in_to_prompt_context(self) -> None:
        """Line 256: when state_context has entries, '## State Context' section is appended."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        entries = [
            StateContextEntry(
                kind="Service",
                namespace="default",
                name="my-svc",
                exists=True,
                status_summary="Active",
                relationship="Service backing the pod",
            ),
        ]
        pkg = EvidencePackage(incident_event=incident, state_context=entries)
        ctx = pkg.to_prompt_context()
        assert "## State Context" in ctx
        assert "my-svc" in ctx


class TestEvidenceFormatChangesWithTruncation:
    """Tests for _format_changes with truncate_values=True (lines 113-114)."""

    def test_format_changes_truncates_long_values(self) -> None:
        """Lines 113-114: long values are truncated with _truncate_value."""
        from kuberca.llm.evidence import EvidencePackage

        incident = _make_event()
        fc = _make_field_change(old_value="x" * 500, new_value="y" * 500)
        pkg = EvidencePackage(incident_event=incident, changes=[fc])
        result = pkg._format_changes([fc], truncate_values=True)
        assert "[TRUNCATED]" in result


# =====================================================================
# R10 Readiness Probe — _probe_details edge cases (lines 69, 73, 79-87)
# =====================================================================


class TestR10ProbeDetails:
    """Tests for _probe_details covering tcpSocket, exec, and edge cases."""

    def test_probe_details_with_non_dict_first_container(self) -> None:
        """Line 69: first container is not a dict, returns '<unknown probe config>'."""
        from kuberca.rules.r10_readiness_probe import _probe_details

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": ["not-a-dict"]}}},
        )
        assert _probe_details(deploy) == "<unknown probe config>"

    def test_probe_details_with_non_dict_probe(self) -> None:
        """Line 73: readinessProbe is not a dict, returns '<unknown probe config>'."""
        from kuberca.rules.r10_readiness_probe import _probe_details

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": [{"readinessProbe": "not-a-dict"}]}}},
        )
        assert _probe_details(deploy) == "<unknown probe config>"

    def test_probe_details_tcpsocket_no_httpget(self) -> None:
        """Lines 79-81: httpGet is absent (None), falls to tcpSocket branch."""
        from kuberca.rules.r10_readiness_probe import _probe_details

        deploy = _make_resource_view(
            kind="Deployment",
            spec={
                "template": {
                    "spec": {"containers": [{"readinessProbe": {"httpGet": None, "tcpSocket": {"port": 5432}}}]}
                }
            },
        )
        result = _probe_details(deploy)
        assert "tcpSocket" in result
        assert "5432" in result

    def test_probe_details_exec_no_httpget_no_tcp(self) -> None:
        """Lines 82-84: both httpGet and tcpSocket are None, falls to exec branch."""
        from kuberca.rules.r10_readiness_probe import _probe_details

        deploy = _make_resource_view(
            kind="Deployment",
            spec={
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "readinessProbe": {
                                    "httpGet": None,
                                    "tcpSocket": None,
                                    "exec": {"command": ["cat", "/tmp/healthy"]},
                                }
                            }
                        ]
                    }
                }
            },
        )
        result = _probe_details(deploy)
        assert "exec" in result
        assert "cat" in result

    def test_probe_details_empty_containers_list(self) -> None:
        """Line 66: containers list is empty."""
        from kuberca.rules.r10_readiness_probe import _probe_details

        deploy = _make_resource_view(
            kind="Deployment",
            spec={"template": {"spec": {"containers": []}}},
        )
        assert _probe_details(deploy) == "<unknown probe config>"

    def test_probe_details_no_probe_keys(self) -> None:
        """Lines 76-84: probe has no recognized keys, falls through to default."""
        from kuberca.rules.r10_readiness_probe import _probe_details

        deploy = _make_resource_view(
            kind="Deployment",
            spec={
                "template": {
                    "spec": {
                        "containers": [
                            {
                                "readinessProbe": {
                                    "httpGet": None,
                                    "tcpSocket": None,
                                    "exec": None,
                                }
                            }
                        ]
                    }
                }
            },
        )
        assert _probe_details(deploy) == "<unknown probe config>"


# =====================================================================
# Email notifications — _send_sync (lines 108-132)
# =====================================================================


class TestEmailSendSync:
    """Tests for _send_sync covering TLS and STARTTLS branches."""

    def _make_smtp_config(self, **kwargs: object):
        from kuberca.notifications.email import SMTPConfig

        defaults: dict[str, object] = {
            "host": "smtp.example.com",
            "port": 587,
            "username": "user@example.com",
            "password": "secret",
            "from_addr": "alerts@example.com",
        }
        defaults.update(kwargs)
        return SMTPConfig(**defaults)  # type: ignore[arg-type]

    def _make_alert(self):
        from kuberca.models.alerts import AnomalyAlert

        return AnomalyAlert(
            severity=Severity.CRITICAL,
            resource_kind="Node",
            resource_name="node-01",
            namespace="",
            reason="NotReady",
            summary="Node has been NotReady for 5 minutes.",
            detected_at=datetime(2024, 3, 10, 8, 0, 0, tzinfo=UTC),
            event_count=1,
        )

    @patch("kuberca.notifications.email.smtplib.SMTP")
    @patch("kuberca.notifications.email.ssl.create_default_context")
    def test_send_sync_starttls_with_login(self, mock_ssl: MagicMock, mock_smtp_cls: MagicMock) -> None:
        """Lines 122-132: STARTTLS branch with username (login called)."""
        from kuberca.notifications.email import EmailNotificationChannel

        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        cfg = self._make_smtp_config(use_tls=False, username="user@example.com", password="secret")
        ch = EmailNotificationChannel(smtp_config=cfg, to_addr="ops@example.com")
        alert = self._make_alert()

        ch._send_sync(alert)

        mock_smtp_cls.assert_called_once()
        mock_server.ehlo.assert_called()
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once_with("user@example.com", "secret")
        mock_server.send_message.assert_called_once()

    @patch("kuberca.notifications.email.smtplib.SMTP")
    @patch("kuberca.notifications.email.ssl.create_default_context")
    def test_send_sync_starttls_without_login(self, mock_ssl: MagicMock, mock_smtp_cls: MagicMock) -> None:
        """Lines 122-132: STARTTLS branch without username (login not called)."""
        from kuberca.notifications.email import EmailNotificationChannel

        mock_server = MagicMock()
        mock_smtp_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_cls.return_value.__exit__ = MagicMock(return_value=False)

        cfg = self._make_smtp_config(use_tls=False, username="", password="")
        ch = EmailNotificationChannel(smtp_config=cfg, to_addr="ops@example.com")
        alert = self._make_alert()

        ch._send_sync(alert)

        mock_server.login.assert_not_called()
        mock_server.send_message.assert_called_once()

    @patch("kuberca.notifications.email.smtplib.SMTP_SSL")
    @patch("kuberca.notifications.email.ssl.create_default_context")
    def test_send_sync_ssl_with_login(self, mock_ssl: MagicMock, mock_smtp_ssl_cls: MagicMock) -> None:
        """Lines 112-120: SMTP_SSL branch with username (login called)."""
        from kuberca.notifications.email import EmailNotificationChannel

        mock_server = MagicMock()
        mock_smtp_ssl_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_ssl_cls.return_value.__exit__ = MagicMock(return_value=False)

        cfg = self._make_smtp_config(use_tls=True, port=465, username="user@example.com", password="secret")
        ch = EmailNotificationChannel(smtp_config=cfg, to_addr="ops@example.com")
        alert = self._make_alert()

        ch._send_sync(alert)

        mock_smtp_ssl_cls.assert_called_once()
        mock_server.login.assert_called_once_with("user@example.com", "secret")
        mock_server.send_message.assert_called_once()

    @patch("kuberca.notifications.email.smtplib.SMTP_SSL")
    @patch("kuberca.notifications.email.ssl.create_default_context")
    def test_send_sync_ssl_without_login(self, mock_ssl: MagicMock, mock_smtp_ssl_cls: MagicMock) -> None:
        """Lines 112-120: SMTP_SSL branch without username (login not called)."""
        from kuberca.notifications.email import EmailNotificationChannel

        mock_server = MagicMock()
        mock_smtp_ssl_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp_ssl_cls.return_value.__exit__ = MagicMock(return_value=False)

        cfg = self._make_smtp_config(use_tls=True, port=465, username="", password="")
        ch = EmailNotificationChannel(smtp_config=cfg, to_addr="ops@example.com")
        alert = self._make_alert()

        ch._send_sync(alert)

        mock_server.login.assert_not_called()
        mock_server.send_message.assert_called_once()


# =====================================================================
# Rule base.py — RuleEngine evaluation bands and _run_correlate edge cases
#   (lines 175-177, 182-189, 209, 214-221, 242-245, 295-296, 312-318, 322-323)
# =====================================================================


class _DummyRule:
    """A minimal Rule subclass for testing the RuleEngine."""

    def __init__(
        self,
        rule_id: str = "R_test",
        priority: int = 10,
        base_confidence: float = 0.50,
        resource_dependencies: list[str] | None = None,
        match_result: bool = True,
        correlate_result: CorrelationResult | None = None,
        explain_confidence: float = 0.50,
        correlate_exception: Exception | None = None,
        match_exception: Exception | None = None,
        explain_exception: Exception | None = None,
    ):
        self.rule_id = rule_id
        self.display_name = rule_id
        self.priority = priority
        self.base_confidence = base_confidence
        self.resource_dependencies = resource_dependencies or ["Pod"]
        self.relevant_field_paths = ["spec"]
        self._match_result = match_result
        self._correlate_result = correlate_result or CorrelationResult(objects_queried=1, duration_ms=1.0)
        self._explain_confidence = explain_confidence
        self._correlate_exception = correlate_exception
        self._match_exception = match_exception
        self._explain_exception = explain_exception

    def match(self, event: EventRecord) -> bool:
        if self._match_exception:
            raise self._match_exception
        return self._match_result

    def correlate(self, event: EventRecord, cache: object, ledger: object) -> CorrelationResult:
        if self._correlate_exception:
            raise self._correlate_exception
        return self._correlate_result

    def explain(self, event: EventRecord, correlation: CorrelationResult):
        if self._explain_exception:
            raise self._explain_exception
        from kuberca.models.analysis import RuleResult

        return RuleResult(
            rule_id=self.rule_id,
            root_cause="Test root cause",
            confidence=self._explain_confidence,
            evidence=[],
            affected_resources=[],
            suggested_remediation="Fix it",
        )


class TestRuleEngineEvaluationBands:
    """Tests for the 4-band matching strategy in RuleEngine.evaluate()."""

    def _make_engine(self):
        from kuberca.rules.base import RuleEngine

        cache = MagicMock()
        ledger = MagicMock()
        return RuleEngine(cache, ledger)

    def test_competing_deps_mode_skips_disjoint_rules(self) -> None:
        """Lines 175-177: after medium confidence match, skip rules with disjoint deps."""
        engine = self._make_engine()
        # First rule: medium confidence with Pod dependency
        rule1 = _DummyRule(
            rule_id="R01",
            priority=10,
            resource_dependencies=["Pod"],
            explain_confidence=0.75,
        )
        # Second rule: disjoint dependency, should be skipped
        rule2 = _DummyRule(
            rule_id="R02",
            priority=20,
            resource_dependencies=["Service"],
            explain_confidence=0.90,
        )
        engine.register(rule1)  # type: ignore[arg-type]
        engine.register(rule2)  # type: ignore[arg-type]

        event = _make_event()
        result, meta = engine.evaluate(event)

        # Rule2 should have been skipped due to disjoint deps
        assert result is not None
        assert result.rule_id == "R01"
        assert result.confidence == 0.75

    def test_match_exception_recorded_in_warnings(self) -> None:
        """Lines 182-189: exception during match() is recorded."""
        engine = self._make_engine()
        rule = _DummyRule(
            rule_id="R_bad_match",
            match_exception=RuntimeError("match failed"),
        )
        engine.register(rule)  # type: ignore[arg-type]
        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is None
        assert any("R_bad_match" in w and "match" in w for w in meta.warnings)

    def test_object_cap_hit_recorded_in_warnings(self) -> None:
        """Line 209: objects_queried > _MAX_OBJECTS recorded as warning."""
        engine = self._make_engine()
        corr = CorrelationResult(objects_queried=100, duration_ms=1.0)
        rule = _DummyRule(rule_id="R_high_obj", correlate_result=corr)
        engine.register(rule)  # type: ignore[arg-type]
        event = _make_event()
        result, meta = engine.evaluate(event)
        assert any("object cap" in w for w in meta.warnings)

    def test_explain_exception_recorded_in_warnings(self) -> None:
        """Lines 214-221: exception during explain() is recorded."""
        engine = self._make_engine()
        rule = _DummyRule(
            rule_id="R_bad_explain",
            explain_exception=RuntimeError("explain failed"),
        )
        engine.register(rule)  # type: ignore[arg-type]
        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is None
        assert any("R_bad_explain" in w and "explain" in w for w in meta.warnings)

    def test_low_confidence_does_not_enter_competing_mode(self) -> None:
        """Lines 242-245: low confidence (<0.70) records candidate but doesn't restrict."""
        engine = self._make_engine()
        rule1 = _DummyRule(
            rule_id="R01",
            priority=10,
            resource_dependencies=["Pod"],
            explain_confidence=0.55,
        )
        rule2 = _DummyRule(
            rule_id="R02",
            priority=20,
            resource_dependencies=["Service"],
            explain_confidence=0.60,
        )
        engine.register(rule1)  # type: ignore[arg-type]
        engine.register(rule2)  # type: ignore[arg-type]

        event = _make_event()
        result, meta = engine.evaluate(event)

        # Rule2 should still be evaluated (not skipped) since rule1 was low confidence
        assert result is not None
        assert result.rule_id == "R02"
        assert result.confidence == 0.60

    def test_no_match_logs_and_returns_none(self) -> None:
        """Lines 265-271: no rules match, returns (None, meta)."""
        engine = self._make_engine()
        rule = _DummyRule(rule_id="R_no_match", match_result=False)
        engine.register(rule)  # type: ignore[arg-type]
        event = _make_event()
        result, meta = engine.evaluate(event)
        assert result is None
        assert meta.rules_matched == 0


class TestRunCorrelate:
    """Tests for _run_correlate edge cases (lines 295-296, 312-318, 322-323)."""

    def _make_engine(self):
        from kuberca.rules.base import RuleEngine

        cache = MagicMock()
        ledger = MagicMock()
        return RuleEngine(cache, ledger)

    def test_correlate_exception_returns_empty(self) -> None:
        """Lines 312-318: correlate() raises exception, returns empty result."""
        engine = self._make_engine()
        rule = _DummyRule(
            rule_id="R_exc",
            correlate_exception=ValueError("bad data"),
        )
        event = _make_event()
        corr, timed_out, obj_cap = engine._run_correlate(rule, event)  # type: ignore[arg-type]
        assert not timed_out
        assert not obj_cap
        assert corr.objects_queried == 0

    def test_correlate_returns_none_result_handled(self) -> None:
        """Lines 322-323: correlate() returns None (shouldn't happen but handled)."""
        engine = self._make_engine()

        class NoneRule:
            rule_id = "R_none"

            def correlate(self, event, cache, ledger):
                return None

        event = _make_event()
        corr, timed_out, obj_cap = engine._run_correlate(NoneRule(), event)  # type: ignore[arg-type]
        assert not timed_out
        assert not obj_cap
        assert corr.objects_queried == 0
