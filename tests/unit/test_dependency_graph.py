"""Tests for kuberca.graph.dependency_graph -- DependencyGraph."""

from __future__ import annotations

from datetime import UTC, datetime

from kuberca.graph.dependency_graph import DependencyGraph
from kuberca.graph.models import EdgeType
from kuberca.models.resources import ResourceSnapshot


def _make_snapshot(
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
    spec: dict | None = None,
) -> ResourceSnapshot:
    return ResourceSnapshot(
        kind=kind,
        namespace=namespace,
        name=name,
        spec_hash="abc123",
        spec=spec or {},
        captured_at=datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC),
        resource_version="1",
    )


class TestGraphBasics:
    def test_empty_graph(self) -> None:
        g = DependencyGraph()
        assert g.node_count == 0
        assert g.edge_count == 0

    def test_add_simple_resource(self) -> None:
        g = DependencyGraph()
        g.add_resource(_make_snapshot())
        assert g.node_count == 1
        node = g.get_node("Pod", "default", "my-pod")
        assert node is not None
        assert node.kind == "Pod"

    def test_remove_resource(self) -> None:
        g = DependencyGraph()
        g.add_resource(_make_snapshot())
        g.remove_resource("Pod", "default", "my-pod")
        assert g.node_count == 0

    def test_remove_nonexistent_resource_no_error(self) -> None:
        g = DependencyGraph()
        g.remove_resource("Pod", "default", "nonexistent")  # Should not raise

    def test_get_node_not_found(self) -> None:
        g = DependencyGraph()
        assert g.get_node("Pod", "default", "nope") is None


class TestPodEdges:
    def test_pod_with_configmap_volume(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={
                    "spec": {
                        "volumes": [
                            {"configMap": {"name": "app-config"}},
                        ],
                    },
                },
            )
        )
        assert g.edge_count == 1
        # Pod -> ConfigMap is a forward (downstream) edge
        result = g.downstream("Pod", "default", "web")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "ConfigMap"
        assert result.resources[0].name == "app-config"

    def test_pod_with_secret_volume(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={
                    "spec": {
                        "volumes": [
                            {"secret": {"secretName": "db-creds"}},
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "web")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "Secret"
        assert result.resources[0].name == "db-creds"

    def test_pod_with_pvc_volume(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="db",
                spec={
                    "spec": {
                        "volumes": [
                            {"persistentVolumeClaim": {"claimName": "data-pvc"}},
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "db")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "PersistentVolumeClaim"
        assert result.resources[0].name == "data-pvc"

    def test_pod_with_node_assignment(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={
                    "spec": {
                        "nodeName": "node-1",
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "web")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "Node"
        assert result.resources[0].name == "node-1"

    def test_pod_with_envfrom_configmap(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={
                    "spec": {
                        "containers": [
                            {
                                "name": "app",
                                "envFrom": [
                                    {"configMapRef": {"name": "env-config"}},
                                ],
                            },
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "web")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "ConfigMap"
        assert result.resources[0].name == "env-config"

    def test_pod_with_multiple_dependencies(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={
                    "spec": {
                        "nodeName": "node-1",
                        "volumes": [
                            {"configMap": {"name": "app-config"}},
                            {"secret": {"secretName": "tls-cert"}},
                            {"persistentVolumeClaim": {"claimName": "data"}},
                        ],
                    },
                },
            )
        )
        result = g.downstream("Pod", "default", "web")
        kinds = {r.kind for r in result.resources}
        assert kinds == {"Node", "ConfigMap", "Secret", "PersistentVolumeClaim"}
        assert g.edge_count == 4


class TestPVCBinding:
    def test_pvc_to_pv_binding(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                name="data-pvc",
                spec={"spec": {"volumeName": "pv-001"}},
            )
        )
        result = g.downstream("PersistentVolumeClaim", "default", "data-pvc")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "PersistentVolume"
        assert result.resources[0].name == "pv-001"


class TestResourceQuotaScoping:
    def test_quota_scoped_to_namespace(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="ResourceQuota",
                namespace="production",
                name="cpu-quota",
            )
        )
        # Namespace -> ResourceQuota is the edge direction
        result = g.downstream("Namespace", "", "production")
        assert len(result.resources) == 1
        assert result.resources[0].kind == "ResourceQuota"
        assert result.resources[0].name == "cpu-quota"


class TestUpstreamTraversal:
    def test_upstream_finds_pods_using_configmap(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web-1",
                spec={"spec": {"volumes": [{"configMap": {"name": "shared-config"}}]}},
            )
        )
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web-2",
                spec={"spec": {"volumes": [{"configMap": {"name": "shared-config"}}]}},
            )
        )
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="worker",
                spec={"spec": {"volumes": [{"secret": {"secretName": "other"}}]}},
            )
        )

        result = g.upstream("ConfigMap", "default", "shared-config")
        names = {r.name for r in result.resources}
        assert names == {"web-1", "web-2"}

    def test_upstream_empty_for_no_dependents(self) -> None:
        g = DependencyGraph()
        g.add_resource(_make_snapshot(kind="ConfigMap", name="lonely"))
        result = g.upstream("ConfigMap", "default", "lonely")
        assert len(result.resources) == 0


class TestMaxDepth:
    def test_depth_limit_respected(self) -> None:
        g = DependencyGraph()
        # Pod -> PVC -> PV chain
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="db",
                spec={"spec": {"volumes": [{"persistentVolumeClaim": {"claimName": "db-pvc"}}]}},
            )
        )
        g.add_resource(
            _make_snapshot(
                kind="PersistentVolumeClaim",
                name="db-pvc",
                spec={"spec": {"volumeName": "pv-001"}},
            )
        )

        # depth=1 should only find PVC, not PV
        result = g.downstream("Pod", "default", "db", max_depth=1)
        kinds = {r.kind for r in result.resources}
        assert "PersistentVolumeClaim" in kinds
        assert "PersistentVolume" not in kinds

        # depth=2 should find both
        result = g.downstream("Pod", "default", "db", max_depth=2)
        kinds = {r.kind for r in result.resources}
        assert "PersistentVolumeClaim" in kinds
        assert "PersistentVolume" in kinds

    def test_nonexistent_node_returns_empty(self) -> None:
        g = DependencyGraph()
        result = g.downstream("Pod", "default", "nonexistent")
        assert len(result.resources) == 0
        assert result.depth_reached == 0


class TestPathBetween:
    def test_path_exists(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "cfg"}}]}},
            )
        )
        pod_node = g.get_node("Pod", "default", "web")
        cm_node = g.get_node("ConfigMap", "default", "cfg")
        assert pod_node is not None
        assert cm_node is not None
        path = g.path_between(pod_node, cm_node)
        assert path is not None
        assert len(path) == 1
        assert path[0].edge_type == EdgeType.CONFIG_REFERENCE

    def test_no_path(self) -> None:
        g = DependencyGraph()
        g.add_resource(_make_snapshot(kind="Pod", name="a"))
        g.add_resource(_make_snapshot(kind="Pod", name="b"))
        a = g.get_node("Pod", "default", "a")
        b = g.get_node("Pod", "default", "b")
        assert a is not None and b is not None
        assert g.path_between(a, b) is None

    def test_path_to_self(self) -> None:
        g = DependencyGraph()
        g.add_resource(_make_snapshot(kind="Pod", name="self"))
        node = g.get_node("Pod", "default", "self")
        assert node is not None
        path = g.path_between(node, node)
        assert path == []


class TestResourceUpdate:
    def test_re_add_updates_edges(self) -> None:
        g = DependencyGraph()
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "old-config"}}]}},
            )
        )
        assert g.downstream("Pod", "default", "web").resources[0].name == "old-config"

        # Re-add with different volume
        g.add_resource(
            _make_snapshot(
                kind="Pod",
                name="web",
                spec={"spec": {"volumes": [{"configMap": {"name": "new-config"}}]}},
            )
        )
        result = g.downstream("Pod", "default", "web")
        assert len(result.resources) == 1
        assert result.resources[0].name == "new-config"
