"""Unit tests for KubeRCAApp startup methods, _kuberca_version, and main()."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kuberca.app import KubeRCAApp, _ComponentError, _kuberca_version, main

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_app() -> KubeRCAApp:
    """Return a KubeRCAApp with log and config pre-wired as MagicMocks."""
    app = KubeRCAApp()
    app._log = MagicMock()
    app.config = MagicMock()
    return app


# ---------------------------------------------------------------------------
# TestStart — the top-level start() method
# ---------------------------------------------------------------------------


class TestStart:
    async def test_start_calls_all_substeps_in_order(self) -> None:
        """start() invokes every _start_* helper and sets _running=True."""
        app = KubeRCAApp()

        step_order = []

        def _make_recorder(name):
            async def _recorded():
                step_order.append(name)

            return _recorded

        with (
            patch("kuberca.app.load_config", return_value=MagicMock()),
            patch("kuberca.app.setup_logging"),
            patch("kuberca.app.get_logger", return_value=MagicMock()),
            patch("kuberca.app._kuberca_version", return_value="0.1.1"),
            patch.object(app, "_start_k8s_client", new=_make_recorder("k8s")),
            patch.object(app, "_start_cache", new=_make_recorder("cache")),
            patch.object(app, "_start_dependency_graph", new=_make_recorder("graph")),
            patch.object(app, "_start_ledger", new=_make_recorder("ledger")),
            patch.object(app, "_start_collector", new=_make_recorder("collector")),
            patch.object(app, "_start_rule_engine", new=_make_recorder("rules")),
            patch.object(app, "_start_llm", new=_make_recorder("llm")),
            patch.object(app, "_start_coordinator", new=_make_recorder("coordinator")),
            patch.object(app, "_start_analysis_queue", new=_make_recorder("queue")),
            patch.object(app, "_start_scout", new=_make_recorder("scout")),
            patch.object(app, "_start_notifications", new=_make_recorder("notifications")),
            patch.object(app, "_start_mcp", new=_make_recorder("mcp")),
            patch.object(app, "_start_rest", new=_make_recorder("rest")),
            patch.object(app, "_start_rate_gauge_updater", new=_make_recorder("gauge")),
        ):
            await app.start()

        assert app._running is True
        assert step_order == [
            "k8s",
            "cache",
            "graph",
            "ledger",
            "collector",
            "rules",
            "llm",
            "coordinator",
            "queue",
            "scout",
            "notifications",
            "mcp",
            "rest",
            "gauge",
        ]

    async def test_start_sets_running_true_after_all_steps(self) -> None:
        """_running is only True once all steps complete without error."""
        app = KubeRCAApp()

        with (
            patch("kuberca.app.load_config", return_value=MagicMock()),
            patch("kuberca.app.setup_logging"),
            patch("kuberca.app.get_logger", return_value=MagicMock()),
            patch("kuberca.app._kuberca_version", return_value="0.1.1"),
            patch.object(app, "_start_k8s_client", new=AsyncMock()),
            patch.object(app, "_start_cache", new=AsyncMock()),
            patch.object(app, "_start_dependency_graph", new=AsyncMock()),
            patch.object(app, "_start_ledger", new=AsyncMock()),
            patch.object(app, "_start_collector", new=AsyncMock()),
            patch.object(app, "_start_rule_engine", new=AsyncMock()),
            patch.object(app, "_start_llm", new=AsyncMock()),
            patch.object(app, "_start_coordinator", new=AsyncMock()),
            patch.object(app, "_start_analysis_queue", new=AsyncMock()),
            patch.object(app, "_start_scout", new=AsyncMock()),
            patch.object(app, "_start_notifications", new=AsyncMock()),
            patch.object(app, "_start_mcp", new=AsyncMock()),
            patch.object(app, "_start_rest", new=AsyncMock()),
            patch.object(app, "_start_rate_gauge_updater", new=AsyncMock()),
        ):
            await app.start()

        assert app._running is True

    async def test_start_propagates_component_error(self) -> None:
        """If a mandatory _start_* raises _ComponentError, start() propagates it."""
        app = KubeRCAApp()

        with (
            patch("kuberca.app.load_config", return_value=MagicMock()),
            patch("kuberca.app.setup_logging"),
            patch("kuberca.app.get_logger", return_value=MagicMock()),
            patch("kuberca.app._kuberca_version", return_value="0.1.1"),
            patch.object(
                app,
                "_start_k8s_client",
                side_effect=_ComponentError("k8s_client", RuntimeError("no cluster")),
            ),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app.start()

        assert exc_info.value.component == "k8s_client"
        assert app._running is False


# ---------------------------------------------------------------------------
# TestStartK8sClient
# ---------------------------------------------------------------------------


class TestStartK8sClient:
    """Tests for _start_k8s_client().

    Strategy: patch functions directly on the already-loaded
    kubernetes_asyncio.config module so the patches survive across test
    ordering (patch.dict on sys.modules is unreliable when the real module
    is already cached in sys.modules before the test runs).
    """

    async def test_incluster_config_success(self) -> None:
        """When load_incluster_config() succeeds, _k8s_client is set to True."""
        import kubernetes_asyncio.config as _real_k8s_cfg

        app = _make_app()
        mock_incluster = MagicMock()  # does not raise

        with patch.object(_real_k8s_cfg, "load_incluster_config", mock_incluster):
            await app._start_k8s_client()

        assert app._k8s_client is True
        mock_incluster.assert_called_once()

    async def test_incluster_config_falls_back_to_kubeconfig(self) -> None:
        """When load_incluster_config raises ConfigException, kubeconfig is loaded instead."""
        import kubernetes_asyncio.config as _real_k8s_cfg

        app = _make_app()
        mock_load_kube = AsyncMock()

        with (
            patch.object(_real_k8s_cfg, "load_incluster_config", MagicMock(side_effect=_real_k8s_cfg.ConfigException)),
            patch.object(_real_k8s_cfg, "load_kube_config", mock_load_kube),
        ):
            await app._start_k8s_client()

        assert app._k8s_client is True
        mock_load_kube.assert_called_once()

    async def test_incluster_config_logs_info_on_success(self) -> None:
        """Successful in-cluster config emits an info log."""
        import kubernetes_asyncio.config as _real_k8s_cfg

        app = _make_app()

        with patch.object(_real_k8s_cfg, "load_incluster_config", MagicMock()):
            await app._start_k8s_client()

        app._log.info.assert_called_once()
        assert "in-cluster" in app._log.info.call_args[0][0]

    async def test_kubeconfig_logs_info_on_fallback(self) -> None:
        """Kubeconfig fallback emits an info log mentioning kubeconfig."""
        import kubernetes_asyncio.config as _real_k8s_cfg

        app = _make_app()

        with (
            patch.object(_real_k8s_cfg, "load_incluster_config", MagicMock(side_effect=_real_k8s_cfg.ConfigException)),
            patch.object(_real_k8s_cfg, "load_kube_config", AsyncMock()),
        ):
            await app._start_k8s_client()

        app._log.info.assert_called_once()
        assert "kubeconfig" in app._log.info.call_args[0][0]

    async def test_failure_raises_component_error(self) -> None:
        """When load_incluster_config raises a non-ConfigException, the outer except fires."""
        import kubernetes_asyncio.config as _real_k8s_cfg

        app = _make_app()

        # Raise a RuntimeError (not ConfigException) so the outer try/except catches it.
        with (
            patch.object(_real_k8s_cfg, "load_incluster_config", MagicMock(side_effect=RuntimeError("both failed"))),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_k8s_client()

        assert exc_info.value.component == "k8s_client"
        assert app._k8s_client is None

    async def test_kubeconfig_failure_raises_component_error(self) -> None:
        """If load_kube_config also raises, the outer except wraps it in _ComponentError."""
        import kubernetes_asyncio.config as _real_k8s_cfg

        app = _make_app()

        with (
            patch.object(_real_k8s_cfg, "load_incluster_config", MagicMock(side_effect=_real_k8s_cfg.ConfigException)),
            patch.object(_real_k8s_cfg, "load_kube_config", AsyncMock(side_effect=OSError("no kubeconfig"))),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_k8s_client()

        assert exc_info.value.component == "k8s_client"
        assert isinstance(exc_info.value.cause, OSError)


# ---------------------------------------------------------------------------
# TestStartCache
# ---------------------------------------------------------------------------


class TestStartCache:
    async def test_success_sets_cache(self) -> None:
        """_start_cache() populates self._cache with a ResourceCache instance."""
        app = _make_app()

        mock_cache = MagicMock()
        mock_cache.populate = AsyncMock()
        mock_resource_cache_cls = MagicMock(return_value=mock_cache)

        # The method does `from kubernetes_asyncio import client as k8s_client` (real import
        # works since kubernetes-asyncio is installed) then `from kuberca.cache import
        # ResourceCache`.  Patch the class in the already-loaded kuberca.cache namespace,
        # and stub the k8s API constructors on the real kubernetes_asyncio.client module
        # so that CoreV1Api() etc. return light MagicMocks and don't dial a real cluster.
        import kubernetes_asyncio.client as _real_k8s_client

        with (
            patch.object(_real_k8s_client, "CoreV1Api", MagicMock()),
            patch.object(_real_k8s_client, "AppsV1Api", MagicMock()),
            patch.object(_real_k8s_client, "BatchV1Api", MagicMock()),
            patch("kuberca.cache.ResourceCache", mock_resource_cache_cls),
        ):
            await app._start_cache()

        assert app._cache is mock_cache
        mock_cache.populate.assert_called_once()

    async def test_success_populate_called_with_18_kinds(self) -> None:
        """populate() is called with a dict containing all 18 resource kinds."""
        app = _make_app()

        mock_cache = MagicMock()
        mock_cache.populate = AsyncMock()

        import kubernetes_asyncio.client as _real_k8s_client

        with (
            patch.object(_real_k8s_client, "CoreV1Api", MagicMock()),
            patch.object(_real_k8s_client, "AppsV1Api", MagicMock()),
            patch.object(_real_k8s_client, "BatchV1Api", MagicMock()),
            patch("kuberca.cache.ResourceCache", MagicMock(return_value=mock_cache)),
        ):
            await app._start_cache()

        api_map = mock_cache.populate.call_args[0][0]
        assert len(api_map) == 18
        expected_kinds = {
            "Pod",
            "Node",
            "Event",
            "Namespace",
            "Service",
            "Endpoints",
            "ConfigMap",
            "Secret",
            "PersistentVolumeClaim",
            "PersistentVolume",
            "ResourceQuota",
            "ServiceAccount",
            "Deployment",
            "ReplicaSet",
            "StatefulSet",
            "DaemonSet",
            "Job",
            "CronJob",
        }
        assert set(api_map.keys()) == expected_kinds

    async def test_failure_raises_component_error(self) -> None:
        """If ResourceCache() raises, _ComponentError('cache', ...) is propagated."""
        app = _make_app()

        import kubernetes_asyncio.client as _real_k8s_client

        with (
            patch.object(_real_k8s_client, "CoreV1Api", MagicMock()),
            patch.object(_real_k8s_client, "AppsV1Api", MagicMock()),
            patch.object(_real_k8s_client, "BatchV1Api", MagicMock()),
            patch("kuberca.cache.ResourceCache", side_effect=RuntimeError("cache init failed")),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_cache()

        assert exc_info.value.component == "cache"
        assert app._cache is None

    async def test_failure_on_populate_raises_component_error(self) -> None:
        """If populate() raises, the exception is wrapped in _ComponentError."""
        app = _make_app()

        mock_cache = MagicMock()
        mock_cache.populate = AsyncMock(side_effect=ConnectionError("api server unreachable"))

        import kubernetes_asyncio.client as _real_k8s_client

        with (
            patch.object(_real_k8s_client, "CoreV1Api", MagicMock()),
            patch.object(_real_k8s_client, "AppsV1Api", MagicMock()),
            patch.object(_real_k8s_client, "BatchV1Api", MagicMock()),
            patch("kuberca.cache.ResourceCache", MagicMock(return_value=mock_cache)),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_cache()

        assert exc_info.value.component == "cache"


# ---------------------------------------------------------------------------
# TestStartDependencyGraph
# ---------------------------------------------------------------------------


class TestStartDependencyGraph:
    async def test_success_sets_dependency_graph(self) -> None:
        """On success, _dependency_graph is set to the DependencyGraph instance."""
        app = _make_app()
        app._cache = MagicMock()

        mock_graph = MagicMock()
        mock_graph.node_count = 5
        mock_graph.edge_count = 3
        mock_dep_graph_cls = MagicMock(return_value=mock_graph)

        with (
            patch("kuberca.graph.DependencyGraph", mock_dep_graph_cls),
            patch.object(app, "_seed_graph_from_cache"),
        ):
            await app._start_dependency_graph()

        assert app._dependency_graph is mock_graph

    async def test_success_calls_seed_graph(self) -> None:
        """_seed_graph_from_cache is called with the new graph and ResourceSnapshot class."""
        app = _make_app()
        app._cache = MagicMock()

        mock_graph = MagicMock()
        mock_graph.node_count = 0
        mock_graph.edge_count = 0

        seed_calls = []

        def _record_seed(graph, cls):
            seed_calls.append((graph, cls))

        with (
            patch("kuberca.graph.DependencyGraph", MagicMock(return_value=mock_graph)),
            patch.object(app, "_seed_graph_from_cache", side_effect=_record_seed),
        ):
            await app._start_dependency_graph()

        assert len(seed_calls) == 1
        assert seed_calls[0][0] is mock_graph

    async def test_failure_is_non_fatal_sets_graph_none(self) -> None:
        """When DependencyGraph() raises, _dependency_graph is None (non-fatal)."""
        app = _make_app()
        app._cache = MagicMock()

        with patch("kuberca.graph.DependencyGraph", side_effect=ImportError("graph module missing")):
            await app._start_dependency_graph()  # must not raise

        assert app._dependency_graph is None

    async def test_failure_logs_warning(self) -> None:
        """When graph init fails, a warning is logged with the error string."""
        app = _make_app()
        app._cache = MagicMock()

        with patch("kuberca.graph.DependencyGraph", side_effect=RuntimeError("bad graph")):
            await app._start_dependency_graph()

        app._log.warning.assert_called_once()
        warning_kwargs = app._log.warning.call_args[1]
        assert "bad graph" in warning_kwargs.get("error", "")

    async def test_seed_failure_is_non_fatal(self) -> None:
        """If _seed_graph_from_cache raises, the outer except still sets graph to None."""
        app = _make_app()
        app._cache = MagicMock()

        mock_graph = MagicMock()
        mock_graph.node_count = 0
        mock_graph.edge_count = 0

        with (
            patch("kuberca.graph.DependencyGraph", MagicMock(return_value=mock_graph)),
            patch.object(app, "_seed_graph_from_cache", side_effect=ValueError("seed error")),
        ):
            await app._start_dependency_graph()

        assert app._dependency_graph is None
        app._log.warning.assert_called_once()


# ---------------------------------------------------------------------------
# TestSeedGraphFromCache
# ---------------------------------------------------------------------------


class TestSeedGraphFromCache:
    def _make_cached_entry(self, kind, namespace, name):
        entry = MagicMock()
        entry.kind = kind
        entry.namespace = namespace
        entry.name = name
        entry.labels = {"app": name}
        entry.annotations = {}
        entry.resource_version = "1"
        entry.spec = {"containers": []}
        entry.status = {"phase": "Running"}
        return entry

    def test_empty_cache_store_is_noop(self) -> None:
        """_seed_graph_from_cache with an empty store does not call add_resource."""
        app = _make_app()
        mock_cache = MagicMock()
        mock_cache._store = {}
        app._cache = mock_cache

        mock_graph = MagicMock()
        mock_snapshot_cls = MagicMock()

        app._seed_graph_from_cache(mock_graph, mock_snapshot_cls)

        mock_graph.add_resource.assert_not_called()

    def test_single_entry_calls_add_resource_once(self) -> None:
        """A single cached entry results in exactly one add_resource call."""
        app = _make_app()
        entry = self._make_cached_entry("Pod", "default", "web-pod")
        mock_cache = MagicMock()
        mock_cache._store = {"Pod": {"default": {"web-pod": entry}}}
        app._cache = mock_cache

        mock_graph = MagicMock()

        # Use the real ResourceSnapshot so we can assert on its fields
        from kuberca.models.resources import ResourceSnapshot

        app._seed_graph_from_cache(mock_graph, ResourceSnapshot)

        mock_graph.add_resource.assert_called_once()
        snapshot = mock_graph.add_resource.call_args[0][0]
        assert isinstance(snapshot, ResourceSnapshot)
        assert snapshot.kind == "Pod"
        assert snapshot.namespace == "default"
        assert snapshot.name == "web-pod"
        assert snapshot.resource_version == "1"

    def test_multiple_entries_across_kinds_all_added(self) -> None:
        """Multiple entries across different kinds and namespaces are all seeded."""
        app = _make_app()
        pod1 = self._make_cached_entry("Pod", "default", "pod-1")
        pod2 = self._make_cached_entry("Pod", "kube-system", "pod-2")
        node1 = self._make_cached_entry("Node", "", "node-1")
        mock_cache = MagicMock()
        mock_cache._store = {
            "Pod": {
                "default": {"pod-1": pod1},
                "kube-system": {"pod-2": pod2},
            },
            "Node": {"": {"node-1": node1}},
        }
        app._cache = mock_cache

        mock_graph = MagicMock()
        from kuberca.models.resources import ResourceSnapshot

        app._seed_graph_from_cache(mock_graph, ResourceSnapshot)

        assert mock_graph.add_resource.call_count == 3

    def test_snapshot_raw_obj_contains_metadata_spec_status(self) -> None:
        """The raw_obj embedded in each snapshot has metadata, spec, and status keys."""
        app = _make_app()
        entry = self._make_cached_entry("Deployment", "staging", "api-deployment")
        entry.spec = {"replicas": 3}
        entry.status = {"readyReplicas": 3}
        mock_cache = MagicMock()
        mock_cache._store = {"Deployment": {"staging": {"api-deployment": entry}}}
        app._cache = mock_cache

        from kuberca.models.resources import ResourceSnapshot

        mock_graph = MagicMock()
        app._seed_graph_from_cache(mock_graph, ResourceSnapshot)

        snapshot = mock_graph.add_resource.call_args[0][0]
        raw = snapshot.spec  # ResourceSnapshot.spec holds the full raw_obj
        assert "metadata" in raw
        assert "spec" in raw
        assert "status" in raw
        assert raw["metadata"]["name"] == "api-deployment"
        assert raw["metadata"]["namespace"] == "staging"


# ---------------------------------------------------------------------------
# TestStartLedger
# ---------------------------------------------------------------------------


class TestStartLedger:
    async def test_hourly_retention_string(self) -> None:
        """A retention string like '6h' is parsed to 6 hours."""
        app = _make_app()
        app.config.change_ledger.retention = "6h"
        app.config.change_ledger.max_versions = 10
        app.config.change_ledger.persistence_enabled = False

        mock_ledger = MagicMock()
        mock_ledger_cls = MagicMock(return_value=mock_ledger)

        with patch("kuberca.ledger.change_ledger.ChangeLedger", mock_ledger_cls):
            await app._start_ledger()

        assert app._ledger is mock_ledger
        mock_ledger_cls.assert_called_once_with(max_versions=10, retention_hours=6)

    async def test_daily_retention_string_multiplied_by_24(self) -> None:
        """A retention string like '2d' is converted to 48 hours."""
        app = _make_app()
        app.config.change_ledger.retention = "2d"
        app.config.change_ledger.max_versions = 5
        app.config.change_ledger.persistence_enabled = False

        mock_ledger = MagicMock()
        mock_ledger_cls = MagicMock(return_value=mock_ledger)

        with patch("kuberca.ledger.change_ledger.ChangeLedger", mock_ledger_cls):
            await app._start_ledger()

        assert app._ledger is mock_ledger
        mock_ledger_cls.assert_called_once_with(max_versions=5, retention_hours=48)

    async def test_minute_retention_string_not_multiplied(self) -> None:
        """A retention string like '30m' strips the suffix but does not multiply."""
        app = _make_app()
        app.config.change_ledger.retention = "30m"
        app.config.change_ledger.max_versions = 3
        app.config.change_ledger.persistence_enabled = False

        mock_ledger = MagicMock()
        mock_ledger_cls = MagicMock(return_value=mock_ledger)

        with patch("kuberca.ledger.change_ledger.ChangeLedger", mock_ledger_cls):
            await app._start_ledger()

        mock_ledger_cls.assert_called_once_with(max_versions=3, retention_hours=30)

    async def test_failure_raises_component_error(self) -> None:
        """If ChangeLedger() raises, _ComponentError('ledger', ...) is propagated."""
        app = _make_app()
        app.config.change_ledger.retention = "1h"
        app.config.change_ledger.max_versions = 5

        with (
            patch("kuberca.ledger.change_ledger.ChangeLedger", side_effect=RuntimeError("sqlite unavailable")),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_ledger()

        assert exc_info.value.component == "ledger"
        assert app._ledger is None

    async def test_success_logs_info(self) -> None:
        """A successful ledger start emits an info log."""
        app = _make_app()
        app.config.change_ledger.retention = "1h"
        app.config.change_ledger.max_versions = 10
        app.config.change_ledger.persistence_enabled = True

        with patch("kuberca.ledger.change_ledger.ChangeLedger", MagicMock()):
            await app._start_ledger()

        app._log.info.assert_called_once()


# ---------------------------------------------------------------------------
# TestStartCollector
# ---------------------------------------------------------------------------


class TestStartCollector:
    def _build_watcher_mock(self):
        w = MagicMock()
        w.start = AsyncMock()
        w._handle_event = AsyncMock()
        return w

    def _collector_patches(self, event_watcher_cls, pod_watcher_cls, node_watcher_cls):
        """Return a list of patch context managers for the three watcher classes.

        The method body uses `from kuberca.collector.X import Y`, which resolves
        against the already-loaded submodule objects.  We must pre-import the
        submodules so `patch` can locate them, then patch the class attribute.
        """
        import kubernetes_asyncio.client as _real_k8s_client

        import kuberca.collector.event_watcher  # noqa: F401 — ensures module is loaded
        import kuberca.collector.node_watcher  # noqa: F401
        import kuberca.collector.pod_watcher  # noqa: F401

        return [
            patch.object(_real_k8s_client, "CoreV1Api", MagicMock()),
            patch("kuberca.collector.event_watcher.EventWatcher", event_watcher_cls),
            patch("kuberca.collector.pod_watcher.PodWatcher", pod_watcher_cls),
            patch("kuberca.collector.node_watcher.NodeWatcher", node_watcher_cls),
        ]

    async def test_success_sets_collector_to_event_watcher(self) -> None:
        """_start_collector() sets _collector to the EventWatcher instance."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._dependency_graph = None
        app.config.cluster_id = "test-cluster"

        event_watcher = self._build_watcher_mock()
        pod_watcher = self._build_watcher_mock()
        node_watcher = self._build_watcher_mock()

        mock_event_watcher_cls = MagicMock(return_value=event_watcher)
        mock_pod_watcher_cls = MagicMock(return_value=pod_watcher)
        mock_node_watcher_cls = MagicMock(return_value=node_watcher)

        patches = self._collector_patches(mock_event_watcher_cls, mock_pod_watcher_cls, mock_node_watcher_cls)
        with patches[0], patches[1], patches[2], patches[3]:
            await app._start_collector()

        assert app._collector is event_watcher

    async def test_success_starts_all_three_watchers(self) -> None:
        """All three watchers have their start() method called."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._dependency_graph = None
        app.config.cluster_id = ""

        event_watcher = self._build_watcher_mock()
        pod_watcher = self._build_watcher_mock()
        node_watcher = self._build_watcher_mock()

        patches = self._collector_patches(
            MagicMock(return_value=event_watcher),
            MagicMock(return_value=pod_watcher),
            MagicMock(return_value=node_watcher),
        )
        with patches[0], patches[1], patches[2], patches[3]:
            await app._start_collector()

        event_watcher.start.assert_called_once()
        pod_watcher.start.assert_called_once()
        node_watcher.start.assert_called_once()

    async def test_failure_raises_component_error(self) -> None:
        """If EventWatcher() raises, _ComponentError('collector', ...) is raised."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._dependency_graph = None
        app.config.cluster_id = ""

        patches = self._collector_patches(
            MagicMock(side_effect=RuntimeError("watcher failed")),
            MagicMock(),
            MagicMock(),
        )
        with patches[0], patches[1], patches[2], patches[3], pytest.raises(_ComponentError) as exc_info:
            await app._start_collector()

        assert exc_info.value.component == "collector"
        assert app._collector is None

    async def test_cluster_id_none_uses_empty_string(self) -> None:
        """When config.cluster_id is None, watchers are constructed with cluster_id=''."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._dependency_graph = None
        app.config.cluster_id = None

        event_watcher = self._build_watcher_mock()
        pod_watcher = self._build_watcher_mock()
        node_watcher = self._build_watcher_mock()

        mock_event_watcher_cls = MagicMock(return_value=event_watcher)
        mock_pod_watcher_cls = MagicMock(return_value=pod_watcher)
        mock_node_watcher_cls = MagicMock(return_value=node_watcher)

        patches = self._collector_patches(mock_event_watcher_cls, mock_pod_watcher_cls, mock_node_watcher_cls)
        with patches[0], patches[1], patches[2], patches[3]:
            await app._start_collector()

        mock_event_watcher_cls.assert_called_once()
        _, kwargs = mock_event_watcher_cls.call_args
        assert kwargs.get("cluster_id") == ""


# ---------------------------------------------------------------------------
# TestStartRuleEngine
# ---------------------------------------------------------------------------


class TestStartRuleEngine:
    async def test_success_sets_rule_engine(self) -> None:
        """_start_rule_engine() sets _rule_engine to the built engine."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app.config.rule_engine.tier2_enabled = True

        mock_engine = MagicMock()

        with patch("kuberca.rules.build_rule_engine", return_value=mock_engine):
            await app._start_rule_engine()

        assert app._rule_engine is mock_engine

    async def test_build_rule_engine_called_with_correct_args(self) -> None:
        """build_rule_engine receives cache, ledger, and tier2_enabled from config."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app.config.rule_engine.tier2_enabled = False

        with patch("kuberca.rules.build_rule_engine", return_value=MagicMock()) as mock_build:
            await app._start_rule_engine()

        mock_build.assert_called_once_with(
            cache=app._cache,
            ledger=app._ledger,
            tier2_enabled=False,
        )

    async def test_failure_raises_component_error(self) -> None:
        """If build_rule_engine raises, _ComponentError('rule_engine', ...) is raised."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app.config.rule_engine.tier2_enabled = True

        with (
            patch("kuberca.rules.build_rule_engine", side_effect=ImportError("rule missing")),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_rule_engine()

        assert exc_info.value.component == "rule_engine"
        assert app._rule_engine is None

    async def test_success_logs_info(self) -> None:
        """A successful rule engine start emits an info log."""
        app = _make_app()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app.config.rule_engine.tier2_enabled = True

        with patch("kuberca.rules.build_rule_engine", return_value=MagicMock()):
            await app._start_rule_engine()

        app._log.info.assert_called_once()


# ---------------------------------------------------------------------------
# TestStartCoordinator
# ---------------------------------------------------------------------------


class TestStartCoordinator:
    async def test_success_sets_coordinator(self) -> None:
        """_start_coordinator() sets _coordinator to the AnalystCoordinator instance."""
        app = _make_app()
        app._rule_engine = MagicMock()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._collector = MagicMock()
        app._llm_analyzer = None
        app._dependency_graph = None

        mock_coordinator = MagicMock()
        mock_coordinator_cls = MagicMock(return_value=mock_coordinator)

        with patch("kuberca.analyst.AnalystCoordinator", mock_coordinator_cls):
            await app._start_coordinator()

        assert app._coordinator is mock_coordinator

    async def test_coordinator_called_with_all_dependencies(self) -> None:
        """AnalystCoordinator is constructed with all required and optional deps."""
        app = _make_app()
        app._rule_engine = MagicMock()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._collector = MagicMock()
        app._llm_analyzer = MagicMock()
        app._dependency_graph = MagicMock()

        mock_coordinator_cls = MagicMock(return_value=MagicMock())

        with patch("kuberca.analyst.AnalystCoordinator", mock_coordinator_cls):
            await app._start_coordinator()

        mock_coordinator_cls.assert_called_once_with(
            rule_engine=app._rule_engine,
            llm_analyzer=app._llm_analyzer,
            cache=app._cache,
            ledger=app._ledger,
            event_buffer=app._collector,
            config=app.config,
            dependency_graph=app._dependency_graph,
        )

    async def test_failure_raises_component_error(self) -> None:
        """If AnalystCoordinator() raises, _ComponentError('coordinator', ...) is raised."""
        app = _make_app()
        app._rule_engine = MagicMock()
        app._cache = MagicMock()
        app._ledger = MagicMock()
        app._collector = MagicMock()
        app._llm_analyzer = None
        app._dependency_graph = None

        with (
            patch("kuberca.analyst.AnalystCoordinator", side_effect=TypeError("bad args")),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_coordinator()

        assert exc_info.value.component == "coordinator"
        assert app._coordinator is None


# ---------------------------------------------------------------------------
# TestStartAnalysisQueue
# ---------------------------------------------------------------------------


class TestStartAnalysisQueue:
    async def test_success_sets_analysis_queue(self) -> None:
        """_start_analysis_queue() sets _analysis_queue to the WorkQueue instance."""
        app = _make_app()
        app._coordinator = MagicMock()

        mock_queue = MagicMock()
        mock_queue.start = AsyncMock()
        mock_queue_cls = MagicMock(return_value=mock_queue)

        with patch("kuberca.analyst.queue.WorkQueue", mock_queue_cls):
            await app._start_analysis_queue()

        assert app._analysis_queue is mock_queue

    async def test_success_creates_background_task(self) -> None:
        """A background task named 'analysis-queue' is added to _background_tasks."""
        app = _make_app()
        app._coordinator = MagicMock()

        mock_queue = MagicMock()
        mock_queue.start = AsyncMock()

        with patch("kuberca.analyst.queue.WorkQueue", MagicMock(return_value=mock_queue)):
            await app._start_analysis_queue()

        assert len(app._background_tasks) == 1
        assert app._background_tasks[0].get_name() == "analysis-queue"
        # Clean up the task
        app._background_tasks[0].cancel()

    async def test_work_queue_receives_analyze_fn(self) -> None:
        """WorkQueue is constructed with coordinator.analyze as the analyze_fn."""
        app = _make_app()
        app._coordinator = MagicMock()
        analyze_fn = app._coordinator.analyze

        mock_queue = MagicMock()
        mock_queue.start = AsyncMock()
        mock_queue_cls = MagicMock(return_value=mock_queue)

        with patch("kuberca.analyst.queue.WorkQueue", mock_queue_cls):
            await app._start_analysis_queue()

        mock_queue_cls.assert_called_once_with(analyze_fn=analyze_fn)

    async def test_failure_raises_component_error(self) -> None:
        """If WorkQueue() raises, _ComponentError('analysis_queue', ...) is raised."""
        app = _make_app()
        app._coordinator = MagicMock()

        with (
            patch("kuberca.analyst.queue.WorkQueue", side_effect=RuntimeError("queue failed")),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_analysis_queue()

        assert exc_info.value.component == "analysis_queue"
        assert app._analysis_queue is None


# ---------------------------------------------------------------------------
# TestStartScout
# ---------------------------------------------------------------------------


class TestStartScout:
    async def test_success_sets_scout(self) -> None:
        """_start_scout() sets _scout to the AnomalyDetector instance."""
        app = _make_app()
        app.config.scout.anomaly_cooldown = 300

        mock_scout = MagicMock()
        mock_scout_cls = MagicMock(return_value=mock_scout)

        with patch("kuberca.scout.detector.AnomalyDetector", mock_scout_cls):
            await app._start_scout()

        assert app._scout is mock_scout

    async def test_scout_receives_cooldown_from_config(self) -> None:
        """AnomalyDetector is constructed with cooldown from config."""
        app = _make_app()
        app.config.scout.anomaly_cooldown = 600

        mock_scout_cls = MagicMock(return_value=MagicMock())

        with patch("kuberca.scout.detector.AnomalyDetector", mock_scout_cls):
            await app._start_scout()

        mock_scout_cls.assert_called_once_with(cooldown=600)

    async def test_failure_raises_component_error(self) -> None:
        """If AnomalyDetector() raises, _ComponentError('scout', ...) is raised."""
        app = _make_app()
        app.config.scout.anomaly_cooldown = 300

        with (
            patch("kuberca.scout.detector.AnomalyDetector", side_effect=ValueError("bad cooldown")),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_scout()

        assert exc_info.value.component == "scout"
        assert app._scout is None

    async def test_success_logs_info(self) -> None:
        """A successful scout start emits an info log."""
        app = _make_app()
        app.config.scout.anomaly_cooldown = 120

        with patch("kuberca.scout.detector.AnomalyDetector", MagicMock()):
            await app._start_scout()

        app._log.info.assert_called_once()


# ---------------------------------------------------------------------------
# TestStartRest
# ---------------------------------------------------------------------------


class TestStartRest:
    async def test_success_sets_rest_server(self) -> None:
        """_start_rest() sets _rest_server to the uvicorn.Server instance."""
        app = _make_app()
        app._coordinator = MagicMock()
        app._cache = MagicMock()
        app._rule_engine = MagicMock()
        app._llm_analyzer = None
        app._analysis_queue = MagicMock()
        app.config.api.port = 8080

        mock_server = MagicMock()
        mock_server.serve = AsyncMock()
        mock_uvicorn = MagicMock()
        mock_uvicorn.Server = MagicMock(return_value=mock_server)
        mock_uvicorn.Config = MagicMock(return_value=MagicMock())
        mock_fastapi_app = MagicMock()

        with (
            patch.dict("sys.modules", {"uvicorn": mock_uvicorn}),
            patch("kuberca.api.build_app", return_value=mock_fastapi_app),
        ):
            await app._start_rest()

        assert app._rest_server is mock_server

    async def test_success_creates_background_task(self) -> None:
        """A background task named 'rest-server' is appended to _background_tasks."""
        app = _make_app()
        app._coordinator = MagicMock()
        app._cache = MagicMock()
        app._rule_engine = MagicMock()
        app._llm_analyzer = None
        app._analysis_queue = MagicMock()
        app.config.api.port = 8080

        mock_server = MagicMock()
        mock_server.serve = AsyncMock()
        mock_uvicorn = MagicMock()
        mock_uvicorn.Server = MagicMock(return_value=mock_server)
        mock_uvicorn.Config = MagicMock(return_value=MagicMock())

        with (
            patch.dict("sys.modules", {"uvicorn": mock_uvicorn}),
            patch("kuberca.api.build_app", return_value=MagicMock()),
        ):
            await app._start_rest()

        assert len(app._background_tasks) == 1
        task = app._background_tasks[0]
        assert task.get_name() == "rest-server"
        task.cancel()

    async def test_build_app_called_with_correct_args(self) -> None:
        """build_app receives coordinator, cache, rule_engine, llm_analyzer, config, queue."""
        app = _make_app()
        app._coordinator = MagicMock()
        app._cache = MagicMock()
        app._rule_engine = MagicMock()
        app._llm_analyzer = MagicMock()
        app._analysis_queue = MagicMock()
        app.config.api.port = 9000

        mock_server = MagicMock()
        mock_server.serve = AsyncMock()
        mock_uvicorn = MagicMock()
        mock_uvicorn.Server = MagicMock(return_value=mock_server)
        mock_uvicorn.Config = MagicMock(return_value=MagicMock())

        with (
            patch.dict("sys.modules", {"uvicorn": mock_uvicorn}),
            patch("kuberca.api.build_app", return_value=MagicMock()) as mock_build_app,
        ):
            await app._start_rest()

        mock_build_app.assert_called_once_with(
            coordinator=app._coordinator,
            cache=app._cache,
            rule_engine=app._rule_engine,
            llm_analyzer=app._llm_analyzer,
            config=app.config,
            analysis_queue=app._analysis_queue,
        )

    async def test_failure_raises_component_error(self) -> None:
        """If uvicorn.Config raises, _ComponentError('rest', ...) is propagated."""
        app = _make_app()
        app._coordinator = MagicMock()
        app._cache = MagicMock()
        app._rule_engine = MagicMock()
        app._llm_analyzer = None
        app._analysis_queue = MagicMock()
        app.config.api.port = 8080

        mock_uvicorn = MagicMock()
        mock_uvicorn.Config = MagicMock(side_effect=RuntimeError("uvicorn config failed"))

        with (
            patch.dict("sys.modules", {"uvicorn": mock_uvicorn}),
            patch("kuberca.api.build_app", return_value=MagicMock()),
            pytest.raises(_ComponentError) as exc_info,
        ):
            await app._start_rest()

        assert exc_info.value.component == "rest"
        assert app._rest_server is None


# ---------------------------------------------------------------------------
# TestStartRateGaugeUpdater
# ---------------------------------------------------------------------------


class TestStartRateGaugeUpdater:
    async def test_creates_background_task(self) -> None:
        """_start_rate_gauge_updater appends exactly one background task."""
        app = _make_app()

        await app._start_rate_gauge_updater()

        assert len(app._background_tasks) == 1
        task = app._background_tasks[0]
        assert task.get_name() == "rate-gauge-updater"
        task.cancel()

    async def test_logs_info(self) -> None:
        """An info log is emitted after creating the updater task."""
        app = _make_app()

        await app._start_rate_gauge_updater()

        app._log.info.assert_called_once()
        # Clean up the task so the test doesn't leave a dangling coroutine
        app._background_tasks[0].cancel()

    async def test_updater_task_calls_compute_rolling_rates(self) -> None:
        """The background _updater coroutine calls compute_rolling_rates() on each iteration."""
        app = _make_app()

        call_count = 0

        def _fake_compute():
            nonlocal call_count
            call_count += 1
            # After first call, cancel the task by raising CancelledError
            raise asyncio.CancelledError

        with patch("kuberca.observability.metrics.compute_rolling_rates", side_effect=_fake_compute):
            await app._start_rate_gauge_updater()
            task = app._background_tasks[0]
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=0.5)
            except (TimeoutError, asyncio.CancelledError):
                task.cancel()

        assert call_count >= 1

    async def test_updater_error_is_swallowed_and_logs_debug(self) -> None:
        """Errors inside the _updater loop are caught and logged at debug level."""
        app = _make_app()

        iterations = 0

        async def _fast_sleep(_):
            nonlocal iterations
            iterations += 1
            if iterations >= 2:
                raise asyncio.CancelledError

        with (
            patch("kuberca.observability.metrics.compute_rolling_rates", side_effect=RuntimeError("metric error")),
            patch("asyncio.sleep", new=_fast_sleep),
        ):
            await app._start_rate_gauge_updater()
            task = app._background_tasks[0]
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=0.5)
            except (TimeoutError, asyncio.CancelledError):
                task.cancel()

        # debug was called with rate_gauge_update_error at some point
        debug_calls = [str(c) for c in app._log.debug.call_args_list]
        assert any("rate_gauge_update_error" in c for c in debug_calls)


# ---------------------------------------------------------------------------
# TestStopK8sClient
# ---------------------------------------------------------------------------


class TestStopK8sClient:
    async def test_noop_when_k8s_client_is_none(self) -> None:
        """_stop_k8s_client() does nothing when _k8s_client is None."""
        app = _make_app()
        app._k8s_client = None

        # Should complete without any mock calls or exceptions
        await app._stop_k8s_client()

    async def test_closes_api_client_when_k8s_client_set(self) -> None:
        """_stop_k8s_client() creates an ApiClient and awaits close() when initialized."""
        app = _make_app()
        app._k8s_client = True

        mock_api_client = MagicMock()
        mock_api_client.close = AsyncMock()

        import kubernetes_asyncio.client as _real_k8s_client

        with patch.object(_real_k8s_client, "ApiClient", MagicMock(return_value=mock_api_client)):
            await app._stop_k8s_client()

        mock_api_client.close.assert_called_once()

    async def test_close_error_is_non_fatal(self) -> None:
        """If ApiClient.close() raises, the error is caught and logged at debug level."""
        app = _make_app()
        app._k8s_client = True

        mock_api_client = MagicMock()
        mock_api_client.close = AsyncMock(side_effect=OSError("connection reset"))

        import kubernetes_asyncio.client as _real_k8s_client

        with patch.object(_real_k8s_client, "ApiClient", MagicMock(return_value=mock_api_client)):
            await app._stop_k8s_client()  # must not raise

        app._log.debug.assert_called_once()
        debug_kwargs = app._log.debug.call_args[1]
        assert "connection reset" in debug_kwargs.get("error", "")

    async def test_close_error_uses_fallback_logger_when_log_is_none(self) -> None:
        """_stop_k8s_client uses get_logger fallback when self._log is None."""
        app = _make_app()
        app._log = None
        app._k8s_client = True

        mock_api_client = MagicMock()
        mock_api_client.close = AsyncMock(side_effect=OSError("oops"))
        mock_fallback_log = MagicMock()

        import kubernetes_asyncio.client as _real_k8s_client

        with (
            patch.object(_real_k8s_client, "ApiClient", MagicMock(return_value=mock_api_client)),
            patch("kuberca.app.get_logger", return_value=mock_fallback_log),
        ):
            await app._stop_k8s_client()

        mock_fallback_log.debug.assert_called_once()


# ---------------------------------------------------------------------------
# TestKubercaVersion
# ---------------------------------------------------------------------------


class TestKubercaVersion:
    def test_returns_version_string(self) -> None:
        """_kuberca_version() returns the __version__ string from kuberca package."""
        result = _kuberca_version()
        assert isinstance(result, str)
        assert len(result) > 0

    def test_returns_correct_version(self) -> None:
        """_kuberca_version() matches kuberca.__version__."""
        from kuberca import __version__

        assert _kuberca_version() == __version__

    def test_version_format(self) -> None:
        """The returned version follows semver-like x.y.z format."""
        version = _kuberca_version()
        parts = version.split(".")
        assert len(parts) >= 2
        assert all(p.isdigit() for p in parts[:2])

    def test_returns_patched_version(self) -> None:
        """_kuberca_version() reflects a patched __version__ value."""
        with patch("kuberca.__version__", "9.9.9"):
            result = _kuberca_version()
        assert result == "9.9.9"


# ---------------------------------------------------------------------------
# TestMain
# ---------------------------------------------------------------------------


class TestMain:
    async def test_main_starts_app_and_runs_until_stop(self) -> None:
        """main() calls app.start() and polls _running until it becomes False."""
        run_calls = []

        async def _fake_stop():
            run_calls.append("stop")

        # Simulate: after start(), _running becomes True, then one sleep tick later False
        sleep_count = 0

        async def _fake_sleep(_):
            nonlocal sleep_count
            sleep_count += 1
            # After one sleep, flip _running off to exit the while loop
            mock_app_instance._running = False

        mock_app_instance = MagicMock()
        mock_app_instance.start = AsyncMock(side_effect=lambda: setattr(mock_app_instance, "_running", True) or None)
        mock_app_instance.stop = AsyncMock(side_effect=_fake_stop)
        mock_app_instance._running = False

        with (
            patch("kuberca.app.KubeRCAApp", return_value=mock_app_instance),
            patch("asyncio.sleep", new=_fake_sleep),
        ):
            await main()

        mock_app_instance.start.assert_called_once()
        assert sleep_count >= 1

    async def test_main_component_error_raises_system_exit_1(self) -> None:
        """When app.start() raises _ComponentError, main() calls SystemExit(1)."""
        cause = RuntimeError("no k8s")
        component_err = _ComponentError("k8s_client", cause)

        mock_app_instance = MagicMock()
        mock_app_instance.start = AsyncMock(side_effect=component_err)
        mock_app_instance.stop = AsyncMock()
        mock_app_instance._running = False

        mock_log = MagicMock()

        with (
            patch("kuberca.app.KubeRCAApp", return_value=mock_app_instance),
            patch("kuberca.app.get_logger", return_value=mock_log),
            pytest.raises(SystemExit) as exc_info,
        ):
            await main()

        assert exc_info.value.code == 1
        mock_app_instance.stop.assert_called_once()

    async def test_main_component_error_logs_critical(self) -> None:
        """_ComponentError in start() triggers a critical log with component name."""
        cause = RuntimeError("ledger dead")
        component_err = _ComponentError("ledger", cause)

        mock_app_instance = MagicMock()
        mock_app_instance.start = AsyncMock(side_effect=component_err)
        mock_app_instance.stop = AsyncMock()
        mock_app_instance._running = False

        mock_log = MagicMock()

        with (
            patch("kuberca.app.KubeRCAApp", return_value=mock_app_instance),
            patch("kuberca.app.get_logger", return_value=mock_log),
            pytest.raises(SystemExit),
        ):
            await main()

        mock_log.critical.assert_called_once()
        critical_kwargs = mock_log.critical.call_args[1]
        assert critical_kwargs.get("component") == "ledger"

    async def test_main_registers_signal_handlers(self) -> None:
        """main() registers handlers for SIGTERM and SIGINT on the event loop."""
        import signal as _signal

        mock_app_instance = MagicMock()

        # Start raises immediately so we don't need to manage the polling loop
        mock_app_instance.start = AsyncMock(side_effect=_ComponentError("x", RuntimeError("abort")))
        mock_app_instance.stop = AsyncMock()
        mock_app_instance._running = False

        registered_sigs = []

        real_loop = asyncio.get_event_loop()

        def _capture_signal(sig, handler):
            registered_sigs.append(sig)
            # Don't actually install signal handlers in tests

        with (
            patch("kuberca.app.KubeRCAApp", return_value=mock_app_instance),
            patch("kuberca.app.get_logger", return_value=MagicMock()),
            patch.object(real_loop, "add_signal_handler", side_effect=_capture_signal),
            pytest.raises(SystemExit),
        ):
            await main()

        assert _signal.SIGTERM in registered_sigs
        assert _signal.SIGINT in registered_sigs

    async def test_main_finally_stops_if_still_running(self) -> None:
        """The finally block in main() calls stop() if _running is True at exit."""

        # Simulate: start sets _running=True, then sleep raises CancelledError
        async def _fake_sleep(_):
            raise asyncio.CancelledError

        mock_app_instance = MagicMock()
        mock_app_instance.start = AsyncMock(side_effect=lambda: setattr(mock_app_instance, "_running", True) or None)
        mock_app_instance.stop = AsyncMock()
        mock_app_instance._running = False

        with (
            patch("kuberca.app.KubeRCAApp", return_value=mock_app_instance),
            patch("asyncio.sleep", new=_fake_sleep),
            pytest.raises(asyncio.CancelledError),
        ):
            await main()

        mock_app_instance.stop.assert_called_once()

    async def test_main_finally_skips_stop_if_not_running(self) -> None:
        """The finally block does NOT call stop() a second time when _running is False."""
        # _ComponentError path already calls stop() once; the finally should not double-call it.
        cause = RuntimeError("bad")
        component_err = _ComponentError("cache", cause)

        mock_app_instance = MagicMock()
        mock_app_instance.start = AsyncMock(side_effect=component_err)
        mock_app_instance.stop = AsyncMock()
        mock_app_instance._running = False

        with (
            patch("kuberca.app.KubeRCAApp", return_value=mock_app_instance),
            patch("kuberca.app.get_logger", return_value=MagicMock()),
            pytest.raises(SystemExit),
        ):
            await main()

        # stop() should be called exactly once (from the except block)
        assert mock_app_instance.stop.call_count == 1
