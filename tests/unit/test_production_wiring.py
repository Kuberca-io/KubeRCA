"""Tests for production wiring of dead-code gaps.

Verifies:
- api_map in app.py covers all 18 default cache kinds
- _feed_graph() builds a valid ResourceSnapshot and the graph receives it
- compute_rolling_rates() computes correct percentages from Counter data
- compute_rolling_rates() handles zero total gracefully
- AnalystCoordinator with a real DependencyGraph returns non-empty blast_radius
  and state_context
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from kuberca.cache.resource_cache import _default_kinds
from kuberca.graph.dependency_graph import DependencyGraph
from kuberca.models.resources import ResourceSnapshot

# ---------------------------------------------------------------------------
# Gap 2: api_map coverage
# ---------------------------------------------------------------------------


class TestApiMapCoverage:
    """Verify that the api_map in _start_cache() covers all default kinds."""

    def test_api_map_covers_all_default_kinds(self) -> None:
        """Read the api_map dict from app.py source and check every kind."""
        import ast
        import pathlib

        app_path = pathlib.Path(__file__).resolve().parents[2] / "kuberca" / "app.py"
        source = app_path.read_text()
        tree = ast.parse(source)

        # Find the api_map assignment inside _start_cache
        api_map_keys: set[str] = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "api_map" and isinstance(node.value, ast.Dict):
                        for key in node.value.keys:
                            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                                api_map_keys.add(key.value)

        default_kinds = set(_default_kinds())
        missing = default_kinds - api_map_keys
        assert not missing, f"api_map is missing kinds: {missing}"

    def test_default_kinds_is_18(self) -> None:
        assert len(_default_kinds()) == 18


# ---------------------------------------------------------------------------
# Gap 1: _feed_graph
# ---------------------------------------------------------------------------


def _make_raw_pod(
    namespace: str = "default",
    name: str = "my-pod",
    node_name: str = "node-1",
) -> dict:
    return {
        "metadata": {
            "namespace": namespace,
            "name": name,
            "labels": {"app": "test"},
            "annotations": {},
            "resourceVersion": "100",
            "ownerReferences": [{"kind": "ReplicaSet", "name": "my-rs", "uid": "rs-uid"}],
        },
        "spec": {
            "nodeName": node_name,
            "containers": [{"name": "main", "image": "nginx"}],
            "volumes": [],
        },
        "status": {"phase": "Running"},
    }


class TestFeedGraph:
    """Verify that _feed_graph builds a valid ResourceSnapshot."""

    def test_feed_graph_adds_resource_to_graph(self) -> None:
        from kuberca.app import _feed_graph

        graph = DependencyGraph()
        raw = _make_raw_pod()
        _feed_graph(graph, "Pod", "default", "my-pod", raw)

        assert graph.node_count >= 1
        node = graph.get_node("Pod", "default", "my-pod")
        assert node is not None

    def test_feed_graph_creates_owner_edge(self) -> None:
        from kuberca.app import _feed_graph

        graph = DependencyGraph()
        raw = _make_raw_pod()
        _feed_graph(graph, "Pod", "default", "my-pod", raw)

        # Should have an edge to the ownerReference ReplicaSet
        rs_node = graph.get_node("ReplicaSet", "default", "my-rs")
        assert rs_node is not None

    def test_feed_graph_creates_node_assignment_edge(self) -> None:
        from kuberca.app import _feed_graph

        graph = DependencyGraph()
        raw = _make_raw_pod(node_name="worker-1")
        _feed_graph(graph, "Pod", "default", "my-pod", raw)

        node = graph.get_node("Node", "", "worker-1")
        assert node is not None

    def test_feed_graph_none_graph_is_noop(self) -> None:
        from kuberca.app import _feed_graph

        # Should not raise
        _feed_graph(None, "Pod", "default", "my-pod", {})

    def test_feed_graph_remove_on_delete(self) -> None:
        from kuberca.app import _feed_graph

        graph = DependencyGraph()
        raw = _make_raw_pod()
        _feed_graph(graph, "Pod", "default", "my-pod", raw)
        assert graph.get_node("Pod", "default", "my-pod") is not None

        graph.remove_resource("Pod", "default", "my-pod")
        assert graph.get_node("Pod", "default", "my-pod") is None


# ---------------------------------------------------------------------------
# Gap 3: compute_rolling_rates
# ---------------------------------------------------------------------------


class TestComputeRollingRates:
    """Verify rate gauge computation from rca_requests_total Counter."""

    def test_zero_total_sets_all_zero(self) -> None:
        from prometheus_client import CollectorRegistry, Counter, Gauge

        from kuberca.observability import metrics

        # Save originals
        orig_counter = metrics.rca_requests_total
        orig_hit = metrics.rule_engine_hit_rate
        orig_esc = metrics.llm_escalation_rate
        orig_inc = metrics.inconclusive_rate

        registry = CollectorRegistry()
        try:
            # Create isolated metrics
            metrics.rca_requests_total = Counter(
                "kuberca_rca_requests_total",
                "test",
                ["diagnosed_by"],
                registry=registry,
            )
            metrics.rule_engine_hit_rate = Gauge("kuberca_rule_engine_hit_rate", "test", registry=registry)
            metrics.llm_escalation_rate = Gauge("kuberca_llm_escalation_rate", "test", registry=registry)
            metrics.inconclusive_rate = Gauge("kuberca_inconclusive_rate", "test", registry=registry)

            metrics.compute_rolling_rates()

            assert metrics.rule_engine_hit_rate._value.get() == 0.0
            assert metrics.llm_escalation_rate._value.get() == 0.0
            assert metrics.inconclusive_rate._value.get() == 0.0
        finally:
            metrics.rca_requests_total = orig_counter
            metrics.rule_engine_hit_rate = orig_hit
            metrics.llm_escalation_rate = orig_esc
            metrics.inconclusive_rate = orig_inc

    def test_computes_correct_percentages(self) -> None:
        from prometheus_client import CollectorRegistry, Counter, Gauge

        from kuberca.observability import metrics

        orig_counter = metrics.rca_requests_total
        orig_hit = metrics.rule_engine_hit_rate
        orig_esc = metrics.llm_escalation_rate
        orig_inc = metrics.inconclusive_rate

        registry = CollectorRegistry()
        try:
            metrics.rca_requests_total = Counter(
                "kuberca_rca_requests_total",
                "test",
                ["diagnosed_by"],
                registry=registry,
            )
            metrics.rule_engine_hit_rate = Gauge("kuberca_rule_engine_hit_rate", "test", registry=registry)
            metrics.llm_escalation_rate = Gauge("kuberca_llm_escalation_rate", "test", registry=registry)
            metrics.inconclusive_rate = Gauge("kuberca_inconclusive_rate", "test", registry=registry)

            # Simulate: 7 rule_engine, 2 llm, 1 inconclusive = 10 total
            for _ in range(7):
                metrics.rca_requests_total.labels(diagnosed_by="rule_engine").inc()
            for _ in range(2):
                metrics.rca_requests_total.labels(diagnosed_by="llm").inc()
            metrics.rca_requests_total.labels(diagnosed_by="inconclusive").inc()

            metrics.compute_rolling_rates()

            assert metrics.rule_engine_hit_rate._value.get() == pytest.approx(70.0)
            assert metrics.llm_escalation_rate._value.get() == pytest.approx(20.0)
            assert metrics.inconclusive_rate._value.get() == pytest.approx(10.0)
        finally:
            metrics.rca_requests_total = orig_counter
            metrics.rule_engine_hit_rate = orig_hit
            metrics.llm_escalation_rate = orig_esc
            metrics.inconclusive_rate = orig_inc


# ---------------------------------------------------------------------------
# Gap 1: Coordinator with real DependencyGraph
# ---------------------------------------------------------------------------


def _make_event(
    reason: str = "OOMKilled",
    kind: str = "Pod",
    namespace: str = "default",
    name: str = "my-pod",
):
    from kuberca.models.events import EventRecord, EventSource, Severity

    ts = datetime(2026, 2, 19, 12, 0, 0, tzinfo=UTC)
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id="test",
        source=EventSource.CORE_EVENT,
        severity=Severity.WARNING,
        reason=reason,
        message="test event",
        namespace=namespace,
        resource_kind=kind,
        resource_name=name,
        first_seen=ts,
        last_seen=ts,
        count=1,
    )


class TestCoordinatorWithGraph:
    """Verify AnalystCoordinator uses the DependencyGraph for blast_radius and state_context."""

    def _build_graph_with_pod(self) -> DependencyGraph:
        """Create a graph with a Pod → ConfigMap dependency."""
        graph = DependencyGraph()
        # Pod with a configMap volume mount
        pod_spec = {
            "metadata": {
                "namespace": "default",
                "name": "web-pod",
                "labels": {},
                "annotations": {},
                "ownerReferences": [],
            },
            "spec": {
                "nodeName": "node-1",
                "containers": [{"name": "app", "image": "nginx"}],
                "volumes": [
                    {"configMap": {"name": "app-config"}},
                ],
            },
            "status": {"phase": "Running"},
        }
        graph.add_resource(
            ResourceSnapshot(
                kind="Pod",
                namespace="default",
                name="web-pod",
                spec_hash="",
                spec=pod_spec,
                captured_at=datetime(2026, 2, 19, tzinfo=UTC),
                resource_version="1",
            )
        )
        # ConfigMap itself
        cm_spec = {
            "metadata": {
                "namespace": "default",
                "name": "app-config",
                "labels": {},
                "annotations": {},
            },
            "spec": {},
            "status": {},
        }
        graph.add_resource(
            ResourceSnapshot(
                kind="ConfigMap",
                namespace="default",
                name="app-config",
                spec_hash="",
                spec=cm_spec,
                captured_at=datetime(2026, 2, 19, tzinfo=UTC),
                resource_version="1",
            )
        )
        return graph

    def test_blast_radius_returns_resources(self) -> None:
        """ConfigMap upstream query should find the Pod that depends on it."""
        graph = self._build_graph_with_pod()
        upstream = graph.upstream("ConfigMap", "default", "app-config")
        assert len(upstream.resources) >= 1
        kinds = [r.kind for r in upstream.resources]
        assert "Pod" in kinds

    def test_coordinator_blast_radius_populated(self) -> None:
        from kuberca.analyst.coordinator import AnalystCoordinator

        graph = self._build_graph_with_pod()

        # Create a minimal coordinator with mocks
        cache = MagicMock()
        cache.readiness.return_value = MagicMock(value="ready")
        cache.get.return_value = None

        coordinator = AnalystCoordinator(
            rule_engine=MagicMock(),
            llm_analyzer=None,
            cache=cache,
            ledger=MagicMock(),
            event_buffer=MagicMock(),
            config=MagicMock(cluster_id="test"),
            dependency_graph=graph,
        )

        blast = coordinator._compute_blast_radius("ConfigMap", "default", "app-config")
        assert blast is not None
        assert len(blast) >= 1
        assert any(r.kind == "Pod" and r.name == "web-pod" for r in blast)

    def test_coordinator_state_context_populated(self) -> None:
        from kuberca.analyst.coordinator import AnalystCoordinator

        graph = self._build_graph_with_pod()

        cache = MagicMock()
        # Return mock cached resource for cache.get calls
        cached_view = MagicMock()
        cached_view.status = {"phase": "Running"}
        cache.get.return_value = cached_view

        coordinator = AnalystCoordinator(
            rule_engine=MagicMock(),
            llm_analyzer=None,
            cache=cache,
            ledger=MagicMock(),
            event_buffer=MagicMock(),
            config=MagicMock(cluster_id="test"),
            dependency_graph=graph,
        )

        entries = coordinator._build_state_context("Pod", "default", "web-pod")
        assert len(entries) >= 1
        # Should have ConfigMap and/or Node in state context
        kinds_found = {e.kind for e in entries}
        assert "ConfigMap" in kinds_found or "Node" in kinds_found

    def test_coordinator_no_graph_returns_none(self) -> None:
        from kuberca.analyst.coordinator import AnalystCoordinator

        coordinator = AnalystCoordinator(
            rule_engine=MagicMock(),
            llm_analyzer=None,
            cache=MagicMock(),
            ledger=MagicMock(),
            event_buffer=MagicMock(),
            config=MagicMock(cluster_id="test"),
            dependency_graph=None,
        )

        assert coordinator._compute_blast_radius("Pod", "default", "x") is None
        assert coordinator._build_state_context("Pod", "default", "x") == []
