"""Lightweight in-memory resource dependency graph.

Built from K8s API declared configuration. No external graph database.
Updated synchronously from ResourceHandler callbacks.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime

from kuberca.graph.models import DependencyResult, EdgeType, GraphEdge, GraphNode
from kuberca.models.resources import ResourceSnapshot
from kuberca.observability.logging import get_logger

_logger = get_logger("dependency_graph")


class DependencyGraph:
    """In-memory resource dependency graph.

    Nodes are keyed by (kind, namespace, name). Edges are typed and carry
    the extraction source field path as metadata.

    The graph exposes two main query methods:
        upstream(kind, namespace, name) -- resources that depend on the target
        downstream(kind, namespace, name) -- resources the target depends on
    Both support max_depth to prevent unbounded traversal.
    """

    def __init__(self) -> None:
        self._nodes: dict[tuple[str, str, str], GraphNode] = {}
        # Forward edges: source_key -> list of (target_key, edge)
        self._forward: dict[tuple[str, str, str], list[tuple[tuple[str, str, str], GraphEdge]]] = defaultdict(list)
        # Reverse edges: target_key -> list of (source_key, edge)
        self._reverse: dict[tuple[str, str, str], list[tuple[tuple[str, str, str], GraphEdge]]] = defaultdict(list)

    @property
    def node_count(self) -> int:
        """Return the number of nodes in the graph."""
        return len(self._nodes)

    @property
    def edge_count(self) -> int:
        """Return the number of edges in the graph."""
        return sum(len(edges) for edges in self._forward.values())

    def add_resource(self, snapshot: ResourceSnapshot) -> None:
        """Add or update a resource and its edges from declared configuration.

        Extracts relationships from ownerReferences, volume mounts,
        PVC->PV bindings, ConfigMap/Secret refs, node assignments,
        and ResourceQuota scoping.
        """
        node = GraphNode(
            kind=snapshot.kind,
            namespace=snapshot.namespace,
            name=snapshot.name,
        )
        key = node.key

        # Remove old edges for this resource before re-extracting
        self._remove_edges_for(key)
        self._nodes[key] = node

        spec = snapshot.spec or {}
        now = datetime.now(tz=UTC)

        # Extract owner references
        metadata = spec.get("metadata", {}) if isinstance(spec, dict) else {}
        if isinstance(metadata, dict):
            owner_refs = metadata.get("ownerReferences", [])
            if isinstance(owner_refs, list):
                for ref in owner_refs:
                    if not isinstance(ref, dict):
                        continue
                    owner_kind = ref.get("kind", "")
                    owner_name = ref.get("name", "")
                    if owner_kind and owner_name:
                        owner_node = self._ensure_node(owner_kind, snapshot.namespace, owner_name)
                        edge = GraphEdge(
                            source=node,
                            target=owner_node,
                            edge_type=EdgeType.OWNER_REFERENCE,
                            source_field="metadata.ownerReferences",
                            discovered_at=now,
                        )
                        self._add_edge(key, owner_node.key, edge)

        # Pod-specific edges
        if snapshot.kind == "Pod":
            self._extract_pod_edges(node, key, spec, now)

        # PVC -> PV binding
        if snapshot.kind == "PersistentVolumeClaim":
            volume_name = ""
            if isinstance(spec, dict):
                volume_name = str(spec.get("volumeName", "") or "")
                if not volume_name:
                    inner_spec = spec.get("spec", {})
                    if isinstance(inner_spec, dict):
                        volume_name = str(inner_spec.get("volumeName", "") or "")
            if volume_name:
                pv_node = self._ensure_node("PersistentVolume", "", str(volume_name))
                edge = GraphEdge(
                    source=node,
                    target=pv_node,
                    edge_type=EdgeType.VOLUME_BINDING,
                    source_field="spec.volumeName",
                    discovered_at=now,
                )
                self._add_edge(key, pv_node.key, edge)

        # ResourceQuota -> Namespace scoping
        if snapshot.kind == "ResourceQuota" and snapshot.namespace:
            ns_node = self._ensure_node("Namespace", "", snapshot.namespace)
            edge = GraphEdge(
                source=ns_node,
                target=node,
                edge_type=EdgeType.QUOTA_SCOPE,
                source_field="metadata.namespace",
                discovered_at=now,
            )
            self._add_edge(ns_node.key, key, edge)

    def remove_resource(self, kind: str, namespace: str, name: str) -> None:
        """Remove a resource and all its edges (both source and target) from the graph."""
        key = (kind, namespace, name)
        # Remove edges where this key is the source
        self._remove_edges_for(key)
        # Also remove edges where this key is the target (created by other resources)
        if key in self._reverse:
            for source_key, _edge in self._reverse[key]:
                if source_key in self._forward:
                    self._forward[source_key] = [(tk, e) for tk, e in self._forward[source_key] if tk != key]
            del self._reverse[key]
        self._nodes.pop(key, None)

    def upstream(
        self,
        kind: str,
        namespace: str,
        name: str,
        max_depth: int = 3,
    ) -> DependencyResult:
        """Return all resources that depend on the target (reverse traversal).

        Follows reverse edges: finds resources where the target appears
        as a dependency (e.g., for a ConfigMap, finds Pods that mount it).
        """
        return self._traverse(kind, namespace, name, max_depth, direction="reverse")

    def downstream(
        self,
        kind: str,
        namespace: str,
        name: str,
        max_depth: int = 3,
    ) -> DependencyResult:
        """Return all resources the target depends on (forward traversal).

        Follows forward edges: finds dependencies of the target
        (e.g., for a Pod, finds its PVC, ConfigMap, Node).
        """
        return self._traverse(kind, namespace, name, max_depth, direction="forward")

    def path_between(
        self,
        source: GraphNode,
        target: GraphNode,
    ) -> list[GraphEdge] | None:
        """Find a path between two nodes. Returns edges or None if no path."""
        if source.key == target.key:
            return []

        visited: set[tuple[str, str, str]] = {source.key}
        queue: list[tuple[tuple[str, str, str], list[GraphEdge]]] = [(source.key, [])]

        while queue:
            current_key, path = queue.pop(0)

            for neighbor_key, edge in self._forward.get(current_key, []):
                if neighbor_key not in visited:
                    new_path = path + [edge]
                    if neighbor_key == target.key:
                        return new_path
                    visited.add(neighbor_key)
                    queue.append((neighbor_key, new_path))

            for neighbor_key, edge in self._reverse.get(current_key, []):
                if neighbor_key not in visited:
                    new_path = path + [edge]
                    if neighbor_key == target.key:
                        return new_path
                    visited.add(neighbor_key)
                    queue.append((neighbor_key, new_path))

        return None

    def get_node(self, kind: str, namespace: str, name: str) -> GraphNode | None:
        """Return a node by its key, or None if not found."""
        return self._nodes.get((kind, namespace, name))

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _extract_pod_edges(
        self,
        node: GraphNode,
        key: tuple[str, str, str],
        spec: dict[str, object],
        now: datetime,
    ) -> None:
        """Extract Pod-specific dependency edges."""
        pod_spec = spec.get("spec", spec) if isinstance(spec, dict) else spec
        if not isinstance(pod_spec, dict):
            return

        # spec.nodeName -> Node
        node_name = pod_spec.get("nodeName", "")
        if node_name:
            node_target = self._ensure_node("Node", "", str(node_name))
            edge = GraphEdge(
                source=node,
                target=node_target,
                edge_type=EdgeType.NODE_ASSIGNMENT,
                source_field="spec.nodeName",
                discovered_at=now,
            )
            self._add_edge(key, node_target.key, edge)

        # spec.volumes
        volumes = pod_spec.get("volumes", [])
        if isinstance(volumes, list):
            for vol in volumes:
                if not isinstance(vol, dict):
                    continue
                # PVC reference
                pvc_ref = vol.get("persistentVolumeClaim")
                if isinstance(pvc_ref, dict):
                    claim_name = pvc_ref.get("claimName", "")
                    if claim_name:
                        pvc_node = self._ensure_node("PersistentVolumeClaim", node.namespace, str(claim_name))
                        edge = GraphEdge(
                            source=node,
                            target=pvc_node,
                            edge_type=EdgeType.VOLUME_BINDING,
                            source_field="spec.volumes[*].persistentVolumeClaim.claimName",
                            discovered_at=now,
                        )
                        self._add_edge(key, pvc_node.key, edge)

                # ConfigMap volume
                cm_ref = vol.get("configMap")
                if isinstance(cm_ref, dict):
                    cm_name = cm_ref.get("name", "")
                    if cm_name:
                        cm_node = self._ensure_node("ConfigMap", node.namespace, str(cm_name))
                        edge = GraphEdge(
                            source=node,
                            target=cm_node,
                            edge_type=EdgeType.CONFIG_REFERENCE,
                            source_field="spec.volumes[*].configMap.name",
                            discovered_at=now,
                        )
                        self._add_edge(key, cm_node.key, edge)

                # Secret volume
                secret_ref = vol.get("secret")
                if isinstance(secret_ref, dict):
                    secret_name = secret_ref.get("secretName", "")
                    if secret_name:
                        secret_node = self._ensure_node("Secret", node.namespace, str(secret_name))
                        edge = GraphEdge(
                            source=node,
                            target=secret_node,
                            edge_type=EdgeType.CONFIG_REFERENCE,
                            source_field="spec.volumes[*].secret.secretName",
                            discovered_at=now,
                        )
                        self._add_edge(key, secret_node.key, edge)

        # spec.containers[*].envFrom
        containers = pod_spec.get("containers", [])
        if isinstance(containers, list):
            for container in containers:
                if not isinstance(container, dict):
                    continue
                env_from = container.get("envFrom", [])
                if isinstance(env_from, list):
                    for ef in env_from:
                        if not isinstance(ef, dict):
                            continue
                        cm_ref = ef.get("configMapRef")
                        if isinstance(cm_ref, dict) and cm_ref.get("name"):
                            cm_node = self._ensure_node("ConfigMap", node.namespace, str(cm_ref["name"]))
                            edge = GraphEdge(
                                source=node,
                                target=cm_node,
                                edge_type=EdgeType.CONFIG_REFERENCE,
                                source_field="spec.containers[*].envFrom[*].configMapRef",
                                discovered_at=now,
                            )
                            self._add_edge(key, cm_node.key, edge)

                        secret_ref = ef.get("secretRef")
                        if isinstance(secret_ref, dict) and secret_ref.get("name"):
                            secret_node = self._ensure_node("Secret", node.namespace, str(secret_ref["name"]))
                            edge = GraphEdge(
                                source=node,
                                target=secret_node,
                                edge_type=EdgeType.CONFIG_REFERENCE,
                                source_field="spec.containers[*].envFrom[*].secretRef",
                                discovered_at=now,
                            )
                            self._add_edge(key, secret_node.key, edge)

    def _ensure_node(self, kind: str, namespace: str, name: str) -> GraphNode:
        """Get or create a node."""
        key = (kind, namespace, name)
        if key not in self._nodes:
            self._nodes[key] = GraphNode(kind=kind, namespace=namespace, name=name)
        return self._nodes[key]

    def _add_edge(
        self,
        source_key: tuple[str, str, str],
        target_key: tuple[str, str, str],
        edge: GraphEdge,
    ) -> None:
        """Add an edge to both forward and reverse adjacency lists."""
        self._forward[source_key].append((target_key, edge))
        self._reverse[target_key].append((source_key, edge))

    def _remove_edges_for(self, key: tuple[str, str, str]) -> None:
        """Remove edges where key is the source (edges created by this resource).

        Edges where key is the TARGET are left intact — they were created
        by other resources and should only be cleaned up when those
        resources are removed or re-added.
        """
        if key in self._forward:
            for target_key, _edge in self._forward[key]:
                if target_key in self._reverse:
                    self._reverse[target_key] = [(sk, e) for sk, e in self._reverse[target_key] if sk != key]
            del self._forward[key]

    def _traverse(
        self,
        kind: str,
        namespace: str,
        name: str,
        max_depth: int,
        direction: str,
    ) -> DependencyResult:
        """BFS traversal in the specified direction."""
        start_key = (kind, namespace, name)
        if start_key not in self._nodes:
            return DependencyResult()

        adj = self._reverse if direction == "reverse" else self._forward
        visited: set[tuple[str, str, str]] = {start_key}
        result_nodes: list[GraphNode] = []
        result_edges: list[GraphEdge] = []
        current_level: list[tuple[str, str, str]] = [start_key]
        depth = 0
        truncated = False

        while current_level and depth < max_depth:
            next_level: list[tuple[str, str, str]] = []
            for current_key in current_level:
                for neighbor_key, edge in adj.get(current_key, []):
                    if neighbor_key not in visited:
                        visited.add(neighbor_key)
                        next_level.append(neighbor_key)
                        if neighbor_key in self._nodes:
                            result_nodes.append(self._nodes[neighbor_key])
                        result_edges.append(edge)
            current_level = next_level
            if next_level:
                depth += 1

        # Check if truncated
        if current_level and depth >= max_depth:
            for current_key in current_level:
                if any(nk not in visited for nk, _ in adj.get(current_key, [])):
                    truncated = True
                    break

        return DependencyResult(
            resources=result_nodes,
            edges=result_edges,
            depth_reached=depth,
            truncated=truncated,
        )
