"""In-memory resource cache backed by Kubernetes watch streams.

Stores ``CachedResource`` objects keyed by (kind, namespace, name).
Exposes only ``CachedResourceView`` (redacted) to callers.

Readiness states
----------------
WARMING       – startup list calls are in progress, no data yet.
PARTIALLY_READY – at least one kind is populated; some may be missing.
READY         – all registered kinds have been listed and are being watched.
DEGRADED      – divergence detected: miss rate, reconnect failures, relist
                timeouts, or 429 bursts indicate stale data.

Divergence detection
--------------------
Any of the following triggers DEGRADED:
    - Miss rate > 5 % over a 5-minute rolling window
    - Reconnect failures > 3 (set by watchers via notify_reconnect_failure)
    - Relist timeouts > 2 in any 10-minute window
    - 429 bursts lasting > 1 minute (set by watchers via notify_burst_error)

PARTIALLY_READY degrades to DEGRADED after 10 minutes of staleness per kind.

List caps
---------
    - Regular kinds: 200 resources
    - Nodes: 2 000 resources

Startup
-------
:meth:`populate` performs sequential list calls with 100 ms delay between
them, covering up to 15 resource kinds.
"""

from __future__ import annotations

import asyncio
import builtins
from collections import defaultdict, deque
from datetime import UTC, datetime, timedelta
from typing import Any

from kuberca.models.resources import CachedResource, CachedResourceView, CacheReadiness
from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import (
    cache_divergence_events_total,
    cache_miss_rate,
    cache_misses_total,
    cache_relist_timeout_total,
    cache_resources,
    cache_state,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_LIST_CAP_DEFAULT: int = 200
_LIST_CAP_NODES: int = 2_000
_NODE_KIND: str = "Node"

_MISS_RATE_THRESHOLD: float = 0.05  # 5 %
_MISS_RATE_WINDOW: timedelta = timedelta(minutes=5)
_RELIST_TIMEOUT_THRESHOLD: int = 2
_RELIST_TIMEOUT_WINDOW: timedelta = timedelta(minutes=10)
_RECONNECT_FAILURE_THRESHOLD: int = 3
_BURST_WINDOW: timedelta = timedelta(minutes=1)
_PARTIALLY_READY_STALENESS_TIMEOUT: timedelta = timedelta(minutes=10)
_STARTUP_DELAY_S: float = 0.1
_MAX_STARTUP_KINDS: int = 18

# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------

# Resource store: kind -> namespace -> name -> CachedResource
_Store = dict[str, dict[str, dict[str, CachedResource]]]


class ResourceCache:
    """Thread-safe (asyncio) in-memory resource cache.

    Watchers call :meth:`update` and :meth:`remove` to maintain freshness.
    All read methods return ``CachedResourceView`` (immutable, redacted).

    Example::

        cache = ResourceCache()
        await cache.populate(v1_api)  # optional startup listing
        view = cache.get("Pod", "default", "my-pod")
    """

    def __init__(self) -> None:
        self._log = get_logger("cache.resource")
        self._store: _Store = defaultdict(lambda: defaultdict(dict))

        # Per-kind metadata
        self._last_updated: dict[str, datetime] = {}
        self._resource_versions: dict[str, str] = {}
        self._truncated: dict[str, bool] = {}
        self._ready_kinds: set[str] = set()
        self._all_kinds: set[str] = set()

        # Readiness state
        self._readiness: CacheReadiness = CacheReadiness.WARMING
        self._partially_ready_since: datetime | None = None

        # Miss-rate tracking: rolling list of (timestamp, hit: bool)
        self._access_log: deque[tuple[datetime, bool]] = deque()

        # Divergence counters
        self._reconnect_failures: int = 0
        self._relist_timeout_times: deque[datetime] = deque()
        self._burst_error_times: deque[datetime] = deque()

    # ------------------------------------------------------------------
    # Startup population
    # ------------------------------------------------------------------

    async def populate(self, api: Any, kinds: list[str] | None = None) -> None:
        """Sequentially list up to 15 resource kinds with 100 ms inter-call delay.

        Args:
            api: A kubernetes_asyncio API instance (e.g. ``CoreV1Api``), or a
                 dict mapping kind name to its owning API instance for multi-API
                 configurations.  When a plain API object is provided, only kinds
                 whose list method exists on that object will be populated; the
                 rest are silently skipped and can be populated later via
                 :meth:`update` as watch events arrive.
            kinds: Optional explicit list of kind names to pre-populate.
                   Defaults to a sensible default set.
        """
        target_kinds = (kinds or _default_kinds())[:_MAX_STARTUP_KINDS]
        self._all_kinds = set(target_kinds)
        self._readiness = CacheReadiness.WARMING
        self._emit_state_metric()

        for kind in target_kinds:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                break
            # Resolve the API instance for this kind
            resolved_api = api.get(kind, None) if isinstance(api, dict) else api
            if resolved_api is None:
                self._log.debug("cache_no_api_for_kind", kind=kind)
                continue
            await self._list_kind(resolved_api, kind)
            await asyncio.sleep(_STARTUP_DELAY_S)

        self._recompute_readiness()
        self._log.info(
            "cache_populated",
            ready_kinds=len(self._ready_kinds),
            total_kinds=len(self._all_kinds),
            readiness=self._readiness.value,
        )

    async def _list_kind(self, api: Any, kind: str) -> None:
        """Call the appropriate list API for a single kind and populate the cache."""
        list_func = _get_list_func(api, kind)
        if list_func is None:
            self._log.warning("cache_no_list_func", kind=kind)
            return

        cap = _LIST_CAP_NODES if kind == _NODE_KIND else _LIST_CAP_DEFAULT
        try:
            result = await list_func(_preload_content=True, watch=False, limit=cap)
            items = getattr(result, "items", []) or []
            rv = ""
            if hasattr(result, "metadata") and result.metadata is not None:
                rv = getattr(result.metadata, "resource_version", "") or ""

            for item in items:
                self._ingest_object(kind, item, raw={})

            self._resource_versions[kind] = rv
            self._last_updated[kind] = datetime.now(tz=UTC)

            # Mark truncated if the server returned a continue token
            is_truncated = False
            if hasattr(result, "metadata") and result.metadata is not None:
                cont = getattr(result.metadata, "_continue", None)
                is_truncated = bool(cont)
            self._truncated[kind] = is_truncated

            self._ready_kinds.add(kind)
            cache_resources.labels(kind=kind).set(self._kind_count(kind))
            self._log.debug(
                "cache_kind_listed",
                kind=kind,
                count=len(items),
                truncated=is_truncated,
                resource_version=rv,
            )
        except Exception as exc:
            self._log.error("cache_list_failed", kind=kind, error=str(exc), exc_info=True)

    # ------------------------------------------------------------------
    # Write interface (called by watchers)
    # ------------------------------------------------------------------

    def update(self, kind: str, namespace: str, name: str, raw_obj: dict[str, Any]) -> None:
        """Upsert a resource from a watch event.

        Args:
            kind: Resource kind string (e.g. ``"Pod"``).
            namespace: Namespace, or empty string for cluster-scoped resources.
            name: Resource name.
            raw_obj: The raw object dict from the watch stream.
        """
        metadata = raw_obj.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}

        labels_raw = metadata.get("labels", {})
        labels: dict[str, str] = {str(k): str(v) for k, v in labels_raw.items()} if isinstance(labels_raw, dict) else {}

        annotations_raw = metadata.get("annotations", {})
        annotations: dict[str, str] = (
            {str(k): str(v) for k, v in annotations_raw.items()} if isinstance(annotations_raw, dict) else {}
        )

        spec = raw_obj.get("spec", {})
        if not isinstance(spec, dict):
            spec = {}

        status = raw_obj.get("status", {})
        if not isinstance(status, dict):
            status = {}

        rv = str(metadata.get("resourceVersion", ""))

        existing = self._store[kind][namespace].get(name)
        if existing is not None:
            existing.resource_version = rv
            existing.labels = labels
            existing.annotations = annotations
            existing.spec = spec
            existing.status = status
            existing.last_updated = datetime.now(tz=UTC)
            existing.invalidate_view()
        else:
            cap = _LIST_CAP_NODES if kind == _NODE_KIND else _LIST_CAP_DEFAULT
            if self._kind_count(kind) >= cap:
                self._truncated[kind] = True
                return
            self._store[kind][namespace][name] = CachedResource(
                kind=kind,
                namespace=namespace,
                name=name,
                resource_version=rv,
                labels=labels,
                annotations=annotations,
                spec=spec,
                status=status,
                last_updated=datetime.now(tz=UTC),
            )

        self._last_updated[kind] = datetime.now(tz=UTC)
        if rv:
            self._resource_versions[kind] = rv
        cache_resources.labels(kind=kind).set(self._kind_count(kind))

    def remove(self, kind: str, namespace: str, name: str) -> None:
        """Remove a resource from the cache (DELETED watch event).

        Args:
            kind: Resource kind.
            namespace: Namespace.
            name: Resource name.
        """
        ns_store = self._store.get(kind, {}).get(namespace)
        if ns_store and name in ns_store:
            del ns_store[name]
            cache_resources.labels(kind=kind).set(self._kind_count(kind))

    # ------------------------------------------------------------------
    # Read interface
    # ------------------------------------------------------------------

    def get(self, kind: str, namespace: str, name: str) -> CachedResourceView | None:
        """Return the redacted view for a specific resource, or None if absent.

        Records a miss for divergence tracking when the resource is not found.

        Args:
            kind: Resource kind.
            namespace: Namespace.
            name: Resource name.
        """
        resource = self._store.get(kind, {}).get(namespace, {}).get(name)
        if resource is None:
            self._record_access(False)
            cache_misses_total.inc()
            return None
        self._record_access(True)
        return resource.redacted_view()

    def list(self, kind: str, ns: str = "") -> list[CachedResourceView]:
        """Return all cached resources of a kind, optionally filtered by namespace.

        Capped at 200 (2000 for Nodes).

        Args:
            kind: Resource kind.
            ns: Optional namespace filter; empty string returns all namespaces.
        """
        cap = _LIST_CAP_NODES if kind == _NODE_KIND else _LIST_CAP_DEFAULT
        results: list[CachedResourceView] = []
        ns_map = self._store.get(kind, {})

        candidates = ns_map.get(ns, {}).values() if ns else (r for per_ns in ns_map.values() for r in per_ns.values())

        for resource in candidates:
            if len(results) >= cap:
                break
            results.append(resource.redacted_view())

        return results

    def list_by_label(
        self,
        kind: str,
        labels: dict[str, str],
        ns: str = "",
    ) -> builtins.list[CachedResourceView]:
        """Return cached resources whose labels are a superset of ``labels``.

        Args:
            kind: Resource kind.
            labels: Label key/value pairs that must all be present.
            ns: Optional namespace filter.
        """
        cap = _LIST_CAP_NODES if kind == _NODE_KIND else _LIST_CAP_DEFAULT
        results: list[CachedResourceView] = []
        ns_map = self._store.get(kind, {})

        namespaces = [ns_map.get(ns, {})] if ns else list(ns_map.values())

        for per_ns in namespaces:
            for resource in per_ns.values():
                if len(results) >= cap:
                    return results
                if labels.items() <= resource.labels.items():
                    results.append(resource.redacted_view())

        return results

    def list_truncated(self, kind: str) -> bool:
        """Return True if the list for this kind was capped at the limit.

        Args:
            kind: Resource kind.
        """
        return self._truncated.get(kind, False)

    def readiness(self) -> CacheReadiness:
        """Return the current 4-state cache readiness.

        Recomputes divergence and staleness on each call so callers always
        see an up-to-date value.
        """
        self._recompute_readiness()
        return self._readiness

    def staleness(self, kind: str) -> timedelta | None:
        """Return how long ago a kind was last updated, or None if never.

        Args:
            kind: Resource kind.
        """
        last = self._last_updated.get(kind)
        if last is None:
            return None
        return datetime.now(tz=UTC) - last

    def resource_version(self, kind: str) -> str:
        """Return the most recent resourceVersion seen for a kind.

        Args:
            kind: Resource kind.
        """
        return self._resource_versions.get(kind, "")

    # ------------------------------------------------------------------
    # Divergence notification (called by watchers)
    # ------------------------------------------------------------------

    def notify_reconnect_failure(self) -> None:
        """Increment the reconnect-failure counter for divergence detection."""
        self._reconnect_failures += 1
        if self._reconnect_failures > _RECONNECT_FAILURE_THRESHOLD:
            cache_divergence_events_total.labels(reason="reconnect_failures").inc()
        self._recompute_readiness()

    def notify_relist_timeout(self) -> None:
        """Record a relist timeout for divergence detection."""
        now = datetime.now(tz=UTC)
        self._relist_timeout_times.append(now)
        cache_relist_timeout_total.inc()
        self._recompute_readiness()

    def notify_burst_error(self) -> None:
        """Record a 429/500 burst error for divergence detection."""
        now = datetime.now(tz=UTC)
        self._burst_error_times.append(now)
        self._recompute_readiness()

    def reset_reconnect_failures(self) -> None:
        """Reset reconnect failure counter after a successful reconnection."""
        self._reconnect_failures = 0

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _ingest_object(self, kind: str, obj: Any, raw: dict[str, Any]) -> None:
        """Populate the store from a list-call item (deserialized object)."""
        metadata = getattr(obj, "metadata", None)
        if metadata is None:
            return

        namespace: str = getattr(metadata, "namespace", None) or ""
        name: str = getattr(metadata, "name", None) or ""
        if not name:
            return

        rv: str = getattr(metadata, "resource_version", None) or ""

        labels_raw = getattr(metadata, "labels", None)
        labels: dict[str, str] = {}
        if isinstance(labels_raw, dict):
            labels = {str(k): str(v) for k, v in labels_raw.items()}

        annotations_raw = getattr(metadata, "annotations", None)
        annotations: dict[str, str] = {}
        if isinstance(annotations_raw, dict):
            annotations = {str(k): str(v) for k, v in annotations_raw.items()}

        spec_obj = getattr(obj, "spec", None)
        spec: dict[str, Any] = {}
        if spec_obj is not None and hasattr(spec_obj, "to_dict"):
            spec = spec_obj.to_dict() or {}
        elif isinstance(spec_obj, dict):
            spec = spec_obj

        status_obj = getattr(obj, "status", None)
        status: dict[str, Any] = {}
        if status_obj is not None and hasattr(status_obj, "to_dict"):
            status = status_obj.to_dict() or {}
        elif isinstance(status_obj, dict):
            status = status_obj

        self._store[kind][namespace][name] = CachedResource(
            kind=kind,
            namespace=namespace,
            name=name,
            resource_version=rv,
            labels=labels,
            annotations=annotations,
            spec=spec,
            status=status,
            last_updated=datetime.now(tz=UTC),
        )

    def _kind_count(self, kind: str) -> int:
        """Return total number of cached objects for a kind."""
        ns_map = self._store.get(kind, {})
        return sum(len(per_ns) for per_ns in ns_map.values())

    def _record_access(self, hit: bool) -> None:
        """Append an access result to the rolling miss-rate window."""
        now = datetime.now(tz=UTC)
        self._access_log.append((now, hit))
        # Trim old entries
        cutoff = now - _MISS_RATE_WINDOW
        while self._access_log and self._access_log[0][0] < cutoff:
            self._access_log.popleft()
        # Emit rolling miss rate
        if self._access_log:
            misses = sum(1 for _, h in self._access_log if not h)
            rate = misses / len(self._access_log)
            cache_miss_rate.set(rate)

    def _current_miss_rate(self) -> float:
        """Compute miss rate over the last 5-minute window."""
        if not self._access_log:
            return 0.0
        now = datetime.now(tz=UTC)
        cutoff = now - _MISS_RATE_WINDOW
        recent = [(ts, hit) for ts, hit in self._access_log if ts >= cutoff]
        if not recent:
            return 0.0
        misses = sum(1 for _, hit in recent if not hit)
        return misses / len(recent)

    def _recent_relist_timeouts(self) -> int:
        """Count relist timeouts in the last 10-minute window."""
        now = datetime.now(tz=UTC)
        cutoff = now - _RELIST_TIMEOUT_WINDOW
        # Trim and count
        while self._relist_timeout_times and self._relist_timeout_times[0] < cutoff:
            self._relist_timeout_times.popleft()
        return len(self._relist_timeout_times)

    def _recent_burst_duration(self) -> float:
        """Return seconds since the first burst error in the current window, or 0."""
        now = datetime.now(tz=UTC)
        cutoff = now - _BURST_WINDOW
        while self._burst_error_times and self._burst_error_times[0] < cutoff:
            self._burst_error_times.popleft()
        if not self._burst_error_times:
            return 0.0
        oldest = self._burst_error_times[0]
        return (now - oldest).total_seconds()

    def _is_diverged(self) -> bool:
        """Return True if any divergence indicator has been triggered."""
        if self._current_miss_rate() > _MISS_RATE_THRESHOLD:
            cache_divergence_events_total.labels(reason="miss_rate").inc()
            return True
        if self._reconnect_failures > _RECONNECT_FAILURE_THRESHOLD:
            return True
        if self._recent_relist_timeouts() > _RELIST_TIMEOUT_THRESHOLD:
            cache_divergence_events_total.labels(reason="relist_timeouts").inc()
            return True
        if self._recent_burst_duration() > _BURST_WINDOW.total_seconds():
            cache_divergence_events_total.labels(reason="burst_errors").inc()
            return True
        return False

    def _recompute_readiness(self) -> None:
        """Recompute and update the readiness state."""
        if not self._all_kinds:
            # No kinds registered yet — remain WARMING
            self._readiness = CacheReadiness.WARMING
            self._emit_state_metric()
            return

        if self._is_diverged():
            self._readiness = CacheReadiness.DEGRADED
            self._emit_state_metric()
            return

        now = datetime.now(tz=UTC)

        if self._ready_kinds >= self._all_kinds:
            self._readiness = CacheReadiness.READY
            self._partially_ready_since = None
        elif self._ready_kinds:
            # Some kinds are ready
            if self._partially_ready_since is None:
                self._partially_ready_since = now
            age = now - self._partially_ready_since
            if age >= _PARTIALLY_READY_STALENESS_TIMEOUT:
                self._readiness = CacheReadiness.DEGRADED
                cache_divergence_events_total.labels(reason="staleness_timeout").inc()
            else:
                self._readiness = CacheReadiness.PARTIALLY_READY
        else:
            self._readiness = CacheReadiness.WARMING

        self._emit_state_metric()

    def _emit_state_metric(self) -> None:
        """Update the cache_state gauge for all states."""
        for state in CacheReadiness:
            cache_state.labels(state=state.value).set(1 if self._readiness == state else 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _default_kinds() -> list[str]:
    """Return the default set of resource kinds to pre-populate on startup."""
    return [
        "Pod",
        "Node",
        "Deployment",
        "ReplicaSet",
        "StatefulSet",
        "DaemonSet",
        "Service",
        "Endpoints",
        "ConfigMap",
        "Secret",
        "PersistentVolumeClaim",
        "PersistentVolume",
        "ResourceQuota",
        "Event",
        "Namespace",
        "Job",
        "CronJob",
        "ServiceAccount",
    ]


def _get_list_func(api: Any, kind: str) -> Any:
    """Return the appropriate list function for a resource kind, or None."""
    kind_to_method: dict[str, str] = {
        "Pod": "list_pod_for_all_namespaces",
        "Node": "list_node",
        "Event": "list_event_for_all_namespaces",
        "Namespace": "list_namespace",
        "Deployment": "list_deployment_for_all_namespaces",
        "ReplicaSet": "list_replica_set_for_all_namespaces",
        "StatefulSet": "list_stateful_set_for_all_namespaces",
        "DaemonSet": "list_daemon_set_for_all_namespaces",
        "Service": "list_service_for_all_namespaces",
        "Endpoints": "list_endpoints_for_all_namespaces",
        "ConfigMap": "list_config_map_for_all_namespaces",
        "Secret": "list_secret_for_all_namespaces",
        "PersistentVolumeClaim": "list_persistent_volume_claim_for_all_namespaces",
        "PersistentVolume": "list_persistent_volume",
        "ResourceQuota": "list_resource_quota_for_all_namespaces",
        "Job": "list_job_for_all_namespaces",
        "CronJob": "list_cron_job_for_all_namespaces",
        "ServiceAccount": "list_service_account_for_all_namespaces",
    }
    method_name = kind_to_method.get(kind)
    if method_name is None:
        return None
    return getattr(api, method_name, None)
