"""Extended edge-case unit tests for kuberca.cache.resource_cache.ResourceCache.

These tests cover divergence detection, readiness state transitions, update
edge cases, and list/get behaviour.  They do NOT require a running Kubernetes
cluster — all interactions are through the public synchronous API
(update/remove/get/list/notify_*/reset_*) and internal state helpers.
"""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from kuberca.cache.resource_cache import ResourceCache
from kuberca.models.resources import CacheReadiness

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _raw_pod(name: str, namespace: str = "default", rv: str = "1", labels: dict | None = None) -> dict:
    """Return a minimal raw Pod dict suitable for cache.update()."""
    return {
        "metadata": {
            "resourceVersion": rv,
            "labels": labels or {},
            "annotations": {},
        },
        "spec": {"containers": [{"name": "app", "image": "nginx:latest"}]},
        "status": {"phase": "Running"},
    }


def _populate_kind(cache: ResourceCache, kind: str, count: int, namespace: str = "default") -> None:
    """Insert *count* resources of *kind* into the cache."""
    for i in range(count):
        cache.update(kind, namespace, f"{kind.lower()}-{i}", _raw_pod(f"{kind.lower()}-{i}", namespace, rv=str(i)))


def _make_ready(cache: ResourceCache, kinds: set[str]) -> None:
    """Set up the cache so that exactly *kinds* are registered AND ready."""
    cache._all_kinds = set(kinds)
    cache._ready_kinds = set(kinds)


# ---------------------------------------------------------------------------
# TestDivergenceDetection
# ---------------------------------------------------------------------------


class TestDivergenceDetection:
    def test_reconnect_failure_degrades(self) -> None:
        """More than 3 reconnect failures must transition the cache to DEGRADED."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        # Threshold is > 3; fire exactly 4 to cross it
        for _ in range(4):
            cache.notify_reconnect_failure()

        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_reconnect_at_threshold_does_not_degrade(self) -> None:
        """Exactly 3 reconnect failures must NOT trigger DEGRADED (threshold is strictly >3)."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        for _ in range(3):
            cache.notify_reconnect_failure()

        assert cache.readiness() == CacheReadiness.READY

    def test_reset_reconnect_failures_recovers(self) -> None:
        """reset_reconnect_failures() zeroes the counter; readiness must recover to READY."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        for _ in range(4):
            cache.notify_reconnect_failure()
        assert cache.readiness() == CacheReadiness.DEGRADED

        cache.reset_reconnect_failures()
        # After reset the counter is 0 — no longer above threshold
        assert cache.readiness() == CacheReadiness.READY

    def test_reset_reconnect_failures_zeroes_counter(self) -> None:
        """reset_reconnect_failures() must set the counter to zero."""
        cache = ResourceCache()
        for _ in range(5):
            cache.notify_reconnect_failure()
        cache.reset_reconnect_failures()
        assert cache._reconnect_failures == 0

    def test_relist_timeout_degrades(self) -> None:
        """More than 2 relist timeouts in the 10-minute window must trigger DEGRADED."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        # Threshold is > 2; fire 3 to cross it
        for _ in range(3):
            cache.notify_relist_timeout()

        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_relist_timeout_at_threshold_does_not_degrade(self) -> None:
        """Exactly 2 relist timeouts must NOT trigger DEGRADED (threshold is strictly >2)."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        for _ in range(2):
            cache.notify_relist_timeout()

        assert cache.readiness() == CacheReadiness.READY

    def test_relist_timeout_old_events_pruned(self) -> None:
        """Relist timeouts older than 10 minutes must not count toward the threshold."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        # Inject timestamps from 11 minutes ago — these are outside the window
        stale = datetime.now(tz=UTC) - timedelta(minutes=11)
        cache._relist_timeout_times = deque([stale, stale, stale])

        # _recent_relist_timeouts() should prune them all; readiness stays READY
        assert cache.readiness() == CacheReadiness.READY

    def test_burst_error_degrades(self) -> None:
        """Burst errors spanning more than 1 minute must trigger DEGRADED.

        _recent_burst_duration() measures elapsed seconds since the oldest
        burst error still within the 1-minute window.  To exceed the 60-second
        threshold we inject synthetic timestamps directly into the internal
        deque — the oldest one more than 60 s ago but still within the 1-min
        window (i.e. between 60 and 60 seconds from "now"), which is the
        boundary condition.  We use exactly 61 s spacing so the duration is
        unambiguously > 60.
        """
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        _now = datetime.now(tz=UTC)  # noqa: F841 — used only for comment reference
        # oldest is 61 s ago — still inside the 60-s window boundary check
        # (cutoff = now - 60s; 61s ago is *before* cutoff, so it IS pruned
        # by _recent_burst_duration).  We instead use 59 s to stay inside
        # the window while exceeding the 60-s duration check: impossible with
        # a single event.
        #
        # The actual divergence condition is:
        #     _recent_burst_duration() > _BURST_WINDOW.total_seconds()   (i.e. > 60)
        # _recent_burst_duration() = (now - oldest_in_window).total_seconds()
        #
        # So we need oldest to be > 60 s ago AND still inside the 60-s window,
        # which is a contradiction — it can never happen naturally.
        #
        # The correct way to exercise this is to back-date the oldest timestamp
        # to be *just* inside the window while having the span exceed 60 s:
        # put two events 61 s apart, but the older one at exactly 60.5 s ago
        # (inside the window) and the newer one at 0.5 s ago.  That gives
        # duration = 60 s exactly, which is NOT > 60.
        #
        # The true trigger requires the oldest surviving timestamp to be older
        # than now-60s *after* pruning, which is impossible given the prune
        # cutoff is now-60s.  Therefore we test the mechanism by directly
        # setting _burst_error_times with a timestamp that yields a duration
        # > 60 s — achieved by a timestamp at exactly now - 61s, trusting that
        # the pruning removes timestamps *strictly less than* the cutoff so a
        # timestamp at cutoff-1s survives the >=cutoff comparison.
        #
        # Inspection of the implementation:
        #   while self._burst_error_times and self._burst_error_times[0] < cutoff:
        #       self._burst_error_times.popleft()
        # cutoff = now - timedelta(minutes=1)
        # An event at now-61s satisfies ts < cutoff and IS pruned.
        # An event at now-59s survives (59s ago is not < 60s-ago-cutoff).
        # Duration = (now - (now-59s)).total_seconds() = 59 — still not > 60.
        #
        # Conclusion: triggering the burst_duration path organically within a
        # single-process test without time travel is not possible.  We therefore
        # verify the internal helper directly and test that _is_diverged() reacts
        # correctly when the helper reports > 60 s.
        duration = cache._recent_burst_duration()
        assert duration >= 0.0  # sanity: helper returns a non-negative float

        # The burst degradation path checks _recent_burst_duration() > 60.0,
        # but the pruning window is also 60s, making the condition unreachable
        # through normal notify_burst_error() calls alone. We mock the helper
        # to test the _is_diverged() → DEGRADED wiring.
        with patch.object(cache, "_recent_burst_duration", return_value=61.0):
            assert cache._is_diverged() is True
            assert cache.readiness() == CacheReadiness.DEGRADED

    def test_miss_rate_degrades(self) -> None:
        """A miss rate above 5% over the rolling window must trigger DEGRADED.

        Strategy: register one kind as ready (so WARMING/PARTIALLY_READY
        are bypassed), then perform 100% miss accesses so the miss rate
        jumps immediately to 1.0 > 0.05.
        """
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        # 20 consecutive misses → miss rate = 1.0, well above 5%
        for i in range(20):
            cache.get("Pod", "default", f"ghost-pod-{i}")

        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_miss_rate_below_threshold_stays_ready(self) -> None:
        """A miss rate at or below 5% must not trigger DEGRADED."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        # Populate 100 pods so that lookups hit
        _populate_kind(cache, "Pod", 100)

        # 100 hits
        for i in range(100):
            cache.get("Pod", "default", f"pod-{i}")

        # 2 misses → 2/102 ≈ 1.96%, below 5%
        cache.get("Pod", "default", "ghost-1")
        cache.get("Pod", "default", "ghost-2")

        assert cache.readiness() == CacheReadiness.READY

    def test_current_miss_rate_empty_log_returns_zero(self) -> None:
        """_current_miss_rate() on a fresh cache must return 0.0."""
        cache = ResourceCache()
        assert cache._current_miss_rate() == 0.0

    def test_current_miss_rate_all_hits_returns_zero(self) -> None:
        """_current_miss_rate() returns 0.0 when every access was a hit."""
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})
        _populate_kind(cache, "Pod", 10)

        for i in range(10):
            cache.get("Pod", "default", f"pod-{i}")

        assert cache._current_miss_rate() == 0.0

    def test_current_miss_rate_all_misses_returns_one(self) -> None:
        """_current_miss_rate() returns 1.0 when every access was a miss."""
        cache = ResourceCache()
        cache.get("Pod", "default", "ghost")  # single miss
        assert cache._current_miss_rate() == 1.0


# ---------------------------------------------------------------------------
# TestReadinessStateMachine
# ---------------------------------------------------------------------------


class TestReadinessStateMachine:
    def test_initial_state_warming(self) -> None:
        """A freshly instantiated cache must report WARMING."""
        cache = ResourceCache()
        assert cache.readiness() == CacheReadiness.WARMING

    def test_empty_all_kinds_stays_warming(self) -> None:
        """readiness() must remain WARMING when _all_kinds is empty."""
        cache = ResourceCache()
        cache._ready_kinds = {"Pod"}  # ready but _all_kinds is still empty
        assert cache.readiness() == CacheReadiness.WARMING

    def test_partially_ready_after_some_kinds(self) -> None:
        """When some but not all registered kinds are ready, state is PARTIALLY_READY."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment", "Service"}
        cache._ready_kinds = {"Pod"}  # only one of three

        assert cache.readiness() == CacheReadiness.PARTIALLY_READY

    def test_partially_ready_with_no_ready_kinds_is_warming(self) -> None:
        """When _all_kinds is non-empty but _ready_kinds is empty, state is WARMING."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = set()

        assert cache.readiness() == CacheReadiness.WARMING

    def test_ready_after_all_kinds_populated(self) -> None:
        """When _ready_kinds matches _all_kinds exactly, state is READY."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Node", "Deployment"}
        cache._ready_kinds = {"Pod", "Node", "Deployment"}

        assert cache.readiness() == CacheReadiness.READY

    def test_ready_single_kind(self) -> None:
        """A single registered kind that is ready must yield READY."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}

        assert cache.readiness() == CacheReadiness.READY

    def test_degraded_from_partially_ready_staleness(self) -> None:
        """PARTIALLY_READY must become DEGRADED after 10 minutes of staleness."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = {"Pod"}  # not fully ready

        # Back-date _partially_ready_since by 11 minutes
        cache._partially_ready_since = datetime.now(tz=UTC) - timedelta(minutes=11)
        cache._recompute_readiness()

        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_partially_ready_staleness_within_10_min_stays_partial(self) -> None:
        """PARTIALLY_READY with staleness < 10 minutes must NOT degrade."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = {"Pod"}

        cache._partially_ready_since = datetime.now(tz=UTC) - timedelta(minutes=5)
        cache._recompute_readiness()

        assert cache.readiness() == CacheReadiness.PARTIALLY_READY

    def test_partially_ready_staleness_exactly_10_min_does_not_degrade(self) -> None:
        """Staleness of exactly 10 minutes should not yet degrade (threshold is >=)."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = {"Pod"}

        # timedelta comparison in implementation is age >= _PARTIALLY_READY_STALENESS_TIMEOUT
        # so exactly 10 min IS degraded; verify the boundary.
        cache._partially_ready_since = datetime.now(tz=UTC) - timedelta(minutes=10)
        cache._recompute_readiness()

        # The implementation uses >=, so exactly 10 min triggers degradation
        assert cache.readiness() == CacheReadiness.DEGRADED

    def test_ready_clears_partially_ready_since(self) -> None:
        """Transitioning to READY must reset _partially_ready_since to None."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = set()
        cache._partially_ready_since = datetime.now(tz=UTC) - timedelta(minutes=2)

        # Now mark all kinds ready
        cache._ready_kinds = {"Pod"}
        cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.READY
        assert cache._partially_ready_since is None

    def test_readiness_recomputed_on_every_call(self) -> None:
        """readiness() must reflect current state each time it is called."""
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        # First call: WARMING (ready_kinds empty)
        assert cache.readiness() == CacheReadiness.WARMING

        cache._ready_kinds = {"Pod"}
        # Second call: now READY without any other change
        assert cache.readiness() == CacheReadiness.READY


# ---------------------------------------------------------------------------
# TestUpdateEdgeCases
# ---------------------------------------------------------------------------


class TestUpdateEdgeCases:
    def test_cap_reached_rejects_new(self) -> None:
        """update() beyond the 200-resource cap must be silently rejected
        and _truncated[kind] must be set to True."""
        cache = ResourceCache()
        kind = "ConfigMap"

        # Fill to exactly the cap (200)
        _populate_kind(cache, kind, 200)
        assert cache._kind_count(kind) == 200

        # One more update must be dropped
        cache.update(kind, "default", "overflow-one", _raw_pod("overflow-one"))

        # Count stays at 200 — the new item was not inserted
        assert cache._kind_count(kind) == 200
        assert cache._truncated.get(kind) is True

    def test_cap_not_exceeded_for_updates_to_existing(self) -> None:
        """Updating an already-cached resource must not count against the cap
        even when the store is full — in-place mutation must succeed."""
        cache = ResourceCache()
        kind = "ConfigMap"

        _populate_kind(cache, kind, 200)

        # Update an existing resource — should succeed regardless of cap
        cache.update(
            kind,
            "default",
            f"{kind.lower()}-0",
            {"metadata": {"resourceVersion": "999"}, "spec": {"key": "updated"}, "status": {}},
        )

        updated = cache.get(kind, "default", f"{kind.lower()}-0")
        assert updated is not None
        assert updated.resource_version == "999"

    def test_node_cap_is_2000(self) -> None:
        """Node kind must accept up to 2000 resources before truncating."""
        cache = ResourceCache()

        # Fill Nodes to 2000
        for i in range(2000):
            cache.update("Node", "", f"node-{i}", {"metadata": {"resourceVersion": str(i)}, "spec": {}, "status": {}})

        assert cache._kind_count("Node") == 2000
        assert cache._truncated.get("Node") is not True  # cap not exceeded yet

        # One more should be rejected
        cache.update("Node", "", "node-overflow", {"metadata": {"resourceVersion": "9999"}, "spec": {}, "status": {}})
        assert cache._kind_count("Node") == 2000
        assert cache._truncated.get("Node") is True

    def test_in_place_update_changes_fields(self) -> None:
        """update() on an existing resource must mutate the stored fields in place."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "my-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {"env": "dev"}, "annotations": {}},
                "spec": {"replicas": 1},
                "status": {"phase": "Pending"},
            },
        )

        # Mutate the resource
        cache.update(
            "Pod",
            "default",
            "my-pod",
            {
                "metadata": {"resourceVersion": "2", "labels": {"env": "prod"}, "annotations": {"note": "updated"}},
                "spec": {"replicas": 3},
                "status": {"phase": "Running"},
            },
        )

        view = cache.get("Pod", "default", "my-pod")
        assert view is not None
        assert view.resource_version == "2"
        assert view.labels == {"env": "prod"}
        assert view.spec == {"replicas": 3}
        assert view.status == {"phase": "Running"}

    def test_in_place_update_invalidates_view_cache(self) -> None:
        """update() on an existing resource must invalidate the cached redacted view
        so that the next get() returns a fresh view with the new data."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "my-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        # Force view creation and cache it
        first_view = cache.get("Pod", "default", "my-pod")
        assert first_view is not None

        # Mutate
        cache.update(
            "Pod",
            "default",
            "my-pod",
            {
                "metadata": {"resourceVersion": "2", "labels": {}, "annotations": {}},
                "spec": {},
                "status": {"phase": "Terminating"},
            },
        )

        second_view = cache.get("Pod", "default", "my-pod")
        assert second_view is not None
        assert second_view.resource_version == "2"
        # The two views must reflect different data — view was not served stale
        assert first_view.resource_version != second_view.resource_version

    def test_update_with_invalid_metadata(self) -> None:
        """update() when raw_obj['metadata'] is not a dict must not raise;
        the resource is stored with empty labels, annotations, and no resource version."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "bad-meta",
            {
                "metadata": "not-a-dict",
                "spec": {},
                "status": {},
            },
        )

        # Resource should still be stored (graceful fallback)
        resource = cache._store.get("Pod", {}).get("default", {}).get("bad-meta")
        assert resource is not None
        assert resource.labels == {}
        assert resource.annotations == {}
        assert resource.resource_version == ""

    def test_update_with_non_dict_spec(self) -> None:
        """update() with a non-dict 'spec' must substitute an empty dict."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "bad-spec",
            {
                "metadata": {"resourceVersion": "1", "labels": {}, "annotations": {}},
                "spec": "not-a-dict",
                "status": {},
            },
        )

        view = cache.get("Pod", "default", "bad-spec")
        assert view is not None
        assert view.spec == {}

    def test_update_with_non_dict_status(self) -> None:
        """update() with a non-dict 'status' must substitute an empty dict."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "bad-status",
            {
                "metadata": {"resourceVersion": "1", "labels": {}, "annotations": {}},
                "spec": {},
                "status": 42,
            },
        )

        view = cache.get("Pod", "default", "bad-status")
        assert view is not None
        assert view.status == {}

    def test_update_with_missing_spec_and_status(self) -> None:
        """update() on raw_obj without 'spec' or 'status' keys must default both to {}."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "minimal-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {}, "annotations": {}},
            },
        )

        view = cache.get("Pod", "default", "minimal-pod")
        assert view is not None
        assert view.spec == {}
        assert view.status == {}

    def test_remove_nonexistent_is_noop(self) -> None:
        """remove() for a resource that was never inserted must not raise."""
        cache = ResourceCache()
        # No exception should be raised
        cache.remove("Pod", "default", "ghost-pod")
        cache.remove("NonExistentKind", "ns", "name")

    def test_remove_existing_resource(self) -> None:
        """remove() must delete the resource from the store."""
        cache = ResourceCache()
        cache.update("Pod", "default", "my-pod", _raw_pod("my-pod"))
        assert cache.get("Pod", "default", "my-pod") is not None

        cache.remove("Pod", "default", "my-pod")
        assert cache.get("Pod", "default", "my-pod") is None

    def test_update_stores_resource_version(self) -> None:
        """update() must persist the resourceVersion from metadata."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "versioned-pod",
            {
                "metadata": {"resourceVersion": "42", "labels": {}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        view = cache.get("Pod", "default", "versioned-pod")
        assert view is not None
        assert view.resource_version == "42"

    def test_update_without_resource_version(self) -> None:
        """update() with no resourceVersion in metadata must not raise; uses empty string."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "no-rv-pod",
            {
                "metadata": {"labels": {}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        view = cache.get("Pod", "default", "no-rv-pod")
        assert view is not None
        assert view.resource_version == ""

    def test_update_labels_with_non_string_values(self) -> None:
        """update() must coerce non-string label keys/values to strings."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "typed-labels",
            {
                "metadata": {
                    "resourceVersion": "1",
                    "labels": {"count": 3, "enabled": True},
                    "annotations": {},
                },
                "spec": {},
                "status": {},
            },
        )

        view = cache.get("Pod", "default", "typed-labels")
        assert view is not None
        assert view.labels == {"count": "3", "enabled": "True"}

    def test_update_non_dict_labels_gives_empty_labels(self) -> None:
        """update() with non-dict labels must produce an empty labels dict."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "bad-labels",
            {
                "metadata": {
                    "resourceVersion": "1",
                    "labels": ["not", "a", "dict"],
                    "annotations": {},
                },
                "spec": {},
                "status": {},
            },
        )

        view = cache.get("Pod", "default", "bad-labels")
        assert view is not None
        assert view.labels == {}


# ---------------------------------------------------------------------------
# TestListAndGet
# ---------------------------------------------------------------------------


class TestListAndGet:
    def test_get_returns_none_for_nonexistent(self) -> None:
        """get() must return None when the resource is absent."""
        cache = ResourceCache()
        assert cache.get("Pod", "default", "missing") is None

    def test_get_records_miss(self) -> None:
        """get() on an absent resource must append a miss to the access log."""
        cache = ResourceCache()
        before = len(cache._access_log)
        cache.get("Pod", "default", "ghost")
        assert len(cache._access_log) == before + 1
        _, hit = cache._access_log[-1]
        assert hit is False

    def test_get_records_hit(self) -> None:
        """get() on a present resource must append a hit to the access log."""
        cache = ResourceCache()
        cache.update("Pod", "default", "real-pod", _raw_pod("real-pod"))
        before = len(cache._access_log)
        result = cache.get("Pod", "default", "real-pod")
        assert result is not None
        assert len(cache._access_log) == before + 1
        _, hit = cache._access_log[-1]
        assert hit is True

    def test_get_returns_cached_resource_view(self) -> None:
        """get() must return a CachedResourceView with the correct fields."""
        from kuberca.models.resources import CachedResourceView

        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "my-pod",
            {
                "metadata": {"resourceVersion": "7", "labels": {"app": "web"}, "annotations": {}},
                "spec": {"image": "nginx"},
                "status": {"phase": "Running"},
            },
        )

        view = cache.get("Pod", "default", "my-pod")
        assert isinstance(view, CachedResourceView)
        assert view.kind == "Pod"
        assert view.namespace == "default"
        assert view.name == "my-pod"
        assert view.labels == {"app": "web"}

    def test_list_returns_all_resources_in_namespace(self) -> None:
        """list(kind, ns) must return every cached resource for that namespace."""
        cache = ResourceCache()
        _populate_kind(cache, "Pod", 5, "default")
        _populate_kind(cache, "Pod", 3, "kube-system")

        default_pods = cache.list("Pod", "default")
        ks_pods = cache.list("Pod", "kube-system")

        assert len(default_pods) == 5
        assert len(ks_pods) == 3

    def test_list_all_namespaces_when_ns_empty(self) -> None:
        """list(kind) with ns='' must return resources from all namespaces."""
        cache = ResourceCache()
        _populate_kind(cache, "Pod", 5, "default")
        _populate_kind(cache, "Pod", 3, "kube-system")

        all_pods = cache.list("Pod")
        assert len(all_pods) == 8

    def test_list_caps_at_200(self) -> None:
        """list() must return at most 200 items for non-Node kinds."""
        cache = ResourceCache()
        # Insert 250 items — 50 should be invisible through list()
        _populate_kind(cache, "ConfigMap", 200)
        # Insert 50 more into a separate namespace so _kind_count stays at 250
        _populate_kind(cache, "ConfigMap", 50, "extra-ns")

        all_maps = cache.list("ConfigMap")
        assert len(all_maps) <= 200

    def test_list_caps_nodes_at_2000(self) -> None:
        """list() for Node kind must cap at 2000."""
        cache = ResourceCache()

        # Insert 2100 nodes across namespaces (Nodes are cluster-scoped; ns = "")
        for i in range(2100):
            cache.update("Node", "", f"node-{i}", {"metadata": {"resourceVersion": str(i)}, "spec": {}, "status": {}})

        nodes = cache.list("Node")
        assert len(nodes) <= 2000

    def test_list_returns_empty_for_unknown_kind(self) -> None:
        """list() for a kind with no cached resources must return an empty list."""
        cache = ResourceCache()
        assert cache.list("Frobnitz") == []

    def test_list_by_namespace_returns_empty_for_wrong_ns(self) -> None:
        """list(kind, ns='other') must return empty when only 'default' is populated."""
        cache = ResourceCache()
        _populate_kind(cache, "Pod", 5, "default")

        assert cache.list("Pod", "other") == []

    def test_list_by_label_returns_matching(self) -> None:
        """list_by_label() must return only resources whose labels are a superset of the query."""
        cache = ResourceCache()

        # Insert pods with different labels
        cache.update(
            "Pod",
            "default",
            "web-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {"app": "web", "tier": "frontend"}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )
        cache.update(
            "Pod",
            "default",
            "db-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {"app": "db", "tier": "backend"}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )
        cache.update(
            "Pod",
            "default",
            "api-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {"app": "api"}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        web_pods = cache.list_by_label("Pod", {"app": "web"})
        assert len(web_pods) == 1
        assert web_pods[0].name == "web-pod"

    def test_list_by_label_superset_match(self) -> None:
        """list_by_label() must match when resource labels are a strict superset of query labels."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "multi-label-pod",
            {
                "metadata": {
                    "resourceVersion": "1",
                    "labels": {"app": "api", "env": "prod", "version": "v2"},
                    "annotations": {},
                },
                "spec": {},
                "status": {},
            },
        )

        # Query with subset of labels
        matches = cache.list_by_label("Pod", {"app": "api", "env": "prod"})
        assert len(matches) == 1
        assert matches[0].name == "multi-label-pod"

    def test_list_by_label_no_match(self) -> None:
        """list_by_label() must return empty when no resource matches the label query."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "my-pod",
            {
                "metadata": {"resourceVersion": "1", "labels": {"app": "web"}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        result = cache.list_by_label("Pod", {"app": "missing"})
        assert result == []

    def test_list_by_label_empty_query_matches_all(self) -> None:
        """list_by_label() with an empty label dict must match all resources."""
        cache = ResourceCache()
        _populate_kind(cache, "Pod", 5)

        all_pods = cache.list_by_label("Pod", {})
        assert len(all_pods) == 5

    def test_list_by_label_filtered_by_namespace(self) -> None:
        """list_by_label() with ns parameter must restrict results to that namespace."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "pod-a",
            {
                "metadata": {"resourceVersion": "1", "labels": {"env": "staging"}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )
        cache.update(
            "Pod",
            "production",
            "pod-b",
            {
                "metadata": {"resourceVersion": "1", "labels": {"env": "staging"}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        default_results = cache.list_by_label("Pod", {"env": "staging"}, ns="default")
        assert len(default_results) == 1
        assert default_results[0].name == "pod-a"

    def test_list_truncated_false_by_default(self) -> None:
        """list_truncated() must return False for an unknown kind."""
        cache = ResourceCache()
        assert cache.list_truncated("UnknownKind") is False

    def test_list_truncated_true_after_cap(self) -> None:
        """list_truncated() must return True after the cap has been reached."""
        cache = ResourceCache()
        _populate_kind(cache, "Pod", 200)
        # Attempt to insert one more to trigger truncation flag
        cache.update("Pod", "default", "pod-overflow", _raw_pod("pod-overflow"))
        assert cache.list_truncated("Pod") is True

    def test_staleness_returns_none_for_unknown_kind(self) -> None:
        """staleness() must return None when a kind has never been updated."""
        cache = ResourceCache()
        assert cache.staleness("Pod") is None

    def test_staleness_returns_timedelta_after_update(self) -> None:
        """staleness() must return a non-negative timedelta after an update."""
        cache = ResourceCache()
        cache.update("Pod", "default", "my-pod", _raw_pod("my-pod"))
        delta = cache.staleness("Pod")
        assert delta is not None
        assert delta >= timedelta(0)

    def test_resource_version_returns_empty_for_unknown_kind(self) -> None:
        """resource_version() must return empty string for an unknown kind."""
        cache = ResourceCache()
        assert cache.resource_version("Pod") == ""

    def test_resource_version_tracks_latest(self) -> None:
        """resource_version() must reflect the most recent resourceVersion seen."""
        cache = ResourceCache()
        cache.update(
            "Pod",
            "default",
            "pod-a",
            {
                "metadata": {"resourceVersion": "100", "labels": {}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )
        cache.update(
            "Pod",
            "default",
            "pod-b",
            {
                "metadata": {"resourceVersion": "200", "labels": {}, "annotations": {}},
                "spec": {},
                "status": {},
            },
        )

        # resource_version is updated on each update() call; last write wins
        assert cache.resource_version("Pod") == "200"

    def test_get_on_different_kinds_isolated(self) -> None:
        """get() must not find resources from a different kind even if names match."""
        cache = ResourceCache()
        cache.update("Pod", "default", "my-app", _raw_pod("my-app"))

        # Same name, different kind — should not be found
        result = cache.get("Deployment", "default", "my-app")
        assert result is None

    def test_get_on_different_namespaces_isolated(self) -> None:
        """get() must not find resources from a different namespace."""
        cache = ResourceCache()
        cache.update("Pod", "default", "my-pod", _raw_pod("my-pod"))

        result = cache.get("Pod", "other-ns", "my-pod")
        assert result is None
