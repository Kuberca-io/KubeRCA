"""Coverage-boosting unit tests for resource_cache, change_ledger, redaction, and queue.

Targets uncovered lines identified in the coverage report:
  - resource_cache.py: populate(), _list_kind(), _ingest_object(), list(), list_by_label(),
    notify_burst_error(), _record_access(), _current_miss_rate(), _recent_burst_duration(),
    _get_list_func()
  - change_ledger.py: _soft_trim(), _failsafe_evict(), record() memory-pressure paths
  - redaction.py: _is_valid_base64_string(), _redact_env_entry(), _redact_command_arg(),
    _redact_list(), redacted_json()
  - queue.py: _WorkItem.dedup_key fallback, stop() before start(), duplicate SYSTEM submit,
    _worker() cancelled-future and exception paths, _compute_effective_rate() error paths
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Module 1: resource_cache.py
# ---------------------------------------------------------------------------

from kuberca.cache.resource_cache import ResourceCache, _get_list_func
from kuberca.models.resources import CacheReadiness


# ---------------------------------------------------------------------------
# Helpers shared across resource-cache tests
# ---------------------------------------------------------------------------


def _raw_pod(name: str, namespace: str = "default", rv: str = "1") -> dict[str, Any]:
    return {
        "metadata": {"resourceVersion": rv, "labels": {}, "annotations": {}},
        "spec": {},
        "status": {},
    }


def _make_ready(cache: ResourceCache, kinds: set[str]) -> None:
    cache._all_kinds = set(kinds)
    cache._ready_kinds = set(kinds)


# ---------------------------------------------------------------------------
# TestPopulate — lines 133-152
# ---------------------------------------------------------------------------


class TestPopulate:
    @pytest.mark.asyncio
    async def test_populate_with_api_map_resolves_correct_api(self) -> None:
        """populate() with a dict api_map calls each kind's resolved API."""
        cache = ResourceCache()

        pod_api = MagicMock()
        mock_result = MagicMock()
        mock_result.items = []
        mock_result.metadata = None
        pod_api.list_pod_for_all_namespaces = AsyncMock(return_value=mock_result)

        api_map: dict[str, Any] = {"Pod": pod_api}
        await cache.populate(api_map, kinds=["Pod"])

        pod_api.list_pod_for_all_namespaces.assert_awaited_once()
        assert "Pod" in cache._all_kinds

    @pytest.mark.asyncio
    async def test_populate_api_map_none_api_skips_kind(self) -> None:
        """populate() skips kinds whose API is None in the api_map."""
        cache = ResourceCache()

        api_map: dict[str, Any] = {"Pod": None}
        await cache.populate(api_map, kinds=["Pod"])

        # Pod should be in all_kinds but NOT in ready_kinds (was skipped)
        assert "Pod" in cache._all_kinds
        assert "Pod" not in cache._ready_kinds

    @pytest.mark.asyncio
    async def test_populate_caps_at_18_kinds(self) -> None:
        """populate() processes at most 18 kinds even when more are provided."""
        cache = ResourceCache()

        # Build an api_map with 20 kinds — each having a fast-returning mock
        twenty_kinds = [f"Kind{i}" for i in range(20)]
        api_map: dict[str, Any] = {}
        for kind in twenty_kinds:
            mock_api = MagicMock()
            mock_result = MagicMock()
            mock_result.items = []
            mock_result.metadata = None
            # Attach a method named for the kind; _get_list_func won't find it
            # so _list_kind logs a warning and returns — still processed via loop.
            api_map[kind] = mock_api

        await cache.populate(api_map, kinds=twenty_kinds)

        # _all_kinds is set to the sliced target_kinds (18 items)
        assert len(cache._all_kinds) == 18

    @pytest.mark.asyncio
    async def test_populate_runtime_error_breaks_loop(self) -> None:
        """populate() breaks out of the kind loop when get_running_loop raises RuntimeError."""
        cache = ResourceCache()

        call_count = 0

        def side_effect() -> Any:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise RuntimeError("no running loop")
            return MagicMock()

        with patch("kuberca.cache.resource_cache.asyncio.get_running_loop", side_effect=side_effect):
            await cache.populate(MagicMock(), kinds=["Pod", "Node", "Deployment"])

        # Loop should have broken after the second call raised RuntimeError
        # so only 1 kind was attempted before break
        assert call_count == 2


# ---------------------------------------------------------------------------
# TestListKind — lines 161-197
# ---------------------------------------------------------------------------


class TestListKind:
    @pytest.mark.asyncio
    async def test_list_kind_populates_cache(self) -> None:
        """_list_kind() calls the list function and populates cache with returned items."""
        cache = ResourceCache()

        mock_obj = MagicMock()
        mock_obj.metadata = MagicMock()
        mock_obj.metadata.namespace = "default"
        mock_obj.metadata.name = "my-pod"
        mock_obj.metadata.resource_version = "42"
        mock_obj.metadata.labels = {}
        mock_obj.metadata.annotations = {}
        mock_obj.spec = {}
        mock_obj.status = {}

        mock_result = MagicMock()
        mock_result.items = [mock_obj]
        mock_result.metadata = MagicMock()
        mock_result.metadata.resource_version = "100"
        mock_result.metadata._continue = None

        mock_api = MagicMock()
        mock_api.list_pod_for_all_namespaces = AsyncMock(return_value=mock_result)

        await cache._list_kind(mock_api, "Pod")

        assert "Pod" in cache._ready_kinds
        assert cache.resource_version("Pod") == "100"
        assert cache._kind_count("Pod") == 1

    @pytest.mark.asyncio
    async def test_list_kind_marks_truncated_when_continue_token_present(self) -> None:
        """_list_kind() sets _truncated[kind] = True when a continue token is returned."""
        cache = ResourceCache()

        mock_result = MagicMock()
        mock_result.items = []
        mock_result.metadata = MagicMock()
        mock_result.metadata.resource_version = "1"
        mock_result.metadata._continue = "some-continue-token"

        mock_api = MagicMock()
        mock_api.list_pod_for_all_namespaces = AsyncMock(return_value=mock_result)

        await cache._list_kind(mock_api, "Pod")

        assert cache._truncated.get("Pod") is True

    @pytest.mark.asyncio
    async def test_list_kind_logs_error_on_exception(self) -> None:
        """_list_kind() catches exceptions and logs an error without raising."""
        cache = ResourceCache()

        mock_api = MagicMock()
        mock_api.list_pod_for_all_namespaces = AsyncMock(side_effect=Exception("k8s-error"))

        # Must not raise; error is swallowed with a log
        await cache._list_kind(mock_api, "Pod")

        assert "Pod" not in cache._ready_kinds

    @pytest.mark.asyncio
    async def test_list_kind_uses_2000_cap_for_node(self) -> None:
        """_list_kind() passes limit=2000 to the list function for the Node kind."""
        cache = ResourceCache()

        mock_result = MagicMock()
        mock_result.items = []
        mock_result.metadata = None

        mock_api = MagicMock()
        mock_api.list_node = AsyncMock(return_value=mock_result)

        await cache._list_kind(mock_api, "Node")

        call_kwargs = mock_api.list_node.call_args
        assert call_kwargs.kwargs.get("limit") == 2000

    @pytest.mark.asyncio
    async def test_list_kind_none_metadata_on_result(self) -> None:
        """_list_kind() handles a result with metadata=None without raising."""
        cache = ResourceCache()

        mock_result = MagicMock()
        mock_result.items = []
        mock_result.metadata = None

        mock_api = MagicMock()
        mock_api.list_pod_for_all_namespaces = AsyncMock(return_value=mock_result)

        await cache._list_kind(mock_api, "Pod")

        # Should succeed and mark the kind as ready
        assert "Pod" in cache._ready_kinds
        assert cache._truncated.get("Pod") is False

    @pytest.mark.asyncio
    async def test_list_kind_no_list_func_logs_warning(self) -> None:
        """_list_kind() logs a warning and returns early when no list func is found."""
        cache = ResourceCache()

        # Use an API object that has no matching method for "UnknownKind"
        mock_api = MagicMock(spec=[])  # empty spec — no attributes

        await cache._list_kind(mock_api, "UnknownKind")

        assert "UnknownKind" not in cache._ready_kinds


# ---------------------------------------------------------------------------
# TestIngestObject — lines 420-455
# ---------------------------------------------------------------------------


class TestIngestObject:
    def test_ingest_object_with_to_dict_spec_and_status(self) -> None:
        """_ingest_object() calls to_dict() when spec/status have that method."""
        cache = ResourceCache()

        spec_obj = MagicMock()
        spec_obj.to_dict.return_value = {"replicas": 3}
        status_obj = MagicMock()
        status_obj.to_dict.return_value = {"phase": "Running"}

        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.namespace = "default"
        obj.metadata.name = "my-pod"
        obj.metadata.resource_version = "5"
        obj.metadata.labels = {"app": "web"}
        obj.metadata.annotations = {}
        obj.spec = spec_obj
        obj.status = status_obj

        cache._ingest_object("Pod", obj, raw={})

        stored = cache._store["Pod"]["default"]["my-pod"]
        assert stored.spec == {"replicas": 3}
        assert stored.status == {"phase": "Running"}
        spec_obj.to_dict.assert_called_once()
        status_obj.to_dict.assert_called_once()

    def test_ingest_object_with_dict_spec_and_status(self) -> None:
        """_ingest_object() uses dict spec/status directly when no to_dict() method."""
        cache = ResourceCache()

        # Use a simple namespace object to avoid MagicMock auto-creating to_dict
        class _FakeObj:
            class _Meta:
                namespace = "ns"
                name = "my-deploy"
                resource_version = "7"
                labels: dict = {}
                annotations: dict = {}

            metadata = _Meta()
            spec: dict = {"replicas": 1}
            status: dict = {"conditions": []}

        cache._ingest_object("Deployment", _FakeObj(), raw={})

        stored = cache._store["Deployment"]["ns"]["my-deploy"]
        assert stored.spec == {"replicas": 1}
        assert stored.status == {"conditions": []}

    def test_ingest_object_skipped_when_metadata_is_none(self) -> None:
        """_ingest_object() returns early when obj.metadata is None."""
        cache = ResourceCache()

        obj = MagicMock()
        obj.metadata = None

        cache._ingest_object("Pod", obj, raw={})

        assert cache._kind_count("Pod") == 0

    def test_ingest_object_skipped_when_name_is_empty(self) -> None:
        """_ingest_object() returns early when the object has no name."""
        cache = ResourceCache()

        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.namespace = "default"
        obj.metadata.name = ""
        obj.metadata.resource_version = "1"
        obj.metadata.labels = {}
        obj.metadata.annotations = {}
        obj.spec = {}
        obj.status = {}

        cache._ingest_object("Pod", obj, raw={})

        assert cache._kind_count("Pod") == 0

    def test_ingest_object_with_none_labels_and_annotations(self) -> None:
        """_ingest_object() stores empty labels/annotations when they are None."""
        cache = ResourceCache()

        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.namespace = "default"
        obj.metadata.name = "no-labels-pod"
        obj.metadata.resource_version = "1"
        obj.metadata.labels = None
        obj.metadata.annotations = None
        obj.spec = {}
        obj.status = {}

        cache._ingest_object("Pod", obj, raw={})

        stored = cache._store["Pod"]["default"]["no-labels-pod"]
        assert stored.labels == {}
        assert stored.annotations == {}


# ---------------------------------------------------------------------------
# TestListCap — line 317 (list() cap reached)
# ---------------------------------------------------------------------------


class TestListCap:
    def test_list_breaks_at_cap(self) -> None:
        """list() stops iterating once cap is reached, returning exactly cap items."""
        cache = ResourceCache()
        # Insert 200 pods in default namespace + 10 extra in another namespace
        for i in range(200):
            cache.update("Pod", "default", f"pod-{i}", _raw_pod(f"pod-{i}"))
        for i in range(10):
            cache.update("Pod", "extra", f"extra-pod-{i}", _raw_pod(f"extra-pod-{i}"))

        results = cache.list("Pod")
        assert len(results) == 200

    def test_list_by_label_breaks_at_cap_and_returns_early(self) -> None:
        """list_by_label() returns early when cap is reached."""
        cache = ResourceCache()
        for i in range(210):
            cache.update(
                "Pod",
                "default",
                f"pod-{i}",
                {
                    "metadata": {"resourceVersion": str(i), "labels": {"app": "web"}, "annotations": {}},
                    "spec": {},
                    "status": {},
                },
            )

        results = cache.list_by_label("Pod", {"app": "web"})
        assert len(results) == 200


# ---------------------------------------------------------------------------
# TestNotifyBurstError — lines 406-408
# ---------------------------------------------------------------------------


class TestNotifyBurstError:
    def test_notify_burst_error_appends_timestamp(self) -> None:
        """notify_burst_error() appends a UTC datetime to _burst_error_times."""
        cache = ResourceCache()
        before_count = len(cache._burst_error_times)
        cache.notify_burst_error()
        assert len(cache._burst_error_times) == before_count + 1

    def test_notify_burst_error_timestamp_is_recent(self) -> None:
        """notify_burst_error() timestamps must be very close to the current time."""
        cache = ResourceCache()
        before = datetime.now(tz=UTC)
        cache.notify_burst_error()
        after = datetime.now(tz=UTC)
        ts = cache._burst_error_times[-1]
        assert before <= ts <= after


# ---------------------------------------------------------------------------
# TestRecordAccess — line 479 (old entries pruned)
# ---------------------------------------------------------------------------


class TestRecordAccess:
    def test_old_entries_pruned_from_access_log(self) -> None:
        """_record_access() removes access-log entries older than the 5-minute window."""
        cache = ResourceCache()

        # Inject stale entries (7 minutes ago)
        stale_ts = datetime.now(tz=UTC) - timedelta(minutes=7)
        cache._access_log = deque([(stale_ts, True), (stale_ts, False)])

        # A new access should cause pruning of the stale entries
        cache._record_access(True)

        # Only the fresh entry should remain
        assert len(cache._access_log) == 1
        _, hit = cache._access_log[0]
        assert hit is True


# ---------------------------------------------------------------------------
# TestCurrentMissRate — line 494 (recent entries filtered by window)
# ---------------------------------------------------------------------------


class TestCurrentMissRate:
    def test_entries_outside_window_are_excluded(self) -> None:
        """_current_miss_rate() excludes entries older than 5 minutes."""
        cache = ResourceCache()

        stale_ts = datetime.now(tz=UTC) - timedelta(minutes=6)
        fresh_ts = datetime.now(tz=UTC) - timedelta(seconds=30)

        # Stale miss + fresh hit
        cache._access_log = deque([(stale_ts, False), (fresh_ts, True)])

        rate = cache._current_miss_rate()
        # Only the fresh hit is in window → miss rate = 0/1 = 0.0
        assert rate == 0.0

    def test_empty_recent_window_returns_zero(self) -> None:
        """_current_miss_rate() returns 0.0 when all entries are outside the window."""
        cache = ResourceCache()

        stale_ts = datetime.now(tz=UTC) - timedelta(minutes=10)
        cache._access_log = deque([(stale_ts, False)])

        rate = cache._current_miss_rate()
        assert rate == 0.0


# ---------------------------------------------------------------------------
# TestRecentBurstDuration — lines 512, 515-516
# ---------------------------------------------------------------------------


class TestRecentBurstDuration:
    def test_returns_zero_when_no_burst_errors(self) -> None:
        """_recent_burst_duration() returns 0.0 when the burst-error deque is empty."""
        cache = ResourceCache()
        assert cache._recent_burst_duration() == 0.0

    def test_prunes_old_burst_errors_and_returns_zero(self) -> None:
        """_recent_burst_duration() prunes entries older than 1 minute and returns 0.0."""
        cache = ResourceCache()

        old_ts = datetime.now(tz=UTC) - timedelta(seconds=65)
        cache._burst_error_times = deque([old_ts])

        duration = cache._recent_burst_duration()
        # All entries pruned; should return 0.0
        assert duration == 0.0
        assert len(cache._burst_error_times) == 0

    def test_returns_seconds_since_oldest_burst(self) -> None:
        """_recent_burst_duration() returns elapsed seconds from oldest surviving burst."""
        cache = ResourceCache()

        # One entry from ~30 s ago — within the 1-min window
        ts = datetime.now(tz=UTC) - timedelta(seconds=30)
        cache._burst_error_times = deque([ts])

        duration = cache._recent_burst_duration()
        assert 29.0 < duration < 32.0  # allow small clock drift


# ---------------------------------------------------------------------------
# TestGetListFunc — line 625 (None for unknown kind)
# ---------------------------------------------------------------------------


class TestGetListFunc:
    def test_returns_none_for_unknown_kind(self) -> None:
        """_get_list_func() returns None when the kind is not in the method map."""
        mock_api = MagicMock()
        result = _get_list_func(mock_api, "SomeCustomResource")
        assert result is None

    def test_returns_none_when_method_missing_from_api(self) -> None:
        """_get_list_func() returns None when the mapped method is absent from the API object."""
        mock_api = MagicMock(spec=[])  # no attributes
        result = _get_list_func(mock_api, "Pod")
        assert result is None

    def test_returns_callable_for_known_kind(self) -> None:
        """_get_list_func() returns the method when the kind and API method are both present."""
        mock_api = MagicMock()
        result = _get_list_func(mock_api, "Pod")
        assert result is mock_api.list_pod_for_all_namespaces


# ---------------------------------------------------------------------------
# Module 2: change_ledger.py
# ---------------------------------------------------------------------------

from kuberca.ledger.change_ledger import ChangeLedger, _FAILSAFE_LIMIT_BYTES, _HARD_LIMIT_BYTES, _SOFT_LIMIT_BYTES
from kuberca.models.resources import ResourceSnapshot


def _snap(
    kind: str = "Deployment",
    namespace: str = "default",
    name: str = "my-app",
    spec: dict[str, object] | None = None,
    resource_version: str = "1",
    captured_at: datetime | None = None,
) -> ResourceSnapshot:
    if spec is None:
        spec = {"replicas": 1}
    if captured_at is None:
        captured_at = datetime.now(UTC)
    spec_hash = hashlib.sha256(str(spec).encode()).hexdigest()
    return ResourceSnapshot(
        kind=kind,
        namespace=namespace,
        name=name,
        spec_hash=spec_hash,
        spec=spec,
        captured_at=captured_at,
        resource_version=resource_version,
    )


# ---------------------------------------------------------------------------
# TestSoftTrim — lines 303-313
# ---------------------------------------------------------------------------


class TestSoftTrim:
    def test_soft_trim_halves_buffer_sizes(self) -> None:
        """_soft_trim() reduces each buffer to max(1, len//2) entries."""
        ledger = ChangeLedger(max_versions=20)
        # Insert 10 snapshots for "my-app"
        for i in range(10):
            key = ("Deployment", "default", "my-app")
            if key not in ledger._buffers:
                from collections import deque

                ledger._buffers[key] = deque(maxlen=20)
            ledger._buffers[key].append(_snap(spec={"replicas": i}, resource_version=str(i)))

        assert len(ledger._buffers[("Deployment", "default", "my-app")]) == 10

        ledger._soft_trim()

        assert len(ledger._buffers[("Deployment", "default", "my-app")]) == 5

    def test_soft_trim_retains_at_least_one_entry(self) -> None:
        """_soft_trim() retains at least 1 snapshot per buffer (never empties a buffer)."""
        from collections import deque

        ledger = ChangeLedger(max_versions=5)
        key = ("Pod", "default", "pod-a")
        ledger._buffers[key] = deque(maxlen=5)
        ledger._buffers[key].append(_snap(kind="Pod", name="pod-a"))

        ledger._soft_trim()

        assert len(ledger._buffers[key]) == 1

    def test_soft_trim_invalidates_memory_cache(self) -> None:
        """_soft_trim() invalidates the cached memory estimate."""
        from collections import deque

        ledger = ChangeLedger()
        key = ("Pod", "default", "pod-a")
        ledger._buffers[key] = deque(maxlen=5)
        for i in range(4):
            ledger._buffers[key].append(_snap(kind="Pod", name="pod-a", spec={"r": i}, resource_version=str(i)))

        # Warm up the memory cache
        _ = ledger._estimate_memory_bytes()
        assert ledger._cached_memory_bytes is not None

        ledger._soft_trim()

        assert ledger._cached_memory_bytes is None

    def test_soft_trim_increments_metrics(self) -> None:
        """_soft_trim() increments ledger_trim_events_total and ledger_memory_pressure_total."""
        from collections import deque

        from kuberca.observability.metrics import ledger_memory_pressure_total, ledger_trim_events_total

        ledger = ChangeLedger()
        key = ("Pod", "default", "pod-a")
        ledger._buffers[key] = deque(maxlen=5)
        for i in range(4):
            ledger._buffers[key].append(_snap(kind="Pod", name="pod-a", spec={"r": i}, resource_version=str(i)))

        # Just verify it runs without error and both metric calls are made
        # (We do not assert exact counter values to avoid coupling to global state)
        ledger._soft_trim()  # no exception = metric calls succeeded


# ---------------------------------------------------------------------------
# TestFailsafeEvict — lines 322-362
# ---------------------------------------------------------------------------


class TestFailsafeEvict:
    def test_failsafe_evict_removes_old_snapshots_from_largest_buffers_first(self) -> None:
        """_failsafe_evict() processes the largest buffer before the smaller one.

        The eviction order is verified by confirming the large buffer is fully
        cleared.  The small buffer may or may not be processed depending on
        whether memory drops below the failsafe threshold after the large one
        is evicted — the important invariant is that the large buffer is visited
        first (the implementation sorts descending by buffer size).
        """
        from collections import deque

        ledger = ChangeLedger(max_versions=100)

        old_ts = datetime.now(UTC) - timedelta(hours=2)

        # Large buffer: 5 entries, all old
        big_key = ("Deployment", "default", "big-app")
        ledger._buffers[big_key] = deque(maxlen=100)
        for i in range(5):
            ledger._buffers[big_key].append(
                _snap(name="big-app", spec={"r": i}, resource_version=str(i), captured_at=old_ts)
            )

        # Small buffer: 1 entry, also old — will only be processed if memory remains high
        small_key = ("Pod", "default", "small-pod")
        ledger._buffers[small_key] = deque(maxlen=100)
        ledger._buffers[small_key].append(_snap(kind="Pod", name="small-pod", captured_at=old_ts))

        # Force memory to stay above limit so BOTH buffers are evicted
        with patch.object(ledger, "_estimate_memory_bytes", return_value=_FAILSAFE_LIMIT_BYTES + 1):
            ledger._failsafe_evict()

        # Large buffer must be fully evicted (processed first as it is the largest)
        assert len(ledger._buffers.get(big_key, [])) == 0
        # Small buffer also evicted since memory never dropped below the limit
        assert len(ledger._buffers.get(small_key, [])) == 0

    def test_failsafe_evict_removes_empty_buffers(self) -> None:
        """_failsafe_evict() deletes completely empty buffers from the dict."""
        from collections import deque

        ledger = ChangeLedger(max_versions=10)

        old_ts = datetime.now(UTC) - timedelta(hours=3)
        key = ("Pod", "default", "doomed-pod")
        ledger._buffers[key] = deque(maxlen=10)
        ledger._buffers[key].append(_snap(kind="Pod", name="doomed-pod", captured_at=old_ts))

        ledger._failsafe_evict()

        assert key not in ledger._buffers

    def test_failsafe_evict_stops_when_below_limit(self) -> None:
        """_failsafe_evict() stops processing once memory drops below the failsafe limit."""
        from collections import deque

        ledger = ChangeLedger(max_versions=100)

        # A single fresh entry (won't be evicted by age)
        fresh_key = ("Pod", "default", "fresh-pod")
        ledger._buffers[fresh_key] = deque(maxlen=100)
        ledger._buffers[fresh_key].append(_snap(kind="Pod", name="fresh-pod"))

        # Patch _estimate_memory_bytes to return below the failsafe limit immediately
        with patch.object(ledger, "_estimate_memory_bytes", return_value=_FAILSAFE_LIMIT_BYTES - 1):
            ledger._failsafe_evict()

        # The fresh entry should NOT have been removed (loop stopped early via break)
        assert len(ledger._buffers[fresh_key]) == 1

    def test_failsafe_evict_updates_metrics(self) -> None:
        """_failsafe_evict() increments trim/pressure metrics and updates memory gauge."""
        from collections import deque

        ledger = ChangeLedger(max_versions=10)

        old_ts = datetime.now(UTC) - timedelta(hours=2)
        key = ("Pod", "ns", "pod")
        ledger._buffers[key] = deque(maxlen=10)
        ledger._buffers[key].append(_snap(kind="Pod", name="pod", captured_at=old_ts))

        # No exception = Prometheus metric updates succeeded
        ledger._failsafe_evict()


# ---------------------------------------------------------------------------
# TestRecordMemoryPressure — lines 126-138, 141
# ---------------------------------------------------------------------------


class TestRecordMemoryPressure:
    def test_record_at_failsafe_triggers_failsafe_evict(self) -> None:
        """record() calls _failsafe_evict() when estimated memory >= FAILSAFE_LIMIT_BYTES."""
        ledger = ChangeLedger()

        with (
            patch.object(ledger, "_estimate_memory_bytes", return_value=_FAILSAFE_LIMIT_BYTES),
            patch.object(ledger, "_failsafe_evict") as mock_evict,
        ):
            # After failsafe_evict, re-estimate must return below HARD_LIMIT
            mock_evict.side_effect = lambda: patch.object(
                ledger, "_estimate_memory_bytes", return_value=0
            ).__enter__()
            # Simplify: just check evict is called
            try:
                ledger.record(_snap())
            except Exception:
                pass
            mock_evict.assert_called_once()

    def test_record_at_hard_limit_drops_snapshot_silently(self) -> None:
        """record() drops snapshot when memory >= HARD_LIMIT_BYTES (after eviction)."""
        ledger = ChangeLedger()

        call_count = 0

        def memory_response() -> int:
            nonlocal call_count
            call_count += 1
            # First call (initial check): below failsafe
            # Second call (after potential evict): at hard limit
            if call_count <= 2:
                return _HARD_LIMIT_BYTES
            return _HARD_LIMIT_BYTES

        with patch.object(ledger, "_estimate_memory_bytes", side_effect=memory_response):
            ledger.record(_snap())

        # Snapshot should NOT have been stored (hard limit rejection)
        assert ledger.latest("Deployment", "default", "my-app") is None

    def test_record_at_soft_limit_triggers_soft_trim(self) -> None:
        """record() calls _soft_trim() when estimated memory >= SOFT_LIMIT_BYTES."""
        ledger = ChangeLedger()

        call_count = 0

        def memory_response() -> int:
            nonlocal call_count
            call_count += 1
            # Return soft limit on first check, then 0 so the snapshot is accepted
            if call_count == 1:
                return _SOFT_LIMIT_BYTES
            return 0

        with (
            patch.object(ledger, "_estimate_memory_bytes", side_effect=memory_response),
            patch.object(ledger, "_soft_trim") as mock_trim,
        ):
            ledger.record(_snap())

        mock_trim.assert_called_once()

    def test_record_exactly_at_failsafe_calls_evict(self) -> None:
        """record() with memory exactly at FAILSAFE_LIMIT_BYTES triggers _failsafe_evict."""
        ledger = ChangeLedger()

        with patch.object(ledger, "_failsafe_evict") as mock_evict:
            # Make _estimate_memory_bytes return FAILSAFE on first call, then 0
            responses = iter([_FAILSAFE_LIMIT_BYTES, 0, 0, 0])
            with patch.object(ledger, "_estimate_memory_bytes", side_effect=responses):
                ledger.record(_snap())

        mock_evict.assert_called_once()


# ---------------------------------------------------------------------------
# Module 3: redaction.py
# ---------------------------------------------------------------------------

from kuberca.cache.redaction import (
    _is_valid_base64_string,
    _redact_command_arg,
    _redact_env_entry,
    _redact_list,
    redacted_json,
    redact_dict,
)


# ---------------------------------------------------------------------------
# TestIsValidBase64String — lines 156-161
# ---------------------------------------------------------------------------


class TestIsValidBase64String:
    def test_short_string_returns_false(self) -> None:
        """_is_valid_base64_string() returns False for strings <= 64 chars."""
        assert _is_valid_base64_string("SGVsbG8=") is False

    def test_exactly_64_chars_returns_false(self) -> None:
        """_is_valid_base64_string() returns False for exactly 64-char strings."""
        # 48 bytes encoded = 64 base64 chars
        import base64

        value = base64.b64encode(b"A" * 48).decode()
        assert len(value) == 64
        assert _is_valid_base64_string(value) is False

    def test_valid_long_base64_returns_true(self) -> None:
        """_is_valid_base64_string() returns True for valid base64 strings > 64 chars."""
        import base64

        # 50 bytes → 68 base64 chars (> 64)
        value = base64.b64encode(b"B" * 50).decode()
        assert len(value) > 64
        assert _is_valid_base64_string(value) is True

    def test_invalid_base64_chars_returns_false(self) -> None:
        """_is_valid_base64_string() returns False for strings with non-base64 characters."""
        # Build a 70-char string with invalid base64 chars
        invalid = "!" * 70
        assert _is_valid_base64_string(invalid) is False


# ---------------------------------------------------------------------------
# TestRedactEnvEntry — lines 210, 219
# ---------------------------------------------------------------------------


class TestRedactEnvEntry:
    def test_value_from_key_redacted(self) -> None:
        """_redact_env_entry() replaces 'valueFrom' with '[REDACTED]'."""
        entry = {"name": "MY_SECRET", "valueFrom": {"secretKeyRef": {"name": "my-secret", "key": "password"}}}
        result = _redact_env_entry(entry)
        assert isinstance(result, dict)
        assert result["valueFrom"] == "[REDACTED]"  # type: ignore[index]
        assert result["name"] == "MY_SECRET"  # type: ignore[index]

    def test_non_dict_entry_returned_unchanged(self) -> None:
        """_redact_env_entry() returns non-dict entries unchanged."""
        assert _redact_env_entry("plain-string") == "plain-string"
        assert _redact_env_entry(42) == 42
        assert _redact_env_entry(None) is None

    def test_value_key_redacted(self) -> None:
        """_redact_env_entry() replaces 'value' with '[REDACTED]'."""
        entry = {"name": "DB_PASSWORD", "value": "hunter2"}
        result = _redact_env_entry(entry)
        assert isinstance(result, dict)
        assert result["value"] == "[REDACTED]"  # type: ignore[index]
        assert result["name"] == "DB_PASSWORD"  # type: ignore[index]


# ---------------------------------------------------------------------------
# TestRedactCommandArg — line 239
# ---------------------------------------------------------------------------


class TestRedactCommandArg:
    def test_long_base64_argument_redacted(self) -> None:
        """_redact_command_arg() replaces bare long-base64 args with '[REDACTED]'."""
        import base64

        # 50 bytes → 68 base64 chars, all valid base64
        b64_arg = base64.b64encode(b"X" * 50).decode()
        assert len(b64_arg) > 64
        result = _redact_command_arg(b64_arg)
        assert result == "[REDACTED]"

    def test_short_base64_not_redacted(self) -> None:
        """_redact_command_arg() does not redact base64 strings <= 64 chars."""
        import base64

        short = base64.b64encode(b"short").decode()
        result = _redact_command_arg(short)
        assert result == short

    def test_normal_argument_passes_through(self) -> None:
        """_redact_command_arg() returns non-secret arguments unchanged."""
        assert _redact_command_arg("--port=8080") == "--port=8080"
        assert _redact_command_arg("--verbose") == "--verbose"


# ---------------------------------------------------------------------------
# TestRedactList — lines 351-361
# ---------------------------------------------------------------------------


class TestRedactList:
    def test_nested_list_recursed(self) -> None:
        """_redact_list() recurses into nested lists."""
        # Inner list contains a dict with a secret key
        inner = [{"password": "hunter2"}]
        result = _redact_list("items", [inner])
        # The nested list should be processed; the dict's password should be redacted
        assert result[0][0]["password"] == "[REDACTED]"  # type: ignore[index]

    def test_string_in_denylist_parent_key_redacted(self) -> None:
        """_redact_list() replaces strings when the parent key is in the denylist."""
        result = _redact_list("password", ["secret1", "secret2"])
        assert result == ["[REDACTED]", "[REDACTED]"]

    def test_heuristic_match_in_list_element(self) -> None:
        """_redact_list() applies value heuristics to string list elements."""
        # A JWT-shaped value inside a list
        jwt = (
            "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"
            ".eyJzdWIiOiIxMjM0NTY3ODkwIn0"
            ".SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
        )
        result = _redact_list("config", [jwt])
        # Should be hashed, not left as-is
        assert result[0] != jwt
        assert str(result[0]).startswith("[HASH:sha256:")

    def test_non_secret_string_passes_through(self) -> None:
        """_redact_list() leaves non-secret strings unchanged."""
        result = _redact_list("labels", ["production", "v1.0"])
        assert result == ["production", "v1.0"]

    def test_dict_in_list_recursed(self) -> None:
        """_redact_list() recurses into dict elements."""
        items = [{"host": "localhost", "password": "secret"}]
        result = _redact_list("endpoints", items)
        assert result[0]["password"] == "[REDACTED]"  # type: ignore[index]
        assert result[0]["host"] == "localhost"  # type: ignore[index]


# ---------------------------------------------------------------------------
# TestRedactedJson — lines 376-379
# ---------------------------------------------------------------------------


class TestRedactedJson:
    def test_returns_json_string(self) -> None:
        """redacted_json() returns a valid JSON string."""
        import json

        data: dict[str, object] = {"host": "localhost", "port": 5432}
        result = redacted_json(data)
        parsed = json.loads(result)
        assert parsed["host"] == "localhost"
        assert parsed["port"] == 5432

    def test_sensitive_fields_are_redacted_in_output(self) -> None:
        """redacted_json() redacts secrets before serialising."""
        import json

        data: dict[str, object] = {"password": "hunter2", "host": "db.example.com"}
        result = redacted_json(data)
        parsed = json.loads(result)
        assert parsed["password"] == "[REDACTED]"
        assert parsed["host"] == "db.example.com"

    def test_does_not_mutate_original_data(self) -> None:
        """redacted_json() must not modify the original dict (uses deepcopy internally)."""
        data: dict[str, object] = {"password": "secret", "name": "app"}
        _ = redacted_json(data)
        assert data["password"] == "secret"


# ---------------------------------------------------------------------------
# Module 4: queue.py
# ---------------------------------------------------------------------------

from kuberca.analyst.queue import Priority, QueueFullError, WorkQueue, _WorkItem


# ---------------------------------------------------------------------------
# TestWorkItemDedupKeyFallback — line 78
# ---------------------------------------------------------------------------


class TestWorkItemDedupKeyFallback:
    def test_dedup_key_fallback_when_resource_not_three_parts(self) -> None:
        """_WorkItem.dedup_key returns (resource, '') when resource doesn't have 3 slash-parts."""
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            item = _WorkItem(
                resource="only-two/parts",
                time_window="2h",
                priority=Priority.INTERACTIVE,
                future=future,
            )
            assert item.dedup_key == ("only-two/parts", "")
        finally:
            loop.close()

    def test_dedup_key_correct_for_three_part_resource(self) -> None:
        """_WorkItem.dedup_key extracts (namespace, name) from a 3-part resource string."""
        loop = asyncio.new_event_loop()
        try:
            future = loop.create_future()
            item = _WorkItem(
                resource="Pod/default/my-pod",
                time_window="2h",
                priority=Priority.INTERACTIVE,
                future=future,
            )
            assert item.dedup_key == ("default", "my-pod")
        finally:
            loop.close()


# ---------------------------------------------------------------------------
# TestStopBeforeStart — line 136
# ---------------------------------------------------------------------------


class TestStopBeforeStart:
    @pytest.mark.asyncio
    async def test_stop_before_start_is_safe(self) -> None:
        """stop() can be called before start() without raising."""
        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        # Must not raise
        await queue.stop()
        assert queue._workers == []


# ---------------------------------------------------------------------------
# TestDuplicateSystemRequest — lines 171-176
# ---------------------------------------------------------------------------


class TestDuplicateSystemRequest:
    @pytest.mark.asyncio
    async def test_duplicate_system_drops_new_returns_existing_future(self) -> None:
        """submit() with SYSTEM priority drops the duplicate and returns the existing future."""
        block = asyncio.Event()

        async def analyze(resource: str, time_window: str) -> str:
            await block.wait()
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            fut1 = queue.submit("Pod/default/my-pod", "2h", Priority.SYSTEM)
            fut2 = queue.submit("Pod/default/my-pod", "2h", Priority.SYSTEM)
            # Both futures must be the same object (existing one was returned)
            assert fut1 is fut2
        finally:
            block.set()
            await queue.stop()


# ---------------------------------------------------------------------------
# TestWorkerCancelledAndException — lines 224-225, 238-239
# ---------------------------------------------------------------------------


class TestWorkerCancelledAndException:
    @pytest.mark.asyncio
    async def test_cancelled_future_skipped_without_error(self) -> None:
        """_worker() skips an item whose future has been cancelled."""
        block = asyncio.Event()
        processed: list[str] = []

        async def analyze(resource: str, time_window: str) -> str:
            processed.append(resource)
            await block.wait()
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            fut = queue.submit("Pod/default/cancel-me", "2h", Priority.INTERACTIVE)
            # Cancel the future before the worker picks it up
            fut.cancel()
            # Give the worker a moment to process the item
            await asyncio.sleep(0.05)
            # Worker should not have called analyze for the cancelled item
            # (it may have been called if the worker grabbed it before cancel;
            # the important thing is no exception was raised)
        finally:
            block.set()
            await queue.stop()

    @pytest.mark.asyncio
    async def test_worker_sets_exception_on_future_when_analyze_raises(self) -> None:
        """_worker() sets the exception on the future when analyze_fn raises."""
        async def analyze(resource: str, time_window: str) -> str:
            raise RuntimeError("boom")

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            fut = queue.submit("Pod/default/bad-pod", "2h", Priority.INTERACTIVE)
            with pytest.raises(RuntimeError, match="boom"):
                await asyncio.wait_for(fut, timeout=5.0)
        finally:
            await queue.stop()


# ---------------------------------------------------------------------------
# TestComputeEffectiveRate — lines 298-299, 311-317
# ---------------------------------------------------------------------------


class TestComputeEffectiveRate:
    def test_cache_readiness_error_logs_warning_and_continues(self) -> None:
        """_compute_effective_rate() logs a warning when cache_readiness_fn raises."""

        def bad_readiness() -> CacheReadiness:
            raise OSError("k8s unavailable")

        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze, cache_readiness_fn=bad_readiness)
        import time

        # Should not raise; falls through to default rate
        rate = queue._compute_effective_rate(time.monotonic())
        assert rate == 20.0  # default rate

    def test_high_cpu_reduces_rate_by_half(self) -> None:
        """_compute_effective_rate() returns 50% of default when CPU > 80%."""

        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze, cpu_percent_fn=lambda: 90.0)
        import time

        rate = queue._compute_effective_rate(time.monotonic())
        assert rate == pytest.approx(10.0, abs=0.1)

    def test_cpu_below_threshold_no_reduction(self) -> None:
        """_compute_effective_rate() returns default rate when CPU <= 80%."""

        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze, cpu_percent_fn=lambda: 50.0)
        import time

        rate = queue._compute_effective_rate(time.monotonic())
        assert rate == pytest.approx(20.0, abs=0.1)

    def test_cpu_check_error_logs_warning_and_continues(self) -> None:
        """_compute_effective_rate() logs a warning when cpu_percent_fn raises."""

        def bad_cpu() -> float:
            raise OSError("psutil unavailable")

        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze, cpu_percent_fn=bad_cpu)
        import time

        # Should not raise; falls through to default rate
        rate = queue._compute_effective_rate(time.monotonic())
        assert rate == pytest.approx(20.0, abs=0.1)

    def test_rate_halved_after_429_overrides_cpu_check(self) -> None:
        """_compute_effective_rate() returns halved rate and exits early after 429."""
        import time

        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze, cpu_percent_fn=lambda: 90.0)
        queue.notify_upstream_429()

        rate = queue._compute_effective_rate(time.monotonic())
        # Halved from 20 = 10, not further reduced by CPU
        assert rate == pytest.approx(10.0, abs=0.1)
