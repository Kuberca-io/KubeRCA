"""Comprehensive unit tests for the watcher modules.

Covers:
- kuberca.collector.watcher   — BaseWatcher, _extract_rv, _extract_rv_from_bookmark
- kuberca.collector.event_watcher — EventWatcher and helpers
- kuberca.collector.pod_watcher   — PodWatcher and helpers
- kuberca.collector.node_watcher  — NodeWatcher and helpers
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from kuberca.collector.event_watcher import (
    EventWatcher,
    _classify_severity,
    _coerce_dt,
    _parse_dt_str,
)
from kuberca.collector.node_watcher import (
    NodeWatcher,
    _build_condition_event,
    _classify_condition,
    _extract_conditions,
    _extract_node_name,
)
from kuberca.collector.pod_watcher import (
    PodWatcher,
    _build_phase_event,
    _extract_phase,
    _extract_spec,
    _hash_spec,
    _pod_key,
)
from kuberca.collector.watcher import BaseWatcher, _extract_rv, _extract_rv_from_bookmark
from kuberca.models.events import EventRecord, EventSource, Severity
from kuberca.models.resources import ResourceSnapshot

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_event_record(
    reason: str = "TestReason",
    severity: Severity = Severity.INFO,
    namespace: str = "default",
    resource_kind: str = "Pod",
    resource_name: str = "my-pod",
    cluster_id: str = "test-cluster",
    count: int = 1,
    source: EventSource = EventSource.CORE_EVENT,
    last_seen: datetime | None = None,
    first_seen: datetime | None = None,
) -> EventRecord:
    ts = last_seen or datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
    fs = first_seen or ts
    return EventRecord(
        event_id=str(uuid4()),
        cluster_id=cluster_id,
        source=source,
        severity=severity,
        reason=reason,
        message=f"{reason} occurred",
        namespace=namespace,
        resource_kind=resource_kind,
        resource_name=resource_name,
        first_seen=fs,
        last_seen=ts,
        count=count,
    )


def _make_metadata_obj(
    name: str = "my-pod",
    namespace: str = "default",
    resource_version: str = "42",
    labels: dict[str, str] | None = None,
) -> MagicMock:
    meta = MagicMock()
    meta.name = name
    meta.namespace = namespace
    meta.resource_version = resource_version
    meta.labels = labels or {}
    return meta


def _make_v1event(
    name: str = "my-event",
    namespace: str = "default",
    resource_kind: str = "Pod",
    resource_name: str = "my-pod",
    reason: str = "Started",
    message: str = "Container started",
    kube_type: str = "Normal",
    count: int = 1,
    resource_version: str = "100",
    first_timestamp: datetime | None = None,
    last_timestamp: datetime | None = None,
    labels: dict[str, str] | None = None,
) -> MagicMock:
    ts = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
    obj = MagicMock()
    obj.metadata = _make_metadata_obj(
        name=name,
        namespace=namespace,
        resource_version=resource_version,
        labels=labels or {},
    )
    involved = MagicMock()
    involved.kind = resource_kind
    involved.name = resource_name
    obj.involved_object = involved
    obj.reason = reason
    obj.message = message
    obj.type = kube_type
    obj.count = count
    obj.first_timestamp = first_timestamp or ts
    obj.last_timestamp = last_timestamp or ts
    return obj


# ---------------------------------------------------------------------------
# Concrete subclass of BaseWatcher for testing abstract class behaviour
# ---------------------------------------------------------------------------


class _TestWatcher(BaseWatcher):
    """Minimal concrete subclass used to test BaseWatcher in isolation."""

    def __init__(self, api: object, cluster_id: str = "test") -> None:
        super().__init__(api, cluster_id=cluster_id, name="test")
        self.handled_events: list[tuple[str, object, dict]] = []
        self._list_fn = AsyncMock()

    def _list_func(self):  # type: ignore[override]
        return self._list_fn

    async def _handle_event(self, event_type: str, obj: object, raw: dict) -> None:
        self.handled_events.append((event_type, obj, raw))


# ===========================================================================
# TestBaseWatcher
# ===========================================================================


class TestBaseWatcher:
    """Tests for BaseWatcher lifecycle, back-off, and error handling."""

    def _make_watcher(self) -> _TestWatcher:
        api = MagicMock()
        return _TestWatcher(api, cluster_id="prod")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def test_start_stop_lifecycle(self) -> None:
        watcher = self._make_watcher()

        # Not running before start
        assert watcher._running is False
        assert watcher._task is None

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher.start()

        assert watcher._running is True
        assert watcher._task is not None
        assert not watcher._task.done()

        await watcher.stop()

        assert watcher._running is False

    async def test_start_idempotent(self) -> None:
        """Calling start() a second time must not create a second task."""
        watcher = self._make_watcher()

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher.start()
            task_first = watcher._task

            await watcher.start()  # second call
            task_second = watcher._task

        assert task_first is task_second, "start() created a new task on second call"
        await watcher.stop()

    # ------------------------------------------------------------------
    # Back-off
    # ------------------------------------------------------------------

    async def test_backoff_doubles(self) -> None:
        """Each _backoff() call should double the stored delay up to the max."""
        watcher = self._make_watcher()

        # Start at minimum (1.0 s)
        assert watcher._backoff_s == 1.0

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await watcher._backoff("test")
            assert mock_sleep.call_count == 1
            # Delay should have doubled
            assert watcher._backoff_s == 2.0

            await watcher._backoff("test")
            assert watcher._backoff_s == 4.0

            await watcher._backoff("test")
            assert watcher._backoff_s == 8.0

    async def test_backoff_capped_at_maximum(self) -> None:
        """_backoff() must cap at _BACKOFF_MAX_S (60 s)."""
        watcher = self._make_watcher()
        # Artificially set near maximum
        watcher._backoff_s = 40.0

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._backoff("test")
            # 40 * 2 = 80, capped at 60
            assert watcher._backoff_s == 60.0

    def test_reset_backoff(self) -> None:
        """_reset_backoff() must restore delay to 1 s and clear failures."""
        watcher = self._make_watcher()
        watcher._backoff_s = 30.0
        watcher._consecutive_failures = 5

        watcher._reset_backoff()

        assert watcher._backoff_s == 1.0
        assert watcher._consecutive_failures == 0

    # ------------------------------------------------------------------
    # Burst error tracking
    # ------------------------------------------------------------------

    def test_record_burst_error(self) -> None:
        """_record_burst_error() appends the current timestamp."""
        watcher = self._make_watcher()
        assert watcher._error_burst_times == []

        watcher._record_burst_error()
        assert len(watcher._error_burst_times) == 1

        watcher._record_burst_error()
        assert len(watcher._error_burst_times) == 2

    def test_record_burst_error_evicts_stale(self) -> None:
        """Timestamps older than 60 s are evicted when a new error is recorded."""
        watcher = self._make_watcher()
        old_time = datetime.utcnow() - timedelta(seconds=120)
        watcher._error_burst_times = [old_time]

        watcher._record_burst_error()

        # The old entry should have been pruned; only the new one remains
        assert len(watcher._error_burst_times) == 1
        assert watcher._error_burst_times[0] > old_time

    def test_should_relist_on_burst_returns_false_when_below_threshold(self) -> None:
        """With only one recent error, burst threshold is not exceeded."""
        watcher = self._make_watcher()
        watcher._record_burst_error()  # 1 error, threshold is >1

        assert watcher._should_relist_on_burst() is False

    def test_should_relist_on_burst_returns_true_when_exceeded(self) -> None:
        """Burst threshold (_BURST_RELIST_THRESHOLD = 1) is exceeded after 2 errors."""
        watcher = self._make_watcher()
        watcher._record_burst_error()
        watcher._record_burst_error()

        assert watcher._should_relist_on_burst() is True

    # ------------------------------------------------------------------
    # _handle_api_exception
    # ------------------------------------------------------------------

    async def test_handle_api_exception_410_clears_resource_version(self) -> None:
        """A 410 Gone should clear the resource_version and trigger relist."""
        watcher = self._make_watcher()
        watcher._resource_version = "some-version"

        exc = MagicMock()
        exc.status = 410
        exc.reason = "Gone"

        with patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist:
            await watcher._handle_api_exception(exc)

        assert watcher._resource_version == ""
        mock_relist.assert_called_once_with(reason="410")

    async def test_handle_api_exception_429_triggers_backoff_or_relist(self) -> None:
        """A 429 should record the burst error and either backoff or relist."""
        watcher = self._make_watcher()
        exc = MagicMock()
        exc.status = 429

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        # Either backoff or relist should have been called — not both
        called_count = mock_backoff.call_count + mock_relist.call_count
        assert called_count == 1

        # Burst error should have been recorded
        assert len(watcher._error_burst_times) == 1

    async def test_handle_api_exception_429_relist_on_burst(self) -> None:
        """A 429 after burst threshold triggers relist (not backoff)."""
        watcher = self._make_watcher()
        # Pre-populate burst times to exceed threshold
        watcher._error_burst_times = [datetime.utcnow(), datetime.utcnow()]

        exc = MagicMock()
        exc.status = 429

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        mock_relist.assert_called_once_with(reason="429_burst")
        mock_backoff.assert_not_called()

    async def test_handle_api_exception_500_increments_failures(self) -> None:
        """A 500 should increment consecutive failures and record burst error."""
        watcher = self._make_watcher()
        assert watcher._consecutive_failures == 0

        exc = MagicMock()
        exc.status = 500

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
            patch.object(watcher, "_relist", new_callable=AsyncMock),
        ):
            await watcher._handle_api_exception(exc)

        assert watcher._consecutive_failures == 1
        assert len(watcher._error_burst_times) == 1

    async def test_handle_api_exception_500_relist_on_consecutive_failures(self) -> None:
        """After 3 consecutive 500s the watcher should relist."""
        watcher = self._make_watcher()
        watcher._consecutive_failures = 2  # Will become 3 on the exception

        exc = MagicMock()
        exc.status = 500

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        mock_relist.assert_called_once()
        mock_backoff.assert_not_called()

    # ------------------------------------------------------------------
    # _relist throttling
    # ------------------------------------------------------------------

    async def test_relist_throttled_within_5_minutes(self) -> None:
        """A second relist within 5 minutes should be throttled (backoff instead)."""
        watcher = self._make_watcher()
        # Set last relist to 1 minute ago
        watcher._last_relist_at = datetime.utcnow() - timedelta(seconds=60)

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock) as mock_do,
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
        ):
            await watcher._relist(reason="test")

        mock_do.assert_not_called()
        mock_backoff.assert_called_once_with("relist_throttled")

    async def test_relist_proceeds_after_5_minutes(self) -> None:
        """A relist after the minimum interval should execute _do_relist."""
        watcher = self._make_watcher()
        # Last relist more than 5 min ago
        watcher._last_relist_at = datetime.utcnow() - timedelta(seconds=400)

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock) as mock_do,
            patch.object(watcher, "_reset_backoff") as mock_reset,
        ):
            await watcher._relist(reason="test")

        mock_do.assert_called_once()
        mock_reset.assert_called_once()

    async def test_first_relist_always_proceeds(self) -> None:
        """The very first relist (no prior relist) should never be throttled."""
        watcher = self._make_watcher()
        assert watcher._last_relist_at is None

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock) as mock_do,
            patch.object(watcher, "_reset_backoff"),
        ):
            await watcher._relist(reason="first")

        mock_do.assert_called_once()


# ===========================================================================
# TestExtractRV
# ===========================================================================


class TestExtractRV:
    """Tests for the _extract_rv and _extract_rv_from_bookmark helpers."""

    def test_extract_rv_from_object(self) -> None:
        """Returns the metadata.resource_version from a deserialized object."""
        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.resource_version = "999"

        result = _extract_rv(obj, {})

        assert result == "999"

    def test_extract_rv_from_raw_dict_when_obj_has_no_metadata(self) -> None:
        """Falls back to raw["metadata"]["resourceVersion"] when obj lacks metadata."""
        obj = MagicMock(spec=[])  # no 'metadata' attribute

        raw = {"metadata": {"resourceVersion": "777"}}
        result = _extract_rv(obj, raw)

        assert result == "777"

    def test_extract_rv_from_raw_dict_when_obj_is_none(self) -> None:
        """Falls back to raw dict when obj is None."""
        raw = {"metadata": {"resourceVersion": "555"}}
        result = _extract_rv(None, raw)

        assert result == "555"

    def test_extract_rv_returns_empty_when_missing_from_both(self) -> None:
        """Returns empty string when neither obj nor raw contain a resourceVersion."""
        obj = MagicMock(spec=[])  # no 'metadata'

        result = _extract_rv(obj, {})

        assert result == ""

    def test_extract_rv_prefers_object_over_raw(self) -> None:
        """When both sources have a version, the deserialized object wins."""
        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.resource_version = "from-obj"

        raw = {"metadata": {"resourceVersion": "from-raw"}}
        result = _extract_rv(obj, raw)

        assert result == "from-obj"

    def test_extract_rv_from_bookmark_valid(self) -> None:
        """Extracts resourceVersion from a BOOKMARK raw_event structure."""
        raw_event = {
            "type": "BOOKMARK",
            "raw_object": {
                "metadata": {"resourceVersion": "12345"}
            },
        }
        result = _extract_rv_from_bookmark(raw_event)

        assert result == "12345"

    def test_extract_rv_from_bookmark_missing_metadata(self) -> None:
        """Returns empty string when bookmark has no metadata."""
        raw_event = {"type": "BOOKMARK", "raw_object": {}}
        result = _extract_rv_from_bookmark(raw_event)

        assert result == ""

    def test_extract_rv_from_bookmark_missing_raw_object(self) -> None:
        """Returns empty string when bookmark has no raw_object key."""
        raw_event: dict = {"type": "BOOKMARK"}
        result = _extract_rv_from_bookmark(raw_event)

        assert result == ""

    def test_extract_rv_from_bookmark_raw_object_not_dict(self) -> None:
        """Returns empty string when raw_object is not a dict."""
        raw_event = {"raw_object": "not-a-dict"}
        result = _extract_rv_from_bookmark(raw_event)

        assert result == ""


# ===========================================================================
# TestEventWatcher
# ===========================================================================


class TestEventWatcher:
    """Tests for EventWatcher and its helpers."""

    def _make_watcher(self, cluster_id: str = "test-cluster") -> EventWatcher:
        api = MagicMock()
        return EventWatcher(api, cluster_id=cluster_id)

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def test_init_creates_empty_buffer_and_callbacks(self) -> None:
        watcher = self._make_watcher()

        assert len(watcher._buffer) == 0
        assert watcher._callbacks == []

    def test_add_callback_appends_to_list(self) -> None:
        watcher = self._make_watcher()
        cb1: AsyncMock = AsyncMock()
        cb2: AsyncMock = AsyncMock()

        watcher.add_callback(cb1)
        watcher.add_callback(cb2)

        assert len(watcher._callbacks) == 2
        assert watcher._callbacks[0] is cb1
        assert watcher._callbacks[1] is cb2

    # ------------------------------------------------------------------
    # _handle_event — V1Event deserialized object path
    # ------------------------------------------------------------------

    async def test_handle_event_converts_v1event_and_buffers(self) -> None:
        watcher = self._make_watcher()
        obj = _make_v1event(
            namespace="kube-system",
            resource_kind="Node",
            resource_name="node-1",
            reason="NodeReady",
            message="Node is ready",
            kube_type="Normal",
        )

        await watcher._handle_event("ADDED", obj, {})

        assert len(watcher._buffer) == 1
        record = watcher._buffer[0]
        assert record.namespace == "kube-system"
        assert record.resource_kind == "Node"
        assert record.resource_name == "node-1"
        assert record.reason == "NodeReady"
        assert record.source == EventSource.CORE_EVENT
        assert record.cluster_id == "test-cluster"

    async def test_handle_event_converts_raw_dict_when_obj_has_no_metadata(self) -> None:
        """When obj lacks 'metadata', the raw dict path is used."""
        watcher = self._make_watcher()
        obj = MagicMock(spec=[])  # no 'metadata' attribute

        raw = {
            "metadata": {"namespace": "staging", "labels": {}},
            "involvedObject": {"kind": "Deployment", "name": "web"},
            "reason": "ScalingReplicaSet",
            "message": "Scaled up",
            "type": "Normal",
            "count": 3,
            "firstTimestamp": "2026-02-18T12:00:00Z",
            "lastTimestamp": "2026-02-18T12:01:00Z",
        }

        await watcher._handle_event("MODIFIED", obj, raw)

        assert len(watcher._buffer) == 1
        record = watcher._buffer[0]
        assert record.namespace == "staging"
        assert record.resource_kind == "Deployment"
        assert record.resource_name == "web"
        assert record.reason == "ScalingReplicaSet"
        assert record.count == 3

    async def test_handle_event_ignores_deleted_events(self) -> None:
        """DELETED watch events must not be buffered."""
        watcher = self._make_watcher()
        obj = _make_v1event()

        await watcher._handle_event("DELETED", obj, {})

        assert len(watcher._buffer) == 0

    async def test_handle_event_returns_none_for_missing_metadata(self) -> None:
        """When obj has no metadata and raw dict has non-dict metadata, nothing is buffered.

        _convert_event skips obj (no metadata attr) and falls through to
        _from_raw_dict, which returns None when metadata is not a dict.
        """
        watcher = self._make_watcher()
        obj = MagicMock(spec=[])  # no 'metadata'

        await watcher._handle_event("ADDED", obj, {"metadata": None})

        assert len(watcher._buffer) == 0

    # ------------------------------------------------------------------
    # _classify_severity
    # ------------------------------------------------------------------

    def test_classify_severity_normal_is_info(self) -> None:
        assert _classify_severity("Normal", "Scheduled") == Severity.INFO

    def test_classify_severity_warning_type_is_warning(self) -> None:
        assert _classify_severity("Warning", "FailedSync") == Severity.WARNING

    def test_classify_severity_backoff_is_error(self) -> None:
        assert _classify_severity("Warning", "BackOff") == Severity.ERROR

    def test_classify_severity_crashloopbackoff_is_error(self) -> None:
        assert _classify_severity("Warning", "CrashLoopBackOff") == Severity.ERROR

    def test_classify_severity_oomkilling_is_critical(self) -> None:
        assert _classify_severity("Warning", "OOMKilling") == Severity.CRITICAL

    def test_classify_severity_nodenotready_is_critical(self) -> None:
        assert _classify_severity("Warning", "NodeNotReady") == Severity.CRITICAL

    def test_classify_severity_evicted_is_critical(self) -> None:
        assert _classify_severity("Warning", "Evicted") == Severity.CRITICAL

    def test_classify_severity_failed_is_error(self) -> None:
        assert _classify_severity("Warning", "Failed") == Severity.ERROR

    def test_classify_severity_critical_reason_overrides_type(self) -> None:
        """A critical reason in a 'Normal' type event is still CRITICAL."""
        assert _classify_severity("Normal", "OOMKilling") == Severity.CRITICAL

    # ------------------------------------------------------------------
    # _coerce_dt
    # ------------------------------------------------------------------

    def test_coerce_dt_with_none_returns_none(self) -> None:
        assert _coerce_dt(None) is None

    def test_coerce_dt_with_aware_datetime_returns_as_is(self) -> None:
        aware = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        result = _coerce_dt(aware)
        assert result == aware
        assert result is not None
        assert result.tzinfo is not None

    def test_coerce_dt_with_naive_datetime_adds_utc(self) -> None:
        naive = datetime(2026, 2, 18, 12, 0, 0)
        result = _coerce_dt(naive)
        assert result is not None
        assert result.tzinfo == UTC
        assert result.year == 2026

    def test_coerce_dt_with_non_datetime_returns_none(self) -> None:
        assert _coerce_dt("not-a-datetime") is None
        assert _coerce_dt(12345) is None

    # ------------------------------------------------------------------
    # _parse_dt_str
    # ------------------------------------------------------------------

    def test_parse_dt_str_iso8601_utc_z_suffix(self) -> None:
        result = _parse_dt_str("2026-02-18T12:00:00Z")
        assert result is not None
        assert result.year == 2026
        assert result.month == 2
        assert result.day == 18
        assert result.hour == 12
        assert result.tzinfo is not None

    def test_parse_dt_str_iso8601_with_offset(self) -> None:
        result = _parse_dt_str("2026-02-18T12:00:00+00:00")
        assert result is not None
        assert result.tzinfo is not None

    def test_parse_dt_str_invalid_returns_none(self) -> None:
        assert _parse_dt_str("not-a-date") is None

    def test_parse_dt_str_none_returns_none(self) -> None:
        assert _parse_dt_str(None) is None

    def test_parse_dt_str_datetime_object_passes_through(self) -> None:
        dt = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        result = _parse_dt_str(dt)
        assert result is not None
        assert result.tzinfo is not None

    # ------------------------------------------------------------------
    # Buffer eviction
    # ------------------------------------------------------------------

    def test_buffer_eviction_removes_stale_records(self) -> None:
        """Events older than 2 hours are removed from the left of the buffer."""
        watcher = self._make_watcher()

        stale_time = datetime.now(tz=UTC) - timedelta(hours=3)
        fresh_time = datetime.now(tz=UTC)

        stale_record = _make_event_record(last_seen=stale_time, first_seen=stale_time)
        fresh_record = _make_event_record(last_seen=fresh_time, first_seen=fresh_time)

        watcher._buffer.append(stale_record)
        watcher._buffer.append(fresh_record)

        watcher._evict_stale()

        assert len(watcher._buffer) == 1
        assert watcher._buffer[0] is fresh_record

    # ------------------------------------------------------------------
    # Callbacks
    # ------------------------------------------------------------------

    async def test_callbacks_fired_with_event_record(self) -> None:
        """All registered async callbacks receive the new EventRecord."""
        watcher = self._make_watcher()
        cb1: AsyncMock = AsyncMock()
        cb2: AsyncMock = AsyncMock()
        watcher.add_callback(cb1)
        watcher.add_callback(cb2)

        obj = _make_v1event(reason="Pulled", kube_type="Normal")
        await watcher._handle_event("ADDED", obj, {})

        cb1.assert_called_once()
        cb2.assert_called_once()
        # The argument should be an EventRecord
        record = cb1.call_args[0][0]
        assert isinstance(record, EventRecord)
        assert record.reason == "Pulled"

    # ------------------------------------------------------------------
    # get_events filtering
    # ------------------------------------------------------------------

    def test_get_events_filters_by_resource_kind_and_name(self) -> None:
        watcher = self._make_watcher()
        now = datetime.now(tz=UTC)

        # Add a matching record
        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="target", last_seen=now)
        )
        # Add a non-matching record (different kind)
        watcher._buffer.append(
            _make_event_record(resource_kind="Deployment", resource_name="target", last_seen=now)
        )
        # Add a non-matching record (different name)
        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="other", last_seen=now)
        )

        results = watcher.get_events("Pod", "default", "target")

        assert len(results) == 1
        assert results[0].resource_kind == "Pod"
        assert results[0].resource_name == "target"

    def test_get_events_filters_by_namespace(self) -> None:
        watcher = self._make_watcher()
        now = datetime.now(tz=UTC)

        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="my-pod", namespace="default", last_seen=now)
        )
        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="my-pod", namespace="kube-system", last_seen=now)
        )

        results = watcher.get_events("Pod", "default", "my-pod")

        assert len(results) == 1
        assert results[0].namespace == "default"

    def test_get_events_empty_namespace_matches_all(self) -> None:
        """Empty namespace string in get_events() matches records from any namespace."""
        watcher = self._make_watcher()
        now = datetime.now(tz=UTC)

        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="p", namespace="ns-a", last_seen=now)
        )
        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="p", namespace="ns-b", last_seen=now)
        )

        results = watcher.get_events("Pod", "", "p")

        assert len(results) == 2

    def test_get_events_excludes_stale_records(self) -> None:
        """Records outside the 2-hour window are excluded from results."""
        watcher = self._make_watcher()
        stale_time = datetime.now(tz=UTC) - timedelta(hours=3)

        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="p", last_seen=stale_time)
        )

        results = watcher.get_events("Pod", "default", "p")

        assert len(results) == 0

    def test_get_events_since_filter(self) -> None:
        """Events before the `since` datetime are excluded."""
        watcher = self._make_watcher()
        # Use times relative to now so events fall within the 2-hour buffer window
        now = datetime.now(tz=UTC)
        earlier = now - timedelta(minutes=90)
        later = now - timedelta(minutes=10)
        since = now - timedelta(minutes=60)

        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="p", last_seen=earlier)
        )
        watcher._buffer.append(
            _make_event_record(resource_kind="Pod", resource_name="p", last_seen=later)
        )

        results = watcher.get_events("Pod", "", "p", since=since)

        assert len(results) == 1
        assert results[0].last_seen == later


# ===========================================================================
# TestPodWatcher
# ===========================================================================


class TestPodWatcher:
    """Tests for PodWatcher and its helpers."""

    def _make_watcher(self, cluster_id: str = "test-cluster") -> PodWatcher:
        api = MagicMock()
        return PodWatcher(api, cluster_id=cluster_id)

    def _make_pod_obj(
        self,
        name: str = "my-pod",
        namespace: str = "default",
        phase: str = "Running",
        resource_version: str = "1",
        spec: dict | None = None,
    ) -> MagicMock:
        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.name = name
        obj.metadata.namespace = namespace
        obj.metadata.resource_version = resource_version
        obj.status = MagicMock()
        obj.status.phase = phase
        if spec is not None:
            obj.spec = MagicMock()
            obj.spec.to_dict = MagicMock(return_value=spec)
        else:
            obj.spec = None
        return obj

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def test_init_creates_empty_tracking_dicts(self) -> None:
        watcher = self._make_watcher()

        assert watcher._pod_phases == {}
        assert watcher._pod_spec_hashes == {}
        assert watcher._event_handlers == []
        assert watcher._resource_handlers == []

    # ------------------------------------------------------------------
    # _pod_key
    # ------------------------------------------------------------------

    def test_pod_key_from_object(self) -> None:
        obj = self._make_pod_obj(name="web-1", namespace="prod")
        key = _pod_key(obj, {})

        assert key == ("prod", "web-1")

    def test_pod_key_from_raw_dict(self) -> None:
        obj = MagicMock(spec=[])  # no 'metadata'
        raw = {"metadata": {"namespace": "staging", "name": "worker-2"}}
        key = _pod_key(obj, raw)

        assert key == ("staging", "worker-2")

    def test_pod_key_returns_none_when_name_missing(self) -> None:
        obj = MagicMock(spec=[])  # no 'metadata'
        raw = {"metadata": {"namespace": "default"}}  # no 'name'
        key = _pod_key(obj, raw)

        assert key is None

    def test_pod_key_returns_none_for_empty_event(self) -> None:
        obj = MagicMock(spec=[])
        key = _pod_key(obj, {})

        assert key is None

    # ------------------------------------------------------------------
    # _extract_phase
    # ------------------------------------------------------------------

    def test_extract_phase_from_object(self) -> None:
        obj = self._make_pod_obj(phase="Pending")
        phase = _extract_phase(obj, {})

        assert phase == "Pending"

    def test_extract_phase_from_raw_dict(self) -> None:
        obj = MagicMock(spec=[])  # no 'status'
        raw = {"status": {"phase": "Failed"}}
        phase = _extract_phase(obj, raw)

        assert phase == "Failed"

    def test_extract_phase_returns_empty_when_missing(self) -> None:
        obj = MagicMock(spec=[])
        phase = _extract_phase(obj, {})

        assert phase == ""

    # ------------------------------------------------------------------
    # _extract_spec
    # ------------------------------------------------------------------

    def test_extract_spec_from_raw_dict_preferred(self) -> None:
        """When both obj and raw are present, raw spec dict is preferred."""
        spec_dict = {"containers": [{"image": "nginx:1.25"}]}
        obj = MagicMock()
        obj.spec = MagicMock()
        raw = {"spec": spec_dict}

        result = _extract_spec(obj, raw)

        assert result == spec_dict

    def test_extract_spec_falls_back_to_obj_to_dict(self) -> None:
        """Falls back to obj.spec.to_dict() when raw spec is absent."""
        spec_dict = {"containers": [{"image": "redis:7"}]}
        obj = MagicMock()
        obj.spec = MagicMock()
        obj.spec.to_dict = MagicMock(return_value=spec_dict)

        result = _extract_spec(obj, {})

        assert result == spec_dict

    def test_extract_spec_returns_none_when_absent(self) -> None:
        obj = MagicMock(spec=[])  # no 'spec'
        result = _extract_spec(obj, {})

        assert result is None

    # ------------------------------------------------------------------
    # _hash_spec
    # ------------------------------------------------------------------

    def test_hash_spec_deterministic(self) -> None:
        """Same spec always produces the same hash."""
        spec = {"containers": [{"image": "nginx:1.25", "name": "web"}], "restartPolicy": "Always"}
        h1 = _hash_spec(spec)
        h2 = _hash_spec(spec)

        assert h1 == h2
        assert len(h1) == 64  # SHA-256 hex digest length

    def test_hash_spec_different_specs_produce_different_hashes(self) -> None:
        spec_a = {"containers": [{"image": "nginx:1.24"}]}
        spec_b = {"containers": [{"image": "nginx:1.25"}]}

        assert _hash_spec(spec_a) != _hash_spec(spec_b)

    def test_hash_spec_key_order_insensitive(self) -> None:
        """json.dumps with sort_keys=True makes hashing key-order insensitive."""
        spec_a = {"b": 2, "a": 1}
        spec_b = {"a": 1, "b": 2}

        assert _hash_spec(spec_a) == _hash_spec(spec_b)

    # ------------------------------------------------------------------
    # _build_phase_event
    # ------------------------------------------------------------------

    def test_build_phase_event_failed_is_error_severity(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_phase_event("default", "my-pod", "Failed", "Running", now, "test-cluster")

        assert record.severity == Severity.ERROR
        assert record.source == EventSource.POD_PHASE
        assert record.reason == "PhaseTransition"
        assert "Failed" in record.message
        assert "Running" in record.message
        assert record.resource_kind == "Pod"
        assert record.resource_name == "my-pod"
        assert record.namespace == "default"
        assert record.cluster_id == "test-cluster"

    def test_build_phase_event_running_is_info_severity(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_phase_event("default", "my-pod", "Running", "Pending", now, "cluster")

        assert record.severity == Severity.INFO

    def test_build_phase_event_unknown_is_warning_severity(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_phase_event("default", "my-pod", "Unknown", None, now, "cluster")

        assert record.severity == Severity.WARNING
        assert "None" in record.message  # prev_phase displayed as "None"

    def test_build_phase_event_succeeded_is_info_severity(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_phase_event("batch", "job-pod", "Succeeded", "Running", now, "cluster")

        assert record.severity == Severity.INFO

    # ------------------------------------------------------------------
    # _handle_event — phase transition
    # ------------------------------------------------------------------

    async def test_phase_transition_emits_event_record(self) -> None:
        watcher = self._make_watcher()
        handler: AsyncMock = AsyncMock()
        watcher.add_event_handler(handler)

        obj = self._make_pod_obj(name="app-1", namespace="prod", phase="Running")

        await watcher._handle_event("ADDED", obj, {})

        handler.assert_called_once()
        record: EventRecord = handler.call_args[0][0]
        assert record.source == EventSource.POD_PHASE
        assert record.resource_name == "app-1"

    async def test_phase_transition_no_event_when_phase_unchanged(self) -> None:
        """If the phase has not changed, no event should be emitted."""
        watcher = self._make_watcher()
        handler: AsyncMock = AsyncMock()
        watcher.add_event_handler(handler)

        obj = self._make_pod_obj(name="app-1", namespace="prod", phase="Running")

        await watcher._handle_event("ADDED", obj, {})
        handler.reset_mock()

        # Same phase again
        await watcher._handle_event("MODIFIED", obj, {})

        handler.assert_not_called()

    async def test_spec_change_emits_resource_snapshot(self) -> None:
        """A spec hash change should call resource_handlers with a ResourceSnapshot."""
        watcher = self._make_watcher()
        resource_handler: AsyncMock = AsyncMock()
        watcher.add_resource_handler(resource_handler)

        spec_v1 = {"containers": [{"image": "nginx:1.24"}]}
        obj = self._make_pod_obj(name="web", namespace="default", phase="Running")
        raw_v1 = {
            "metadata": {"namespace": "default", "name": "web", "resourceVersion": "1"},
            "status": {"phase": "Running"},
            "spec": spec_v1,
        }

        await watcher._handle_event("ADDED", obj, raw_v1)
        resource_handler.assert_called_once()

        snapshot: ResourceSnapshot = resource_handler.call_args[0][0]
        assert snapshot.kind == "Pod"
        assert snapshot.name == "web"
        assert snapshot.spec == spec_v1

    async def test_spec_no_duplicate_snapshot_when_unchanged(self) -> None:
        """A second event with the same spec must not fire resource_handlers again."""
        watcher = self._make_watcher()
        resource_handler: AsyncMock = AsyncMock()
        watcher.add_resource_handler(resource_handler)

        spec = {"containers": [{"image": "nginx:1.25"}]}
        raw = {
            "metadata": {"namespace": "default", "name": "web", "resourceVersion": "1"},
            "status": {"phase": "Running"},
            "spec": spec,
        }
        obj = self._make_pod_obj(name="web", namespace="default")

        await watcher._handle_event("ADDED", obj, raw)
        resource_handler.reset_mock()

        # Same spec again
        await watcher._handle_event("MODIFIED", obj, raw)

        resource_handler.assert_not_called()

    async def test_deleted_removes_tracked_phases_and_hashes(self) -> None:
        """DELETED events must clean up _pod_phases and _pod_spec_hashes."""
        watcher = self._make_watcher()
        key = ("default", "doomed-pod")
        watcher._pod_phases[key] = "Running"
        watcher._pod_spec_hashes[key] = "abc123"

        obj = self._make_pod_obj(name="doomed-pod", namespace="default")

        await watcher._handle_event("DELETED", obj, {})

        assert key not in watcher._pod_phases
        assert key not in watcher._pod_spec_hashes


# ===========================================================================
# TestNodeWatcher
# ===========================================================================


class TestNodeWatcher:
    """Tests for NodeWatcher and its helpers."""

    def _make_watcher(self, cluster_id: str = "test-cluster") -> NodeWatcher:
        api = MagicMock()
        return NodeWatcher(api, cluster_id=cluster_id)

    def _make_node_obj(
        self,
        name: str = "node-1",
        conditions: list[dict] | None = None,
    ) -> MagicMock:
        obj = MagicMock()
        obj.metadata = MagicMock()
        obj.metadata.name = name

        obj.status = MagicMock()
        if conditions is not None:
            cond_mocks = []
            for cond in conditions:
                c = MagicMock()
                c.type = cond.get("type", "")
                c.status = cond.get("status", "")
                c.reason = cond.get("reason", "")
                c.message = cond.get("message", "")
                cond_mocks.append(c)
            obj.status.conditions = cond_mocks
        else:
            obj.status.conditions = None
        return obj

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def test_init_creates_empty_condition_states(self) -> None:
        watcher = self._make_watcher()

        assert watcher._condition_states == {}
        assert watcher._event_handlers == []

    # ------------------------------------------------------------------
    # _extract_node_name
    # ------------------------------------------------------------------

    def test_extract_node_name_from_object(self) -> None:
        obj = self._make_node_obj(name="worker-42")
        name = _extract_node_name(obj, {})

        assert name == "worker-42"

    def test_extract_node_name_from_raw_dict(self) -> None:
        obj = MagicMock(spec=[])  # no 'metadata'
        raw = {"metadata": {"name": "master-1"}}
        name = _extract_node_name(obj, raw)

        assert name == "master-1"

    def test_extract_node_name_returns_empty_when_missing(self) -> None:
        obj = MagicMock(spec=[])
        name = _extract_node_name(obj, {})

        assert name == ""

    # ------------------------------------------------------------------
    # _extract_conditions
    # ------------------------------------------------------------------

    def test_extract_conditions_from_object(self) -> None:
        obj = self._make_node_obj(
            name="node-1",
            conditions=[
                {"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "kubelet is ready"},
                {"type": "MemoryPressure", "status": "False", "reason": "", "message": ""},
            ],
        )

        conditions = _extract_conditions(obj, {})

        assert len(conditions) == 2
        assert conditions[0]["type"] == "Ready"
        assert conditions[0]["status"] == "True"
        assert conditions[1]["type"] == "MemoryPressure"
        assert conditions[1]["status"] == "False"

    def test_extract_conditions_from_raw_dict(self) -> None:
        obj = MagicMock(spec=[])  # no 'status'
        raw = {
            "status": {
                "conditions": [
                    {"type": "DiskPressure", "status": "True", "reason": "", "message": "disk is full"},
                    {"type": "Ready", "status": "False", "reason": "KubeletNotReady", "message": ""},
                ]
            }
        }

        conditions = _extract_conditions(obj, raw)

        assert len(conditions) == 2
        assert conditions[0]["type"] == "DiskPressure"
        assert conditions[0]["status"] == "True"
        assert conditions[1]["type"] == "Ready"
        assert conditions[1]["status"] == "False"

    def test_extract_conditions_returns_empty_when_absent(self) -> None:
        obj = MagicMock(spec=[])
        conditions = _extract_conditions(obj, {})

        assert conditions == []

    # ------------------------------------------------------------------
    # _classify_condition
    # ------------------------------------------------------------------

    def test_classify_condition_ready_true_is_not_adverse(self) -> None:
        """Ready=True means the node is healthy — not adverse."""
        reason_key, is_adverse = _classify_condition("Ready", "True")

        assert reason_key == "NotReady"
        assert is_adverse is False

    def test_classify_condition_ready_false_is_adverse(self) -> None:
        """Ready=False means the node is not ready — adverse."""
        reason_key, is_adverse = _classify_condition("Ready", "False")

        assert reason_key == "NotReady"
        assert is_adverse is True

    def test_classify_condition_ready_unknown_is_adverse(self) -> None:
        """Ready=Unknown is treated as adverse."""
        reason_key, is_adverse = _classify_condition("Ready", "Unknown")

        assert reason_key == "NotReady"
        assert is_adverse is True

    def test_classify_condition_memory_pressure_true_is_adverse(self) -> None:
        """MemoryPressure=True means pressure is active — adverse."""
        reason_key, is_adverse = _classify_condition("MemoryPressure", "True")

        assert reason_key == "MemoryPressure"
        assert is_adverse is True

    def test_classify_condition_memory_pressure_false_is_not_adverse(self) -> None:
        """MemoryPressure=False means no pressure — not adverse."""
        reason_key, is_adverse = _classify_condition("MemoryPressure", "False")

        assert reason_key == "MemoryPressure"
        assert is_adverse is False

    def test_classify_condition_disk_pressure_true_is_adverse(self) -> None:
        reason_key, is_adverse = _classify_condition("DiskPressure", "True")

        assert reason_key == "DiskPressure"
        assert is_adverse is True

    def test_classify_condition_pid_pressure_false_is_not_adverse(self) -> None:
        reason_key, is_adverse = _classify_condition("PIDPressure", "False")

        assert reason_key == "PIDPressure"
        assert is_adverse is False

    # ------------------------------------------------------------------
    # _build_condition_event
    # ------------------------------------------------------------------

    def test_build_condition_event_notready_is_critical(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_condition_event(
            node_name="node-1",
            condition_type="Ready",
            reason_key="NotReady",
            message="kubelet not responding",
            at=now,
            cluster_id="prod",
        )

        assert record.severity == Severity.CRITICAL
        assert record.source == EventSource.NODE_CONDITION
        assert record.resource_kind == "Node"
        assert record.resource_name == "node-1"
        assert record.reason == "NotReady"
        assert record.message == "kubelet not responding"
        assert record.cluster_id == "prod"
        assert record.namespace == ""

    def test_build_condition_event_memory_pressure_is_warning(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_condition_event(
            node_name="node-2",
            condition_type="MemoryPressure",
            reason_key="MemoryPressure",
            message="",
            at=now,
            cluster_id="dev",
        )

        assert record.severity == Severity.WARNING
        # Default message when provided message is empty
        assert "node-2" in record.message

    def test_build_condition_event_default_message_when_empty(self) -> None:
        now = datetime(2026, 2, 18, 12, 0, 0, tzinfo=UTC)
        record = _build_condition_event(
            node_name="node-3",
            condition_type="DiskPressure",
            reason_key="DiskPressure",
            message="",
            at=now,
            cluster_id="test",
        )

        assert "node-3" in record.message
        assert "DiskPressure" in record.message

    # ------------------------------------------------------------------
    # _handle_event — adverse transition fires handler
    # ------------------------------------------------------------------

    async def test_adverse_transition_fires_event_handler(self) -> None:
        """An adverse condition change must invoke all registered event handlers."""
        watcher = self._make_watcher()
        handler: AsyncMock = AsyncMock()
        watcher.add_event_handler(handler)

        obj = self._make_node_obj(
            name="node-1",
            conditions=[{"type": "Ready", "status": "False", "reason": "KubeletNotReady", "message": "timeout"}],
        )

        await watcher._handle_event("MODIFIED", obj, {})

        handler.assert_called_once()
        record: EventRecord = handler.call_args[0][0]
        assert record.source == EventSource.NODE_CONDITION
        assert record.severity == Severity.CRITICAL
        assert record.reason == "NotReady"

    async def test_no_handler_fired_for_non_adverse_condition(self) -> None:
        """Non-adverse condition changes (e.g., Ready=True) must not fire handlers."""
        watcher = self._make_watcher()
        handler: AsyncMock = AsyncMock()
        watcher.add_event_handler(handler)

        obj = self._make_node_obj(
            name="node-1",
            conditions=[{"type": "Ready", "status": "True", "reason": "KubeletReady", "message": "all good"}],
        )

        await watcher._handle_event("MODIFIED", obj, {})

        handler.assert_not_called()

    async def test_no_duplicate_event_when_condition_unchanged(self) -> None:
        """A second event with the same condition status must not re-fire the handler."""
        watcher = self._make_watcher()
        handler: AsyncMock = AsyncMock()
        watcher.add_event_handler(handler)

        obj = self._make_node_obj(
            name="node-1",
            conditions=[{"type": "MemoryPressure", "status": "True", "reason": "", "message": "OOM"}],
        )

        await watcher._handle_event("MODIFIED", obj, {})
        handler.reset_mock()

        # Same status again — no change
        await watcher._handle_event("MODIFIED", obj, {})

        handler.assert_not_called()

    async def test_deleted_cleans_up_condition_states(self) -> None:
        """DELETED events must remove the node's tracked condition states."""
        watcher = self._make_watcher()
        node_name = "doomed-node"
        watcher._condition_states[(node_name, "Ready")] = "False"
        watcher._condition_states[(node_name, "MemoryPressure")] = "True"
        # Another node should not be affected
        watcher._condition_states[("other-node", "Ready")] = "True"

        obj = self._make_node_obj(name=node_name)
        await watcher._handle_event("DELETED", obj, {})

        assert (node_name, "Ready") not in watcher._condition_states
        assert (node_name, "MemoryPressure") not in watcher._condition_states
        # Unrelated node preserved
        assert ("other-node", "Ready") in watcher._condition_states

    async def test_untracked_condition_type_is_ignored(self) -> None:
        """Conditions not in _WATCHED_CONDITIONS must be silently ignored."""
        watcher = self._make_watcher()
        handler: AsyncMock = AsyncMock()
        watcher.add_event_handler(handler)

        obj = self._make_node_obj(
            name="node-1",
            conditions=[{"type": "NetworkUnavailable", "status": "True", "reason": "", "message": ""}],
        )

        await watcher._handle_event("MODIFIED", obj, {})

        handler.assert_not_called()

    async def test_multiple_adverse_conditions_fire_handler_for_each(self) -> None:
        """All adverse conditions in a single event should each produce a record."""
        watcher = self._make_watcher()
        received_records: list[EventRecord] = []

        async def capture(record: EventRecord) -> None:
            received_records.append(record)

        watcher.add_event_handler(capture)

        obj = self._make_node_obj(
            name="stressed-node",
            conditions=[
                {"type": "Ready", "status": "False", "reason": "", "message": "kubelet down"},
                {"type": "MemoryPressure", "status": "True", "reason": "", "message": "OOM"},
                {"type": "DiskPressure", "status": "True", "reason": "", "message": "disk full"},
            ],
        )

        await watcher._handle_event("ADDED", obj, {})

        assert len(received_records) == 3
        reasons = {r.reason for r in received_records}
        assert reasons == {"NotReady", "MemoryPressure", "DiskPressure"}
