"""Unit tests for kuberca.collector.watcher — BaseWatcher missing coverage.

Targets the following uncovered lines:
- _watch_loop (147-155): normal loop, CancelledError, generic exception, not-running exit
- _run_watch (159-207): BOOKMARK, ADDED/MODIFIED events, stream end failure tracking,
  relist at MAX_CONSECUTIVE_FAILURES, backoff below threshold, non-dict raw_object,
  _running=False early exit
- _handle_api_exception other codes (246-257): unknown status increments failures,
  threshold triggers relist, below threshold calls backoff
- _handle_loop_exception (261-273): increments failures, logs error, relist at threshold,
  backoff below threshold
- _relist rate limiting (345-352): throttled within 5 minutes calls backoff
- _do_relist (369-386): clears resource_version, calls list func, updates rv, warns when
  no rv returned
- _relist_pods_fallback (394-408): calls list_pod_for_all_namespaces, updates rv on success,
  handles failure gracefully
- _extract_rv_from_bookmark (443): extracts rv from raw_object metadata, returns "" for
  non-dict raw_object and non-dict metadata
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kuberca.collector.watcher import BaseWatcher, _extract_rv_from_bookmark

# ---------------------------------------------------------------------------
# Concrete subclass of BaseWatcher for testing
# ---------------------------------------------------------------------------


class _TestWatcher(BaseWatcher):
    """Minimal concrete subclass used to test BaseWatcher in isolation."""

    def __init__(self, api: object, **kwargs: object) -> None:
        super().__init__(api, name="test-watcher", **kwargs)  # type: ignore[arg-type]
        self.handled_events: list[tuple[str, object, dict]] = []

    def _list_func(self):  # type: ignore[override]
        return self._api.list_pod_for_all_namespaces

    async def _handle_event(self, event_type: str, obj: object, raw: dict) -> None:
        self.handled_events.append((event_type, obj, raw))


def _make_watcher(cluster_id: str = "test") -> _TestWatcher:
    api = MagicMock()
    api.list_pod_for_all_namespaces = AsyncMock()
    return _TestWatcher(api, cluster_id=cluster_id)


# ===========================================================================
# TestWatchLoop — lines 147-155
# ===========================================================================


class TestWatchLoop:
    """Tests for _watch_loop — the outer retry loop."""

    async def test_watch_loop_runs_and_exits_when_not_running(self) -> None:
        """Loop must execute _run_watch at least once and stop when _running=False."""
        watcher = _make_watcher()

        call_count = 0

        async def fake_run_watch() -> None:
            nonlocal call_count
            call_count += 1
            # After first call, flip the running flag so the loop exits
            watcher._running = False

        with patch.object(watcher, "_run_watch", side_effect=fake_run_watch):
            watcher._running = True
            await watcher._watch_loop()

        assert call_count == 1

    async def test_watch_loop_cancelled_error_returns_cleanly(self) -> None:
        """CancelledError inside _run_watch must cause _watch_loop to return without re-raising."""
        watcher = _make_watcher()

        async def raise_cancelled() -> None:
            raise asyncio.CancelledError

        with patch.object(watcher, "_run_watch", side_effect=raise_cancelled):
            watcher._running = True
            # Should not propagate the CancelledError
            await watcher._watch_loop()

        # After CancelledError the loop exits; _running state is irrelevant

    async def test_watch_loop_generic_exception_calls_handle_loop_exception(self) -> None:
        """A generic exception inside _run_watch must invoke _handle_loop_exception."""
        watcher = _make_watcher()

        exc = RuntimeError("boom")
        call_count = 0

        async def raise_once() -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise exc
            watcher._running = False

        with (
            patch.object(watcher, "_run_watch", side_effect=raise_once),
            patch.object(watcher, "_handle_loop_exception", new_callable=AsyncMock) as mock_handle,
        ):
            watcher._running = True
            await watcher._watch_loop()

        mock_handle.assert_called_once_with(exc)

    async def test_watch_loop_exception_when_not_running_returns_cleanly(self) -> None:
        """When _running is False at the time of the exception, the loop exits silently."""
        watcher = _make_watcher()

        async def raise_exc() -> None:
            watcher._running = False  # flip before the exception propagates
            raise RuntimeError("should be ignored")

        with (
            patch.object(watcher, "_handle_loop_exception", new_callable=AsyncMock) as mock_handle,
            patch.object(watcher, "_run_watch", side_effect=raise_exc),
        ):
            watcher._running = True
            await watcher._watch_loop()

        # _handle_loop_exception must NOT be called because _running was False
        mock_handle.assert_not_called()


# ===========================================================================
# TestRunWatch — lines 159-207
# ===========================================================================


class TestRunWatch:
    """Tests for _run_watch — one watch stream iteration."""

    async def test_bookmark_event_updates_resource_version(self) -> None:
        """BOOKMARK events must update _resource_version and not call _handle_event."""
        watcher = _make_watcher()

        async def mock_stream(*args, **kwargs):
            yield {
                "type": "BOOKMARK",
                "object": None,
                "raw_object": {"metadata": {"resourceVersion": "42"}},
            }

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch):
            watcher._running = True
            await watcher._run_watch()

        assert watcher._resource_version == "42"
        assert watcher.handled_events == []

    async def test_added_event_calls_handle_event(self) -> None:
        """ADDED events must invoke _handle_event with the event type, object, and raw dict."""
        watcher = _make_watcher()
        mock_obj = MagicMock()
        mock_obj.metadata = MagicMock()
        mock_obj.metadata.resource_version = "10"
        raw = {"metadata": {"resourceVersion": "10"}}

        async def mock_stream(*args, **kwargs):
            yield {"type": "ADDED", "object": mock_obj, "raw_object": raw}

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch):
            watcher._running = True
            await watcher._run_watch()

        assert len(watcher.handled_events) == 1
        event_type, obj, received_raw = watcher.handled_events[0]
        assert event_type == "ADDED"
        assert obj is mock_obj
        assert received_raw is raw

    async def test_modified_event_calls_handle_event(self) -> None:
        """MODIFIED events must also invoke _handle_event."""
        watcher = _make_watcher()
        mock_obj = MagicMock()
        mock_obj.metadata = MagicMock()
        mock_obj.metadata.resource_version = "11"
        raw = {"metadata": {"resourceVersion": "11"}}

        async def mock_stream(*args, **kwargs):
            yield {"type": "MODIFIED", "object": mock_obj, "raw_object": raw}

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch):
            watcher._running = True
            await watcher._run_watch()

        assert len(watcher.handled_events) == 1
        assert watcher.handled_events[0][0] == "MODIFIED"

    async def test_stream_end_increments_consecutive_failures(self) -> None:
        """When the stream ends cleanly, consecutive_failures must be incremented."""
        watcher = _make_watcher()

        async def mock_stream(*args, **kwargs):
            return
            yield  # make it an async generator

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
            patch.object(watcher, "_relist", new_callable=AsyncMock),
        ):
            watcher._running = True
            await watcher._run_watch()

        assert watcher._consecutive_failures == 1

    async def test_stream_end_at_max_failures_triggers_relist(self) -> None:
        """At MAX_CONSECUTIVE_FAILURES (3) stream ends must trigger _relist."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 2  # will become 3 on this stream end

        async def mock_stream(*args, **kwargs):
            return
            yield  # make it an async generator

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            watcher._running = True
            await watcher._run_watch()

        mock_relist.assert_called_once_with(reason="consecutive_failures")
        mock_backoff.assert_not_called()

    async def test_stream_end_below_max_failures_calls_backoff(self) -> None:
        """Below MAX_CONSECUTIVE_FAILURES, stream end must call _backoff instead of _relist."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 0  # will become 1, below threshold of 3

        async def mock_stream(*args, **kwargs):
            return
            yield  # make it an async generator

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            watcher._running = True
            await watcher._run_watch()

        mock_backoff.assert_called_once_with("stream_end")
        mock_relist.assert_not_called()

    async def test_non_dict_raw_object_falls_back_to_empty_dict(self) -> None:
        """When raw_object is not a dict, _handle_event must receive an empty dict."""
        watcher = _make_watcher()
        mock_obj = MagicMock()
        mock_obj.metadata = MagicMock()
        mock_obj.metadata.resource_version = "5"

        async def mock_stream(*args, **kwargs):
            # raw_object is a string, not a dict
            yield {"type": "ADDED", "object": mock_obj, "raw_object": "not-a-dict"}

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
        ):
            watcher._running = True
            await watcher._run_watch()

        assert len(watcher.handled_events) == 1
        _, _, raw = watcher.handled_events[0]
        assert raw == {}

    async def test_running_false_exits_before_handling_event(self) -> None:
        """When _running is set to False during iteration, _handle_event must not be called."""
        watcher = _make_watcher()

        async def mock_stream(*args, **kwargs):
            watcher._running = False  # simulate stop() being called mid-stream
            yield {"type": "ADDED", "object": MagicMock(), "raw_object": {}}

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch):
            watcher._running = True
            await watcher._run_watch()

        # Event must NOT be processed because _running was False at event dispatch time
        assert watcher.handled_events == []

    async def test_run_watch_uses_resource_version_when_set(self) -> None:
        """When _resource_version is non-empty, it must be passed to Watch.stream()."""
        watcher = _make_watcher()
        watcher._resource_version = "999"

        captured_kwargs: dict = {}

        async def mock_stream(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return
            yield  # make it an async generator

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
        ):
            watcher._running = True
            await watcher._run_watch()

        assert captured_kwargs.get("resource_version") == "999"

    async def test_run_watch_omits_resource_version_when_empty(self) -> None:
        """When _resource_version is empty, resource_version must not be passed to stream()."""
        watcher = _make_watcher()
        watcher._resource_version = ""

        captured_kwargs: dict = {}

        async def mock_stream(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return
            yield  # make it an async generator

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
        ):
            watcher._running = True
            await watcher._run_watch()

        assert "resource_version" not in captured_kwargs

    async def test_run_watch_closes_watch_on_api_exception(self) -> None:
        """Watch.close() must be called even when an ApiException is raised."""
        from kubernetes_asyncio.client.exceptions import ApiException

        watcher = _make_watcher()

        async def mock_stream(*args, **kwargs):
            raise ApiException(status=500, reason="Internal Server Error")
            yield  # make it an async generator

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_handle_api_exception", new_callable=AsyncMock),
        ):
            watcher._running = True
            await watcher._run_watch()

        mock_watch.close.assert_called_once()

    async def test_run_watch_updates_resource_version_from_event_object(self) -> None:
        """resource_version in _resource_version must be updated after a normal event."""
        watcher = _make_watcher()

        mock_obj = MagicMock()
        mock_obj.metadata = MagicMock()
        mock_obj.metadata.resource_version = "77"

        async def mock_stream(*args, **kwargs):
            yield {"type": "ADDED", "object": mock_obj, "raw_object": {}}

        mock_watch = MagicMock()
        mock_watch.stream = mock_stream
        mock_watch.close = AsyncMock()

        with (
            patch("kuberca.collector.watcher.watch.Watch", return_value=mock_watch),
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
        ):
            watcher._running = True
            await watcher._run_watch()

        assert watcher._resource_version == "77"


# ===========================================================================
# TestHandleApiExceptionOtherCodes — lines 246-257
# ===========================================================================


class TestHandleApiExceptionOtherCodes:
    """Tests for _handle_api_exception with unknown status codes (else branch)."""

    def _make_exc(self, status: int, reason: str = "Unknown") -> MagicMock:
        exc = MagicMock()
        exc.status = status
        exc.reason = reason
        return exc

    async def test_unknown_status_increments_consecutive_failures(self) -> None:
        """Any non-410/429/5xx status must increment consecutive_failures."""
        watcher = _make_watcher()
        assert watcher._consecutive_failures == 0

        exc = self._make_exc(status=403, reason="Forbidden")

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
            patch.object(watcher, "_relist", new_callable=AsyncMock),
        ):
            await watcher._handle_api_exception(exc)

        assert watcher._consecutive_failures == 1

    async def test_unknown_status_at_threshold_triggers_relist(self) -> None:
        """At MAX_CONSECUTIVE_FAILURES (3), unknown status must trigger _relist."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 2  # will become 3

        exc = self._make_exc(status=403)

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        mock_relist.assert_called_once_with(reason="api_error_limit")
        mock_backoff.assert_not_called()

    async def test_unknown_status_below_threshold_calls_backoff(self) -> None:
        """Below MAX_CONSECUTIVE_FAILURES, unknown status must call _backoff."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 0  # will become 1

        exc = self._make_exc(status=403)

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        mock_backoff.assert_called_once_with("api_error")
        mock_relist.assert_not_called()

    async def test_unknown_status_exactly_at_threshold_triggers_relist(self) -> None:
        """Exactly at the threshold (consecutive_failures == MAX), relist is triggered."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 2  # 2 + 1 = 3 == MAX_CONSECUTIVE_FAILURES

        exc = self._make_exc(status=422, reason="Unprocessable Entity")

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        mock_relist.assert_called_once()
        mock_backoff.assert_not_called()

    async def test_unknown_status_two_below_threshold_calls_backoff(self) -> None:
        """Two failures below max: consecutive_failures 1 -> 2, backoff called."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 1  # 1 + 1 = 2 < MAX_CONSECUTIVE_FAILURES (3)

        exc = self._make_exc(status=400)

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_api_exception(exc)

        mock_backoff.assert_called_once_with("api_error")
        mock_relist.assert_not_called()
        assert watcher._consecutive_failures == 2


# ===========================================================================
# TestHandleLoopException — lines 261-273
# ===========================================================================


class TestHandleLoopException:
    """Tests for _handle_loop_exception — unexpected exception handler."""

    async def test_increments_consecutive_failures(self) -> None:
        """_handle_loop_exception must increment consecutive_failures by one."""
        watcher = _make_watcher()
        assert watcher._consecutive_failures == 0

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
            patch.object(watcher, "_relist", new_callable=AsyncMock),
        ):
            await watcher._handle_loop_exception(RuntimeError("unexpected"))

        assert watcher._consecutive_failures == 1

    async def test_at_threshold_triggers_relist(self) -> None:
        """At MAX_CONSECUTIVE_FAILURES the method must call _relist."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 2  # will become 3

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_loop_exception(ValueError("oops"))

        mock_relist.assert_called_once_with(reason="unexpected_limit")
        mock_backoff.assert_not_called()

    async def test_below_threshold_calls_backoff(self) -> None:
        """Below MAX_CONSECUTIVE_FAILURES, _backoff must be called instead of _relist."""
        watcher = _make_watcher()
        watcher._consecutive_failures = 0  # will become 1

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
            patch.object(watcher, "_relist", new_callable=AsyncMock) as mock_relist,
        ):
            await watcher._handle_loop_exception(OSError("network error"))

        mock_backoff.assert_called_once_with("unexpected")
        mock_relist.assert_not_called()

    async def test_increments_watcher_reconnects_metric(self) -> None:
        """_handle_loop_exception must increment the watcher_reconnects_total metric."""
        watcher = _make_watcher()

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
            patch.object(watcher, "_relist", new_callable=AsyncMock),
            patch("kuberca.collector.watcher.watcher_reconnects_total") as mock_metric,
        ):
            await watcher._handle_loop_exception(RuntimeError("test"))

        mock_metric.labels.assert_called_once_with(watcher="test-watcher", reason="unexpected")
        mock_metric.labels.return_value.inc.assert_called_once()

    async def test_error_is_logged(self) -> None:
        """The error string must be passed to the logger."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        with (
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
            patch.object(watcher, "_relist", new_callable=AsyncMock),
        ):
            await watcher._handle_loop_exception(RuntimeError("logged error"))

        mock_log.error.assert_called_once()
        call_kwargs = mock_log.error.call_args[1]
        assert "logged error" in call_kwargs.get("error", "")


# ===========================================================================
# TestRelistRateLimiting — lines 345-352
# ===========================================================================


class TestRelistRateLimiting:
    """Tests for _relist rate limiting — throttle within 5 minutes."""

    async def test_relist_throttled_within_5_minutes_calls_backoff(self) -> None:
        """A relist requested within 5 minutes of the last one must be throttled."""
        watcher = _make_watcher()
        # Last relist was 2 minutes ago (within the 5 minute window)
        watcher._last_relist_at = datetime.now(UTC) - timedelta(seconds=120)

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock) as mock_do,
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
        ):
            await watcher._relist(reason="test")

        mock_do.assert_not_called()
        mock_backoff.assert_called_once_with("relist_throttled")

    async def test_relist_throttled_returns_before_updating_last_relist_at(self) -> None:
        """A throttled relist must not update _last_relist_at."""
        watcher = _make_watcher()
        original_time = datetime.now(UTC) - timedelta(seconds=60)
        watcher._last_relist_at = original_time

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock),
            patch.object(watcher, "_backoff", new_callable=AsyncMock),
        ):
            await watcher._relist(reason="test")

        # _last_relist_at should not have changed (early return)
        assert watcher._last_relist_at == original_time

    async def test_relist_not_throttled_when_just_over_5_minutes(self) -> None:
        """A relist requested just over 5 minutes after the last one must NOT be throttled."""
        watcher = _make_watcher()
        watcher._last_relist_at = datetime.now(UTC) - timedelta(seconds=310)

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock) as mock_do,
            patch.object(watcher, "_reset_backoff"),
        ):
            await watcher._relist(reason="test")

        mock_do.assert_called_once()

    async def test_relist_throttled_at_exactly_5_minutes_minus_1s(self) -> None:
        """A relist at exactly 299 seconds (just under 300s) must still be throttled."""
        watcher = _make_watcher()
        watcher._last_relist_at = datetime.now(UTC) - timedelta(seconds=299)

        with (
            patch.object(watcher, "_do_relist", new_callable=AsyncMock) as mock_do,
            patch.object(watcher, "_backoff", new_callable=AsyncMock) as mock_backoff,
        ):
            await watcher._relist(reason="test")

        mock_do.assert_not_called()
        mock_backoff.assert_called_once_with("relist_throttled")


# ===========================================================================
# TestDoRelist — lines 369-386
# ===========================================================================


class TestDoRelist:
    """Tests for _do_relist — the actual relist operation."""

    async def test_do_relist_clears_resource_version(self) -> None:
        """_do_relist must set _resource_version to '' before calling the list function."""
        watcher = _make_watcher()
        watcher._resource_version = "old-rv"

        captured_rv_at_call: list[str] = []
        result = MagicMock()
        result.metadata = MagicMock()
        result.metadata.resource_version = "new-rv"

        async def fake_list(*args, **kwargs) -> MagicMock:
            captured_rv_at_call.append(watcher._resource_version)
            return result

        watcher._api.list_pod_for_all_namespaces = fake_list

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._do_relist()

        # resource_version was cleared before the call
        assert captured_rv_at_call == [""]
        # and updated after
        assert watcher._resource_version == "new-rv"

    async def test_do_relist_calls_list_func(self) -> None:
        """_do_relist must call the list function returned by _list_func."""
        watcher = _make_watcher()

        result = MagicMock()
        result.metadata = MagicMock()
        result.metadata.resource_version = "55"

        mock_list = AsyncMock(return_value=result)
        watcher._api.list_pod_for_all_namespaces = mock_list

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._do_relist()

        mock_list.assert_called_once_with(_preload_content=True, watch=False)

    async def test_do_relist_updates_resource_version_from_result(self) -> None:
        """_do_relist must set _resource_version to the value returned by the list call."""
        watcher = _make_watcher()

        result = MagicMock()
        result.metadata = MagicMock()
        result.metadata.resource_version = "789"

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=result)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._do_relist()

        assert watcher._resource_version == "789"

    async def test_do_relist_warns_when_no_rv_returned(self) -> None:
        """When the list result carries no resourceVersion, a warning must be logged."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        result = MagicMock()
        result.metadata = MagicMock()
        result.metadata.resource_version = ""  # empty string — no rv

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=result)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._do_relist()

        mock_log.warning.assert_called_once()
        call_args = mock_log.warning.call_args[0]
        assert "relist_no_rv" in call_args

    async def test_do_relist_warns_when_metadata_is_none(self) -> None:
        """When result.metadata is None, a warning for missing rv must be logged."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        result = MagicMock()
        result.metadata = None

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=result)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._do_relist()

        mock_log.warning.assert_called_once()

    async def test_do_relist_enforces_qps_delay(self) -> None:
        """_do_relist must call asyncio.sleep once for QPS throttling."""
        watcher = _make_watcher()

        result = MagicMock()
        result.metadata = MagicMock()
        result.metadata.resource_version = "1"

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=result)

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await watcher._do_relist()

        mock_sleep.assert_called_once()
        # QPS delay is 1/5 = 0.2 seconds
        delay = mock_sleep.call_args[0][0]
        assert pytest.approx(delay, abs=1e-9) == 0.2

    async def test_do_relist_leaves_resource_version_empty_when_no_rv(self) -> None:
        """When no rv is returned, _resource_version must remain empty."""
        watcher = _make_watcher()
        watcher._resource_version = ""

        result = MagicMock()
        result.metadata = MagicMock()
        result.metadata.resource_version = None  # getattr returns None

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=result)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await watcher._do_relist()

        assert watcher._resource_version == ""


# ===========================================================================
# TestRelistPodsFallback — lines 394-408
# ===========================================================================


class TestRelistPodsFallback:
    """Tests for _relist_pods_fallback — fallback relist using only pods."""

    async def test_calls_list_pod_for_all_namespaces(self) -> None:
        """_relist_pods_fallback must call _api.list_pod_for_all_namespaces."""
        watcher = _make_watcher()

        pod_list = MagicMock()
        pod_list.metadata = MagicMock()
        pod_list.metadata.resource_version = "pod-rv-123"

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=pod_list)

        await watcher._relist_pods_fallback()

        watcher._api.list_pod_for_all_namespaces.assert_called_once_with(_preload_content=True, watch=False)

    async def test_updates_resource_version_on_success(self) -> None:
        """On success, _resource_version must be updated to the pod list's rv."""
        watcher = _make_watcher()

        pod_list = MagicMock()
        pod_list.metadata = MagicMock()
        pod_list.metadata.resource_version = "fallback-rv-99"

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=pod_list)

        await watcher._relist_pods_fallback()

        assert watcher._resource_version == "fallback-rv-99"

    async def test_handles_failure_gracefully(self) -> None:
        """An exception in the fallback call must be logged but not propagate."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        watcher._api.list_pod_for_all_namespaces = AsyncMock(side_effect=RuntimeError("network failure"))

        # Must not raise
        await watcher._relist_pods_fallback()

        mock_log.error.assert_called_once()
        call_kwargs = mock_log.error.call_args[1]
        assert "network failure" in call_kwargs.get("error", "")

    async def test_does_not_update_resource_version_on_failure(self) -> None:
        """When the fallback call fails, _resource_version must remain unchanged."""
        watcher = _make_watcher()
        watcher._resource_version = "original-rv"

        watcher._api.list_pod_for_all_namespaces = AsyncMock(side_effect=OSError("timeout"))

        await watcher._relist_pods_fallback()

        assert watcher._resource_version == "original-rv"

    async def test_warns_before_calling_fallback(self) -> None:
        """A warning log must be emitted before executing the fallback relist."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        pod_list = MagicMock()
        pod_list.metadata = MagicMock()
        pod_list.metadata.resource_version = "rv"

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=pod_list)

        await watcher._relist_pods_fallback()

        mock_log.warning.assert_called()
        first_call_args = mock_log.warning.call_args_list[0][0]
        assert "relist_fallback_pods_only" in first_call_args

    async def test_no_rv_update_when_metadata_is_none(self) -> None:
        """When pod_list.metadata is None, _resource_version must not be changed."""
        watcher = _make_watcher()
        watcher._resource_version = "stable-rv"

        pod_list = MagicMock()
        pod_list.metadata = None

        watcher._api.list_pod_for_all_namespaces = AsyncMock(return_value=pod_list)

        await watcher._relist_pods_fallback()

        assert watcher._resource_version == "stable-rv"


# ===========================================================================
# TestExtractRvFromBookmark — line 443 and surrounding guard clauses
# ===========================================================================


class TestExtractRvFromBookmark:
    """Tests for the _extract_rv_from_bookmark module-level helper."""

    def test_extracts_rv_from_raw_object_metadata(self) -> None:
        """Extracts resourceVersion from bookmark raw_object.metadata."""
        raw_event = {
            "type": "BOOKMARK",
            "raw_object": {"metadata": {"resourceVersion": "12345"}},
        }
        assert _extract_rv_from_bookmark(raw_event) == "12345"

    def test_returns_empty_for_non_dict_raw_object(self) -> None:
        """Returns '' when raw_object is not a dict (e.g., a string)."""
        raw_event = {"raw_object": "not-a-dict"}
        assert _extract_rv_from_bookmark(raw_event) == ""

    def test_returns_empty_for_non_dict_metadata(self) -> None:
        """Returns '' when metadata inside raw_object is not a dict."""
        raw_event = {"raw_object": {"metadata": "not-a-dict"}}
        assert _extract_rv_from_bookmark(raw_event) == ""

    def test_returns_empty_when_raw_object_key_missing(self) -> None:
        """Returns '' when raw_object key is absent from the event dict."""
        raw_event: dict = {"type": "BOOKMARK"}
        assert _extract_rv_from_bookmark(raw_event) == ""

    def test_returns_empty_when_resource_version_key_missing(self) -> None:
        """Returns '' when resourceVersion key is absent from metadata."""
        raw_event = {"raw_object": {"metadata": {}}}
        assert _extract_rv_from_bookmark(raw_event) == ""

    def test_returns_stringified_numeric_resource_version(self) -> None:
        """Returns str() of the resourceVersion value from metadata."""
        raw_event = {"raw_object": {"metadata": {"resourceVersion": "98765"}}}
        result = _extract_rv_from_bookmark(raw_event)
        assert result == "98765"
        assert isinstance(result, str)

    def test_returns_empty_for_none_raw_object(self) -> None:
        """Returns '' when raw_object is explicitly None."""
        raw_event = {"raw_object": None}
        assert _extract_rv_from_bookmark(raw_event) == ""


# ===========================================================================
# TestRelistExceptionBranches — lines 345-358
# ===========================================================================


class TestRelistExceptionBranches:
    """Tests for the TimeoutError and generic Exception branches inside _relist."""

    async def test_relist_timeout_triggers_pods_fallback(self) -> None:
        """When _do_relist times out, _relist_pods_fallback must be called."""
        watcher = _make_watcher()

        async def timeout_do_relist() -> None:
            raise TimeoutError

        with (
            patch.object(watcher, "_do_relist", side_effect=timeout_do_relist),
            patch.object(watcher, "_relist_pods_fallback", new_callable=AsyncMock) as mock_fallback,
            patch.object(watcher, "_reset_backoff"),
        ):
            await watcher._relist(reason="timeout_test")

        mock_fallback.assert_called_once()

    async def test_relist_timeout_increments_timeout_metric(self) -> None:
        """On timeout, cache_relist_timeout_total (in observability.metrics) must be incremented."""
        watcher = _make_watcher()

        async def timeout_do_relist() -> None:
            raise TimeoutError

        # cache_relist_timeout_total is imported locally inside the except TimeoutError block,
        # so we patch it at its definition site in kuberca.observability.metrics.
        mock_metric = MagicMock()
        with (
            patch.object(watcher, "_do_relist", side_effect=timeout_do_relist),
            patch.object(watcher, "_relist_pods_fallback", new_callable=AsyncMock),
            patch.object(watcher, "_reset_backoff"),
            patch("kuberca.observability.metrics.cache_relist_timeout_total", mock_metric),
        ):
            await watcher._relist(reason="timeout_test")

        mock_metric.inc.assert_called_once()

    async def test_relist_timeout_logs_warning(self) -> None:
        """On timeout, a warning must be logged with the reason."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        async def timeout_do_relist() -> None:
            raise TimeoutError

        with (
            patch.object(watcher, "_do_relist", side_effect=timeout_do_relist),
            patch.object(watcher, "_relist_pods_fallback", new_callable=AsyncMock),
            patch.object(watcher, "_reset_backoff"),
        ):
            await watcher._relist(reason="my_reason")

        mock_log.warning.assert_called()
        # At least one warning call should contain "relist_timeout"
        warning_calls = [c[0][0] for c in mock_log.warning.call_args_list]
        assert any("relist_timeout" in c for c in warning_calls)

    async def test_relist_generic_exception_is_logged_and_swallowed(self) -> None:
        """A generic exception in _do_relist must be logged as error but not propagate."""
        watcher = _make_watcher()
        mock_log = MagicMock()
        watcher._log = mock_log

        async def bad_do_relist() -> None:
            raise ValueError("unexpected db error")

        with (
            patch.object(watcher, "_do_relist", side_effect=bad_do_relist),
            patch.object(watcher, "_reset_backoff"),
        ):
            # Must not raise
            await watcher._relist(reason="failure_test")

        mock_log.error.assert_called()
        call_kwargs = mock_log.error.call_args[1]
        assert "unexpected db error" in call_kwargs.get("error", "")

    async def test_relist_generic_exception_still_resets_backoff(self) -> None:
        """Even after a generic exception in _do_relist, _reset_backoff must be called."""
        watcher = _make_watcher()

        async def bad_do_relist() -> None:
            raise RuntimeError("transient")

        with (
            patch.object(watcher, "_do_relist", side_effect=bad_do_relist),
            patch.object(watcher, "_reset_backoff") as mock_reset,
        ):
            await watcher._relist(reason="test")

        mock_reset.assert_called_once()
