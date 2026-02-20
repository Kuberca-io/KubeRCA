"""Unit tests for kuberca.app — KubeRCAApp lifecycle, _ComponentError, and _feed_graph."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kuberca.app import KubeRCAApp, _ComponentError, _feed_graph

# ---------------------------------------------------------------------------
# TestComponentError
# ---------------------------------------------------------------------------


class TestComponentError:
    def test_component_error_stores_fields(self) -> None:
        """_ComponentError stores component name and original cause on the instance."""
        cause = ValueError("something went wrong")
        err = _ComponentError("cache", cause)

        assert err.component == "cache"
        assert err.cause is cause

    def test_component_error_message_includes_component_and_cause(self) -> None:
        """The exception message contains both the component name and the cause text."""
        cause = RuntimeError("fail")
        err = _ComponentError("ledger", cause)

        assert "ledger" in str(err)
        assert "fail" in str(err)

    def test_component_error_is_exception(self) -> None:
        """_ComponentError is an Exception subclass and can be raised and caught."""
        cause = OSError("disk full")
        with pytest.raises(_ComponentError) as exc_info:
            raise _ComponentError("rest", cause)

        assert exc_info.value.component == "rest"
        assert exc_info.value.cause is cause


# ---------------------------------------------------------------------------
# TestFeedGraph
# ---------------------------------------------------------------------------


class TestFeedGraph:
    def test_feed_graph_with_none_graph_is_noop(self) -> None:
        """_feed_graph(None, ...) returns without raising."""
        raw = {"metadata": {"resourceVersion": "123"}}
        # No exception should be raised at all
        _feed_graph(None, "Pod", "default", "my-pod", raw)

    def test_feed_graph_creates_snapshot_and_calls_add_resource(self) -> None:
        """_feed_graph calls graph.add_resource with a ResourceSnapshot built from the raw object."""
        from kuberca.models.resources import ResourceSnapshot

        mock_graph = MagicMock()
        raw = {
            "metadata": {"resourceVersion": "42"},
            "spec": {"containers": []},
        }

        _feed_graph(mock_graph, "Pod", "default", "web-pod", raw)

        mock_graph.add_resource.assert_called_once()
        call_arg = mock_graph.add_resource.call_args[0][0]
        assert isinstance(call_arg, ResourceSnapshot)
        assert call_arg.kind == "Pod"
        assert call_arg.namespace == "default"
        assert call_arg.name == "web-pod"
        assert call_arg.resource_version == "42"
        assert call_arg.spec is raw

    def test_feed_graph_missing_resource_version_defaults_to_empty_string(self) -> None:
        """_feed_graph uses an empty string for resource_version when metadata is absent."""
        mock_graph = MagicMock()
        raw: dict = {}

        _feed_graph(mock_graph, "Node", "", "node-1", raw)

        call_arg = mock_graph.add_resource.call_args[0][0]
        assert call_arg.resource_version == ""

    def test_feed_graph_exception_is_silent(self) -> None:
        """If graph.add_resource raises, _feed_graph silently swallows the error."""
        mock_graph = MagicMock()
        mock_graph.add_resource.side_effect = RuntimeError("graph exploded")
        raw = {"metadata": {"resourceVersion": "1"}}

        # Must not propagate the exception
        _feed_graph(mock_graph, "Pod", "default", "crasher", raw)


# ---------------------------------------------------------------------------
# TestKubeRCAAppInit
# ---------------------------------------------------------------------------


class TestKubeRCAAppInit:
    def test_init_defaults(self) -> None:
        """A freshly constructed KubeRCAApp has None config, _running=False, all components None."""
        app = KubeRCAApp()

        assert app.config is None
        assert app._running is False
        assert app._log is None
        assert app._k8s_client is None
        assert app._cache is None
        assert app._ledger is None
        assert app._collector is None
        assert app._rule_engine is None
        assert app._llm_analyzer is None
        assert app._coordinator is None
        assert app._analysis_queue is None
        assert app._scout is None
        assert app._notifications is None
        assert app._mcp_server is None
        assert app._rest_server is None
        assert app._dependency_graph is None
        assert app._background_tasks == []


# ---------------------------------------------------------------------------
# TestStartLlm
# ---------------------------------------------------------------------------


class TestStartLlm:
    async def test_llm_disabled_leaves_analyzer_none(self) -> None:
        """When ollama.enabled is False, _llm_analyzer stays None after _start_llm."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app.config.ollama.enabled = False
        app._log = MagicMock()

        await app._start_llm()

        assert app._llm_analyzer is None

    async def test_llm_disabled_logs_info(self) -> None:
        """When ollama.enabled is False, an info log is emitted."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app.config.ollama.enabled = False
        app._log = MagicMock()

        await app._start_llm()

        app._log.info.assert_called_once()
        call_args = app._log.info.call_args[0]
        assert "llm" in call_args[0].lower() or "ollama" in call_args[0].lower()

    async def test_llm_fails_gracefully_leaves_analyzer_none(self) -> None:
        """When LLMAnalyzer construction raises, _llm_analyzer stays None (non-fatal)."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app.config.ollama.enabled = True
        app._log = MagicMock()

        with patch("kuberca.llm.analyzer.LLMAnalyzer", side_effect=RuntimeError("ollama unreachable")):
            await app._start_llm()

        assert app._llm_analyzer is None

    async def test_llm_fails_gracefully_logs_warning(self) -> None:
        """When LLMAnalyzer init fails, a warning is logged instead of raising."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app.config.ollama.enabled = True
        app._log = MagicMock()

        with patch("kuberca.llm.analyzer.LLMAnalyzer", side_effect=RuntimeError("connection refused")):
            await app._start_llm()

        app._log.warning.assert_called_once()

    async def test_llm_health_check_failure_is_non_fatal(self) -> None:
        """Even if health_check() raises after construction, _llm_analyzer stays None."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app.config.ollama.enabled = True
        app._log = MagicMock()

        mock_analyzer = MagicMock()
        mock_analyzer.health_check = AsyncMock(side_effect=OSError("connection timeout"))

        with patch("kuberca.llm.analyzer.LLMAnalyzer", return_value=mock_analyzer):
            await app._start_llm()

        assert app._llm_analyzer is None


# ---------------------------------------------------------------------------
# TestStartNotifications
# ---------------------------------------------------------------------------


class TestStartNotifications:
    async def test_notifications_fail_gracefully_leaves_dispatcher_none(self) -> None:
        """When build_notification_dispatcher raises, _notifications stays None (non-fatal)."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app._log = MagicMock()
        app._scout = MagicMock()

        with patch(
            "kuberca.notifications.build_notification_dispatcher",
            side_effect=RuntimeError("bad config"),
        ):
            await app._start_notifications()

        assert app._notifications is None

    async def test_notifications_fail_gracefully_logs_warning(self) -> None:
        """When build_notification_dispatcher raises, a warning is logged."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app._log = MagicMock()
        app._scout = MagicMock()

        with patch(
            "kuberca.notifications.build_notification_dispatcher",
            side_effect=ValueError("missing webhook url"),
        ):
            await app._start_notifications()

        app._log.warning.assert_called_once()

    async def test_notifications_success_sets_dispatcher(self) -> None:
        """When build_notification_dispatcher succeeds, _notifications is populated."""
        app = KubeRCAApp()
        app.config = MagicMock()
        app._log = MagicMock()
        app._scout = MagicMock()

        mock_dispatcher = MagicMock()
        with patch(
            "kuberca.notifications.build_notification_dispatcher",
            return_value=mock_dispatcher,
        ):
            await app._start_notifications()

        assert app._notifications is mock_dispatcher


# ---------------------------------------------------------------------------
# TestStartMcp
# ---------------------------------------------------------------------------


class TestStartMcp:
    async def test_mcp_fails_gracefully_leaves_server_none(self) -> None:
        """When MCPServer instantiation raises, _mcp_server stays None (non-fatal)."""
        app = KubeRCAApp()
        app._log = MagicMock()
        app._coordinator = MagicMock()
        app._cache = MagicMock()

        with patch("kuberca.mcp.MCPServer", side_effect=RuntimeError("mcp init failed")):
            await app._start_mcp()

        assert app._mcp_server is None

    async def test_mcp_fails_gracefully_logs_warning(self) -> None:
        """When MCPServer instantiation raises, a warning is logged."""
        app = KubeRCAApp()
        app._log = MagicMock()
        app._coordinator = MagicMock()
        app._cache = MagicMock()

        with patch("kuberca.mcp.MCPServer", side_effect=OSError("port in use")):
            await app._start_mcp()

        app._log.warning.assert_called_once()

    async def test_mcp_success_sets_server_and_creates_background_task(self) -> None:
        """When MCPServer starts successfully, _mcp_server is set and a task is appended."""
        app = KubeRCAApp()
        app._log = MagicMock()
        app._coordinator = MagicMock()
        app._cache = MagicMock()

        mock_mcp = MagicMock()
        mock_mcp.start = AsyncMock(return_value=None)

        with patch("kuberca.mcp.MCPServer", return_value=mock_mcp):
            await app._start_mcp()

        assert app._mcp_server is mock_mcp
        assert len(app._background_tasks) == 1


# ---------------------------------------------------------------------------
# TestStop
# ---------------------------------------------------------------------------


class TestStop:
    async def test_stop_never_started_is_noop(self) -> None:
        """Calling stop() on a fresh KubeRCAApp that was never started completes without error."""
        app = KubeRCAApp()
        # _running is False and _log is None — stop() must treat this as a no-op
        await app.stop()  # must not raise

    async def test_stop_cancels_background_tasks(self) -> None:
        """stop() cancels all pending background tasks."""
        app = KubeRCAApp()
        app._log = MagicMock()

        # Create real asyncio tasks that just sleep forever
        async def _forever() -> None:
            await asyncio.sleep(3600)

        task1 = asyncio.create_task(_forever(), name="fake-task-1")
        task2 = asyncio.create_task(_forever(), name="fake-task-2")
        app._background_tasks = [task1, task2]

        # Patch _stop_k8s_client so we don't need a real k8s client
        with patch.object(app, "_stop_k8s_client", new=AsyncMock()):
            await app.stop()

        assert task1.cancelled()
        assert task2.cancelled()

    async def test_stop_clears_background_tasks_list(self) -> None:
        """After stop(), _background_tasks is empty."""
        app = KubeRCAApp()
        app._log = MagicMock()

        async def _forever() -> None:
            await asyncio.sleep(3600)

        app._background_tasks = [asyncio.create_task(_forever())]

        with patch.object(app, "_stop_k8s_client", new=AsyncMock()):
            await app.stop()

        assert app._background_tasks == []

    async def test_stop_component_with_none_is_noop(self) -> None:
        """_stop_component with None component returns without doing anything."""
        app = KubeRCAApp()
        app._log = MagicMock()

        await app._stop_component("cache", None)  # must not raise

        # No log calls — nothing happened
        app._log.warning.assert_not_called()
        app._log.error.assert_not_called()

    async def test_stop_component_without_stop_method_is_noop(self) -> None:
        """_stop_component with a component that has no stop() method returns safely."""
        app = KubeRCAApp()
        app._log = MagicMock()

        bare_object = object()  # has no .stop attribute
        await app._stop_component("ledger", bare_object)  # must not raise

        app._log.warning.assert_not_called()
        app._log.error.assert_not_called()

    async def test_stop_component_with_sync_stop_calls_it(self) -> None:
        """_stop_component calls a synchronous stop() method on the component."""
        app = KubeRCAApp()
        app._log = MagicMock()

        component = MagicMock()
        component.stop = MagicMock(return_value=None)  # sync, not a coroutine

        await app._stop_component("rule_engine", component)

        component.stop.assert_called_once()

    async def test_stop_component_with_async_stop_awaits_it(self) -> None:
        """_stop_component awaits an async stop() coroutine on the component."""
        app = KubeRCAApp()
        app._log = MagicMock()

        component = MagicMock()
        component.stop = AsyncMock(return_value=None)

        await app._stop_component("coordinator", component)

        component.stop.assert_called_once()

    async def test_stop_component_handles_timeout(self) -> None:
        """_stop_component logs a warning when stop() takes longer than the grace period."""
        app = KubeRCAApp()
        app._log = MagicMock()

        async def _slow_stop() -> None:
            await asyncio.sleep(9999)

        component = MagicMock()
        component.stop = _slow_stop  # returns a coroutine that never resolves

        # Patch _SHUTDOWN_GRACE_SECONDS to a tiny value so the test is fast
        with patch("kuberca.app._SHUTDOWN_GRACE_SECONDS", 0.01):
            await app._stop_component("slow-component", component)

        app._log.warning.assert_called_once()
        warning_call_kwargs = app._log.warning.call_args[1]
        assert warning_call_kwargs.get("component") == "slow-component"

    async def test_stop_component_handles_exception(self) -> None:
        """_stop_component logs an error when stop() raises an exception."""
        app = KubeRCAApp()
        app._log = MagicMock()

        component = MagicMock()
        component.stop = MagicMock(side_effect=RuntimeError("stop failed"))

        await app._stop_component("exploding-component", component)

        app._log.error.assert_called_once()
        error_call_kwargs = app._log.error.call_args[1]
        assert error_call_kwargs.get("component") == "exploding-component"
        assert "stop failed" in error_call_kwargs.get("error", "")

    async def test_stop_sets_running_false(self) -> None:
        """stop() sets _running to False even if it was True before."""
        app = KubeRCAApp()
        app._log = MagicMock()
        app._running = True  # simulate a started app

        with patch.object(app, "_stop_k8s_client", new=AsyncMock()):
            await app.stop()

        assert app._running is False

    async def test_stop_with_already_done_tasks_skips_cancel(self) -> None:
        """stop() does not attempt to cancel tasks that are already done."""
        app = KubeRCAApp()
        app._log = MagicMock()

        async def _instant() -> None:
            return

        # Run the task to completion before stop() is called
        task = asyncio.create_task(_instant())
        await asyncio.sleep(0)  # allow the task to complete
        assert task.done()

        app._background_tasks = [task]

        with patch.object(app, "_stop_k8s_client", new=AsyncMock()):
            await app.stop()

        # task was already done — cancel() should not have been called on a completed task
        # (and the gather should still succeed without error)
        assert app._background_tasks == []
