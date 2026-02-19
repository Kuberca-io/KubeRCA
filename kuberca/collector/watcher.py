"""Base watcher with automatic reconnection and relist recovery.

Wraps kubernetes_asyncio's Watch to provide:
- Resumable watches via resourceVersion
- Exponential back-off (1 s – 60 s) on 429 responses
- Recovery relist triggered by 410, 3 consecutive failures,
  unexpected stream termination, and repeated 429/500 bursts
- Relist bounded to 10 s budget, QPS 5, at most once per 5 min,
  with a Pods-only fallback if the full relist times out
"""

from __future__ import annotations

import asyncio
import contextlib
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from datetime import datetime, timedelta
from typing import Any

from kubernetes_asyncio import watch
from kubernetes_asyncio.client.exceptions import ApiException

from kuberca.observability.logging import get_logger
from kuberca.observability.metrics import (
    watcher_backoff_seconds,
    watcher_errors_total,
    watcher_events_total,
    watcher_reconnects_total,
    watcher_relistings_total,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_BACKOFF_MIN_S: float = 1.0
_BACKOFF_MAX_S: float = 60.0
_BACKOFF_MULTIPLIER: float = 2.0

_MAX_CONSECUTIVE_FAILURES: int = 3
_RELIST_MIN_INTERVAL_S: float = 300.0  # 5 minutes
_RELIST_BUDGET_S: float = 10.0
_RELIST_QPS: int = 5
_RELIST_TIMEOUT_S: float = 10.0

# 429/500 burst: relist if seen more than this many in 1 minute
_BURST_WINDOW_S: float = 60.0
_BURST_RELIST_THRESHOLD: int = 1


class WatcherError(Exception):
    """Raised when a watcher cannot recover from a terminal error."""


class BaseWatcher(ABC):
    """Async base class for all Kubernetes resource watchers.

    Subclasses implement :meth:`_list_func` (which API function to call)
    and :meth:`_handle_event` (what to do with each watch event dict).

    Lifecycle::

        watcher = MyWatcher(v1_api, cluster_id="prod")
        await watcher.start()
        # ... runs until cancelled or stop() is called
        await watcher.stop()
    """

    def __init__(self, api: Any, cluster_id: str = "", name: str = "base") -> None:
        """Initialise the watcher.

        Args:
            api: A kubernetes_asyncio API instance (e.g. CoreV1Api).
            cluster_id: Cluster identifier forwarded to EventRecord.cluster_id.
            name: Short identifier used in log/metric labels.
        """
        self._api = api
        self._cluster_id = cluster_id
        self._name = name
        self._log = get_logger(f"watcher.{name}")

        self._resource_version: str = ""
        self._running: bool = False
        self._task: asyncio.Task[None] | None = None

        # Failure tracking
        self._consecutive_failures: int = 0
        self._last_relist_at: datetime | None = None

        # 429/500 burst tracking: list of timestamps in the current window
        self._error_burst_times: list[datetime] = []

        # Current back-off delay for the retry loop
        self._backoff_s: float = _BACKOFF_MIN_S

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the watch loop as a background asyncio task."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._watch_loop(), name=f"watcher-{self._name}")
        self._log.info("watcher_started", watcher=self._name)

    async def stop(self) -> None:
        """Signal the watch loop to stop and wait for it to exit cleanly."""
        self._running = False
        if self._task is not None and not self._task.done():
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
        self._log.info("watcher_stopped", watcher=self._name)

    # ------------------------------------------------------------------
    # Abstract interface for subclasses
    # ------------------------------------------------------------------

    @abstractmethod
    def _list_func(self) -> Callable[..., Coroutine[Any, Any, Any]]:
        """Return the API list function used by Watch.stream().

        Example::

            return self._api.list_event_for_all_namespaces
        """

    @abstractmethod
    async def _handle_event(self, event_type: str, obj: Any, raw: dict[str, Any]) -> None:
        """Process a single watch event.

        Args:
            event_type: One of "ADDED", "MODIFIED", "DELETED".
            obj: Deserialized Kubernetes object (e.g. V1Pod).
            raw: The raw dict from the watch stream.
        """

    # ------------------------------------------------------------------
    # Internal watch loop
    # ------------------------------------------------------------------

    async def _watch_loop(self) -> None:
        """Main watch loop; runs until :attr:`_running` is False."""
        while self._running:
            try:
                await self._run_watch()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                await self._handle_loop_exception(exc)

    async def _run_watch(self) -> None:
        """Open one watch stream and iterate until it terminates or raises."""
        list_func = self._list_func()
        kwargs: dict[str, Any] = {
            "allow_watch_bookmarks": True,
        }
        if self._resource_version:
            kwargs["resource_version"] = self._resource_version

        w = watch.Watch()
        try:
            async for raw_event in w.stream(list_func, **kwargs):
                if not self._running:
                    return
                event_type: str = raw_event.get("type", "")
                if event_type == "BOOKMARK":
                    # Update our resourceVersion from bookmark metadata
                    rv = _extract_rv_from_bookmark(raw_event)
                    if rv:
                        self._resource_version = rv
                    continue

                obj = raw_event.get("object")
                raw = raw_event.get("raw_object", {})
                if not isinstance(raw, dict):
                    raw = {}

                # Update resource version after each event
                new_rv = _extract_rv(obj, raw)
                if new_rv:
                    self._resource_version = new_rv

                watcher_events_total.labels(watcher=self._name, event_type=event_type).inc()
                await self._handle_event(event_type, obj, raw)

            # Stream ended without error — treat as transient termination
            self._consecutive_failures += 1
            self._log.debug(
                "watch_stream_ended",
                watcher=self._name,
                consecutive_failures=self._consecutive_failures,
            )
            if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                await self._relist(reason="consecutive_failures")
            else:
                await self._backoff("stream_end")

        except ApiException as exc:
            await self._handle_api_exception(exc)
        finally:
            await w.close()

    async def _handle_api_exception(self, exc: ApiException) -> None:
        """Route an ApiException to the correct recovery path."""
        status = exc.status
        watcher_errors_total.labels(watcher=self._name, status_code=str(status)).inc()

        if status == 410:
            # Gone — resource version too old, must relist
            self._log.warning("watch_gone_410", watcher=self._name)
            watcher_reconnects_total.labels(watcher=self._name, reason="410").inc()
            self._resource_version = ""
            await self._relist(reason="410")

        elif status == 429:
            self._record_burst_error()
            self._log.warning("watch_rate_limited_429", watcher=self._name)
            watcher_reconnects_total.labels(watcher=self._name, reason="429").inc()
            if self._should_relist_on_burst():
                await self._relist(reason="429_burst")
            else:
                await self._backoff("429")

        elif status in (500, 503, 504):
            self._consecutive_failures += 1
            self._record_burst_error()
            self._log.warning(
                "watch_server_error",
                watcher=self._name,
                status=status,
                consecutive_failures=self._consecutive_failures,
            )
            watcher_reconnects_total.labels(watcher=self._name, reason=str(status)).inc()
            if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES or self._should_relist_on_burst():
                await self._relist(reason=f"{status}_burst")
            else:
                await self._backoff(str(status))

        else:
            self._consecutive_failures += 1
            self._log.error(
                "watch_api_error",
                watcher=self._name,
                status=status,
                reason=exc.reason,
                consecutive_failures=self._consecutive_failures,
            )
            if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                await self._relist(reason="api_error_limit")
            else:
                await self._backoff("api_error")

    async def _handle_loop_exception(self, exc: Exception) -> None:
        """Handle unexpected exceptions from the watch loop."""
        self._consecutive_failures += 1
        self._log.error(
            "watch_unexpected_error",
            watcher=self._name,
            error=str(exc),
            consecutive_failures=self._consecutive_failures,
            exc_info=True,
        )
        watcher_reconnects_total.labels(watcher=self._name, reason="unexpected").inc()
        if self._consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
            await self._relist(reason="unexpected_limit")
        else:
            await self._backoff("unexpected")

    # ------------------------------------------------------------------
    # Back-off
    # ------------------------------------------------------------------

    async def _backoff(self, reason: str) -> None:
        """Sleep for the current back-off duration, then increase it."""
        delay = min(self._backoff_s, _BACKOFF_MAX_S)
        self._log.debug("watcher_backoff", watcher=self._name, reason=reason, delay_s=delay)
        watcher_backoff_seconds.labels(watcher=self._name).observe(delay)
        await asyncio.sleep(delay)
        self._backoff_s = min(self._backoff_s * _BACKOFF_MULTIPLIER, _BACKOFF_MAX_S)

    def _reset_backoff(self) -> None:
        """Reset back-off to minimum after a successful relist or run."""
        self._backoff_s = _BACKOFF_MIN_S
        self._consecutive_failures = 0

    # ------------------------------------------------------------------
    # Burst error tracking
    # ------------------------------------------------------------------

    def _record_burst_error(self) -> None:
        """Record a 429/500 error timestamp for burst detection."""
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=_BURST_WINDOW_S)
        self._error_burst_times = [t for t in self._error_burst_times if t >= cutoff]
        self._error_burst_times.append(now)

    def _should_relist_on_burst(self) -> bool:
        """Return True if burst threshold has been exceeded in the window."""
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=_BURST_WINDOW_S)
        recent = [t for t in self._error_burst_times if t >= cutoff]
        return len(recent) > _BURST_RELIST_THRESHOLD

    # ------------------------------------------------------------------
    # Relist
    # ------------------------------------------------------------------

    async def _relist(self, reason: str = "unknown") -> None:
        """Perform a bounded relist to recover a diverged watch stream.

        Enforces:
        - At most once per 5 minutes (unless first relist)
        - 10 s total budget (timeout)
        - QPS 5 across all list pages
        - Falls back to Pods-only relist if full relist times out
        """
        now = datetime.utcnow()

        # Rate-limit: at most once every 5 minutes
        if self._last_relist_at is not None:
            elapsed = (now - self._last_relist_at).total_seconds()
            if elapsed < _RELIST_MIN_INTERVAL_S:
                self._log.debug(
                    "relist_throttled",
                    watcher=self._name,
                    reason=reason,
                    next_allowed_in_s=_RELIST_MIN_INTERVAL_S - elapsed,
                )
                await self._backoff("relist_throttled")
                return

        self._last_relist_at = now
        watcher_relistings_total.labels(watcher=self._name).inc()
        self._log.info("relist_start", watcher=self._name, reason=reason)

        try:
            async with asyncio.timeout(_RELIST_BUDGET_S):
                await self._do_relist()
        except TimeoutError:
            self._log.warning("relist_timeout", watcher=self._name, reason=reason)
            from kuberca.observability.metrics import cache_relist_timeout_total

            cache_relist_timeout_total.inc()
            await self._relist_pods_fallback()
        except Exception as exc:
            self._log.error(
                "relist_failed",
                watcher=self._name,
                reason=reason,
                error=str(exc),
                exc_info=True,
            )

        self._reset_backoff()

    async def _do_relist(self) -> None:
        """Perform the actual relist: call the list function without a
        resourceVersion so we get a fresh snapshot, then update our stored
        resourceVersion from the list response metadata.

        The QPS cap (5 req/s) is enforced by spacing calls 200 ms apart.
        """
        self._resource_version = ""
        qps_delay = 1.0 / _RELIST_QPS  # 200 ms between calls
        list_func = self._list_func()

        # We do a single list call (no pagination needed for relist) to obtain
        # a fresh resource_version that the subsequent Watch can resume from.
        await asyncio.sleep(qps_delay)
        result = await list_func(_preload_content=True, watch=False)

        rv = ""
        if hasattr(result, "metadata") and result.metadata is not None:
            rv = getattr(result.metadata, "resource_version", "") or ""

        if rv:
            self._resource_version = rv
            self._log.info("relist_complete", watcher=self._name, resource_version=rv)
        else:
            self._log.warning("relist_no_rv", watcher=self._name)

    async def _relist_pods_fallback(self) -> None:
        """Pods-only fallback relist when the full relist times out.

        Calls list_pod_for_all_namespaces to obtain a fresh resourceVersion,
        so the stream can resume from a known-good point.
        """
        from kuberca.observability.metrics import cache_relist_fallback_total

        self._log.warning("relist_fallback_pods_only", watcher=self._name)
        cache_relist_fallback_total.inc()
        try:
            async with asyncio.timeout(_RELIST_BUDGET_S):
                pod_list = await self._api.list_pod_for_all_namespaces(_preload_content=True, watch=False)
                rv = ""
                if hasattr(pod_list, "metadata") and pod_list.metadata is not None:
                    rv = getattr(pod_list.metadata, "resource_version", "") or ""
                if rv:
                    self._resource_version = rv
                    self._log.info("relist_fallback_complete", watcher=self._name, resource_version=rv)
        except Exception as exc:
            self._log.error(
                "relist_fallback_failed",
                watcher=self._name,
                error=str(exc),
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_rv(obj: Any, raw: dict[str, Any]) -> str:
    """Extract resourceVersion from a watch event's deserialized object or raw dict."""
    if obj is not None and hasattr(obj, "metadata") and obj.metadata is not None:
        rv = getattr(obj.metadata, "resource_version", None)
        if rv:
            return str(rv)
    # Fall back to raw dict
    metadata = raw.get("metadata")
    if isinstance(metadata, dict):
        rv = metadata.get("resourceVersion", "")
        if rv:
            return str(rv)
    return ""


def _extract_rv_from_bookmark(raw_event: dict[str, Any]) -> str:
    """Extract resourceVersion from a BOOKMARK event."""
    raw_obj = raw_event.get("raw_object", {})
    if not isinstance(raw_obj, dict):
        return ""
    metadata = raw_obj.get("metadata", {})
    if not isinstance(metadata, dict):
        return ""
    return str(metadata.get("resourceVersion", ""))
