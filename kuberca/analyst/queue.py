"""Bounded async work queue for RCA analysis requests.

Implements deduplication, priority-based drop policy, and adaptive rate
limiting that responds to system state (cache readiness and CPU load).
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING

import structlog

from kuberca.models.resources import CacheReadiness
from kuberca.observability.metrics import (
    analysis_dropped_total,
    analysis_queue_depth,
    analysis_rate_reduction_total,
)

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

_logger = structlog.get_logger(component="work_queue")

_QUEUE_MAX_SIZE: int = 100
_DEFAULT_RATE_PER_MINUTE: int = 20
_DEGRADED_RATE_PER_MINUTE: int = 5
_RATE_HALVE_DURATION_S: float = 300.0  # 5 minutes
_QUEUE_BACKGROUND_REJECT_THRESHOLD: int = 80  # >80% full = reject background
_CPU_REDUCTION_THRESHOLD: float = 80.0  # >80% CPU = 50% rate reduction
_CPU_POLL_INTERVAL_S: float = 5.0


class Priority(IntEnum):
    """Work item priority levels. Lower integer = higher priority."""

    INTERACTIVE = 1
    SYSTEM = 2
    BACKGROUND = 3


class QueueFullError(Exception):
    """Raised when a work item must be rejected under the drop policy.

    Maps to HTTP 429 for interactive requests.
    """

    def __init__(self, message: str, priority: Priority) -> None:
        super().__init__(message)
        self.priority = priority


@dataclass
class _WorkItem:
    """Internal representation of a queued analysis request."""

    resource: str
    time_window: str
    priority: Priority
    future: asyncio.Future  # type: ignore[type-arg]
    enqueued_at: float = field(default_factory=time.monotonic)

    @property
    def dedup_key(self) -> tuple[str, str]:
        """Deduplication key: (namespace, workload) extracted from resource string.

        Falls back to the full resource string if parsing fails.
        """
        parts = self.resource.split("/")
        if len(parts) == 3:
            _kind, namespace, name = parts
            return (namespace, name)
        return (self.resource, "")


class WorkQueue:
    """Bounded, deduplicated, priority-aware async work queue.

    Capacity: 100 items maximum.
    Deduplication: by (namespace, workload) — duplicate system requests are
    silently dropped; duplicate interactive requests return the existing future.
    Workers: one per CPU core (os.cpu_count()).

    Drop policy under pressure:
      - Queue >80% full: reject BACKGROUND items (drop silently).
      - Duplicate SYSTEM request: drop new, return existing future.
      - Queue 100% full (all priorities): reject INTERACTIVE with 429.
    """

    def __init__(
        self,
        analyze_fn: Callable[[str, str], Awaitable[object]],
        cache_readiness_fn: Callable[[], CacheReadiness] | None = None,
        cpu_percent_fn: Callable[[], float] | None = None,
    ) -> None:
        """Create a WorkQueue.

        analyze_fn: async callable (resource, time_window) -> RCAResponse.
        cache_readiness_fn: optional callable returning current CacheReadiness.
        cpu_percent_fn: optional callable returning current CPU utilization (0-100).
        """
        self._analyze_fn = analyze_fn
        self._cache_readiness_fn = cache_readiness_fn
        self._cpu_percent_fn = cpu_percent_fn

        # Initialized in start() — not usable before start() is called
        self._queue: asyncio.PriorityQueue  # type: ignore[type-arg]
        self._workers: list[asyncio.Task] = []  # type: ignore[type-arg]
        self._dedup: dict[tuple[str, str], _WorkItem] = {}
        self._running = False

        # Rate limiting state
        self._effective_rate: float = float(_DEFAULT_RATE_PER_MINUTE)
        self._rate_halved_until: float = 0.0  # monotonic timestamp
        self._request_timestamps: list[float] = []  # sliding window for rate enforcement

    async def start(self) -> None:
        """Start worker tasks. Must be called before submit()."""
        num_workers = max(1, os.cpu_count() or 1)
        self._queue = asyncio.PriorityQueue(maxsize=_QUEUE_MAX_SIZE)
        self._running = True
        self._workers = [
            asyncio.create_task(self._worker(i), name=f"work_queue_worker_{i}") for i in range(num_workers)
        ]
        _logger.info("work_queue_started", workers=num_workers)

    async def stop(self) -> None:
        """Stop all worker tasks gracefully. Safe to call before start()."""
        self._running = False
        if not self._workers:
            return
        for _ in self._workers:
            await self._queue.put((_SENTINEL_PRIORITY, time.monotonic(), None))
        for task in self._workers:
            await task
        self._workers = []
        _logger.info("work_queue_stopped")

    def submit(
        self,
        resource: str,
        time_window: str = "2h",
        priority: Priority = Priority.INTERACTIVE,
    ) -> asyncio.Future:  # type: ignore[type-arg]
        """Submit a resource for analysis.

        Returns a Future that resolves to the RCAResponse.
        Raises QueueFullError if the item must be rejected per the drop policy.
        """
        loop = asyncio.get_event_loop()
        item = _WorkItem(
            resource=resource,
            time_window=time_window,
            priority=priority,
            future=loop.create_future(),
        )

        # Rate limiting check
        self._enforce_rate_limit(priority)

        # Deduplication check
        existing = self._dedup.get(item.dedup_key)
        if existing is not None:
            if priority == Priority.SYSTEM:
                # Drop duplicate system request silently
                _logger.debug(
                    "duplicate_system_request_dropped",
                    resource=resource,
                )
                analysis_dropped_total.labels(priority=priority.name).inc()
                return existing.future
            # For interactive and background: return existing future (dedup)
            return existing.future

        # Drop policy under pressure
        current_depth = self._queue.qsize()
        fill_ratio = current_depth / _QUEUE_MAX_SIZE

        if fill_ratio > (_QUEUE_BACKGROUND_REJECT_THRESHOLD / _QUEUE_MAX_SIZE) and priority == Priority.BACKGROUND:
            analysis_dropped_total.labels(priority=Priority.BACKGROUND.name).inc()
            _logger.debug("background_request_dropped_queue_pressure", depth=current_depth)
            raise QueueFullError(
                f"Queue >80% full ({current_depth}/{_QUEUE_MAX_SIZE}). Background requests are not accepted.",
                priority=priority,
            )

        if self._queue.full():
            # All priorities rejected at 100% capacity
            analysis_dropped_total.labels(priority=priority.name).inc()
            _logger.warning("queue_full_request_rejected", resource=resource, priority=priority.name)
            raise QueueFullError(
                f"Analysis queue is full ({_QUEUE_MAX_SIZE} items). Please retry later.",
                priority=priority,
            )

        # Enqueue: (priority_int, timestamp, item) — timestamp breaks ties for same priority
        self._dedup[item.dedup_key] = item
        self._queue.put_nowait((int(priority), time.monotonic(), item))
        analysis_queue_depth.set(self._queue.qsize())

        _logger.debug(
            "request_enqueued",
            resource=resource,
            priority=priority.name,
            queue_depth=self._queue.qsize(),
        )
        return item.future

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        """Worker coroutine: pull items from the queue and execute them."""
        _logger.debug("worker_started", worker_id=worker_id)
        while self._running:
            try:
                entry = await self._queue.get()
            except asyncio.CancelledError:
                break

            priority_int, _ts, work_item = entry

            # Sentinel item signals shutdown
            if work_item is None:
                self._queue.task_done()
                break

            analysis_queue_depth.set(self._queue.qsize())
            self._dedup.pop(work_item.dedup_key, None)

            if work_item.future.cancelled():
                self._queue.task_done()
                continue

            try:
                result = await self._analyze_fn(work_item.resource, work_item.time_window)
                if not work_item.future.done():
                    work_item.future.set_result(result)
            except Exception as exc:
                _logger.error(
                    "worker_analysis_error",
                    worker_id=worker_id,
                    resource=work_item.resource,
                    error=str(exc),
                )
                if not work_item.future.done():
                    work_item.future.set_exception(exc)
            finally:
                self._queue.task_done()

        _logger.debug("worker_stopped", worker_id=worker_id)

    def _enforce_rate_limit(self, priority: Priority) -> None:
        """Check and enforce the sliding-window rate limit.

        Raises QueueFullError (429) if the rate limit is exceeded.
        Adapts the rate based on cache readiness and CPU utilization.
        """
        now = time.monotonic()

        # Recompute effective rate
        effective_rate = self._compute_effective_rate(now)

        # Sliding 1-minute window
        window_start = now - 60.0
        self._request_timestamps = [t for t in self._request_timestamps if t >= window_start]

        if len(self._request_timestamps) >= effective_rate:
            analysis_dropped_total.labels(priority=priority.name).inc()
            raise QueueFullError(
                f"Rate limit exceeded: {int(effective_rate)} requests/min. Please retry later.",
                priority=priority,
            )

        self._request_timestamps.append(now)

    def _compute_effective_rate(self, now: float) -> float:
        """Compute the current effective rate limit per minute.

        Factors:
        - DEGRADED cache: 5/min
        - 429 response triggered halving: half for 5 minutes
        - CPU >80%: 50% reduction
        - Default: 20/min
        """
        # Cache state takes highest precedence
        if self._cache_readiness_fn is not None:
            try:
                readiness = self._cache_readiness_fn()
                if readiness == CacheReadiness.DEGRADED:
                    return float(_DEGRADED_RATE_PER_MINUTE)
            except Exception as exc:
                _logger.warning("rate_limit_cache_readiness_error", error=str(exc))

        base = float(_DEFAULT_RATE_PER_MINUTE)

        # Temporary halving after 429
        if now < self._rate_halved_until:
            base = base / 2.0
            # No further reduction stacking — exit early
            return max(1.0, base)

        # CPU pressure reduction
        if self._cpu_percent_fn is not None:
            try:
                cpu = self._cpu_percent_fn()
                if cpu > _CPU_REDUCTION_THRESHOLD:
                    base = base * 0.5
                    analysis_rate_reduction_total.inc()
            except Exception as exc:
                _logger.warning("rate_limit_cpu_check_error", error=str(exc))

        return max(1.0, base)

    def notify_upstream_429(self) -> None:
        """Called by the HTTP layer when a 429 is received from Ollama or the API server.

        Halves the effective rate limit for 5 minutes.
        """
        self._rate_halved_until = time.monotonic() + _RATE_HALVE_DURATION_S
        analysis_rate_reduction_total.inc()
        _logger.warning(
            "rate_limit_halved",
            duration_s=_RATE_HALVE_DURATION_S,
        )


# Sentinel priority integer — higher than any real priority, used for shutdown signals
_SENTINEL_PRIORITY: int = 999
