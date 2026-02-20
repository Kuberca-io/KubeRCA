"""Tests for kuberca.analyst.queue — WorkQueue priority and drop policy."""

from __future__ import annotations

import asyncio

import pytest

from kuberca.analyst.queue import Priority, QueueFullError, WorkQueue
from kuberca.models.resources import CacheReadiness


class TestWorkQueueSubmit:
    @pytest.mark.asyncio
    async def test_submit_returns_future(self) -> None:
        results: list[str] = []

        async def analyze(resource: str, time_window: str) -> str:
            results.append(resource)
            return f"result:{resource}"

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            fut = queue.submit("pod/default/my-pod", "2h", Priority.INTERACTIVE)
            assert isinstance(fut, asyncio.Future)
            result = await asyncio.wait_for(fut, timeout=5.0)
            assert result == "result:pod/default/my-pod"
        finally:
            await queue.stop()

    @pytest.mark.asyncio
    async def test_duplicate_submit_returns_same_future(self) -> None:
        event = asyncio.Event()

        async def analyze(resource: str, time_window: str) -> str:
            await event.wait()
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            fut1 = queue.submit("pod/default/my-pod", "2h", Priority.INTERACTIVE)
            fut2 = queue.submit("pod/default/my-pod", "2h", Priority.INTERACTIVE)
            assert fut1 is fut2
        finally:
            event.set()
            await queue.stop()

    @pytest.mark.asyncio
    async def test_background_rejected_when_queue_over_80_percent(self) -> None:
        blocks: list[asyncio.Event] = [asyncio.Event() for _ in range(90)]

        call_count = 0

        async def analyze(resource: str, time_window: str) -> str:
            nonlocal call_count
            idx = call_count
            call_count += 1
            if idx < len(blocks):
                await blocks[idx].wait()
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        # Manually set the internal queue to simulate high depth
        # We override _queue to a mock that reports high qsize
        await queue.start()
        try:
            # Fill queue to 85 items manually by patching qsize
            import unittest.mock as mock

            with (
                mock.patch.object(queue._queue, "qsize", return_value=85),
                mock.patch.object(queue._queue, "full", return_value=False),
                pytest.raises(QueueFullError) as exc_info,
            ):
                queue.submit("pod/default/bg-pod", "2h", Priority.BACKGROUND)
            assert exc_info.value.priority == Priority.BACKGROUND
        finally:
            for b in blocks:
                b.set()
            await queue.stop()

    @pytest.mark.asyncio
    async def test_full_queue_rejects_all_priorities(self) -> None:
        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            import unittest.mock as mock

            with mock.patch.object(queue._queue, "full", return_value=True), pytest.raises(QueueFullError):
                queue.submit("pod/default/my-pod", "2h", Priority.INTERACTIVE)
        finally:
            await queue.stop()

    @pytest.mark.asyncio
    async def test_analysis_exception_propagates_to_future(self) -> None:
        async def analyze(resource: str, time_window: str) -> str:
            raise ValueError(f"Analysis failed for {resource}")

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            fut = queue.submit("pod/default/bad", "2h", Priority.INTERACTIVE)
            with pytest.raises(ValueError, match="Analysis failed"):
                await asyncio.wait_for(fut, timeout=5.0)
        finally:
            await queue.stop()


class TestRateLimiting:
    @pytest.mark.asyncio
    async def test_degraded_cache_reduces_rate_to_5(self) -> None:
        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(
            analyze_fn=analyze,
            cache_readiness_fn=lambda: CacheReadiness.DEGRADED,
        )
        await queue.start()
        try:
            # Submit 5 requests — should succeed
            futs = []
            for i in range(5):
                fut = queue.submit(f"pod/default/pod-{i}", "2h", Priority.INTERACTIVE)
                futs.append(fut)

            # 6th request should be rate-limited
            with pytest.raises(QueueFullError):
                queue.submit("pod/default/pod-6", "2h", Priority.INTERACTIVE)

            # Clean up
            for fut in futs:
                await asyncio.wait_for(fut, timeout=5.0)
        finally:
            await queue.stop()

    @pytest.mark.asyncio
    async def test_notify_429_halves_rate(self) -> None:
        async def analyze(resource: str, time_window: str) -> str:
            return "done"

        queue = WorkQueue(analyze_fn=analyze)
        await queue.start()
        try:
            queue.notify_upstream_429()
            # Effective rate should now be 10/min (default 20 halved)
            effective = queue._compute_effective_rate(__import__("time").monotonic())
            assert effective == pytest.approx(10.0, abs=0.1)
        finally:
            await queue.stop()


class TestPriorityEnum:
    def test_interactive_higher_priority_than_background(self) -> None:
        assert Priority.INTERACTIVE < Priority.BACKGROUND

    def test_system_between_interactive_and_background(self) -> None:
        assert Priority.INTERACTIVE < Priority.SYSTEM < Priority.BACKGROUND
