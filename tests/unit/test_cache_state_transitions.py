"""Unit tests for the cache_state_transitions_total Prometheus Counter.

Each test verifies that ``_recompute_readiness()`` increments the counter with
the correct ``from_state``/``to_state`` labels exactly when the readiness state
changes, and emits nothing when the state is unchanged.

A fresh ``Counter`` instance is created per test class via a pytest fixture so
that counter values never bleed across tests from the global Prometheus registry.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from prometheus_client import Counter

from kuberca.cache.resource_cache import ResourceCache
from kuberca.models.resources import CacheReadiness

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_fresh_counter() -> Counter:
    """Create a new, unregistered Counter with the same label schema."""
    return Counter(
        "kuberca_cache_state_transitions_test",
        "Test counter for state transitions",
        ["from_state", "to_state"],
        registry=None,  # do not register — avoids duplicate-metric errors
    )


def _counter_value(counter: Counter, from_state: str, to_state: str) -> float:
    """Return the current value for a specific label combination."""
    return counter.labels(from_state=from_state, to_state=to_state)._value.get()


def _make_ready(cache: ResourceCache, kinds: set[str]) -> None:
    """Configure *cache* so that all *kinds* are registered and fully ready."""
    cache._all_kinds = set(kinds)
    cache._ready_kinds = set(kinds)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestWarmingToReady:
    """WARMING -> READY: all registered kinds become ready."""

    def test_counter_incremented_on_warming_to_ready(self) -> None:
        counter = _make_fresh_counter()
        cache = ResourceCache()

        # Start in WARMING (default), then mark all kinds ready
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.READY
        assert _counter_value(counter, "warming", "ready") == 1.0

    def test_labels_are_exactly_from_warming_to_ready(self) -> None:
        """Verifies the ``from_state`` and ``to_state`` label values are the enum .value strings."""
        counter = _make_fresh_counter()
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Node"}
        cache._ready_kinds = {"Pod", "Node"}

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        # "warming" and "ready" are the StrEnum .value strings
        assert _counter_value(counter, "warming", "ready") == 1.0
        # No other transition must have fired
        assert _counter_value(counter, "ready", "warming") == 0.0
        assert _counter_value(counter, "warming", "degraded") == 0.0


class TestReadyToDegraded:
    """READY -> DEGRADED: divergence is detected while cache is fully ready."""

    def test_counter_incremented_on_ready_to_degraded(self) -> None:
        counter = _make_fresh_counter()
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})

        # Ensure readiness starts at READY before the transition
        cache._readiness = CacheReadiness.READY

        with (
            patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter),
            patch.object(cache, "_is_diverged", return_value=True),
        ):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.DEGRADED
        assert _counter_value(counter, "ready", "degraded") == 1.0

    def test_counter_not_incremented_when_already_degraded(self) -> None:
        """If the cache is already DEGRADED and divergence persists, no new increment."""
        counter = _make_fresh_counter()
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})
        cache._readiness = CacheReadiness.DEGRADED

        with (
            patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter),
            patch.object(cache, "_is_diverged", return_value=True),
        ):
            cache._recompute_readiness()

        # State unchanged: DEGRADED -> DEGRADED, counter must stay at zero
        assert _counter_value(counter, "degraded", "degraded") == 0.0
        assert _counter_value(counter, "ready", "degraded") == 0.0


class TestPartiallyReadyToDegraded:
    """PARTIALLY_READY -> DEGRADED: staleness timeout exceeded."""

    def test_counter_incremented_on_staleness_timeout(self) -> None:
        counter = _make_fresh_counter()
        cache = ResourceCache()

        # Some kinds registered, only one ready -> PARTIALLY_READY
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = {"Pod"}
        # Back-date partially_ready_since by 11 minutes to exceed the 10-min timeout
        cache._partially_ready_since = datetime.now(tz=UTC) - timedelta(minutes=11)
        cache._readiness = CacheReadiness.PARTIALLY_READY

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.DEGRADED
        assert _counter_value(counter, "partially_ready", "degraded") == 1.0

    def test_counter_not_incremented_within_staleness_window(self) -> None:
        """State stays PARTIALLY_READY when staleness < 10 min: no counter increment."""
        counter = _make_fresh_counter()
        cache = ResourceCache()
        cache._all_kinds = {"Pod", "Deployment"}
        cache._ready_kinds = {"Pod"}
        cache._partially_ready_since = datetime.now(tz=UTC) - timedelta(minutes=5)
        cache._readiness = CacheReadiness.PARTIALLY_READY

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.PARTIALLY_READY
        assert _counter_value(counter, "partially_ready", "degraded") == 0.0
        assert _counter_value(counter, "partially_ready", "partially_ready") == 0.0


class TestDegradedToReady:
    """DEGRADED -> READY: divergence clears and all kinds are ready."""

    def test_counter_incremented_on_degraded_to_ready(self) -> None:
        counter = _make_fresh_counter()
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})
        cache._readiness = CacheReadiness.DEGRADED

        with (
            patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter),
            patch.object(cache, "_is_diverged", return_value=False),
        ):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.READY
        assert _counter_value(counter, "degraded", "ready") == 1.0


class TestNoTransitionWhenStateUnchanged:
    """Counter must NOT be incremented when state remains the same."""

    def test_ready_stays_ready_no_increment(self) -> None:
        counter = _make_fresh_counter()
        cache = ResourceCache()
        _make_ready(cache, {"Pod"})
        cache._readiness = CacheReadiness.READY

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.READY
        # No transition occurred; all label combinations must be zero
        for from_s in CacheReadiness:
            for to_s in CacheReadiness:
                assert _counter_value(counter, from_s.value, to_s.value) == 0.0

    def test_warming_stays_warming_no_increment(self) -> None:
        counter = _make_fresh_counter()
        cache = ResourceCache()
        # _all_kinds is empty -> state stays WARMING
        cache._readiness = CacheReadiness.WARMING

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            cache._recompute_readiness()

        assert cache._readiness == CacheReadiness.WARMING
        for from_s in CacheReadiness:
            for to_s in CacheReadiness:
                assert _counter_value(counter, from_s.value, to_s.value) == 0.0


class TestMultipleTransitions:
    """Counter accumulates correctly across multiple back-to-back transitions."""

    def test_counter_accumulates_across_transitions(self) -> None:
        """Simulate WARMING -> READY -> DEGRADED -> READY: each transition fires once."""
        counter = _make_fresh_counter()
        cache = ResourceCache()
        cache._all_kinds = {"Pod"}
        cache._ready_kinds = {"Pod"}
        # Starting state is WARMING (default)

        with patch("kuberca.cache.resource_cache.cache_state_transitions_total", counter):
            # WARMING -> READY
            with patch.object(cache, "_is_diverged", return_value=False):
                cache._recompute_readiness()
            assert cache._readiness == CacheReadiness.READY
            assert _counter_value(counter, "warming", "ready") == 1.0

            # READY -> DEGRADED
            with patch.object(cache, "_is_diverged", return_value=True):
                cache._recompute_readiness()
            assert cache._readiness == CacheReadiness.DEGRADED
            assert _counter_value(counter, "ready", "degraded") == 1.0

            # DEGRADED -> READY
            with patch.object(cache, "_is_diverged", return_value=False):
                cache._recompute_readiness()
            assert cache._readiness == CacheReadiness.READY
            assert _counter_value(counter, "degraded", "ready") == 1.0

            # READY stable (no change)
            with patch.object(cache, "_is_diverged", return_value=False):
                cache._recompute_readiness()
            assert _counter_value(counter, "ready", "ready") == 0.0

        # Totals: three distinct transitions, each fired exactly once
        assert _counter_value(counter, "warming", "ready") == 1.0
        assert _counter_value(counter, "ready", "degraded") == 1.0
        assert _counter_value(counter, "degraded", "ready") == 1.0
