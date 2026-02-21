# Design: Invariant Protection, Confidence Under Penalties, Stress Intersections

**Date**: 2026-02-21
**Branch**: feat/mvp-review-test-coverage
**Trigger**: External review identified three areas for hardening after 97% coverage milestone

## Context

KubeRCA has 1536 tests at 97% coverage with determinism verification, bounded memory, and cache transition instrumentation. The next phase addresses interaction complexity and operator trust:

1. **Invariant protection** — codify system invariants so future contributors can't accidentally break the trust layer
2. **Confidence under cache penalties** — prove the same incident degrades gracefully across cache states without rule flipping
3. **Stress test failure intersections** — test real failure mode combinations, not isolated scenarios

## Approach

Parallel implementation of all three tracks on the current branch.

---

## Track 1: Invariant Protection

### 1.1 INVARIANTS.md

Living document at project root listing every system invariant by subsystem.

**Confidence Scoring Invariants:**
- Confidence is always in `[0.0, 0.95]` — never 1.0
- `compute_confidence()` is a pure function — same inputs always produce same output
- LLM confidence cap is 0.70, rule confidence cap is 0.95
- High band (>=0.85) short-circuits — no further rules evaluated
- Medium band (0.70-0.84) restricts to shared-dependency rules only
- Rule evaluation order is deterministic: `sorted(rules, key=(priority, rule_id))`
- Competing-deps mode only activates on medium-confidence matches, not low

**Cache State Invariants:**
- Valid transitions: WARMING→PARTIALLY_READY, WARMING→DEGRADED, PARTIALLY_READY→READY, PARTIALLY_READY→DEGRADED, READY→DEGRADED, DEGRADED→READY, DEGRADED→PARTIALLY_READY
- Invalid transitions: READY→WARMING, DEGRADED→WARMING (never go back to WARMING once populated)
- DEGRADED suppresses LLM — always returns INCONCLUSIVE
- PARTIALLY_READY applies -0.15 confidence penalty
- DEGRADED applies -0.10 confidence penalty
- Penalties applied in coordinator AFTER rule evaluation (rule selection unaffected by cache state)
- State transitions always emit `cache_state_transitions_total` metric
- No-op recomputations (same state) do NOT increment transition counter
- PARTIALLY_READY staleness timeout: 10 minutes → DEGRADED

**Analysis Invariants:**
- Rule engine never mutates shared state between evaluations
- Correlate timeout is 500ms hard wall-clock — exceeded rules are skipped
- Max 50 objects queried per correlate call
- LLM quality check is deterministic (no LLM call in quality check)
- LLM retry is capped at 2 attempts
- Quality check failure caps LLM confidence at 0.35
- Work queue deduplicates by canonical_resource

**Divergence Trigger Invariants:**
- Miss rate > 5% over 5-minute rolling window → DEGRADED
- Reconnect failures > 3 → DEGRADED
- Relist timeouts > 2 in any 10-minute window → DEGRADED
- 429 burst errors lasting > 1 minute → DEGRADED

### 1.2 Runtime Invariant Checks

Add logging-only checks at key boundaries in production code (never crash):

| Location | Check | Action on violation |
|----------|-------|-------------------|
| `compute_confidence()` exit | score in [0.0, 0.95] | `logger.error("invariant_violated", ...)` |
| `_recompute_readiness()` | transition in valid set | `logger.error("invariant_violated", ...)` + metric |
| `_rule_result_to_rca_response()` | final confidence in [0.0, 0.95] | `logger.error("invariant_violated", ...)` |
| `RuleEngine.evaluate()` | rules sorted by (priority, rule_id) | `logger.error("invariant_violated", ...)` |

New metric: `kuberca_invariant_violations_total` with labels `invariant_name`.

### 1.3 Test Suite: `tests/unit/test_invariants.py`

One test per documented invariant. Hard-fail assertions. ~30-40 tests.

---

## Track 2: Confidence Under Cache Penalties

### File: `tests/unit/test_confidence_under_penalties.py`

**Test matrix**: same incident evaluated across all cache states.

| # | Scenario | Assertion |
|---|----------|-----------|
| 1 | Rule winner stability | R01 OOMKilled wins at READY, same rule wins at PARTIALLY_READY (lower confidence) |
| 2 | No rule flipping | Medium-band result at READY doesn't flip to different rule at PARTIALLY_READY |
| 3 | Graceful degradation curve | confidence: READY > PARTIALLY_READY > DEGRADED (0.0) |
| 4 | Boundary: 0.70 minus penalty | Rule at 0.70 → 0.55 after PARTIALLY_READY penalty. Still same rule. |
| 5 | Short-circuit preservation | High-band (0.85+) minus penalty (0.70) — no short-circuit at coordinator level |
| 6 | Floor at 0.0 | Low-confidence (0.10) minus penalty doesn't go negative |
| 7 | DEGRADED always INCONCLUSIVE | Regardless of rule confidence, DEGRADED returns INCONCLUSIVE with confidence 0.0 |
| 8 | Penalty applied post-selection | Rule engine runs on raw scores; penalty only affects reported confidence |
| 9 | 100x replay stability | Each scenario replayed 100 times — same result every time |
| 10 | PARTIALLY_READY warning present | Response includes cache warning when PARTIALLY_READY |
| 11 | DEGRADED suppression message | Response includes LLM suppression explanation |

**Key architectural insight to verify**: Penalties are applied in the coordinator AFTER rule evaluation. Rule selection is cache-state-independent. This is an invariant.

---

## Track 3: Stress Test Failure Intersections

### Added to: `tests/integration/test_load_stress.py`

8 new scenarios, all marked `@pytest.mark.stress`:

| # | Scenario | What it proves |
|---|----------|---------------|
| 1 | Burst + Cache Oscillation | 500 events, cache oscillates READY→DEGRADED→READY every 50 events. No diagnosis flip-flops. |
| 2 | Readiness State Oscillation | Rapid WARMING→PARTIALLY_READY→READY→DEGRADED→READY transitions (10ms). Transition counter correct. |
| 3 | Ledger Trim During Analysis | Fill to soft-trim, trigger trim + 100 concurrent evaluations. No corrupted diffs. |
| 4 | LLM Suppression Flip | READY→DEGRADED mid-analysis. In-flight completes, next request INCONCLUSIVE. |
| 5 | Concurrent Penalty Application | 100 simultaneous coordinator calls, mixed READY/PARTIALLY_READY. No cross-contamination. |
| 6 | API 410/429 Error Injection | Simulate watch errors. Cache degrades, divergence counter correct, recovery works. |
| 7 | Memory Pressure Under GC | Fill to failsafe-evict (200MB), evict under concurrent load. Memory drops, no crashes. |
| 8 | Scout + Rule Engine Race | Scout enqueues while engine evaluates same resource. No duplicates, no deadlocks. |

---

## Non-Goals

- No changes to production behavior (only observability + tests + docs)
- No E2E tests against real clusters (that's the next phase)
- No changes to confidence formulas or cache state machine
- No LLM integration tests (mock-only)

## Success Criteria

- All new tests pass
- Existing 1536 tests still pass
- `uv run ruff check .` clean
- `uv run mypy kuberca/` clean
- INVARIANTS.md committed and reviewable
- Runtime invariant checks emit metrics but never crash
