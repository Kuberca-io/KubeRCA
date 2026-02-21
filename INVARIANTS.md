# KubeRCA System Invariants

This document catalogues every invariant that KubeRCA relies on for correct
behaviour.  Each invariant has a unique ID, a plain-English statement, an
enforcement mechanism, and the location of its check.

The `kuberca_invariant_violations_total` Prometheus counter is incremented
whenever a runtime invariant check detects a violation.  In a healthy system
this counter is always zero.

---

## Confidence Scoring Invariants

| ID | Statement | Enforcement | Location |
|----|-----------|-------------|----------|
| INV-C01 | `compute_confidence()` always returns a value in [0.0, 0.95]. | Runtime check + unit test | `kuberca/rules/confidence.py`, `tests/unit/test_invariants.py::TestConfidenceScoreRange` |
| INV-C02 | After cache-state penalties, the final confidence in `_rule_result_to_rca_response()` remains in [0.0, 0.95]. | Runtime check + unit test | `kuberca/analyst/coordinator.py`, `tests/unit/test_invariants.py::TestCoordinatorConfidenceRange` |
| INV-C03 | Rules are always evaluated in ascending `(priority, rule_id)` order. | Runtime check + unit test | `kuberca/rules/base.py::RuleEngine.evaluate()`, `tests/unit/test_invariants.py::TestRuleEngineEvaluationOrder` |
| INV-C04 | `compute_confidence()` is a pure function: identical inputs always produce the identical output. | Unit test (1000 repetitions) | `tests/unit/test_invariants.py::TestConfidencePureFunctionInvariant` |
| INV-C05 | LLM system-computed confidence is hard-capped at 0.70. | Code design (`min(..., 0.70)`) + unit test | `kuberca/llm/analyzer.py::_compute_confidence()`, `tests/unit/test_invariants.py::TestLLMConfidenceCapInvariant` |
| INV-C06 | High-band (>= 0.85) short-circuits: the first rule to reach this band is returned immediately. | Code design + unit test | `kuberca/rules/base.py::RuleEngine.evaluate()`, `tests/unit/test_invariants.py::TestFourBandInvariants::test_high_band_short_circuits` |
| INV-C07 | Medium-band (0.70-0.84) enters competing-deps-only mode: only rules sharing at least one `resource_dependency` with the matched rule continue evaluation. | Code design + unit test | `kuberca/rules/base.py::RuleEngine.evaluate()`, `tests/unit/test_invariants.py::TestFourBandInvariants::test_medium_restricts_shared_deps` |
| INV-C08 | Low-band (< 0.70) evaluates all remaining rules without restriction. | Code design + unit test | `kuberca/rules/base.py::RuleEngine.evaluate()`, `tests/unit/test_invariants.py::TestFourBandInvariants::test_low_evaluates_all` |
| INV-C09 | Competing-deps-only mode is activated exclusively by a medium-band match; a low-band match never activates it. | Code design + unit test | `kuberca/rules/base.py::RuleEngine.evaluate()`, `tests/unit/test_invariants.py::TestCompetingDepsOnlyMediumBand` |

## Cache State Invariants

| ID | Statement | Enforcement | Location |
|----|-----------|-------------|----------|
| INV-CS01 | Cache readiness transitions are restricted to the set: {WARMING->PARTIALLY_READY, WARMING->DEGRADED, WARMING->READY, PARTIALLY_READY->READY, PARTIALLY_READY->DEGRADED, READY->DEGRADED, DEGRADED->READY, DEGRADED->PARTIALLY_READY}. | Runtime check + unit test | `kuberca/cache/resource_cache.py::_recompute_readiness()`, `tests/unit/test_invariants.py::TestCacheStateTransitions` |
| INV-CS02 | A no-op readiness recomputation (state unchanged) does not increment the transition counter. | Unit test | `tests/unit/test_invariants.py::TestCacheStateTransitions::test_noop_no_counter` |

## Analysis Pipeline Invariants

| ID | Statement | Enforcement | Location |
|----|-----------|-------------|----------|
| INV-A01 | `correlate()` wall-clock timeout is exactly 500 ms. | Constant (`_CORRELATE_TIMEOUT_MS = 500.0`) + unit test | `kuberca/rules/base.py`, `tests/unit/test_invariants.py::TestAnalysisInvariants::test_correlate_timeout_500ms` |
| INV-A02 | Maximum cached objects queried per `correlate()` is 50. | Constant (`_MAX_OBJECTS = 50`) + unit test | `kuberca/rules/base.py`, `tests/unit/test_invariants.py::TestAnalysisInvariants::test_max_objects_50` |
| INV-A03 | LLM quality-check retry cap is 2 attempts. | Code design (`max_quality_retries = 2`) + unit test | `kuberca/analyst/coordinator.py::AnalystCoordinator.analyze()`, `tests/unit/test_invariants.py::TestAnalysisInvariants::test_llm_retry_cap_2` |
| INV-A04 | After quality-check failure, LLM confidence is capped at 0.35. | Code design (`min(llm_result.confidence, 0.35)`) + unit test | `kuberca/analyst/coordinator.py::AnalystCoordinator.analyze()`, `tests/unit/test_invariants.py::TestAnalysisInvariants::test_quality_failure_cap_035` |

## Divergence Trigger Invariants

| ID | Statement | Enforcement | Location |
|----|-----------|-------------|----------|
| INV-DT01 | Cache miss rate exceeding 5% over a 5-minute rolling window triggers DEGRADED state. | Code design (`_MISS_RATE_THRESHOLD = 0.05`) + unit test | `kuberca/cache/resource_cache.py`, `tests/unit/test_invariants.py::TestDivergenceTriggerInvariants::test_miss_rate_above_threshold_degrades` |
| INV-DT02 | More than 3 reconnect failures trigger DEGRADED state. | Code design (`_RECONNECT_FAILURE_THRESHOLD = 3`) + unit test | `kuberca/cache/resource_cache.py`, `tests/unit/test_invariants.py::TestDivergenceTriggerInvariants::test_reconnect_failures_above_threshold_degrades` |
| INV-DT03 | More than 2 relist timeouts within a 10-minute window trigger DEGRADED state. | Code design (`_RELIST_TIMEOUT_THRESHOLD = 2`) + unit test | `kuberca/cache/resource_cache.py`, `tests/unit/test_invariants.py::TestDivergenceTriggerInvariants::test_relist_timeouts_above_threshold_degrades` |
| INV-DT04 | 429/500 burst errors lasting longer than 1 minute trigger DEGRADED state. | Code design (`_BURST_WINDOW = timedelta(minutes=1)`) + unit test | `kuberca/cache/resource_cache.py`, `tests/unit/test_invariants.py::TestDivergenceTriggerInvariants::test_burst_errors_above_threshold_degrades` |
