# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

[Unreleased]: https://github.com/KubeRCA-io/KubeRCA/compare/v0.1.2...HEAD

## [0.1.2] - 2026-02-22

### Added

- Test coverage increased from 85% to 97% (1007 → 1607 tests)
- 5 new test files:
  - `test_app_startup.py` — 68 tests for app lifecycle startup methods
  - `test_coordinator_helpers.py` — 75 tests for analyst coordinator internal helpers
  - `test_watcher_base.py` — 55 tests for BaseWatcher reconnection, relist, and backoff logic
  - `test_coverage_boost.py` — 69 tests for ResourceCache, ChangeLedger, redaction engine, WorkQueue
  - `test_coverage_misc.py` — 79 tests for CLI, API routes, dependency graph, MCP server, logging
- Tier 1 rule tests (`test_rules_r01_r03.py`) — comprehensive match/correlate/explain coverage for OOMKilled, CrashLoopBackOff, FailedScheduling
- Confidence stability tests (`test_confidence_stability.py`) — 29 tests proving rule engine determinism under adversarial conditions (competing rule pairs, 100-replay stability, 1000-call idempotency, band transition logic)
- CLI test coverage expanded (`test_cli_main.py`) — 34 new tests (14 → 48) covering all cache states, confidence color thresholds, analyze edge cases, anomaly display, event truncation, HTTP error handling, invalid resource formats
- Load/stress integration tests (`test_load_stress.py`) — 12 tests validating 1000-event throughput, p95 latency < 500ms, ledger memory bounds, soft trim recovery, deterministic confidence under sustained load
- Cache state transition Prometheus counter (`kuberca_cache_state_transitions_total`) with `from_state`/`to_state` labels for PromQL-based churn alerting
- Cache state transition tests (`test_cache_state_transitions.py`) — 10 tests covering all transition paths and no-op stability
- Invariant protection system:
  - `INVARIANTS.md` documenting all system invariants (INV-C01 through INV-DT04)
  - Runtime invariant checks at 4 production boundaries (`compute_confidence`, `_recompute_readiness`, `_rule_result_to_rca_response`, `RuleEngine.evaluate`) — log + metric, never crash
  - `kuberca_invariant_violations_total` Prometheus counter with `invariant_name` label
  - Invariant test suite (`test_invariants.py`) — 31 tests covering confidence range, cache state transitions, rule ordering, pure functions, LLM cap, 4-band strategy, competing deps, divergence triggers, analysis pipeline constants
- Confidence under cache penalties test suite (`test_confidence_under_penalties.py`) — 32 tests proving graceful degradation across READY/PARTIALLY_READY/DEGRADED cache states (rule winner stability, no rule flipping, degradation curve, boundary penalties, post-selection application, 100x replay stability, warning messages)
- Stress test failure intersections — 8 new scenarios in `test_load_stress.py`: burst+cache oscillation, readiness state oscillation, ledger trim during analysis, LLM suppression flip, concurrent penalty application, API error injection, memory pressure under GC, work queue dedup race

### Fixed

- Replace deprecated `datetime.utcnow()` with `datetime.now(tz=UTC)` across watcher, ledger, diff, and models

[0.1.2]: https://github.com/KubeRCA-io/KubeRCA/compare/v0.1.1...v0.1.2

## [0.1.1] - 2026-02-20

### Security

- Switch Docker base image from `python:3.12-slim` (Debian) to `python:3.12-alpine` — eliminates 23 Debian-package CVEs
- Remove pip/setuptools from runtime image — eliminates CVE-2025-8869 and CVE-2026-1703
- Remaining: 1 medium busybox CVE (CVE-2025-60876, no upstream fix)

### Changed

- Docker image size reduced from ~150 MB to ~45 MB
- Use Alpine's built-in nobody user (UID/GID 65534) instead of creating one

### Added

- Test coverage increased from 64% to 85% (623 → 1036 tests)
- 8 new test files covering rules R04-R10, collectors, MCP, notifications, rules init, cache, app lifecycle

[0.1.1]: https://github.com/KubeRCA-io/KubeRCA/compare/v0.1.0...v0.1.1

## [0.1.0] - 2025-02-19

### Added

- **Rule engine** with 18 diagnostic rules across two tiers:
  - Tier 1 (always enabled): OOMKilled (R01), CrashLoopBackOff (R02), FailedScheduling (R03)
  - Tier 2 (configurable): ImagePull (R04), HPA (R05), ServiceUnreachable (R06), ConfigDrift (R07), VolumeMount (R08), NodePressure (R09), ReadinessProbe (R10), FailedMount ConfigMap (R11), FailedMount Secret (R12), FailedMount PVC (R13), FailedMount NFS (R14), FailedScheduling Node (R15), ExceedQuota (R16), Evicted (R17), ClaimLost (R18)
- **LLM fallback** via Ollama integration for incidents not covered by deterministic rules
- **Event watchers** for Pods, Nodes, and Kubernetes Events with real-time streaming
- **Resource cache** with in-memory store for fast lookups across 18 resource types
- **Change ledger** with optional SQLite persistence for tracking resource spec diffs
- **Dependency graph** for blast-radius analysis of affected resources
- **Analyst coordinator** that orchestrates rule engine, LLM, and evidence collection
- **Analysis queue** with bounded concurrency for request processing
- **Scout anomaly detector** with configurable cooldown for proactive alerting
- **REST API** (FastAPI) with endpoints: `/api/v1/analyze`, `/api/v1/status`, `/api/v1/health`
- **MCP server** (Model Context Protocol) for AI-assisted Kubernetes debugging
- **Notification dispatcher** supporting Slack, email, and generic webhooks
- **Prometheus metrics** with counters, gauges, and rolling rate computation
- **Helm chart** with security-hardened defaults (non-root, read-only filesystem, seccomp, dropped capabilities)
- **Docker image** with multi-stage build (Python 3.12-slim, uv package manager)
- **CI/CD workflows** for Docker build/push, Helm chart release, and GitHub Release creation

[0.1.0]: https://github.com/KubeRCA-io/KubeRCA/releases/tag/v0.1.0
