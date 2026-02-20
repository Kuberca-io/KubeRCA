# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
