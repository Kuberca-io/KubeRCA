# KubeRCA v0.1.2 (Unreleased)

Test coverage release — 97% unit test coverage.

## Testing

- **Test coverage increased from 85% to 97%** (5624 statements, 189 missed)
- **1353 tests** (up from 1007)
- 5 new test files covering:
  - App lifecycle startup methods (K8s client, cache, ledger, collectors, coordinator, queue, scout, REST, MCP)
  - Analyst coordinator internal helpers (status event synthesis, state context, blast radius, warming/degraded warnings)
  - BaseWatcher reconnection, relist, rate limiting, backoff, and pod-only fallback
  - ResourceCache populate/ingest, ChangeLedger memory pressure (soft trim, failsafe eviction), redaction edge cases, WorkQueue rate limiting
  - CLI HTTP helpers, API route error paths, dependency graph edge cases, MCP tool handlers, logging setup
- Tier 1 rule tests — comprehensive match/correlate/explain coverage for OOMKilled, CrashLoopBackOff, FailedScheduling

## Fixed

- Replace deprecated `datetime.utcnow()` with `datetime.now(tz=UTC)` across watcher, ledger, diff, and models

---

# KubeRCA v0.1.1

Security patch and test-coverage release.

## Security

- **Base image switched from Debian (`python:3.12-slim`) to Alpine (`python:3.12-alpine`)**
  - Eliminates all 23 low-severity Debian-package CVEs (apt, openssl, shadow, glibc, systemd, coreutils, util-linux, perl, sqlite3)
  - Image size reduced from ~150 MB to ~45 MB
- **pip removed from runtime image** — eliminates CVE-2025-8869 (medium) and CVE-2026-1703 (low)
- Remaining: 1 medium busybox CVE (CVE-2025-60876, no upstream fix available)

## Testing

- **Test coverage increased from 64% to 85%** (5624 statements, 833 missed)
- **1036 tests** (up from 623)
- New test files covering:
  - Rules R04-R10 (match, correlate, explain)
  - Collectors (BaseWatcher, EventWatcher, PodWatcher, NodeWatcher)
  - MCP server handlers and serialization
  - Notification dispatcher factory and SMTP DSN parsing
  - Rule discovery and engine construction
  - ResourceCache edge cases (divergence, readiness states)
  - App lifecycle (init, start, stop, component errors)
  - Coverage gap tests (R02, R10, R17, base, email, evidence)

## Housekeeping

- `CLAUDE.md` removed from version control (remains local-only via `.gitignore`)

## Install

### Helm (GitHub Pages)

```bash
helm repo add kuberca https://kubeRCA-io.github.io/KubeRCA
helm install kuberca kuberca/kuberca --namespace kuberca --create-namespace
```

### Helm (OCI)

```bash
helm install kuberca oci://ghcr.io/kuberca-io/charts/kuberca \
  --version 0.1.1 --namespace kuberca --create-namespace
```

### Docker

```bash
docker pull ghcr.io/kuberca-io/kuberca:0.1.1
# or
docker pull kuberca/kuberca:0.1.1
```

---

# KubeRCA v0.1.0

The first release of KubeRCA - a Kubernetes Root Cause Analysis system that automatically diagnoses cluster incidents using a deterministic rule engine with optional LLM fallback.

## Highlights

### Rule Engine

- **18 diagnostic rules** covering the most common Kubernetes failure modes
- **Tier 1** (R01-R03): OOMKilled, CrashLoopBackOff, FailedScheduling - always enabled, highest confidence
- **Tier 2** (R04-R18): ImagePull errors, HPA scaling issues, service connectivity, config drift, volume mount failures, node pressure, readiness probe failures, resource quota exceeded, pod eviction, and PVC claim lost
- Deterministic, explainable results with confidence scoring and evidence chains

### LLM Fallback

- Optional integration with **Ollama** for incidents that don't match any rule
- Supports any Ollama-compatible model (default: `qwen2.5:7b`)
- Graceful degradation: runs in rule-engine-only mode if Ollama is unavailable

### Deployment

- **Helm chart** with security-hardened defaults:
  - Non-root container (UID 65534)
  - Read-only root filesystem
  - Seccomp profile (RuntimeDefault)
  - All capabilities dropped
  - Optional NetworkPolicy
- **Docker images** published to:
  - `ghcr.io/kuberca-io/kuberca:0.1.0`
  - `kuberca/kuberca:0.1.0` (Docker Hub)
- Multi-platform: `linux/amd64` and `linux/arm64`

### Integration Points

- **REST API**: FastAPI-based with `/api/v1/analyze`, `/api/v1/status`, `/api/v1/health`
- **MCP Server**: Model Context Protocol for AI-assisted Kubernetes debugging
- **Prometheus Metrics**: Rule hit rates, LLM escalation rates, analysis latency
- **Notifications**: Slack, email, and generic webhook channels

## Install

### Helm (GitHub Pages)

```bash
helm repo add kuberca https://kubeRCA-io.github.io/KubeRCA
helm install kuberca kuberca/kuberca --namespace kuberca --create-namespace
```

### Helm (OCI)

```bash
helm install kuberca oci://ghcr.io/kuberca-io/charts/kuberca \
  --version 0.1.0 --namespace kuberca --create-namespace
```

### Docker

```bash
docker pull ghcr.io/kuberca-io/kuberca:0.1.0
# or
docker pull kuberca/kuberca:0.1.0
```

## What's Next

- Additional diagnostic rules for networking and storage edge cases
- Multi-cluster support
- Web dashboard for visualization
- Historical trend analysis
