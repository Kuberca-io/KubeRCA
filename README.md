<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="images/kuberca-logo-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="images/kuberca-logo-light.svg">
    <img alt="KubeRCA" src="images/kuberca-logo-light.svg" width="400">
  </picture>
</p>

<p align="center">
  <strong>Kubernetes Root Cause Analysis</strong><br>
  Deterministic incident diagnosis with optional LLM fallback
</p>

<p align="center">
  <a href="https://github.com/KubeRCA-io/KubeRCA/actions/workflows/docker-publish.yml"><img src="https://github.com/KubeRCA-io/KubeRCA/actions/workflows/docker-publish.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/KubeRCA-io/KubeRCA/releases"><img src="https://img.shields.io/github/v/release/KubeRCA-io/KubeRCA" alt="Release"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-Apache_2.0-blue.svg" alt="License"></a>
  <a href="https://kubeRCA-io.github.io/KubeRCA"><img src="https://img.shields.io/badge/Helm-v0.1.0-blue" alt="Helm"></a>
  <a href="https://github.com/KubeRCA-io/KubeRCA/pkgs/container/kuberca"><img src="https://img.shields.io/badge/Docker-ghcr.io-blue" alt="Docker"></a>
  <img src="https://img.shields.io/badge/python-%3E%3D3.12-blue" alt="Python >= 3.12">
  <a href="https://codecov.io/gh/Kuberca-io/KubeRCA"><img src="https://codecov.io/gh/Kuberca-io/KubeRCA/graph/badge.svg" alt="codecov"></a>
</p>

## Why KubeRCA?

Kubernetes incidents typically take 30-45 minutes to diagnose manually. Engineers sift through pod logs, events, node conditions, and resource specs, mentally correlating signals across dozens of API objects. Under pressure, it's easy to miss the real root cause and chase symptoms instead.

KubeRCA replaces that manual investigation with 18 deterministic diagnostic rules that cover the most common Kubernetes failure modes -- from OOMKilled and CrashLoopBackOff to FailedScheduling, volume mount errors, and quota exhaustion. Each rule follows a three-phase pipeline (match, correlate, explain) that produces structured, explainable results in under 2 seconds. For incidents that don't match any rule, an optional local LLM fallback (via Ollama) provides best-effort analysis.

KubeRCA is fully self-hosted. Your cluster data never leaves your infrastructure -- there are no external API calls, no cloud dependencies, and no API keys to manage. It runs as a single process in your cluster with minimal resource requirements.

## Overview

KubeRCA watches your Kubernetes cluster in real time, correlates events across resources, and produces structured root-cause analysis reports. It uses a two-tier rule engine covering 18 common failure modes and can optionally escalate to an LLM (via Ollama) for incidents that don't match any deterministic rule.

## Key Features

- **18 diagnostic rules** - OOMKilled, CrashLoopBackOff, FailedScheduling, ImagePull errors, HPA issues, service connectivity, config drift, volume mount failures, node pressure, readiness probe failures, quota exceeded, eviction, and more
- **LLM fallback** - Optional Ollama integration for unmatched incidents with graceful degradation
- **Scout anomaly detector** - Proactive alerting with configurable cooldown
- **MCP server** - Model Context Protocol for AI-assisted Kubernetes debugging
- **Notifications** - Slack, email, and generic webhook channels
- **Prometheus metrics** - Rule hit rates, LLM escalation rates, analysis latency
- **Security hardened** - Non-root, read-only filesystem, seccomp, all capabilities dropped

## Architecture

```
                    ┌─────────────────────────────────────────────┐
                    │              KubeRCA Process                │
                    │                                             │
  K8s API ────────► │  Watchers ──► Cache ──► Coordinator ──────► │ ──► REST API
  (Events,Pods,     │              │   │       │        │         │     /api/v1/analyze
   Nodes)           │              │   │       ▼        ▼         │     /api/v1/status
                    │              │   │   Rule Engine  LLM       │     /api/v1/health
                    │              │   │   (18 rules)  (Ollama)   │
                    │              │   ▼                          │ ──► MCP Server
                    │              │  Ledger ──► Graph            │
                    │              │            (blast radius)    │ ──► Prometheus
                    │              │                              │     /metrics
                    │              └──► Scout ──► Notifications   │
                    │                   (anomaly   (Slack/Email/  │
                    │                    detect)    Webhook)      │
                    └─────────────────────────────────────────────┘
```

For detailed component documentation, see [Architecture Overview](docs/architecture.md).

## Quick Start

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
# GitHub Container Registry
docker pull ghcr.io/kuberca-io/kuberca:0.1.0

# Docker Hub
docker pull kuberca/kuberca:0.1.0
```

For a complete walkthrough, see the [Getting Started guide](docs/getting-started.md).

## Configuration

All settings are configured via `KUBERCA_*` environment variables (or Helm `values.yaml`).

| Variable | Default | Description |
|----------|---------|-------------|
| `KUBERCA_CLUSTER_ID` | `""` | Human-readable cluster identifier |
| `KUBERCA_OLLAMA_ENABLED` | `false` | Enable LLM fallback via Ollama |
| `KUBERCA_OLLAMA_ENDPOINT` | `http://localhost:11434` | Ollama API endpoint |
| `KUBERCA_OLLAMA_MODEL` | `qwen2.5:7b` | Ollama model tag |
| `KUBERCA_OLLAMA_TIMEOUT` | `30` | Per-request timeout (10-90s) |
| `KUBERCA_OLLAMA_MAX_RETRIES` | `3` | Retries on JSON parse failure (0-5) |
| `KUBERCA_OLLAMA_TEMPERATURE` | `0.1` | Sampling temperature |
| `KUBERCA_OLLAMA_MAX_TOKENS` | `2048` | Max tokens per response |
| `KUBERCA_OLLAMA_HEALTH_CHECK_INTERVAL` | `60` | Health check interval (seconds) |
| `KUBERCA_RULE_ENGINE_TIME_WINDOW` | `2h` | Event correlation lookback window |
| `KUBERCA_RULE_ENGINE_TIER2_ENABLED` | `true` | Enable Tier 2 rules (R04-R18) |
| `KUBERCA_CHANGE_LEDGER_MAX_VERSIONS` | `5` | Max snapshot versions per resource (2-20) |
| `KUBERCA_CHANGE_LEDGER_RETENTION` | `6h` | Snapshot retention window |
| `KUBERCA_LEDGER_PERSISTENCE_ENABLED` | `false` | Persist ledger to SQLite |
| `KUBERCA_SCOUT_ANOMALY_COOLDOWN` | `15m` | Cooldown between repeated alerts |
| `KUBERCA_NOTIFICATIONS_SLACK_SECRET_REF` | `""` | K8s Secret name for Slack webhook |
| `KUBERCA_NOTIFICATIONS_EMAIL_SECRET_REF` | `""` | K8s Secret name for SMTP credentials |
| `KUBERCA_NOTIFICATIONS_EMAIL_TO` | `""` | Recipient email address |
| `KUBERCA_NOTIFICATIONS_WEBHOOK_SECRET_REF` | `""` | K8s Secret name for generic webhook |
| `KUBERCA_API_PORT` | `8080` | REST API listen port (1024-65535) |
| `KUBERCA_LOG_LEVEL` | `info` | Log level (debug, info, warning, error) |

## API Endpoints

All endpoints are under the `/api/v1` prefix.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/analyze` | Trigger root-cause analysis for a resource |
| `GET` | `/api/v1/status` | Cluster status snapshot (pods, nodes, events) |
| `GET` | `/api/v1/health` | Liveness probe |
| `GET` | `/metrics` | Prometheus metrics |

### Example: Analyze a resource

```bash
curl -X POST http://localhost:8080/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{"resource": "Pod/default/my-pod", "time_window": "2h"}'
```

#### Example Response

```json
{
  "root_cause": "Container 'app' was OOMKilled (exit code 137). Memory limit 128Mi is insufficient — peak usage reached the limit, triggering the kernel OOM killer.",
  "confidence": 0.85,
  "diagnosed_by": "rule_engine",
  "rule_id": "R01_oom_killed",
  "evidence": [
    {
      "type": "event",
      "timestamp": "2025-01-15T10:32:17Z",
      "summary": "Container app terminated with exit code 137 (OOMKilled)"
    },
    {
      "type": "event",
      "timestamp": "2025-01-15T10:32:15Z",
      "summary": "Pod exceeded memory limit (128Mi)"
    }
  ],
  "affected_resources": [
    {
      "kind": "Pod",
      "namespace": "default",
      "name": "my-pod"
    }
  ],
  "blast_radius": [
    {
      "kind": "Deployment",
      "namespace": "default",
      "name": "my-deployment"
    },
    {
      "kind": "Service",
      "namespace": "default",
      "name": "my-service"
    }
  ],
  "suggested_remediation": "Increase the container memory limit. Check application memory usage patterns and consider setting requests equal to limits to avoid unexpected OOM kills.",
  "meta": {
    "kuberca_version": "0.1.0",
    "schema_version": "1.0",
    "cluster_id": "prod-us-east-1",
    "timestamp": "2025-01-15T10:32:45Z",
    "response_time_ms": 142,
    "cache_state": "ready",
    "warnings": []
  }
}
```

#### Response Fields

| Field | Description |
|-------|-------------|
| `root_cause` | Human-readable diagnosis of the incident |
| `confidence` | Score from 0.0 to 0.95. Formula: `base_confidence + 0.15` (rule-relevant diffs found) `+ 0.10` (change within 30 min) `+ 0.05` (event count >= 3) `+ 0.05` (single resource affected), capped at 0.95 |
| `diagnosed_by` | `rule_engine` for deterministic matches, `llm` for LLM fallback |
| `rule_id` | Rule identifier (e.g. `R01_oom_killed`), `null` for LLM diagnoses |
| `evidence` | List of events and signals that support the diagnosis |
| `affected_resources` | Kubernetes resources directly involved in the incident |
| `blast_radius` | Resources connected via the dependency graph (6 edge types: owner_reference, volume_binding, config_reference, service_selector, node_assignment, quota_scope) |
| `suggested_remediation` | Actionable fix recommendation |
| `meta.warnings` | Operational warnings (e.g. cache still warming, LLM timeout) |
| `meta.response_time_ms` | End-to-end analysis time in milliseconds |
| `meta.cache_state` | `ready`, `warming`, `partially_ready`, or `degraded` |

### Interactive API Documentation

KubeRCA auto-generates interactive API documentation via FastAPI:

| URL | Description |
|-----|-------------|
| `/api/v1/docs` | Swagger UI -- interactive API explorer with try-it-out |
| `/api/v1/redoc` | ReDoc -- clean, readable API reference |
| `/api/v1/openapi.json` | Raw OpenAPI 3.x specification |

## CLI

KubeRCA includes a command-line interface that communicates with the REST API.

### Global Options

| Option | Env Var | Default | Description |
|--------|---------|---------|-------------|
| `--api-url` | `KUBERCA_API_URL` | `http://localhost:8080` | KubeRCA REST API base URL |

### Commands

#### `kuberca version`

Print the KubeRCA version and exit.

```bash
$ kuberca version
kuberca 0.1.0
```

#### `kuberca status [--namespace NS] [--json]`

Show current cluster status including pod counts, node conditions, active anomalies, and recent events.

```bash
# All namespaces
$ kuberca status

# Specific namespace
$ kuberca status --namespace production

# Raw JSON output
$ kuberca status --json
```

#### `kuberca analyze <Kind/namespace/name> [--time-window WINDOW] [--json]`

Trigger root-cause analysis for a specific resource. The resource must be specified in `Kind/namespace/name` format.

```bash
# Analyze a pod
$ kuberca analyze Pod/default/my-pod

# With custom time window
$ kuberca analyze Pod/production/api-server --time-window 30m

# Raw JSON output
$ kuberca analyze Deployment/default/web --json
```

## Development

### Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/)

### Setup

```bash
git clone https://github.com/KubeRCA-io/KubeRCA.git
cd KubeRCA
uv sync --dev
```

### Tests, Lint, Type Check

```bash
uv run pytest                        # run tests
uv run ruff check .                  # lint
uv run ruff format .                 # format
uv run mypy kuberca/                 # type check (strict)
```

### Build Docker Image

```bash
docker build -t kuberca:dev .
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development workflow, commit conventions, and PR process.

## License

[Apache License 2.0](LICENSE)
