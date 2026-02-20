# Architecture Overview

KubeRCA is a single-process async Python application that watches Kubernetes clusters in real time and produces structured root-cause analysis reports. This document describes the component architecture, data flow, and key design decisions.

## Startup Order

Components start in strict dependency order. Each step depends on the previous ones being available:

```
 1. Config           Load KUBERCA_* environment variables
 2. Logging          Configure structlog (JSON in production, console in dev)
 3. K8s Client       Initialize kubernetes-asyncio (in-cluster or kubeconfig)
 4. Resource Cache   Populate in-memory cache via K8s list calls
 5. Dependency Graph Build resource relationship graph from cache
 6. Change Ledger    Initialize snapshot ring buffers (+ optional SQLite)
 7. Collectors       Start Event, Pod, and Node watch loops
 8. Rule Engine      Register 18 diagnostic rules in priority order
 9. LLM Analyzer     Connect to Ollama (optional, non-fatal if unavailable)
10. Coordinator      Wire rule engine + LLM + cache + ledger together
11. Work Queue       Bounded async analysis request queue
12. Scout            Anomaly detector with threshold + rate-of-change rules
13. Notifications    Configure Slack, email, and webhook dispatchers
14. MCP Server       Model Context Protocol stdio server
15. REST API         FastAPI + uvicorn HTTP server
```

Shutdown proceeds in reverse order. Each component's start and stop is independently error-handled so that a single failure does not prevent others from operating.

## Data Flow

```
K8s API Server
     │
     ▼
 ┌──────────┐     ┌──────────────┐     ┌────────────────┐
 │ Watchers  │────►│ Resource     │────►│  Dependency    │
 │ (Event,   │     │ Cache        │     │  Graph         │
 │  Pod,     │     │ (18 types)   │     │  (6 edge types)│
 │  Node)    │     └──────┬───────┘     └────────────────┘
 └──────────┘            │
                          ▼
                   ┌──────────────┐
                   │  Change      │
                   │  Ledger      │
                   │  (ring buf + │
                   │   SQLite)    │
                   └──────┬───────┘
                          │
     ┌────────────────────▼───────────────────┐
     │           Coordinator                   │
     │  ┌─────────────┐   ┌────────────────┐  │
     │  │ Rule Engine  │   │ LLM Analyzer   │  │
     │  │ (18 rules,   │   │ (Ollama,       │  │
     │  │  3-phase)    │──►│  optional)     │  │
     │  └─────────────┘   └────────────────┘  │
     └────────────────────┬───────────────────┘
                          │
              ┌───────────┼───────────┐
              ▼           ▼           ▼
         ┌────────┐  ┌────────┐  ┌────────┐
         │REST API│  │  Scout │  │  MCP   │
         │(FastAPI│  │(anomaly│  │ Server │
         │  :8080)│  │ detect)│  │(stdio) │
         └────────┘  └───┬────┘  └────────┘
                         ▼
                   ┌───────────┐
                   │Notifications│
                   │(Slack/Email/│
                   │ Webhook)   │
                   └───────────┘
```

## Components

### Watchers

Three concurrent watch loops maintain real-time cluster state:

- **EventWatcher** -- watches K8s Events for incident signals (OOMKilled, CrashLoopBackOff, FailedScheduling, etc.)
- **PodWatcher** -- tracks pod lifecycle changes and updates the cache and graph
- **NodeWatcher** -- tracks node conditions (memory pressure, disk pressure, PID pressure)

All watchers use `kubernetes-asyncio` watch streams with automatic reconnection.

### Resource Cache

An in-memory cache holding 18 Kubernetes resource types:

| API Group | Resource Types |
|-----------|---------------|
| Core (v1) | Pod, Node, Event, Namespace, Service, Endpoints, ConfigMap, Secret, PersistentVolumeClaim, PersistentVolume, ResourceQuota, ServiceAccount |
| Apps (v1) | Deployment, ReplicaSet, StatefulSet, DaemonSet |
| Batch (v1) | Job, CronJob |

The cache is populated at startup via K8s list calls and kept current by the Pod and Node watchers. It provides `get`, `list`, and `list_by_label` queries used by the rule engine during correlation.

Cache readiness states: `ready`, `warming`, `partially_ready`, `degraded`.

### Dependency Graph

A lightweight in-memory directed graph built from declared Kubernetes configuration. No external graph database required. The graph tracks 6 relationship types:

| Edge Type | Example |
|-----------|---------|
| `owner_reference` | Deployment -> ReplicaSet -> Pod |
| `volume_binding` | Pod -> PVC -> PV |
| `config_reference` | Pod -> ConfigMap, Pod -> Secret |
| `service_selector` | Service -> Pod (via label selector) |
| `node_assignment` | Pod -> Node (via `spec.nodeName`) |
| `quota_scope` | ResourceQuota -> Namespace |

Used for blast-radius analysis: when a resource fails, the graph identifies all connected upstream and downstream resources.

### Change Ledger

Tracks resource spec changes over time using per-resource ring buffers.

- **Ring buffer**: Each resource key `(kind, namespace, name)` has a deque of `ResourceSnapshot` entries, capped at `max_versions` (default 5)
- **Retention**: Snapshots older than the retention window (default 6h) are evicted
- **SQLite persistence**: Optional write-ahead to SQLite for durability across restarts
- **3-tier memory enforcement**:
  - Soft limit (150 MB) -- adaptive trim: reduce each buffer to `max(1, len//2)`
  - Hard limit (200 MB) -- backpressure: stop accepting non-incident snapshots
  - Fail-safe (300 MB) -- emergency eviction from largest buffers

The ledger provides `diff()` to compute field-level changes between snapshots, used by the confidence scoring formula to award the +0.15 "rule-relevant diffs found" bonus.

### Rule Engine

18 diagnostic rules organized in two tiers with priority ordering:

| Rule | Name | Tier | Priority |
|------|------|------|----------|
| R01 | OOMKilled | 1 | 10 |
| R02 | CrashLoopBackOff | 1 | 20 |
| R03 | FailedScheduling | 1 | 30 |
| R04 | ImagePullBackOff | 2 | 40 |
| R05 | HPA Issues | 2 | 50 |
| R06 | ServiceUnreachable | 2 | 60 |
| R07 | ConfigDrift | 2 | 70 |
| R08 | VolumeMountFailure | 2 | 80 |
| R09 | NodePressure | 2 | 90 |
| R10 | ReadinessProbeFailure | 2 | 100 |
| R11 | FailedMount (ConfigMap) | 2 | 110 |
| R12 | FailedMount (Secret) | 2 | 120 |
| R13 | FailedMount (PVC) | 2 | 130 |
| R14 | FailedMount (NFS) | 2 | 140 |
| R15 | FailedScheduling (Node) | 2 | 150 |
| R16 | ExceedQuota | 2 | 160 |
| R17 | Evicted | 2 | 170 |
| R18 | ClaimLost | 2 | 180 |

Each rule follows a **3-phase pipeline**:

1. **match()** -- O(1), pure check: does this event match the rule's trigger conditions?
2. **correlate()** -- bounded: gather related resources (max 50 objects, 500ms wall-clock) and diffs from the ledger
3. **explain()** -- convert correlation into a structured `RuleResult` with root cause, evidence, and remediation

Rules are evaluated in priority order. The first match wins (no further rules are evaluated).

### Confidence Scoring

Confidence is deterministic and explainable. The formula is identical across all rules -- only `base_confidence` varies per rule:

```
score = base_confidence                          (typically 0.50 - 0.60)
      + 0.15  if rule-relevant diffs were found
      + 0.10  if any change occurred within 30 min of the incident
      + 0.05  if the event has been observed >= 3 times
      + 0.05  if the correlation implicates exactly one resource
      → capped at 0.95 (never 1.0)
```

The four confidence bands:

| Band | Range | Interpretation |
|------|-------|----------------|
| High | >= 0.75 | Strong match with corroborating evidence |
| Medium | 0.50 - 0.74 | Likely match, some signals missing |
| Low | 0.25 - 0.49 | Possible match, limited evidence |
| Minimal | < 0.25 | Weak signal, consider LLM fallback |

### LLM Analyzer

Optional fallback for incidents that don't match any deterministic rule. Connects to a local Ollama instance (default model: `qwen2.5:7b`).

- Constructs a structured prompt with event context, pod spec, and node conditions
- Parses the LLM response into the same `RuleResult` schema as rule engine results
- Retries on JSON parse failure (configurable, default 3 retries)
- Health checks at configurable intervals (default 60s)
- Graceful degradation: if Ollama is unreachable, analysis continues in rule-engine-only mode

### Coordinator

Central orchestration point that wires the rule engine and LLM analyzer together:

1. Receives an analysis request (resource identifier + time window)
2. Resolves the resource from the cache
3. Gathers relevant events from the event buffer
4. Runs the rule engine pipeline
5. If no rule matches and LLM is enabled, escalates to the LLM analyzer
6. Enriches the result with blast-radius data from the dependency graph
7. Returns the structured response

### Work Queue

A bounded async queue that serializes analysis requests to prevent resource exhaustion. Requests are processed FIFO. Returns `QUEUE_FULL` (HTTP 429) when the queue is at capacity.

### Scout (Anomaly Detector)

Proactive alerting based on two rule types:

- **Threshold rules**: Alert when an event's count exceeds a threshold (e.g. OOMKilled >= 3)
- **Rate-of-change rules**: Alert when the event rate accelerates beyond a configured factor

Per-(resource, reason) cooldown prevents alert storms. Default cooldown: 15 minutes.

### Notifications

Three dispatcher channels, each configured via K8s Secrets:

| Channel | Secret Variable | Description |
|---------|----------------|-------------|
| Slack | `KUBERCA_NOTIFICATIONS_SLACK_SECRET_REF` | Webhook URL |
| Email | `KUBERCA_NOTIFICATIONS_EMAIL_SECRET_REF` | SMTP credentials |
| Webhook | `KUBERCA_NOTIFICATIONS_WEBHOOK_SECRET_REF` | Generic HTTP endpoint |

Non-fatal: if notification setup fails, analysis continues without alerting.

### REST API

FastAPI application serving on port 8080 (configurable via `KUBERCA_API_PORT`):

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/analyze` | POST | Trigger root-cause analysis |
| `/api/v1/status` | GET | Cluster status snapshot |
| `/api/v1/health` | GET | Liveness probe |
| `/metrics` | GET | Prometheus metrics |
| `/api/v1/docs` | GET | Swagger UI |
| `/api/v1/redoc` | GET | ReDoc documentation |
| `/api/v1/openapi.json` | GET | OpenAPI specification |

### MCP Server

Model Context Protocol server running over stdio. Enables AI coding assistants (Claude, Cursor, etc.) to query KubeRCA for cluster diagnostics during debugging sessions. Exposes the same analysis and status capabilities as the REST API.

## Observability

- **Logging**: structlog with JSON output in production, colorized console in development
- **Metrics**: Prometheus counters and gauges exposed at `/metrics`
  - Rule hit rates (per rule)
  - LLM escalation rate
  - Analysis latency histogram
  - Cache readiness state
  - Ledger memory usage
  - Scout alert counts
