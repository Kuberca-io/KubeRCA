# Getting Started with KubeRCA

This guide walks you through deploying KubeRCA to a local Kubernetes cluster, triggering a test incident, and diagnosing it with both the CLI and REST API.

## Prerequisites

- A running Kubernetes cluster (Docker Desktop, kind, or minikube)
- [kubectl](https://kubernetes.io/docs/tasks/tools/) configured to access your cluster
- [Helm](https://helm.sh/docs/intro/install/) v3+
- [Docker](https://docs.docker.com/get-docker/) (for building the image locally)
- [curl](https://curl.se/) and [jq](https://jqlang.github.io/jq/) (for API calls)

## 1. Choose Your Environment

KubeRCA runs on any Kubernetes cluster. The image loading step differs by environment:

| Environment | Image Loading |
|------------|---------------|
| **Docker Desktop** | Images are shared automatically -- no extra steps needed |
| **kind** | `kind load docker-image kuberca:0.1.2` |
| **minikube** | `minikube image load kuberca:0.1.2` |

## 2. Build the Docker Image

```bash
git clone https://github.com/KubeRCA-io/KubeRCA.git
cd KubeRCA
docker build -t kuberca:0.1.2 .
```

If using **kind**, load the image into the cluster:

```bash
kind load docker-image kuberca:0.1.2
```

## 3. Install via Helm

```bash
# Create namespace and install
helm install kuberca helm/kuberca \
  --namespace kuberca \
  --create-namespace \
  -f e2e/values-local.yaml

# Wait for the deployment to be ready
kubectl -n kuberca rollout status deployment/kuberca --timeout=120s
```

The `values-local.yaml` file sets `imagePullPolicy: Never` so Kubernetes uses the locally built image.

## 4. Set Up Port-Forward

```bash
kubectl -n kuberca port-forward svc/kuberca 8080:8080 &
```

## 5. Health Check

Verify KubeRCA is running:

```bash
curl -s http://localhost:8080/api/v1/health | jq .
```

Expected output:

```json
{
  "status": "ok",
  "version": "0.1.2",
  "cache_state": "ready"
}
```

## 6. Trigger a Test Incident

Deploy a pod that will be OOMKilled (the most recognizable Kubernetes failure):

```bash
kubectl apply -f e2e/manifests/oom-victim.yaml
```

This creates a pod in the `kuberca-e2e` namespace that allocates memory beyond its 10Mi limit. Wait for it to be terminated:

```bash
# Watch until you see OOMKilled in the REASON column
kubectl -n kuberca-e2e get pod oom-victim -w
```

Give KubeRCA a few seconds to ingest the events, then proceed to diagnosis.

## 7. Diagnose with the CLI

```bash
# Install the CLI (if not already)
uv sync --dev

# Analyze the OOMKilled pod
uv run kuberca analyze Pod/kuberca-e2e/oom-victim
```

You should see output like:

```
Analyzing Pod/kuberca-e2e/oom-victim ...

Root Cause Analysis

  Root Cause:      Container 'oom' was OOMKilled (exit code 137). Memory limit 10Mi
                   is insufficient.
  Confidence:      85%
  Diagnosed By:    rule_engine  (rule: R01_oom_killed)

Evidence (2 items):
  [event] 2025-01-15T10:32:17Z  Container oom terminated with exit code 137 (OOMKilled)
  [event] 2025-01-15T10:32:15Z  Pod exceeded memory limit (10Mi)

Suggested Remediation:
  Increase the container memory limit.
```

## 8. Diagnose with the REST API

```bash
curl -s -X POST http://localhost:8080/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{"resource": "Pod/kuberca-e2e/oom-victim"}' | jq .
```

### Understanding the Response

| Field | What It Means |
|-------|---------------|
| `root_cause` | Human-readable explanation of what went wrong |
| `confidence` | Score from 0.0 to 0.95 indicating diagnostic certainty |
| `diagnosed_by` | `rule_engine` (deterministic) or `llm` (LLM fallback) |
| `rule_id` | Which rule matched (e.g. `R01_oom_killed`) |
| `evidence` | Events and signals supporting the diagnosis |
| `affected_resources` | Kubernetes resources directly involved |
| `blast_radius` | Connected resources via the dependency graph |
| `suggested_remediation` | Actionable fix recommendation |
| `meta.response_time_ms` | How long the analysis took |
| `meta.cache_state` | Cache readiness: `ready`, `warming`, `partially_ready`, or `degraded` |
| `meta.warnings` | Any operational warnings (e.g. cache warming, LLM timeout) |

## 9. Try More Scenarios

The `e2e/manifests/` directory includes 11 test scenarios covering different failure modes:

| Manifest | Rule | Failure Mode |
|----------|------|--------------|
| `oom-victim.yaml` | R01 | OOMKilled |
| `crash-loop.yaml` | R02 | CrashLoopBackOff |
| `bad-schedule.yaml` | R03 | FailedScheduling |
| `bad-image.yaml` | R04 | ImagePullBackOff |
| `bad-probe.yaml` | R06/R10 | Readiness probe failure |
| `missing-configmap.yaml` | R11 | ConfigMap mount failure |
| `missing-secret.yaml` | R12 | Secret mount failure |
| `unbound-pvc.yaml` | R13 | PVC binding failure |
| `exceed-quota.yaml` | R16 | Resource quota exceeded |
| `eviction-pressure.yaml` | R17 | Pod eviction |
| `node-constraint.yaml` | R15 | Node scheduling constraint |

Deploy any manifest and analyze the resulting pod:

```bash
kubectl apply -f e2e/manifests/crash-loop.yaml

# Wait for CrashLoopBackOff
kubectl -n kuberca-e2e get pod crash-loop -w

# Analyze
curl -s -X POST http://localhost:8080/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{"resource": "Pod/kuberca-e2e/crash-loop"}' | jq .
```

## 10. Explore the API Documentation

KubeRCA auto-generates interactive API documentation:

- **Swagger UI**: [http://localhost:8080/api/v1/docs](http://localhost:8080/api/v1/docs) -- interactive API explorer with try-it-out
- **ReDoc**: [http://localhost:8080/api/v1/redoc](http://localhost:8080/api/v1/redoc) -- clean, readable reference
- **OpenAPI spec**: [http://localhost:8080/api/v1/openapi.json](http://localhost:8080/api/v1/openapi.json) -- raw schema

## 11. Clean Up

```bash
# Remove test workloads
kubectl delete namespace kuberca-e2e

# Remove KubeRCA
helm uninstall kuberca -n kuberca
kubectl delete namespace kuberca

# Stop port-forward
kill %1 2>/dev/null
```

## Next Steps

- **Notifications**: Configure Slack, email, or webhook alerts via `KUBERCA_NOTIFICATIONS_*` environment variables
- **LLM fallback**: Enable Ollama for incidents that don't match any rule (`KUBERCA_OLLAMA_ENABLED=true`)
- **Architecture**: Read the [Architecture Overview](architecture.md) for component-level details
- **Configuration**: See the full [configuration reference](../README.md#configuration) for all `KUBERCA_*` variables
