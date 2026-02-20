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
