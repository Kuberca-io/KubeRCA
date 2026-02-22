---
title: "We Built a Tool That Diagnoses Kubernetes Incidents in 2 Seconds"
published: false
description: "18 deterministic rules that replace 30 minutes of manual kubectl investigation"
tags: kubernetes, devops, sre, opensource
cover_image: # TODO: add cover image URL
---

We got tired of spending 30-45 minutes investigating the same Kubernetes failures over and over. So we built [KubeRCA](https://github.com/KubeRCA-io/KubeRCA) — an open-source root cause analysis engine that diagnoses most Kubernetes incidents in under 2 seconds using 18 deterministic rules. No cloud dependencies. No API keys. No data leaving your cluster.

## The Problem

Kubernetes incidents have a familiar rhythm. A pod starts failing. You open a terminal. You run `kubectl describe pod`, scroll through a wall of events, check the node conditions, maybe pull logs from a sidecar. Then you jump to the owning Deployment, check the HPA status, look at whether a recent rollout touched the resource limits. You mentally correlate signals across a half-dozen API objects while a Slack thread is piling up and your on-call buddy is asking for an ETA on the fix.

The frustrating part is that most incidents are not mysterious. OOMKilled, CrashLoopBackOff, FailedScheduling, image pull errors, a ConfigMap that got out of sync after a deployment — these are the same failures, diagnosed the same way, every single time. The investigation process is fully deterministic. You are not solving a novel problem; you are executing a checklist under pressure.

The deeper problem is that executing a checklist under pressure is exactly when humans make mistakes. Engineers chase symptoms — "the pod is restarting" — instead of causes — "the memory limit is 128Mi, peak RSS is 130Mi, and the OOM killer fires every 90 minutes like clockwork." Every team builds their own mental model of how to investigate, their own runbooks, their own shortcuts. Execution varies, oncall fatigue accumulates, and the same incident gets diagnosed from scratch by a different person next quarter.

## How KubeRCA Works

KubeRCA replaces that manual investigation loop with 18 deterministic diagnostic rules that cover the most common Kubernetes failure modes. Each rule follows a strict three-phase pipeline.

**Phase 1: match.** A pure, O(1) function that looks at the incoming event and asks: does this rule apply? This phase has no side effects and makes no external calls. If the event is not a candidate, the rule is skipped entirely.

**Phase 2: correlate.** Gather supporting evidence from the in-memory resource cache and change ledger. This phase is bounded by design: it cannot query more than 50 cached objects, and it must complete within 500ms (enforced by a daemon thread with a hard wall-clock timeout). Rules read only from cache and ledger — no direct Kubernetes API calls during analysis.

**Phase 3: explain.** Convert the correlation results into a structured diagnosis: root cause text, confidence score, evidence list, affected resources, blast radius, and a suggested remediation.

The confidence scoring is additive and transparent. Every rule starts at a base confidence (typically 0.50-0.60), then gains: +0.15 if rule-relevant field changes were found in the ledger, +0.10 if a change happened within 30 minutes of the incident, +0.05 if at least 3 corroborating events were observed, +0.05 if only a single resource was affected. The score is capped at 0.95 — KubeRCA never claims certainty. A score at or above 0.85 short-circuits evaluation and returns immediately. Scores between 0.70 and 0.84 narrow the remaining rule set to those sharing resource dependencies with the matched rule. Scores below 0.70 continue full evaluation.

The 18 rules span the failure modes that account for the overwhelming majority of real incidents: OOMKilled (R01), CrashLoopBackOff (R02), FailedScheduling (R03), image pull errors (R04), HPA misbehavior (R05), service unreachability (R06), config drift (R07), volume mount failures (R08), node pressure (R09), readiness probe failures (R10), ConfigMap mount failures (R11), Secret mount failures (R12), PVC mount failures (R13), NFS mount failures (R14), node-level scheduling failures (R15), quota exceeded (R16), pod eviction (R17), and PVC claim lost (R18). For incidents that do not match any rule, an optional Ollama LLM fallback provides best-effort analysis using a local model — no external API call required.

## What Makes It Different

- **Fully self-hosted.** Cluster data never leaves your infrastructure. There are no external API calls, no cloud dependencies, and no API keys to manage. KubeRCA runs as a single process inside your cluster.
- **No black-box AI.** The primary diagnosis path is deterministic rule evaluation. Every conclusion is traceable to specific events, resource diffs, and field changes. The LLM is an optional fallback for the long tail of incidents that don't match a rule — not the primary engine.
- **Blast radius analysis.** The dependency graph tracks six edge types (owner reference, volume binding, config reference, service selector, node assignment, quota scope) and reports which Deployments, Services, and other resources are connected to the failing pod. You know the scope of impact immediately.
- **MCP server.** KubeRCA ships with a Model Context Protocol server, which means you can connect it directly to Claude or another MCP-compatible AI assistant for conversational Kubernetes debugging backed by live cluster data.
- **Prometheus metrics.** Rule hit rates, LLM escalation rates, and analysis latency are all exposed at `/metrics`. You can see which rules fire most often in your cluster, which tells you something useful about your failure patterns.
- **Security hardened.** The container runs non-root, with a read-only filesystem, seccomp enabled, and all Linux capabilities dropped. It requires only read access to the Kubernetes API.
- **Optional LLM fallback via Ollama.** When no deterministic rule fires, KubeRCA can escalate to a locally-hosted model. The default is `qwen2.5:7b`. No data leaves your infrastructure.

## See It in Action

Trigger an analysis with a single HTTP call:

```bash
curl -X POST http://localhost:8080/api/v1/analyze \
  -H "Content-Type: application/json" \
  -d '{"resource": "Pod/default/my-pod", "time_window": "2h"}'
```

For an OOMKilled pod, the response looks like this:

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
    { "kind": "Pod", "namespace": "default", "name": "my-pod" }
  ],
  "blast_radius": [
    { "kind": "Deployment", "namespace": "default", "name": "my-deployment" },
    { "kind": "Service", "namespace": "default", "name": "my-service" }
  ],
  "suggested_remediation": "Increase the container memory limit. Check application memory usage patterns and consider setting requests equal to limits to avoid unexpected OOM kills.",
  "meta": {
    "kuberca_version": "0.1.2",
    "schema_version": "1.0",
    "cluster_id": "prod-us-east-1",
    "timestamp": "2025-01-15T10:32:45Z",
    "response_time_ms": 142,
    "cache_state": "ready",
    "warnings": []
  }
}
```

142 milliseconds. Root cause identified, blast radius mapped, remediation suggested. No `kubectl describe`, no log scrolling, no mental correlation across six API objects.

KubeRCA also ships a CLI for interactive use:

```bash
kuberca analyze Pod/production/api-server --time-window 30m
kuberca status --namespace production
```

## Getting Started

Three ways to run KubeRCA, depending on your setup.

**Helm (recommended for production):**

```bash
helm repo add kuberca https://kubeRCA-io.github.io/KubeRCA
helm install kuberca kuberca/kuberca --namespace kuberca --create-namespace
```

**pip (for local development or scripting):**

```bash
pip install kuberca
```

**Docker:**

```bash
docker pull ghcr.io/kuberca-io/kuberca:0.1.2
```

Configuration is entirely through `KUBERCA_*` environment variables — no config files required. The Helm chart exposes all settings through `values.yaml`. A full configuration reference is in the README.

## Try It

KubeRCA is [Apache-2.0 licensed](https://github.com/KubeRCA-io/KubeRCA/blob/main/LICENSE) and built to run anywhere you run Kubernetes.

If you're on-call for a cluster, install it on a staging environment and trigger a few test incidents. See whether the diagnosis matches what you would have concluded after 20 minutes of `kubectl` investigation. If it misses something or gets it wrong, [open an issue](https://github.com/KubeRCA-io/KubeRCA/issues) — the rule set is designed to expand, and feedback from real production failure patterns is exactly what makes it better.

Star the repo on GitHub: [https://github.com/KubeRCA-io/KubeRCA](https://github.com/KubeRCA-io/KubeRCA)

The 30-minute incident investigation is an unnecessary tax on every SRE and platform engineer running Kubernetes at scale. We built the tool we wished existed. We hope it saves you some oncall nights.
