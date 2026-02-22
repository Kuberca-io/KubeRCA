# Show HN Post

**Title:** Show HN: KubeRCA -- Diagnose Kubernetes incidents in 2s with deterministic rules

**Body:**

KubeRCA watches your Kubernetes cluster in real time and diagnoses incidents using 18 deterministic rules covering the most common failure modes (OOMKilled, CrashLoopBackOff, FailedScheduling, etc.). Each diagnosis includes structured evidence, blast radius analysis, and remediation suggestions -- typically completing in under 2 seconds.

It's fully self-hosted (no data leaves your cluster), has an optional LLM fallback via Ollama for incidents that don't match any rule, and includes an MCP server for AI-assisted debugging. Apache-2.0 licensed, written in Python.

GitHub: https://github.com/KubeRCA-io/KubeRCA
