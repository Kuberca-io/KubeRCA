# Twitter/X Launch Thread

**Tweet 1 (Hook):**
Kubernetes incidents take 30-45 minutes to diagnose manually.

We built a tool that does it in under 2 seconds.

Here's how. (thread)

**Tweet 2 (Problem):**
The typical workflow: kubectl describe pod, scroll through events, check node conditions, cross-reference deployments...

All while an incident is active and the clock is ticking.

**Tweet 3 (Solution):**
KubeRCA uses 18 deterministic diagnostic rules covering the most common K8s failure modes.

Each rule follows a three-phase pipeline: match, correlate, explain.

Structured output with confidence scores and blast radius analysis.

**Tweet 4 (Differentiators):**
Fully self-hosted -- your cluster data never leaves your infrastructure.

Optional LLM fallback via Ollama. MCP server for AI-assisted debugging. Zero API keys to manage.

**Tweet 5 (Install):**
Get started in one command:

helm repo add kuberca https://kuberca-io.github.io/KubeRCA
helm install kuberca kuberca/kuberca

Or: pip install kuberca

**Tweet 6 (CTA):**
Apache-2.0. Open source. Built for SREs and platform engineers.

GitHub: https://github.com/KubeRCA-io/KubeRCA

Feedback welcome -- open an issue or reply here.
