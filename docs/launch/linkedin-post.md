# LinkedIn Launch Post

If you've been on-call for a Kubernetes cluster, you know the drill. Pod is crashlooping, users are paged, and you're running kubectl describe, scrolling through events, checking node pressure, cross-referencing deployments -- all while the incident clock is ticking. 30-45 minutes to root cause is a good day.

We built KubeRCA to close that gap.

KubeRCA watches your cluster in real time and diagnoses incidents using 18 deterministic rules covering the most common failure modes: OOMKilled, CrashLoopBackOff, FailedScheduling, image pull errors, node pressure evictions, and more. Each diagnosis runs a match-correlate-explain pipeline and returns structured output with confidence scores, blast radius analysis, and remediation suggestions -- typically in under 2 seconds.

A few things that matter to us:

- Fully self-hosted. Your cluster data never leaves your infrastructure.
- Optional LLM fallback via Ollama for incidents that don't match any rule -- no external API keys required.
- MCP server included, so you can wire it into AI-assisted debugging workflows today.
- Apache-2.0 licensed. No vendor lock-in, no usage limits.

Install via Helm or pip:

helm repo add kuberca https://kuberca-io.github.io/KubeRCA && helm install kuberca kuberca/kuberca

pip install kuberca

GitHub: https://github.com/KubeRCA-io/KubeRCA

If you're an SRE, platform engineer, or DevOps engineer running Kubernetes in production, I'd genuinely love your feedback. What failure modes are missing? What would make this fit better into your incident workflow? Open an issue or drop a comment here.
