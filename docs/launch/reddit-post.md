# Reddit Launch Posts

---

## r/kubernetes

**Title:** I built an open-source tool that diagnoses K8s incidents in under 2 seconds using deterministic rules

**Body:**

For the last couple of years I kept running into the same problem during incidents: I'd open up five terminal tabs, run `kubectl describe pod`, `kubectl get events`, `kubectl describe node`, stare at the output, and try to mentally correlate signals across a dozen different API objects -- all while someone was paging me asking for an ETA on the fix.

The failure modes are almost always the same. OOMKilled because a memory limit is too low. CrashLoopBackOff from a bad env var reference. FailedScheduling because every node is under memory pressure. ImagePullBackOff from a missing registry secret. The investigation steps are repetitive. You're just running the same mental playbook every time, except it's slow and error-prone at 2am.

So I built KubeRCA. It watches your cluster in real time and runs 18 deterministic diagnostic rules across the most common failure modes: OOMKilled, CrashLoopBackOff, FailedScheduling, ImagePull errors, HPA issues, service connectivity, config drift, volume mount failures (ConfigMap, Secret, PVC, NFS), node pressure, readiness probe failures, quota exhaustion, eviction, and PVC claim loss.

Each rule follows a three-phase pipeline -- match, correlate, explain. It correlates events across related resources, checks the dependency graph for blast radius, and produces a structured result with a confidence score. Typically under 2 seconds.

A few things I cared about getting right:

- Fully self-hosted. No data leaves your cluster. No external API calls, no API keys, nothing phoning home.
- When a rule doesn't match, there's an optional LLM fallback via Ollama for best-effort analysis on the weird stuff.
- MCP server included if you want to wire it into an AI-assisted debugging workflow.
- Apache-2.0 licensed.

You can install it with Helm, Docker, or `pip install kuberca`. The REST API takes a resource identifier and returns a structured diagnosis with evidence, affected resources, blast radius, and a remediation suggestion.

Would love feedback from the community -- especially on which failure modes you'd want covered next, or where the current rules fall short in your clusters. Here's the repo: https://github.com/KubeRCA-io/KubeRCA

Happy to answer any questions about the architecture or the rule engine design.

---

## r/devops

**Title:** Show r/devops: KubeRCA -- automated root cause analysis for Kubernetes

**Body:**

I've been working on an open-source tool called KubeRCA that tries to shift Kubernetes incident diagnosis from reactive (you get paged, you go spelunking) to something closer to proactive.

The core is a rule engine with 18 deterministic rules covering the most common K8s failure modes. When an incident matches a rule, you get a structured diagnosis -- root cause, confidence score, supporting evidence, blast radius, and a remediation suggestion -- in under 2 seconds. No waiting for log aggregation, no correlating events manually.

On the proactive side, there's a Scout component that runs anomaly detection against the cluster state and fires alerts with a configurable cooldown, so you're not getting paged every 30 seconds for the same flapping pod. Notifications go out via Slack, email, or a generic webhook -- whatever fits your existing alerting stack.

For observability into the tool itself: Prometheus metrics are exposed at `/metrics` covering rule hit rates, LLM escalation rates, and analysis latency. Useful if you want to track which failure modes are actually showing up in your clusters over time.

Everything is self-hosted -- your cluster data never touches an external service. For incidents that don't match any deterministic rule, there's an optional Ollama integration as a local LLM fallback. Apache-2.0 licensed.

Install via Helm, Docker, or `pip install kuberca`.

Repo: https://github.com/KubeRCA-io/KubeRCA

Would genuinely appreciate feedback -- what failure modes are missing, what the output should look like, whether the notification integration covers your setup. Still early so this is the right time to influence the direction.
