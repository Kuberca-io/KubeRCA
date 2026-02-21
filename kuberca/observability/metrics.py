"""Prometheus metrics for KubeRCA."""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

# Event metrics
events_total = Counter(
    "kuberca_events_total",
    "Total events received",
    ["source", "severity"],
)

# RCA metrics
rca_requests_total = Counter(
    "kuberca_rca_requests_total",
    "Total RCA requests",
    ["diagnosed_by"],
)

rca_duration_seconds = Histogram(
    "kuberca_rca_duration_seconds",
    "RCA analysis duration in seconds",
    ["diagnosed_by", "rule_id"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)

# Rule metrics
rule_matches_total = Counter(
    "kuberca_rule_matches_total",
    "Total rule matches",
    ["rule_id"],
)

# LLM metrics
llm_requests_total = Counter(
    "kuberca_llm_requests_total",
    "Total LLM requests",
    ["success", "retries"],
)

llm_available = Gauge(
    "kuberca_llm_available",
    "Whether LLM is available (0 or 1)",
)

# Cache metrics
cache_resources = Gauge(
    "kuberca_cache_resources",
    "Number of cached resources",
    ["kind"],
)

cache_state = Gauge(
    "kuberca_cache_state",
    "Cache readiness state",
    ["state"],
)

cache_miss_rate = Gauge(
    "kuberca_cache_miss_rate",
    "Cache miss rate over rolling window",
)

cache_misses_total = Counter(
    "kuberca_cache_misses_total",
    "Total cache misses",
)

cache_divergence_events_total = Counter(
    "kuberca_cache_divergence_events_total",
    "Total cache divergence events",
    ["reason"],
)

cache_relist_duration_seconds = Histogram(
    "kuberca_cache_relist_duration_seconds",
    "Cache relist duration in seconds",
    buckets=(1.0, 2.5, 5.0, 10.0, 15.0, 30.0),
)

cache_relist_timeout_total = Counter(
    "kuberca_cache_relist_timeout_total",
    "Total cache relist timeouts",
)

cache_relist_fallback_total = Counter(
    "kuberca_cache_relist_fallback_total",
    "Total cache relist fallbacks to partial",
)

cache_state_transitions_total = Counter(
    "kuberca_cache_state_transitions_total",
    "Total cache state transitions",
    ["from_state", "to_state"],
)

# Ledger metrics
ledger_snapshots = Gauge(
    "kuberca_ledger_snapshots",
    "Number of ledger snapshots",
    ["kind"],
)

ledger_memory_bytes = Gauge(
    "kuberca_ledger_memory_bytes",
    "Ledger memory usage in bytes",
)

ledger_trim_events_total = Counter(
    "kuberca_ledger_trim_events_total",
    "Total ledger trim events",
)

ledger_memory_pressure_total = Counter(
    "kuberca_ledger_memory_pressure_total",
    "Total ledger memory pressure events",
)

sqlite_backpressure_total = Counter(
    "kuberca_sqlite_backpressure_total",
    "Total SQLite backpressure events",
)

# Notification metrics
notifications_total = Counter(
    "kuberca_notifications_total",
    "Total notifications sent",
    ["channel", "success"],
)

# Analysis queue metrics
analysis_queue_depth = Gauge(
    "kuberca_analysis_queue_depth",
    "Current analysis queue depth",
)

analysis_dropped_total = Counter(
    "kuberca_analysis_dropped_total",
    "Total analysis requests dropped",
    ["priority"],
)

analysis_rate_reduction_total = Counter(
    "kuberca_analysis_rate_reduction_total",
    "Total rate reduction events",
)

# LLM quality check metrics
llm_quality_check_failures_total = Counter(
    "kuberca_llm_quality_check_failures_total",
    "Total LLM quality check failures by type",
    ["failure_type"],
)

# Rule engine coverage metrics (rolling 24h window)
rule_engine_hit_rate = Gauge(
    "kuberca_rule_engine_hit_rate",
    "Percentage of incidents matched by rule engine (24h rolling)",
)

llm_escalation_rate = Gauge(
    "kuberca_llm_escalation_rate",
    "Percentage of incidents escalated to LLM (24h rolling)",
)

inconclusive_rate = Gauge(
    "kuberca_inconclusive_rate",
    "Percentage of incidents returning INCONCLUSIVE (24h rolling)",
)

# Scheduler pattern metrics
scheduler_unknown_pattern_total = Counter(
    "kuberca_scheduler_unknown_pattern_total",
    "Total unrecognized scheduler message patterns",
)

# Degradation metrics
degraded_seconds_total = Counter(
    "kuberca_degraded_seconds_total",
    "Total seconds in degraded state",
)

# Watcher metrics
watcher_events_total = Counter(
    "kuberca_watcher_events_total",
    "Total watch events received by type",
    ["watcher", "event_type"],
)

watcher_reconnects_total = Counter(
    "kuberca_watcher_reconnects_total",
    "Total watcher reconnection attempts",
    ["watcher", "reason"],
)

watcher_relistings_total = Counter(
    "kuberca_watcher_relistings_total",
    "Total watcher relist operations",
    ["watcher"],
)

watcher_errors_total = Counter(
    "kuberca_watcher_errors_total",
    "Total watcher errors",
    ["watcher", "status_code"],
)

watcher_backoff_seconds = Histogram(
    "kuberca_watcher_backoff_seconds",
    "Watcher backoff duration in seconds",
    ["watcher"],
    buckets=(0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0),
)


def compute_rolling_rates() -> None:
    """Recompute rolling rate gauges from ``rca_requests_total`` Counter samples.

    Reads the current counter values for each ``diagnosed_by`` label
    (rule_engine, llm, inconclusive), computes percentages from the total,
    and updates the corresponding gauges.
    """
    # Collect per-label totals from the Counter's internal metrics
    counts: dict[str, float] = {}
    for metric in rca_requests_total.collect():
        for sample in metric.samples:
            if sample.name.endswith("_created"):
                continue
            label_val = sample.labels.get("diagnosed_by", "")
            if label_val:
                counts[label_val] = sample.value

    total = sum(counts.values())
    if total == 0:
        rule_engine_hit_rate.set(0.0)
        llm_escalation_rate.set(0.0)
        inconclusive_rate.set(0.0)
        return

    rule_engine_hit_rate.set(counts.get("rule_engine", 0.0) / total * 100.0)
    llm_escalation_rate.set(counts.get("llm", 0.0) / total * 100.0)
    inconclusive_rate.set(counts.get("inconclusive", 0.0) / total * 100.0)
