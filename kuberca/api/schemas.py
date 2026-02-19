"""Pydantic request/response models for the KubeRCA REST API.

All models use Pydantic v2 syntax.  Field descriptions are also used
by FastAPI to generate the OpenAPI spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pydantic import BaseModel, Field, field_validator

# ---------------------------------------------------------------------------
# ClusterStatus (dataclass — also used by the MCP server)
# ---------------------------------------------------------------------------


@dataclass
class ClusterStatus:
    """Point-in-time snapshot of cluster health exposed by the API and MCP.

    Attributes:
        pod_counts:       Mapping of pod phase (e.g. ``"Running"``) to count.
        node_conditions:  List of node condition dicts, one entry per node.
                          Each dict has keys: ``name``, ``type``, ``status``,
                          ``reason`` (optional).
        active_anomalies: Serialised AnomalyAlert dicts currently active.
        recent_events:    Up to 50 most-recent EventRecord dicts.
        cache_state:      Current CacheReadiness value as a string.
    """

    pod_counts: dict[str, int] = field(default_factory=dict)
    node_conditions: list[dict[str, object]] = field(default_factory=list)
    active_anomalies: list[dict[str, object]] = field(default_factory=list)
    recent_events: list[dict[str, object]] = field(default_factory=list)
    cache_state: str = "ready"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class AnalyzeRequest(BaseModel):
    """Request body for ``POST /api/v1/analyze``."""

    resource: str = Field(
        ...,
        description=("Kubernetes resource in ``kind/namespace/name`` format, e.g. ``Pod/default/my-pod``."),
        examples=["Pod/default/my-pod", "Deployment/production/api-server"],
    )
    time_window: str | None = Field(
        default=None,
        description=(
            "Analysis look-back window.  Accepts integers followed by "
            "``m`` (minutes), ``h`` (hours), or ``d`` (days).  "
            "Example: ``2h``.  Defaults to the server-configured value."
        ),
        examples=["2h", "30m", "1d"],
    )

    @field_validator("resource")
    @classmethod
    def validate_resource_format(cls, value: str) -> str:
        """Ensure resource is ``Kind/namespace/name``."""
        parts = value.split("/")
        if len(parts) != 3 or not all(parts):
            raise ValueError(f"resource must be in Kind/namespace/name format, got: {value!r}")
        return value

    @field_validator("time_window")
    @classmethod
    def validate_time_window_format(cls, value: str | None) -> str | None:
        """Ensure time_window matches ``\\d+(m|h|d)``."""
        if value is None:
            return value
        import re

        if not re.match(r"^\d+(m|h|d)$", value):
            raise ValueError(f"time_window must be a positive integer followed by m, h, or d, got: {value!r}")
        return value


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class HealthStatus(BaseModel):
    """Response body for ``GET /api/v1/health``."""

    status: str = Field(
        ...,
        description="Always ``ok`` while the process is running.",
        examples=["ok"],
    )
    version: str = Field(
        ...,
        description="KubeRCA version string.",
        examples=["0.1.0"],
    )
    cache_state: str = Field(
        ...,
        description="Current cache readiness state.",
        examples=["ready", "warming", "partially_ready", "degraded"],
    )


class ErrorResponse(BaseModel):
    """Standard error envelope returned for all 4xx and 5xx responses."""

    error: str = Field(
        ...,
        description="Machine-readable error code.",
        examples=[
            "INVALID_RESOURCE_FORMAT",
            "INVALID_TIME_WINDOW",
            "RESOURCE_NOT_FOUND",
            "QUEUE_FULL",
            "INTERNAL_ERROR",
        ],
    )
    detail: str = Field(
        ...,
        description="Human-readable description of the error.",
        examples=["resource must be in Kind/namespace/name format"],
    )


class EvidenceItemResponse(BaseModel):
    """Serialised EvidenceItem for API responses."""

    type: str
    timestamp: str
    summary: str
    debug_context: dict[str, object] | None = None


class AffectedResourceResponse(BaseModel):
    """Serialised AffectedResource for API responses."""

    kind: str
    namespace: str
    name: str


class ResponseMetaResponse(BaseModel):
    """Serialised ResponseMeta for API responses."""

    kuberca_version: str
    schema_version: str
    cluster_id: str
    timestamp: str
    response_time_ms: int
    cache_state: str
    warnings: list[str]


class RCAResponseSchema(BaseModel):
    """Serialised RCAResponse returned by ``POST /api/v1/analyze``."""

    root_cause: str
    confidence: float
    diagnosed_by: str
    rule_id: str | None = None
    evidence: list[EvidenceItemResponse] = Field(default_factory=list)
    affected_resources: list[AffectedResourceResponse] = Field(default_factory=list)
    blast_radius: list[AffectedResourceResponse] | None = None
    suggested_remediation: str = ""
    meta: ResponseMetaResponse | None = None


class RuleEngineCoverage(BaseModel):
    """Rule engine coverage metrics (rolling 24h window)."""

    hit_rate: float = Field(
        default=0.0,
        description="Percentage of incidents matched by the rule engine (24h rolling).",
    )
    escalation_rate: float = Field(
        default=0.0,
        description="Percentage of incidents escalated to LLM (24h rolling).",
    )
    inconclusive_rate: float = Field(
        default=0.0,
        description="Percentage of incidents returning INCONCLUSIVE (24h rolling).",
    )
    total_rules: int = Field(
        default=0,
        description="Total number of registered rules.",
    )
    top_rules_matched: list[str] = Field(
        default_factory=list,
        description="Top 5 rule IDs ordered by match frequency (descending).",
    )


class ClusterStatusResponse(BaseModel):
    """Serialised ClusterStatus for ``GET /api/v1/status``."""

    pod_counts: dict[str, int]
    node_conditions: list[dict[str, object]]
    active_anomalies: list[dict[str, object]]
    recent_events: list[dict[str, object]]
    cache_state: str
    rule_engine_coverage: RuleEngineCoverage | None = None
