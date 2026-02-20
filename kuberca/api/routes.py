"""FastAPI route handlers for the KubeRCA REST API.

All routes are registered on a single APIRouter that ``app.py`` mounts
under the ``/api/v1`` prefix.

Error code conventions:
    400 INVALID_RESOURCE_FORMAT  -- resource path doesn't match Kind/ns/name
    400 INVALID_TIME_WINDOW      -- time_window doesn't match \\d+(m|h|d)
    404 RESOURCE_NOT_FOUND       -- named resource absent from the cache
    429 QUEUE_FULL               -- analysis queue has no capacity
    500 INTERNAL_ERROR           -- unexpected server-side failure
    (503 is never returned; LLM unavailability is surfaced through RCAResponse)
"""

from __future__ import annotations

from typing import Any

import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from kuberca.api.schemas import (
    AnalyzeRequest,
    ClusterStatus,
    ClusterStatusResponse,
    ErrorResponse,
    HealthStatus,
    RCAResponseSchema,
    RuleEngineCoverage,
)
from kuberca.models.analysis import RCAResponse
from kuberca.observability.metrics import (
    inconclusive_rate,
    llm_escalation_rate,
    rca_requests_total,
    rule_engine_hit_rate,
    rule_matches_total,
)

_log = structlog.get_logger(component="api.routes")

router = APIRouter()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rca_to_schema(rca: RCAResponse) -> RCAResponseSchema:
    """Convert an RCAResponse dataclass to its Pydantic schema."""
    from kuberca.api.schemas import (
        AffectedResourceResponse,
        EvidenceItemResponse,
        ResponseMetaResponse,
    )

    evidence = [
        EvidenceItemResponse(
            type=e.type.value,
            timestamp=e.timestamp,
            summary=e.summary,
            debug_context=e.debug_context,
        )
        for e in rca.evidence
    ]

    affected = [
        AffectedResourceResponse(kind=a.kind, namespace=a.namespace, name=a.name) for a in rca.affected_resources
    ]

    meta: ResponseMetaResponse | None = None
    if rca._meta is not None:
        meta = ResponseMetaResponse(
            kuberca_version=rca._meta.kuberca_version,
            schema_version=rca._meta.schema_version,
            cluster_id=rca._meta.cluster_id,
            timestamp=rca._meta.timestamp,
            response_time_ms=rca._meta.response_time_ms,
            cache_state=rca._meta.cache_state,
            warnings=rca._meta.warnings,
        )

    blast_radius = None
    if rca.blast_radius:
        blast_radius = [
            AffectedResourceResponse(kind=a.kind, namespace=a.namespace, name=a.name) for a in rca.blast_radius
        ]

    return RCAResponseSchema(
        root_cause=rca.root_cause,
        confidence=rca.confidence,
        diagnosed_by=rca.diagnosed_by.value,
        rule_id=rca.rule_id,
        evidence=evidence,
        affected_resources=affected,
        blast_radius=blast_radius,
        suggested_remediation=rca.suggested_remediation,
        meta=meta,
    )


def _cluster_status_to_response(
    status: ClusterStatus,
    coverage: RuleEngineCoverage | None = None,
) -> ClusterStatusResponse:
    """Convert ClusterStatus dataclass to its Pydantic schema."""
    return ClusterStatusResponse(
        pod_counts=status.pod_counts,
        node_conditions=status.node_conditions,
        active_anomalies=status.active_anomalies,
        recent_events=status.recent_events,
        cache_state=status.cache_state,
        rule_engine_coverage=coverage,
    )


async def _build_cluster_status(
    cache: Any,
    namespace: str | None,
) -> ClusterStatus:
    """Assemble a ClusterStatus from live cache state.

    ``cache`` is the ResourceCache (untyped to avoid circular imports at the
    routes layer; the cache protocol is duck-typed here).
    """
    pod_counts: dict[str, int] = {}
    node_conditions: list[dict[str, object]] = []
    active_anomalies: list[dict[str, object]] = []
    recent_events: list[dict[str, object]] = []
    cache_state = "ready"

    # cache_readiness_state
    state_attr = getattr(cache, "readiness_state", None)
    if state_attr is not None:
        cache_state = str(state_attr.value) if hasattr(state_attr, "value") else str(state_attr)

    # pod counts grouped by phase
    list_pods = getattr(cache, "list_pods", None)
    if callable(list_pods):
        try:
            pods = list_pods(namespace=namespace) if namespace else list_pods()
            for pod in pods:
                phase = str(pod.status.get("phase", "Unknown")) if pod.status else "Unknown"
                pod_counts[phase] = pod_counts.get(phase, 0) + 1
        except Exception as exc:  # noqa: BLE001
            _log.warning("cluster_status_pod_list_failed", error=str(exc))

    # node conditions
    list_nodes = getattr(cache, "list_nodes", None)
    if callable(list_nodes):
        try:
            nodes = list_nodes()
            for node in nodes:
                conditions = node.status.get("conditions", []) if node.status else []
                for cond in conditions:
                    node_conditions.append(
                        {
                            "name": node.name,
                            "type": cond.get("type", ""),
                            "status": cond.get("status", ""),
                            "reason": cond.get("reason", ""),
                        }
                    )
        except Exception as exc:  # noqa: BLE001
            _log.warning("cluster_status_node_list_failed", error=str(exc))

    # recent events (up to 50)
    list_events = getattr(cache, "list_events", None)
    if callable(list_events):
        try:
            evts = list_events(namespace=namespace, limit=50) if namespace else list_events(limit=50)
            for evt in evts:
                recent_events.append(
                    {
                        "namespace": evt.namespace,
                        "resource_kind": evt.resource_kind,
                        "resource_name": evt.resource_name,
                        "reason": evt.reason,
                        "message": evt.message,
                        "severity": evt.severity.value,
                        "last_seen": evt.last_seen.isoformat(),
                        "count": evt.count,
                    }
                )
        except Exception as exc:  # noqa: BLE001
            _log.warning("cluster_status_event_list_failed", error=str(exc))

    return ClusterStatus(
        pod_counts=pod_counts,
        node_conditions=node_conditions,
        active_anomalies=active_anomalies,
        recent_events=recent_events,
        cache_state=cache_state,
    )


def _get_registered_rule_count(request: Request) -> int:
    """Get the number of registered rules from the coordinator's rule engine."""
    try:
        coordinator = getattr(request.app.state, "coordinator", None)
        if coordinator is None:
            return 0
        rule_engine = getattr(coordinator, "_rule_engine", None)
        if rule_engine is None:
            return 0
        rules = getattr(rule_engine, "_rules", None)
        if isinstance(rules, list | dict):
            return len(rules)
        return 0
    except Exception:
        return 0


def _get_top_rules_matched(limit: int = 5) -> list[str]:
    """Return the top N rule IDs ordered by match frequency (descending).

    Reads from the ``rule_matches_total`` Prometheus Counter, which is
    labelled by ``rule_id``.
    """
    try:
        counts: list[tuple[str, float]] = []
        # Iterate over the Counter's metric samples
        for metric in rule_matches_total.collect():
            for sample in metric.samples:
                if sample.name == "kuberca_rule_matches_total":
                    rule_id = sample.labels.get("rule_id", "")
                    if rule_id:
                        counts.append((rule_id, sample.value))
        counts.sort(key=lambda x: x[1], reverse=True)
        return [rule_id for rule_id, _ in counts[:limit]]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get(
    "/status",
    response_model=ClusterStatusResponse,
    summary="Get cluster status",
    description=(
        "Returns a point-in-time snapshot of the cluster including pod phase "
        "counts, node conditions, active anomalies, and recent events."
    ),
)
async def get_status(
    request: Request,
    namespace: str | None = None,
) -> ClusterStatusResponse:
    """``GET /api/v1/status?namespace={ns}``"""
    cache = request.app.state.cache
    try:
        status = await _build_cluster_status(cache, namespace)

        # Collect rule engine coverage from Prometheus gauges
        coverage = RuleEngineCoverage(
            hit_rate=rule_engine_hit_rate._value.get(),
            escalation_rate=llm_escalation_rate._value.get(),
            inconclusive_rate=inconclusive_rate._value.get(),
            total_rules=_get_registered_rule_count(request),
            top_rules_matched=_get_top_rules_matched(),
        )

        return _cluster_status_to_response(status, coverage=coverage)
    except Exception as exc:
        _log.error("status_endpoint_error", error=str(exc))
        return JSONResponse(  # type: ignore[return-value]
            status_code=500,
            content=ErrorResponse(
                error="INTERNAL_ERROR",
                detail="An unexpected error occurred.",
            ).model_dump(),
        )


@router.post(
    "/analyze",
    response_model=RCAResponseSchema,
    summary="Analyze a resource incident",
    description=(
        "Triggers root-cause analysis for the specified resource.  "
        "Returns the analysis result synchronously; LLM unavailability "
        "is surfaced in the response body, never as an error status."
    ),
    responses={
        400: {"model": ErrorResponse},
        404: {"model": ErrorResponse},
        429: {"model": ErrorResponse},
        500: {"model": ErrorResponse},
    },
)
async def post_analyze(
    request: Request,
    body: AnalyzeRequest,
) -> RCAResponseSchema:
    """``POST /api/v1/analyze``"""
    coordinator = request.app.state.coordinator

    # AnalyzeRequest.validate_resource_format already validated the format;
    # ValidationError from Pydantic is caught by FastAPI and returned as 422.
    # We separately map it to our 400 schema via the exception handler in app.py.
    parts = body.resource.split("/")
    resource_kind, namespace, resource_name = parts[0], parts[1], parts[2]

    # Check resource exists in cache (best-effort; cache may be warming)
    cache = request.app.state.cache
    get_resource = getattr(cache, "get", None)
    if callable(get_resource):
        try:
            resource = get_resource(
                kind=resource_kind,
                namespace=namespace,
                name=resource_name,
            )
            if resource is None:
                return JSONResponse(  # type: ignore[return-value]
                    status_code=404,
                    content=ErrorResponse(
                        error="RESOURCE_NOT_FOUND",
                        detail=(f"{resource_kind} '{resource_name}' not found in namespace '{namespace}'"),
                    ).model_dump(),
                )
        except Exception as exc:  # noqa: BLE001
            _log.warning("cache_lookup_failed", error=str(exc))

    # Check analysis queue capacity
    queue = getattr(request.app.state, "analysis_queue", None)
    if queue is not None:
        is_full = getattr(queue, "is_full", None)
        if callable(is_full) and is_full():
            return JSONResponse(  # type: ignore[return-value]
                status_code=429,
                content=ErrorResponse(
                    error="QUEUE_FULL",
                    detail="Analysis queue is at capacity; retry after a moment.",
                ).model_dump(),
            )

    resource_str = f"{resource_kind}/{namespace}/{resource_name}"
    try:
        rca: RCAResponse = await coordinator.analyze(
            resource=resource_str,
            time_window=body.time_window or "2h",
        )
        rca_requests_total.labels(diagnosed_by=rca.diagnosed_by.value).inc()
        return _rca_to_schema(rca)
    except Exception as exc:
        _log.error(
            "analyze_endpoint_error",
            resource=body.resource,
            error=str(exc),
        )
        return JSONResponse(  # type: ignore[return-value]
            status_code=500,
            content=ErrorResponse(
                error="INTERNAL_ERROR",
                detail="An unexpected error occurred.",
            ).model_dump(),
        )


@router.get(
    "/health",
    response_model=HealthStatus,
    summary="Health check",
    description="Lightweight liveness probe.  Always returns 200 if the process is up.",
)
async def get_health(request: Request) -> HealthStatus:
    """``GET /api/v1/health``"""
    from kuberca import __version__

    cache = request.app.state.cache
    cache_state = "ready"
    state_attr = getattr(cache, "readiness_state", None)
    if state_attr is not None:
        cache_state = str(state_attr.value) if hasattr(state_attr, "value") else str(state_attr)

    return HealthStatus(
        status="ok",
        version=__version__,
        cache_state=cache_state,
    )
