"""MCP stdio server for KubeRCA.

Exposes two tools to AI assistants via the Model Context Protocol:

``k8s_cluster_status``
    Returns a JSON snapshot of current cluster health (pod counts, node
    conditions, active anomalies, recent events, cache state).

``k8s_analyze_incident``
    Triggers root-cause analysis and returns the RCAResponse as JSON.

Transport: stdio (read from stdin, write to stdout).

Usage::

    from kuberca.mcp.server import MCPServer

    server = MCPServer(coordinator=coordinator, cache=cache)
    await server.start()  # blocks until stdin is closed
"""

from __future__ import annotations

import dataclasses
import json
from typing import Any

import structlog
from mcp.server import NotificationOptions, Server
from mcp.server.models import InitializationOptions
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from kuberca import __version__
from kuberca.api.schemas import ClusterStatus

_log = structlog.get_logger(component="mcp.server")

_SERVER_NAME = "kuberca"


class MCPServer:
    """MCP stdio server wrapping the KubeRCA coordinator and cache.

    Args:
        coordinator: AnalystCoordinator with an ``analyze()`` method.
        cache:       ResourceCache used to build cluster status snapshots.
    """

    def __init__(self, coordinator: Any, cache: Any) -> None:
        self._coordinator = coordinator
        self._cache = cache
        self._server = Server(_SERVER_NAME)
        self._register_handlers()

    def _register_handlers(self) -> None:
        """Wire list-tools and call-tool handlers onto the MCP Server."""

        @self._server.list_tools()  # type: ignore[no-untyped-call, untyped-decorator]
        async def _list_tools() -> list[Tool]:
            return [
                Tool(
                    name="k8s_cluster_status",
                    description=(
                        "Returns a JSON snapshot of the current Kubernetes cluster "
                        "state: pod phase counts, node conditions, active anomalies, "
                        "recent events, and cache readiness."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "namespace": {
                                "type": "string",
                                "description": ("Optional namespace filter.  Omit to include all namespaces."),
                            },
                        },
                        "additionalProperties": False,
                    },
                ),
                Tool(
                    name="k8s_analyze_incident",
                    description=(
                        "Triggers root-cause analysis for a Kubernetes resource.  "
                        "Returns an RCAResponse JSON object with root_cause, "
                        "confidence, evidence, affected_resources, and remediation."
                    ),
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "resource": {
                                "type": "string",
                                "description": ("Resource in Kind/namespace/name format, e.g. 'Pod/default/my-pod'."),
                            },
                            "time_window": {
                                "type": "string",
                                "description": (
                                    "Look-back window, e.g. '2h', '30m', '1d'.  Defaults to server configuration."
                                ),
                            },
                        },
                        "required": ["resource"],
                        "additionalProperties": False,
                    },
                ),
            ]

        @self._server.call_tool()  # type: ignore[untyped-decorator]
        async def _call_tool(
            name: str,
            arguments: dict[str, Any] | None,
        ) -> list[TextContent]:
            args = arguments or {}
            if name == "k8s_cluster_status":
                return await self._handle_cluster_status(args)
            if name == "k8s_analyze_incident":
                return await self._handle_analyze_incident(args)
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"error": f"Unknown tool: {name}"}),
                )
            ]

    async def _handle_cluster_status(
        self,
        args: dict[str, Any],
    ) -> list[TextContent]:
        """Build and return ClusterStatus as a JSON TextContent."""
        namespace: str | None = args.get("namespace")
        try:
            # Reuse the same helper used by the REST API
            from kuberca.api.routes import _build_cluster_status

            status: ClusterStatus = await _build_cluster_status(self._cache, namespace)
            payload = dataclasses.asdict(status)
            return [TextContent(type="text", text=json.dumps(payload))]
        except Exception as exc:
            _log.error("mcp_cluster_status_error", error=str(exc))
            return [
                TextContent(
                    type="text",
                    text=json.dumps({"isError": True, "error": "INTERNAL_ERROR", "detail": str(exc)}),
                )
            ]

    async def _handle_analyze_incident(
        self,
        args: dict[str, Any],
    ) -> list[TextContent]:
        """Run RCA and return RCAResponse as a JSON TextContent."""
        resource: str = args.get("resource", "")
        time_window: str | None = args.get("time_window")

        if not resource:
            return [
                TextContent(
                    type="text",
                    text=json.dumps(
                        {
                            "isError": True,
                            "error": "INVALID_RESOURCE_FORMAT",
                            "detail": "'resource' argument is required",
                        }
                    ),
                )
            ]

        parts = resource.split("/")
        if len(parts) != 3 or not all(parts):
            return [
                TextContent(
                    type="text",
                    text=json.dumps(
                        {
                            "isError": True,
                            "error": "INVALID_RESOURCE_FORMAT",
                            "detail": (f"resource must be Kind/namespace/name, got: {resource!r}"),
                        }
                    ),
                )
            ]

        resource_kind, namespace, resource_name = parts[0], parts[1], parts[2]

        resource_str = f"{resource_kind}/{namespace}/{resource_name}"
        try:
            rca = await self._coordinator.analyze(
                resource=resource_str,
                time_window=time_window or "2h",
            )

            payload = _rca_response_to_dict(rca)
            return [TextContent(type="text", text=json.dumps(payload))]
        except Exception as exc:
            _log.error(
                "mcp_analyze_incident_error",
                resource=resource,
                error=str(exc),
            )
            return [
                TextContent(
                    type="text",
                    text=json.dumps(
                        {
                            "isError": True,
                            "error": "INTERNAL_ERROR",
                            "detail": str(exc),
                        }
                    ),
                )
            ]

    async def start(self) -> None:
        """Run the MCP server until stdin is closed.

        Blocks the calling coroutine; intended to run as a background task.
        """
        _log.info("mcp_server_starting", version=__version__)
        init_options = InitializationOptions(
            server_name=_SERVER_NAME,
            server_version=__version__,
            capabilities=self._server.get_capabilities(
                notification_options=NotificationOptions(),
                experimental_capabilities={},
            ),
        )
        async with stdio_server() as (read_stream, write_stream):
            await self._server.run(read_stream, write_stream, init_options)

        _log.info("mcp_server_stopped")


# ---------------------------------------------------------------------------
# Serialisation helpers
# ---------------------------------------------------------------------------


def _rca_response_to_dict(rca: Any) -> dict[str, Any]:
    """Convert an RCAResponse dataclass to a plain JSON-serialisable dict."""
    evidence = [
        {
            "type": e.type.value if hasattr(e.type, "value") else e.type,
            "timestamp": e.timestamp,
            "summary": e.summary,
            "debug_context": e.debug_context,
        }
        for e in rca.evidence
    ]

    affected_resources = [{"kind": a.kind, "namespace": a.namespace, "name": a.name} for a in rca.affected_resources]

    result: dict[str, Any] = {
        "root_cause": rca.root_cause,
        "confidence": rca.confidence,
        "diagnosed_by": rca.diagnosed_by.value if hasattr(rca.diagnosed_by, "value") else rca.diagnosed_by,
        "rule_id": rca.rule_id,
        "evidence": evidence,
        "affected_resources": affected_resources,
        "suggested_remediation": rca.suggested_remediation,
    }

    if rca._meta is not None:
        result["_meta"] = {
            "kuberca_version": rca._meta.kuberca_version,
            "schema_version": rca._meta.schema_version,
            "cluster_id": rca._meta.cluster_id,
            "timestamp": rca._meta.timestamp,
            "response_time_ms": rca._meta.response_time_ms,
            "cache_state": rca._meta.cache_state,
            "warnings": rca._meta.warnings,
        }

    return result
