"""KubeRCA command-line interface.

Commands:
    kuberca status [--namespace NS]          Query cluster status via REST API.
    kuberca analyze <resource> [--time-window 2h]  Trigger RCA via REST API.
    kuberca version                          Print version and exit.

All commands call the REST API at http://localhost:8080 (configurable via
``--api-url``).  Output is colourised for readability.
"""

from __future__ import annotations

import json

import click
import httpx

from kuberca import __version__

_DEFAULT_API_URL = "http://localhost:8080"

# ---------------------------------------------------------------------------
# Colour helpers
# ---------------------------------------------------------------------------

_SEVERITY_COLORS: dict[str, str] = {
    "info": "green",
    "warning": "yellow",
    "error": "red",
    "critical": "bright_red",
}

_CACHE_STATE_COLORS: dict[str, str] = {
    "ready": "green",
    "warming": "yellow",
    "partially_ready": "yellow",
    "degraded": "red",
}

_CONFIDENCE_THRESHOLDS: list[tuple[float, str]] = [
    (0.75, "green"),
    (0.5, "yellow"),
    (0.0, "red"),
]


def _confidence_color(confidence: float) -> str:
    for threshold, color in _CONFIDENCE_THRESHOLDS:
        if confidence >= threshold:
            return color
    return "red"


def _styled_severity(severity: str) -> str:
    color = _SEVERITY_COLORS.get(severity.lower(), "white")
    return click.style(severity.upper(), fg=color, bold=True)


def _styled_cache_state(state: str) -> str:
    color = _CACHE_STATE_COLORS.get(state.lower(), "white")
    return click.style(state, fg=color)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _get(api_url: str, path: str, params: dict[str, str] | None = None) -> dict[str, object]:
    """Perform a GET request and return the parsed JSON body.

    Raises click.ClickException on connection errors or non-2xx responses.
    """
    url = api_url.rstrip("/") + path
    try:
        with httpx.Client(timeout=30.0) as client:
            response = client.get(url, params=params or {})
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]
    except httpx.ConnectError as err:
        raise click.ClickException(f"Cannot connect to KubeRCA API at {api_url}. Is the server running?") from err
    except httpx.HTTPStatusError as exc:
        _handle_error_response(exc.response)
        raise  # unreachable — _handle_error_response always raises


def _post(api_url: str, path: str, body: dict[str, object]) -> dict[str, object]:
    """Perform a POST request and return the parsed JSON body.

    Raises click.ClickException on connection errors or non-2xx responses.
    """
    url = api_url.rstrip("/") + path
    try:
        with httpx.Client(timeout=60.0) as client:
            response = client.post(url, json=body)
        response.raise_for_status()
        return response.json()  # type: ignore[no-any-return]
    except httpx.ConnectError as err:
        raise click.ClickException(f"Cannot connect to KubeRCA API at {api_url}. Is the server running?") from err
    except httpx.HTTPStatusError as exc:
        _handle_error_response(exc.response)
        raise  # unreachable — _handle_error_response always raises


def _handle_error_response(response: httpx.Response) -> None:
    """Parse an error response body and raise a friendly ClickException."""
    try:
        data: dict[str, object] = response.json()
        error_code = str(data.get("error", "ERROR"))
        detail = str(data.get("detail", "Unknown error"))
        msg = f"{error_code}: {detail}"
    except Exception:  # noqa: BLE001
        msg = f"HTTP {response.status_code}: {response.text[:200]}"
    raise click.ClickException(msg)


# ---------------------------------------------------------------------------
# CLI group
# ---------------------------------------------------------------------------


@click.group()
@click.option(
    "--api-url",
    default=_DEFAULT_API_URL,
    envvar="KUBERCA_API_URL",
    show_default=True,
    help="KubeRCA REST API base URL.",
)
@click.pass_context
def cli(ctx: click.Context, api_url: str) -> None:
    """KubeRCA — Kubernetes Root Cause Analysis CLI."""
    ctx.ensure_object(dict)
    ctx.obj["api_url"] = api_url


# ---------------------------------------------------------------------------
# kuberca version
# ---------------------------------------------------------------------------


@cli.command("version")
def cmd_version() -> None:
    """Print the KubeRCA version and exit."""
    click.echo(f"kuberca {__version__}")


# ---------------------------------------------------------------------------
# kuberca status
# ---------------------------------------------------------------------------


@cli.command("status")
@click.option(
    "--namespace",
    "-n",
    default=None,
    metavar="NS",
    help="Filter to a specific namespace.  Omit for all namespaces.",
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Print raw JSON response.",
)
@click.pass_context
def cmd_status(ctx: click.Context, namespace: str | None, output_json: bool) -> None:
    """Show current cluster status (pod counts, node conditions, anomalies)."""
    api_url: str = ctx.obj["api_url"]
    params: dict[str, str] = {}
    if namespace:
        params["namespace"] = namespace

    data = _get(api_url, "/api/v1/status", params=params or None)

    if output_json:
        click.echo(json.dumps(data, indent=2))
        return

    _print_status(data)


def _print_status(data: dict[str, object]) -> None:
    """Pretty-print a ClusterStatusResponse dict."""
    cache_state = str(data.get("cache_state", "unknown"))
    click.echo(click.style("Cluster Status", bold=True) + "  cache: " + _styled_cache_state(cache_state))
    click.echo("")

    # Pod counts
    pod_counts: dict[str, int] = data.get("pod_counts", {})  # type: ignore[assignment]
    if pod_counts:
        click.echo(click.style("Pod Counts:", bold=True))
        for phase, count in sorted(pod_counts.items()):
            color = "green" if phase == "Running" else "yellow" if phase in ("Pending", "Unknown") else "red"
            styled_phase = click.style(phase, fg=color)
            padding = max(0, 18 - len(phase)) * " "
            click.echo(f"  {styled_phase}{padding} {count}")
        click.echo("")

    # Node conditions (only non-True conditions — True means healthy)
    node_conditions: list[dict[str, object]] = data.get("node_conditions", [])  # type: ignore[assignment]
    unhealthy = [c for c in node_conditions if str(c.get("status", "")) != "True"]
    if unhealthy:
        click.echo(click.style("Node Conditions (unhealthy):", bold=True, fg="yellow"))
        for cond in unhealthy:
            click.echo(
                f"  {cond.get('name', '?')}  "
                f"{click.style(str(cond.get('type', '?')), fg='yellow')}  "
                f"reason={cond.get('reason', '')}"
            )
        click.echo("")
    else:
        click.echo(click.style("All nodes healthy.", fg="green"))
        click.echo("")

    # Active anomalies
    anomalies: list[dict[str, object]] = data.get("active_anomalies", [])  # type: ignore[assignment]
    if anomalies:
        click.echo(click.style(f"Active Anomalies ({len(anomalies)}):", bold=True, fg="red"))
        for a in anomalies:
            sev = str(a.get("severity", "info"))
            click.echo(
                f"  [{_styled_severity(sev)}] "
                f"{a.get('resource_kind', '?')}/{a.get('resource_name', '?')} "
                f"({a.get('namespace', '?')}): {a.get('reason', '')}"
            )
        click.echo("")
    else:
        click.echo(click.style("No active anomalies.", fg="green"))
        click.echo("")

    # Recent events (top 10)
    events: list[dict[str, object]] = data.get("recent_events", [])  # type: ignore[assignment]
    if events:
        click.echo(click.style(f"Recent Events (showing up to 10 of {len(events)}):", bold=True))
        for evt in events[:10]:
            sev = str(evt.get("severity", "info"))
            click.echo(
                f"  [{_styled_severity(sev)}] "
                f"{evt.get('resource_kind', '?')}/{evt.get('resource_name', '?')} "
                f"({evt.get('namespace', '?')}): "
                f"{evt.get('reason', '')} — {str(evt.get('message', ''))[:80]}"
            )


# ---------------------------------------------------------------------------
# kuberca analyze
# ---------------------------------------------------------------------------


@cli.command("analyze")
@click.argument("resource")
@click.option(
    "--time-window",
    default=None,
    metavar="WINDOW",
    help=("Look-back window for analysis, e.g. '2h', '30m', '1d'.  Defaults to server configuration."),
)
@click.option(
    "--json",
    "output_json",
    is_flag=True,
    default=False,
    help="Print raw JSON response.",
)
@click.pass_context
def cmd_analyze(
    ctx: click.Context,
    resource: str,
    time_window: str | None,
    output_json: bool,
) -> None:
    """Trigger root-cause analysis for RESOURCE (Kind/namespace/name).

    Example:

        kuberca analyze Pod/default/my-pod --time-window 2h
    """
    api_url: str = ctx.obj["api_url"]

    # Validate locally before making the network call to give instant feedback.
    parts = resource.split("/")
    if len(parts) != 3 or not all(parts):
        raise click.UsageError(f"RESOURCE must be in Kind/namespace/name format, got: {resource!r}")

    body: dict[str, object] = {"resource": resource}
    if time_window:
        body["time_window"] = time_window

    if not output_json:
        click.echo(
            click.style("Analyzing", bold=True)
            + f" {resource}"
            + (f" (window: {time_window})" if time_window else "")
            + " ..."
        )

    data = _post(api_url, "/api/v1/analyze", body)

    if output_json:
        click.echo(json.dumps(data, indent=2))
        return

    _print_rca(data)


def _print_rca(data: dict[str, object]) -> None:
    """Pretty-print an RCAResponseSchema dict."""
    raw_confidence = data.get("confidence", 0.0)
    confidence = float(raw_confidence) if isinstance(raw_confidence, int | float | str) else 0.0
    diagnosed_by = str(data.get("diagnosed_by", "?"))
    root_cause = str(data.get("root_cause", ""))
    rule_id = data.get("rule_id")
    remediation = str(data.get("suggested_remediation", ""))

    conf_pct = f"{confidence * 100:.0f}%"
    conf_str = click.style(conf_pct, fg=_confidence_color(confidence), bold=True)

    click.echo("")
    click.echo(click.style("Root Cause Analysis", bold=True, underline=True))
    click.echo("")
    click.echo(f"  {click.style('Root Cause:', bold=True)}      {root_cause}")
    click.echo(f"  {click.style('Confidence:', bold=True)}      {conf_str}")
    click.echo(
        f"  {click.style('Diagnosed By:', bold=True)}    {diagnosed_by}" + (f"  (rule: {rule_id})" if rule_id else "")
    )

    evidence: list[dict[str, object]] = data.get("evidence", [])  # type: ignore[assignment]
    if evidence:
        click.echo("")
        click.echo(click.style(f"Evidence ({len(evidence)} items):", bold=True))
        for item in evidence:
            ev_type = str(item.get("type", "?"))
            ev_time = str(item.get("timestamp", ""))
            ev_summary = str(item.get("summary", ""))
            click.echo(f"  [{click.style(ev_type, fg='cyan')}] {ev_time}  {ev_summary}")

    affected: list[dict[str, object]] = data.get("affected_resources", [])  # type: ignore[assignment]
    if affected:
        click.echo("")
        click.echo(click.style(f"Affected Resources ({len(affected)}):", bold=True))
        for res in affected:
            click.echo(f"  {res.get('kind', '?')}/{res.get('name', '?')} ({res.get('namespace', '?')})")

    if remediation:
        click.echo("")
        click.echo(click.style("Suggested Remediation:", bold=True))
        click.echo(f"  {remediation}")

    meta: dict[str, object] | None = data.get("meta")  # type: ignore[assignment]
    if meta:
        click.echo("")
        warnings: list[object] = meta.get("warnings", [])  # type: ignore[assignment]
        if warnings:
            for warn in warnings:
                click.echo(click.style(f"  Warning: {warn}", fg="yellow"))
        resp_ms = meta.get("response_time_ms", 0)
        cache = meta.get("cache_state", "?")
        click.echo(
            click.style("  Meta:", fg="bright_black")
            + f" response_time={resp_ms}ms  cache={_styled_cache_state(str(cache))}"
        )

    click.echo("")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    cli()
