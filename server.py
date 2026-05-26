"""
Prefect Debugger MCP Server

Exposes a single tool — get_flow_run_logs — that fetches the full logs
for a Prefect Cloud flow run given its flow_run_id.  The Cursor Automation
calls this tool to retrieve the traceback and error context before running
the debugger prompt.

Environment variables required (set all three as secrets — never hard-code them):
    PREFECT_API_KEY       — your Prefect Cloud API key
    PREFECT_ACCOUNT_ID    — account UUID from your Prefect Cloud URL
    PREFECT_WORKSPACE_ID  — workspace UUID from your Prefect Cloud URL
"""

import os
from datetime import datetime, timedelta, timezone

import httpx
from fastmcp import FastMCP

mcp = FastMCP("Prefect Debugger")

PREFECT_CLOUD_BASE = "https://api.prefect.cloud/api"

# Log levels: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
# Fetch WARNING and above to keep the output focused on failures.
MIN_LOG_LEVEL = 30
# Cap at 200 lines to stay within context limits while capturing full tracebacks.
MAX_LOG_LINES = 200


def _api_headers() -> dict[str, str]:
    api_key = os.environ.get("PREFECT_API_KEY", "")
    if not api_key:
        raise ValueError(
            "PREFECT_API_KEY environment variable is not set. "
            "Add it to your Cursor Cloud Agents dashboard under Secrets."
        )
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _logs_endpoint() -> str:
    account_id = os.environ.get("PREFECT_ACCOUNT_ID", "")
    workspace_id = os.environ.get("PREFECT_WORKSPACE_ID", "")
    if not account_id:
        raise ValueError(
            "PREFECT_ACCOUNT_ID environment variable is not set. "
            "Add it to your Cursor Cloud Agents dashboard under Secrets."
        )
    if not workspace_id:
        raise ValueError(
            "PREFECT_WORKSPACE_ID environment variable is not set. "
            "Add it to your Cursor Cloud Agents dashboard under Secrets."
        )
    return (
        f"{PREFECT_CLOUD_BASE}/accounts/{account_id}"
        f"/workspaces/{workspace_id}/logs/filter"
    )


def _flow_runs_endpoint() -> str:
    account_id = os.environ.get("PREFECT_ACCOUNT_ID", "")
    workspace_id = os.environ.get("PREFECT_WORKSPACE_ID", "")
    if not account_id:
        raise ValueError(
            "PREFECT_ACCOUNT_ID environment variable is not set. "
            "Add it to your Cursor Cloud Agents dashboard under Secrets."
        )
    if not workspace_id:
        raise ValueError(
            "PREFECT_WORKSPACE_ID environment variable is not set. "
            "Add it to your Cursor Cloud Agents dashboard under Secrets."
        )
    return (
        f"{PREFECT_CLOUD_BASE}/accounts/{account_id}"
        f"/workspaces/{workspace_id}/flow_runs/filter"
    )


def _flow_run_url(flow_run_id: str) -> str:
    account_id = os.environ.get("PREFECT_ACCOUNT_ID", "")
    workspace_id = os.environ.get("PREFECT_WORKSPACE_ID", "")
    return (
        f"https://app.prefect.cloud/account/{account_id}"
        f"/workspace/{workspace_id}/flow-run/{flow_run_id}"
    )


@mcp.tool()
def get_flow_run_logs(flow_run_id: str) -> str:
    """
    Fetch logs for a Prefect Cloud flow run.

    Returns WARNING-and-above log lines (level >= 30) for the given
    flow_run_id, ordered by timestamp ascending.  The output includes
    the full exception traceback so the debugger prompt can pinpoint
    the root cause.

    Args:
        flow_run_id: The UUID of the flow run, e.g.
                     '069afc1d-d409-761d-8000-03761d6bc319'.
                     Extract this from the Slack notification URL — it is
                     the UUID that appears after /flow-run/ in the URL.
    """
    try:
        headers = _api_headers()
        endpoint = _logs_endpoint()
    except ValueError as exc:
        return f"Configuration error: {exc}"

    payload = {
        "logs": {
            "flow_run_id": {"any_": [flow_run_id]},
            "level": {"ge_": MIN_LOG_LEVEL},
        },
        "sort": "TIMESTAMP_ASC",
        "limit": MAX_LOG_LINES,
        "offset": 0,
    }

    try:
        response = httpx.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return (
            f"Prefect API error {exc.response.status_code}: {exc.response.text}\n"
            f"Check that PREFECT_API_KEY is valid and the flow_run_id is correct."
        )
    except httpx.RequestError as exc:
        return f"Network error reaching Prefect Cloud: {exc}"

    logs = response.json()

    if not logs:
        # Re-try with no level filter — the flow may have only INFO logs
        payload["logs"] = {"flow_run_id": {"any_": [flow_run_id]}}  # type: ignore[assignment]
        try:
            response = httpx.post(endpoint, headers=headers, json=payload, timeout=30)
            response.raise_for_status()
            logs = response.json()
        except (httpx.HTTPStatusError, httpx.RequestError):
            pass

    if not logs:
        return (
            f"No logs found for flow_run_id={flow_run_id}.\n"
            "Verify the UUID is correct and that the flow run exists in your workspace."
        )

    level_names = {10: "DEBUG", 20: "INFO", 30: "WARNING", 40: "ERROR", 50: "CRITICAL"}

    lines: list[str] = []
    for entry in logs:
        level_int = entry.get("level", 20)
        level_str = level_names.get(level_int, str(level_int))
        timestamp = entry.get("timestamp", "")[:19].replace("T", " ")
        message = entry.get("message", "")
        lines.append(f"[{timestamp}] {level_str}: {message}")

    return "\n".join(lines)


@mcp.tool()
def search_recent_failed_flow_runs(
    hours: int = 24,
    limit: int = 50,
    state_types: list[str] | None = None,
) -> list[dict]:
    """
    Search the workspace for flow runs that failed within the last `hours`.

    Returns the most recent failures first, each entry containing the
    flow_run_id, name, deployment_id, state, start/end times, and a direct
    Prefect Cloud URL so you can drill into the run.

    Args:
        hours: How far back to look, in hours (default 24).
        limit: Maximum number of flow runs to return (default 50).
        state_types: Which terminal states to consider failures.
                     Defaults to ["FAILED", "CRASHED"]. Pass e.g.
                     ["FAILED", "CRASHED", "TIMED_OUT"] to widen the search.
    """
    if state_types is None:
        state_types = ["FAILED", "CRASHED"]

    try:
        headers = _api_headers()
        endpoint = _flow_runs_endpoint()
    except ValueError as exc:
        return [{"error": f"Configuration error: {exc}"}]

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    payload = {
        "flow_runs": {
            "state": {"type": {"any_": state_types}},
            "start_time": {"after_": cutoff},
        },
        "sort": "START_TIME_DESC",
        "limit": limit,
        "offset": 0,
    }

    try:
        response = httpx.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return [
            {
                "error": (
                    f"Prefect API error {exc.response.status_code}: "
                    f"{exc.response.text}"
                )
            }
        ]
    except httpx.RequestError as exc:
        return [{"error": f"Network error reaching Prefect Cloud: {exc}"}]

    runs = response.json() or []

    results: list[dict] = []
    for run in runs:
        state = run.get("state") or {}
        run_id = run.get("id", "")
        results.append(
            {
                "id": run_id,
                "name": run.get("name", ""),
                "deployment_id": run.get("deployment_id"),
                "state_type": state.get("type"),
                "state_name": state.get("name"),
                "start_time": run.get("start_time"),
                "end_time": run.get("end_time"),
                "flow_run_url": _flow_run_url(run_id) if run_id else "",
            }
        )

    return results


if __name__ == "__main__":
    mcp.run(transport="stdio")
