"""
Prefect Debugger MCP Server

Exposes two tools:

  get_flow_run_logs(flow_run_id)
      Fetches WARNING-and-above logs for a single flow run, including the full
      exception traceback, so the debugger prompt can pinpoint the root cause.

  get_recent_deployment_runs(deployment_name, limit=20)
      Returns the most recent flow runs for a named deployment (newest first),
      with state_type, start_time, and end_time per run.  Use this to check
      whether a deployment has been failing consecutively across multiple days.

Environment variables required (set all three as secrets — never hard-code them):
    PREFECT_API_KEY       — your Prefect Cloud API key
    PREFECT_ACCOUNT_ID    — account UUID from your Prefect Cloud URL
    PREFECT_WORKSPACE_ID  — workspace UUID from your Prefect Cloud URL
"""

import os

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


def _workspace_base() -> str:
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
    return f"{PREFECT_CLOUD_BASE}/accounts/{account_id}/workspaces/{workspace_id}"


def _logs_endpoint() -> str:
    return f"{_workspace_base()}/logs/filter"


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
def get_recent_deployment_runs(deployment_name: str, limit: int = 20) -> list[dict]:
    """
    Returns the most recent flow runs for a named deployment, ordered newest first.
    Each entry has: id, state_type (COMPLETED/FAILED/CRASHED), start_time, end_time.
    Use this to check if a deployment has been failing consecutively across multiple days.

    Args:
        deployment_name: The exact name of the deployment in Prefect Cloud.
        limit: Number of recent runs to return (default 20).
    """
    try:
        headers = _api_headers()
        base = _workspace_base()
    except ValueError as exc:
        return [{"error": str(exc)}]

    url = f"{base}/flow_runs/filter"
    payload = {
        "sort": "START_TIME_DESC",
        "limit": limit,
        "deployments": {
            "name": {"any_": [deployment_name]}
        },
    }

    try:
        resp = httpx.post(url, json=payload, headers=headers, timeout=15)
        resp.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return [{"error": f"Prefect API error {exc.response.status_code}: {exc.response.text}"}]
    except httpx.RequestError as exc:
        return [{"error": f"Network error reaching Prefect Cloud: {exc}"}]

    return [
        {
            "id": r["id"],
            "state_type": r.get("state_type"),
            "start_time": r.get("start_time"),
            "end_time": r.get("end_time"),
        }
        for r in resp.json()
    ]


if __name__ == "__main__":
    mcp.run(transport="stdio")
