"""
Prefect Debugger MCP Server

Exposes read-only Prefect Cloud tools for flow-run logs, granular flow and task
run searches, deployment lookups, artifacts, counts, and failure summaries.

Environment variables required (set all three as secrets — never hard-code them):
    PREFECT_API_KEY       — your Prefect Cloud API key
    PREFECT_ACCOUNT_ID    — account UUID from your Prefect Cloud URL
    PREFECT_WORKSPACE_ID  — workspace UUID from your Prefect Cloud URL
"""

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from fastmcp import FastMCP

mcp = FastMCP("Prefect Debugger")

PREFECT_CLOUD_BASE = "https://api.prefect.cloud/api"

# Log levels: DEBUG=10, INFO=20, WARNING=30, ERROR=40, CRITICAL=50
# Fetch WARNING and above to keep the output focused on failures.
MIN_LOG_LEVEL = 30
# Cap at 200 lines to stay within context limits while capturing full tracebacks.
MAX_LOG_LINES = 200
VALID_FLOW_RUN_SORTS = {
    "ID_DESC",
    "START_TIME_ASC",
    "START_TIME_DESC",
    "EXPECTED_START_TIME_ASC",
    "EXPECTED_START_TIME_DESC",
    "NAME_ASC",
    "NAME_DESC",
    "NEXT_SCHEDULED_START_TIME_ASC",
    "END_TIME_DESC",
}
VALID_TASK_RUN_SORTS = {
    "ID_DESC",
    "EXPECTED_START_TIME_ASC",
    "EXPECTED_START_TIME_DESC",
    "NAME_ASC",
    "NAME_DESC",
    "NEXT_SCHEDULED_START_TIME_ASC",
    "END_TIME_DESC",
}
VALID_DEPLOYMENT_SORTS = {
    "CREATED_DESC",
    "UPDATED_DESC",
    "NAME_ASC",
    "NAME_DESC",
}
VALID_ARTIFACT_SORTS = {
    "CREATED_DESC",
    "UPDATED_DESC",
    "ID_DESC",
    "KEY_DESC",
    "KEY_ASC",
}


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
    return _workspace_api_url("/logs/filter")


def _flow_runs_endpoint() -> str:
    return _workspace_api_url("/flow_runs/filter")


def _workspace_api_url(path: str) -> str:
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{_workspace_base()}{path}"


def _get_workspace_resource(path: str) -> dict:
    try:
        headers = _api_headers()
        endpoint = _workspace_api_url(path)
    except ValueError as exc:
        return {"error": f"Configuration error: {exc}"}

    try:
        response = httpx.get(endpoint, headers=headers, timeout=30)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return {
            "error": (
                f"Prefect API error {exc.response.status_code}: "
                f"{exc.response.text}"
            )
        }
    except httpx.RequestError as exc:
        return {"error": f"Network error reaching Prefect Cloud: {exc}"}

    data = response.json()
    return data if isinstance(data, dict) else {"data": data}


def _post_workspace_resource(path: str, payload: dict) -> Any:
    try:
        headers = _api_headers()
        endpoint = _workspace_api_url(path)
    except ValueError as exc:
        return {"error": f"Configuration error: {exc}"}

    try:
        response = httpx.post(endpoint, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        return {
            "error": (
                f"Prefect API error {exc.response.status_code}: "
                f"{exc.response.text}"
            )
        }
    except httpx.RequestError as exc:
        return {"error": f"Network error reaching Prefect Cloud: {exc}"}

    return response.json()


def _flow_run_url(flow_run_id: str) -> str:
    account_id = os.environ.get("PREFECT_ACCOUNT_ID", "")
    workspace_id = os.environ.get("PREFECT_WORKSPACE_ID", "")
    return (
        f"https://app.prefect.cloud/account/{account_id}"
        f"/workspace/{workspace_id}/flow-run/{flow_run_id}"
    )


def _time_filter(after: str | None = None, before: str | None = None) -> dict:
    time_filter = {}
    if after:
        time_filter["after_"] = after
    if before:
        time_filter["before_"] = before
    return time_filter


def _name_filter(
    like: str | None = None,
    names: list[str] | None = None,
) -> dict:
    name_filter = {}
    if like:
        name_filter["like_"] = like
    if names:
        name_filter["any_"] = names
    return name_filter


def _state_filter(
    state_types: list[str] | None = None,
    state_names: list[str] | None = None,
) -> dict:
    state = {}
    if state_types:
        state["type"] = {"any_": state_types}
    if state_names:
        state["name"] = {"any_": state_names}
    return state


def _build_flow_run_filters(
    start_time_after: str | None = None,
    start_time_before: str | None = None,
    end_time_after: str | None = None,
    end_time_before: str | None = None,
    expected_start_time_after: str | None = None,
    expected_start_time_before: str | None = None,
    state_types: list[str] | None = None,
    state_names: list[str] | None = None,
    flow_name_like: str | None = None,
    flow_run_name_like: str | None = None,
    flow_names: list[str] | None = None,
    flow_run_names: list[str] | None = None,
    deployment_ids: list[str] | None = None,
    work_queue_names: list[str] | None = None,
    tags: list[str] | None = None,
) -> dict:
    flow_runs: dict = {}
    flows: dict = {}

    start_time = _time_filter(start_time_after, start_time_before)
    if start_time:
        flow_runs["start_time"] = start_time

    end_time = _time_filter(end_time_after, end_time_before)
    if end_time:
        flow_runs["end_time"] = end_time

    expected_start_time = _time_filter(
        expected_start_time_after,
        expected_start_time_before,
    )
    if expected_start_time:
        flow_runs["expected_start_time"] = expected_start_time

    state = _state_filter(state_types, state_names)
    if state:
        flow_runs["state"] = state

    flow_run_name = _name_filter(flow_run_name_like, flow_run_names)
    if flow_run_name:
        flow_runs["name"] = flow_run_name

    flow_name = _name_filter(flow_name_like, flow_names)
    if flow_name:
        flows["name"] = flow_name

    if deployment_ids:
        flow_runs["deployment_id"] = {"any_": deployment_ids}
    if work_queue_names:
        flow_runs["work_queue_name"] = {"any_": work_queue_names}
    if tags:
        flow_runs["tags"] = {"all_": tags}

    filters = {}
    if flow_runs:
        filters["flow_runs"] = flow_runs
    if flows:
        filters["flows"] = flows
    return filters


def _compact_flow_run(run: dict) -> dict:
    state = run.get("state") or {}
    run_id = run.get("id", "")
    return {
        "id": run_id,
        "name": run.get("name", ""),
        "flow_id": run.get("flow_id"),
        "deployment_id": run.get("deployment_id"),
        "work_queue_name": run.get("work_queue_name"),
        "state_type": state.get("type"),
        "state_name": state.get("name"),
        "start_time": run.get("start_time"),
        "end_time": run.get("end_time"),
        "expected_start_time": run.get("expected_start_time"),
        "tags": run.get("tags") or [],
        "flow_run_url": _flow_run_url(run_id) if run_id else "",
    }


def _compact_task_run(run: dict) -> dict:
    state = run.get("state") or {}
    flow_run_id = run.get("flow_run_id")
    return {
        "id": run.get("id", ""),
        "name": run.get("name", ""),
        "task_key": run.get("task_key"),
        "dynamic_key": run.get("dynamic_key"),
        "flow_run_id": flow_run_id,
        "state_type": state.get("type"),
        "state_name": state.get("name"),
        "start_time": run.get("start_time"),
        "end_time": run.get("end_time"),
        "expected_start_time": run.get("expected_start_time"),
        "tags": run.get("tags") or [],
        "flow_run_url": _flow_run_url(flow_run_id) if flow_run_id else "",
    }


def _compact_deployment(deployment: dict) -> dict:
    return {
        "id": deployment.get("id", ""),
        "name": deployment.get("name", ""),
        "flow_id": deployment.get("flow_id"),
        "work_pool_name": deployment.get("work_pool_name"),
        "work_queue_name": deployment.get("work_queue_name"),
        "paused": deployment.get("paused"),
        "status": deployment.get("status"),
        "tags": deployment.get("tags") or [],
        "created": deployment.get("created"),
        "updated": deployment.get("updated"),
    }


def _compact_artifact(artifact: dict) -> dict:
    return {
        "id": artifact.get("id") or artifact.get("latest_id") or "",
        "key": artifact.get("key"),
        "type": artifact.get("type"),
        "flow_run_id": artifact.get("flow_run_id"),
        "task_run_id": artifact.get("task_run_id"),
        "description": artifact.get("description"),
        "data": artifact.get("data"),
        "metadata": artifact.get("metadata_") or artifact.get("metadata"),
        "created": artifact.get("created"),
        "updated": artifact.get("updated"),
    }


def _post_flow_run_search(payload: dict) -> list[dict]:
    result = _post_workspace_resource("/flow_runs/filter", payload)
    if isinstance(result, dict) and "error" in result:
        return [result]
    return [_compact_flow_run(run) for run in result or []]


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
def get_flow_run_details(flow_run_id: str) -> dict:
    """
    Fetch read-only metadata for a single Prefect Cloud flow run.

    Returns a compact but richer view than search results, including
    parameters, state message/data, timing fields, tags, and the Cloud URL.
    """
    run = _get_workspace_resource(f"/flow_runs/{flow_run_id}")
    if "error" in run:
        return run

    state = run.get("state") or {}
    details = _compact_flow_run(run)
    details.update(
        {
            "parameters": run.get("parameters") or {},
            "state_message": state.get("message"),
            "state_data": state.get("data"),
            "state_timestamp": state.get("timestamp"),
            "parent_task_run_id": run.get("parent_task_run_id"),
            "infrastructure_document_id": run.get("infrastructure_document_id"),
            "idempotency_key": run.get("idempotency_key"),
            "auto_scheduled": run.get("auto_scheduled"),
        }
    )
    return details


@mcp.tool()
def search_flow_runs(
    start_time_after: str | None = None,
    start_time_before: str | None = None,
    end_time_after: str | None = None,
    end_time_before: str | None = None,
    expected_start_time_after: str | None = None,
    expected_start_time_before: str | None = None,
    state_types: list[str] | None = None,
    state_names: list[str] | None = None,
    flow_name_like: str | None = None,
    flow_run_name_like: str | None = None,
    flow_names: list[str] | None = None,
    flow_run_names: list[str] | None = None,
    deployment_ids: list[str] | None = None,
    work_queue_names: list[str] | None = None,
    tags: list[str] | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: str = "START_TIME_DESC",
) -> list[dict]:
    """
    Search Prefect Cloud flow runs with read-only filters.

    Time filters should be ISO 8601 datetimes, for example
    "2026-06-23T00:00:00Z". State types are Prefect state type names such as
    "COMPLETED", "FAILED", "CRASHED", "CANCELLED", "RUNNING", or "PENDING".
    Tags are matched as a superset, so returned runs must include all tags.
    """
    if sort not in VALID_FLOW_RUN_SORTS:
        return [
            {
                "error": (
                    f"Invalid sort '{sort}'. Expected one of: "
                    f"{', '.join(sorted(VALID_FLOW_RUN_SORTS))}"
                )
            }
        ]

    payload = {
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }
    payload.update(
        _build_flow_run_filters(
            start_time_after=start_time_after,
            start_time_before=start_time_before,
            end_time_after=end_time_after,
            end_time_before=end_time_before,
            expected_start_time_after=expected_start_time_after,
            expected_start_time_before=expected_start_time_before,
            state_types=state_types,
            state_names=state_names,
            flow_name_like=flow_name_like,
            flow_run_name_like=flow_run_name_like,
            flow_names=flow_names,
            flow_run_names=flow_run_names,
            deployment_ids=deployment_ids,
            work_queue_names=work_queue_names,
            tags=tags,
        )
    )

    return _post_flow_run_search(payload)


@mcp.tool()
def search_task_runs(
    flow_run_id: str | None = None,
    start_time_after: str | None = None,
    start_time_before: str | None = None,
    end_time_after: str | None = None,
    end_time_before: str | None = None,
    expected_start_time_after: str | None = None,
    expected_start_time_before: str | None = None,
    state_types: list[str] | None = None,
    state_names: list[str] | None = None,
    task_run_name_like: str | None = None,
    task_run_names: list[str] | None = None,
    tags: list[str] | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: str = "ID_DESC",
) -> list[dict]:
    """
    Search Prefect Cloud task runs with read-only filters.

    Use flow_run_id to inspect tasks within a specific flow run, or combine
    state, name, tag, and time filters to search across the workspace.
    """
    if sort not in VALID_TASK_RUN_SORTS:
        return [
            {
                "error": (
                    f"Invalid sort '{sort}'. Expected one of: "
                    f"{', '.join(sorted(VALID_TASK_RUN_SORTS))}"
                )
            }
        ]

    task_runs: dict = {}

    if flow_run_id:
        task_runs["flow_run_id"] = {"any_": [flow_run_id]}

    start_time = _time_filter(start_time_after, start_time_before)
    if start_time:
        task_runs["start_time"] = start_time

    end_time = _time_filter(end_time_after, end_time_before)
    if end_time:
        task_runs["end_time"] = end_time

    expected_start_time = _time_filter(
        expected_start_time_after,
        expected_start_time_before,
    )
    if expected_start_time:
        task_runs["expected_start_time"] = expected_start_time

    state = _state_filter(state_types, state_names)
    if state:
        task_runs["state"] = state

    task_run_name = _name_filter(task_run_name_like, task_run_names)
    if task_run_name:
        task_runs["name"] = task_run_name

    if tags:
        task_runs["tags"] = {"all_": tags}

    payload = {
        "task_runs": task_runs,
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }
    result = _post_workspace_resource("/task_runs/filter", payload)
    if isinstance(result, dict) and "error" in result:
        return [result]
    return [_compact_task_run(run) for run in result or []]


@mcp.tool()
def get_task_run_logs(
    task_run_id: str,
    min_log_level: int = MIN_LOG_LEVEL,
    limit: int = MAX_LOG_LINES,
) -> str:
    """
    Fetch logs for a Prefect Cloud task run.

    Returns log lines for the given task_run_id ordered by timestamp ascending.
    By default, only WARNING-and-above logs are returned.
    """
    payload = {
        "logs": {
            "task_run_id": {"any_": [task_run_id]},
            "level": {"ge_": min_log_level},
        },
        "sort": "TIMESTAMP_ASC",
        "limit": limit,
        "offset": 0,
    }

    result = _post_workspace_resource("/logs/filter", payload)
    if isinstance(result, dict) and "error" in result:
        return result["error"]

    logs = result or []
    if not logs and min_log_level > 0:
        payload["logs"] = {"task_run_id": {"any_": [task_run_id]}}
        result = _post_workspace_resource("/logs/filter", payload)
        if isinstance(result, dict) and "error" in result:
            return result["error"]
        logs = result or []

    if not logs:
        return (
            f"No logs found for task_run_id={task_run_id}.\n"
            "Verify the UUID is correct and that the task run exists."
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
def summarize_flow_run_failures(flow_run_id: str) -> dict:
    """
    Summarize failure context for a flow run using read-only API calls.

    Combines flow-run details, failed/crashed task runs, and warning-or-higher
    flow-run logs into one debugger-friendly response.
    """
    flow_run = get_flow_run_details(flow_run_id)
    if "error" in flow_run:
        return {"flow_run": flow_run}

    failed_tasks = search_task_runs(
        flow_run_id=flow_run_id,
        state_types=["FAILED", "CRASHED"],
        limit=25,
        sort="ID_DESC",
    )
    warning_logs = get_flow_run_logs(flow_run_id)

    return {
        "flow_run": flow_run,
        "failed_or_crashed_task_runs": failed_tasks,
        "warning_or_error_logs": warning_logs,
    }


@mcp.tool()
def search_deployments(
    deployment_name_like: str | None = None,
    deployment_names: list[str] | None = None,
    flow_or_deployment_name_like: str | None = None,
    flow_name_like: str | None = None,
    flow_names: list[str] | None = None,
    work_pool_names: list[str] | None = None,
    work_queue_names: list[str] | None = None,
    tags: list[str] | None = None,
    paused: bool | None = None,
    limit: int = 50,
    offset: int = 0,
    sort: str = "NAME_ASC",
) -> list[dict]:
    """
    Search Prefect Cloud deployments with read-only filters.

    Supports deployment names, flow names, tags, paused status, work pool names,
    and work queue names.
    """
    if sort not in VALID_DEPLOYMENT_SORTS:
        return [
            {
                "error": (
                    f"Invalid sort '{sort}'. Expected one of: "
                    f"{', '.join(sorted(VALID_DEPLOYMENT_SORTS))}"
                )
            }
        ]

    deployments: dict = {}
    flows: dict = {}
    work_pools: dict = {}

    deployment_name = _name_filter(deployment_name_like, deployment_names)
    if deployment_name:
        deployments["name"] = deployment_name
    if flow_or_deployment_name_like:
        deployments["flow_or_deployment_name"] = {
            "like_": flow_or_deployment_name_like
        }
    if tags:
        deployments["tags"] = {"all_": tags}
    if paused is not None:
        deployments["paused"] = {"eq_": paused}
    if work_queue_names:
        deployments["work_queue_name"] = {"any_": work_queue_names}

    flow_name = _name_filter(flow_name_like, flow_names)
    if flow_name:
        flows["name"] = flow_name
    if work_pool_names:
        work_pools["name"] = {"any_": work_pool_names}

    payload = {
        "sort": sort,
        "limit": limit,
        "offset": offset,
    }
    if deployments:
        payload["deployments"] = deployments
    if flows:
        payload["flows"] = flows
    if work_pools:
        payload["work_pools"] = work_pools

    result = _post_workspace_resource("/deployments/filter", payload)
    if isinstance(result, dict) and "error" in result:
        return [result]
    return [_compact_deployment(deployment) for deployment in result or []]


@mcp.tool()
def get_flow_run_artifacts(
    flow_run_id: str,
    latest_only: bool = True,
    artifact_key_like: str | None = None,
    artifact_keys: list[str] | None = None,
    artifact_types: list[str] | None = None,
    limit: int = 20,
    offset: int = 0,
    sort: str = "ID_DESC",
) -> list[dict]:
    """
    Fetch artifacts associated with a flow run.

    Uses `/artifacts/latest/filter` by default to avoid returning many
    historical versions of the same artifact key.
    """
    if sort not in VALID_ARTIFACT_SORTS:
        return [
            {
                "error": (
                    f"Invalid sort '{sort}'. Expected one of: "
                    f"{', '.join(sorted(VALID_ARTIFACT_SORTS))}"
                )
            }
        ]

    artifacts: dict = {"flow_run_id": {"any_": [flow_run_id]}}

    artifact_key = _name_filter(artifact_key_like, artifact_keys)
    if artifact_key:
        artifacts["key"] = artifact_key
    if artifact_types:
        artifacts["type"] = {"any_": artifact_types}

    endpoint = "/artifacts/latest/filter" if latest_only else "/artifacts/filter"
    result = _post_workspace_resource(
        endpoint,
        {
            "artifacts": artifacts,
            "limit": limit,
            "offset": offset,
            "sort": sort,
        },
    )
    if isinstance(result, dict) and "error" in result:
        return [result]
    return [_compact_artifact(artifact) for artifact in result or []]


@mcp.tool()
def count_flow_runs(
    start_time_after: str | None = None,
    start_time_before: str | None = None,
    end_time_after: str | None = None,
    end_time_before: str | None = None,
    expected_start_time_after: str | None = None,
    expected_start_time_before: str | None = None,
    state_types: list[str] | None = None,
    state_names: list[str] | None = None,
    flow_name_like: str | None = None,
    flow_run_name_like: str | None = None,
    flow_names: list[str] | None = None,
    flow_run_names: list[str] | None = None,
    deployment_ids: list[str] | None = None,
    work_queue_names: list[str] | None = None,
    tags: list[str] | None = None,
) -> dict:
    """
    Count Prefect Cloud flow runs matching the same filters as search_flow_runs.
    """
    payload = _build_flow_run_filters(
        start_time_after=start_time_after,
        start_time_before=start_time_before,
        end_time_after=end_time_after,
        end_time_before=end_time_before,
        expected_start_time_after=expected_start_time_after,
        expected_start_time_before=expected_start_time_before,
        state_types=state_types,
        state_names=state_names,
        flow_name_like=flow_name_like,
        flow_run_name_like=flow_run_name_like,
        flow_names=flow_names,
        flow_run_names=flow_run_names,
        deployment_ids=deployment_ids,
        work_queue_names=work_queue_names,
        tags=tags,
    )
    result = _post_workspace_resource("/flow_runs/count", payload)
    if isinstance(result, dict) and "error" in result:
        return result
    return {"count": result}


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

    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()

    return search_flow_runs(
        start_time_after=cutoff,
        state_types=state_types,
        limit=limit,
        sort="START_TIME_DESC",
    )


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
    payload = {
        "sort": "START_TIME_DESC",
        "limit": limit,
        "deployments": {"name": {"any_": [deployment_name]}},
    }

    return _post_flow_run_search(payload)


if __name__ == "__main__":
    mcp.run(transport="stdio")
