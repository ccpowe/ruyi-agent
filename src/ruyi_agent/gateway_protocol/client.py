from __future__ import annotations
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from typing import Any
import httpx
from ruyi_agent.gateway_protocol.sse import GatewayTaskEvent
from ruyi_agent.gateway_protocol.transport import GatewayHTTPTransport
class GatewayProtocolClient:
    def __init__(self, *, base_url: str, timeout: float, headers: Mapping[str, str] | None = None, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self._http = GatewayHTTPTransport(base_url=base_url, timeout=timeout, headers=headers, transport=transport)
    async def request_json(self, method: str, path: str, *, params: Mapping[str, str] | None = None, json: Mapping[str, Any] | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        return await self._http.request_json(method, path, params=params, json=json, idempotency_key=idempotency_key)
    async def request_raw(self, method: str, path: str, *, json: Mapping[str, Any] | None = None) -> httpx.Response:
        return await self._http.request_raw(method, path, json=json)
    @asynccontextmanager
    async def stream_raw(self, method: str, path: str, *, json: Mapping[str, Any] | None = None) -> AsyncIterator[httpx.Response]:
        async with self._http.stream_raw(method, path, json=json) as response:
            yield response
    async def list_agents(self) -> dict[str, Any]:
        return await self.request_json("GET", "agents")
    async def list_tasks(self, *, agent_name: str | None = None, metadata: Mapping[str, str], limit: int = 1, root_task_id: str | None = None) -> dict[str, Any]:
        params = {"limit": str(limit), **{f"metadata.{key}": value for key, value in metadata.items()}}
        if agent_name is not None:
            params["agent_name"] = agent_name
        if root_task_id is not None:
            params["root_task_id"] = root_task_id
        return await self.request_json("GET", "tasks", params=params)
    async def create_task(self, *, agent_name: str, content: str, metadata: Mapping[str, Any], attachments: list[Mapping[str, Any]] | None = None, webhook: Mapping[str, Any] | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"input": task_input_payload(content, attachments), "metadata": dict(metadata)}
        if webhook is not None:
            payload["webhook"] = dict(webhook)
        return await self.request_json("POST", f"agents/{agent_name}/tasks", json=payload, idempotency_key=idempotency_key)
    async def send_input(self, *, task_id: str, content: str, attachments: list[Mapping[str, Any]] | None = None, idempotency_key: str | None = None) -> dict[str, Any]:
        return await self.request_json("POST", f"tasks/{task_id}/input", json={"input": task_input_payload(content, attachments)}, idempotency_key=idempotency_key)
    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        return await self.request_json("GET", f"tasks/{task_id}")
    async def list_task_messages(self, *, task_id: str, cursor: str | None, limit: int) -> dict[str, Any]:
        params = {"limit": str(limit)}
        if cursor is not None:
            params["cursor"] = cursor
        return await self.request_json("GET", f"tasks/{task_id}/messages", params=params)
    @asynccontextmanager
    async def open_task_event_stream(self, *, task_id: str, run_count: int, last_event_id: str | None) -> AsyncIterator[AsyncIterator[GatewayTaskEvent]]:
        async with self._http.stream_task_events(f"tasks/{task_id}/events", run_count=run_count, last_event_id=last_event_id) as events:
            yield events
    async def cancel_task(self, *, task_id: str) -> dict[str, Any]:
        return await self.request_json("POST", f"tasks/{task_id}/cancel")
    async def submit_review_decision(self, *, task_id: str, review_id: str, decisions: list[Mapping[str, Any]]) -> dict[str, Any]:
        return await self.request_json("POST", f"tasks/{task_id}/reviews/{review_id}/decision", json=review_decision_payload(decisions))
    async def download_artifact_response(self, *, path: str) -> httpx.Response:
        return await self.request_raw("POST", artifact_download_path(), json=artifact_download_payload(path))
    async def download_task_artifact_response(self, *, task_id: str, artifact_id: str) -> httpx.Response:
        return await self.request_raw("GET", task_artifact_download_path(task_id, artifact_id))
def task_input_payload(content: str, attachments: list[Mapping[str, Any]] | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {"content": content}
    if attachments:
        payload["attachments"] = [dict(item) for item in attachments]
    return payload
def review_decision_payload(decisions: list[Mapping[str, Any]]) -> dict[str, Any]:
    return {"decisions": [dict(item) for item in decisions]}
def artifact_download_path() -> str:
    return "artifacts/download"
def artifact_download_payload(path: str) -> dict[str, str]:
    return {"path": path}
def task_artifact_download_path(task_id: str, artifact_id: str) -> str:
    return f"tasks/{task_id}/artifacts/{artifact_id}/download"
