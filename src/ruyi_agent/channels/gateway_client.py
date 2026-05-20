from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

import httpx


@dataclass(slots=True)
class GatewayArtifact:
    kind: str
    filename: str
    content_type: str | None
    content: bytes


class GatewayClientError(Exception):
    def __init__(self, *, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


class GatewayTaskClient(Protocol):
    async def list_agents(self) -> list[dict[str, Any]]: ...

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
    ) -> list[dict[str, Any]]: ...

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]: ...

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]: ...

    async def download_artifact(self, *, path: str) -> GatewayArtifact: ...

    async def get_task(self, *, task_id: str) -> dict[str, Any]: ...

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]: ...


def _filename_from_content_disposition(value: str) -> str | None:
    match = re.search(r'filename="([^"]+)"', value)
    if match:
        return match.group(1)
    match = re.search(r"filename=([^;]+)", value)
    if match:
        return match.group(1).strip()
    return None


class GatewayHTTPClient:
    def __init__(self, *, base_url: str, bearer_token: str, timeout: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._bearer_token = bearer_token
        self._timeout = timeout

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        params = {
            "limit": str(limit),
            **{f"metadata.{key}": value for key, value in metadata.items()},
        }
        if agent_name is not None:
            params["agent_name"] = agent_name
        payload = await self._request("GET", "/tasks", params=params)
        items = payload.get("items")
        return items if isinstance(items, list) else []

    async def list_agents(self) -> list[dict[str, Any]]:
        payload = await self._request("GET", "/agents")
        items = payload.get("items")
        return items if isinstance(items, list) else []

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        input_payload: dict[str, Any] = {"content": content}
        if attachments:
            input_payload["attachments"] = attachments
        return await self._request(
            "POST",
            f"/agents/{agent_name}/tasks",
            json={"input": input_payload, "metadata": metadata},
        )

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        input_payload: dict[str, Any] = {"content": content}
        if attachments:
            input_payload["attachments"] = attachments
        return await self._request(
            "POST",
            f"/tasks/{task_id}/input",
            json={"input": input_payload},
        )

    async def download_artifact(self, *, path: str) -> GatewayArtifact:
        response = await self._request_raw(
            "POST",
            "/artifacts/download",
            json={"path": path},
        )
        content_disposition = response.headers.get("content-disposition", "")
        filename = _filename_from_content_disposition(content_disposition) or Path(path).name
        return GatewayArtifact(
            kind="file",
            filename=filename or "artifact",
            content_type=response.headers.get("content-type"),
            content=response.content,
        )

    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/tasks/{task_id}")

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/tasks/{task_id}/reviews/{review_id}/decision",
            json={"decisions": decisions},
        )

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Accept": "application/json",
        }
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
        ) as client:
            response = await client.request(method, path, params=params, json=json)
        payload = self._decode_json(response)
        if response.is_success:
            return payload
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            raise GatewayClientError(
                status_code=response.status_code,
                code=str(error.get("code", "gateway_error")),
                message=str(error.get("message", "Gateway request failed")),
            )
        raise GatewayClientError(
            status_code=response.status_code,
            code="gateway_error",
            message="Gateway request failed",
        )

    async def _request_raw(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        headers = {
            "Authorization": f"Bearer {self._bearer_token}",
            "Accept": "*/*",
        }
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers=headers,
        ) as client:
            response = await client.request(method, path, json=json)
        if response.is_success:
            return response
        payload = self._decode_json(response)
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            raise GatewayClientError(
                status_code=response.status_code,
                code=str(error.get("code", "gateway_error")),
                message=str(error.get("message", "Gateway request failed")),
            )
        raise GatewayClientError(
            status_code=response.status_code,
            code="gateway_error",
            message="Gateway request failed",
        )

    def _decode_json(self, response: httpx.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid JSON",
            ) from exc
        if not isinstance(payload, dict):
            raise GatewayClientError(
                status_code=502,
                code="gateway_error",
                message="Gateway returned invalid payload",
            )
        return payload
