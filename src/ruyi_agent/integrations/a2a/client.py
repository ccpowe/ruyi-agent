"""
A2A Client - Agent-to-Agent 协议客户端

这个模块实现了 Agent-to-Agent (A2A) 协议的 HTTP 客户端，用于与远程网关通信。

核心功能：
1. 通过 HTTP API 调用远程网关的 agent
2. 支持任务创建、查询、输入、取消等操作
3. 处理认证（Bearer Token）
4. 统一的错误处理和异常转换

使用场景：
- 本地网关需要委托任务给远程网关
- 跨网关的 agent 调用
- 多网关级联部署

数据流：
  本地网关 → A2AClient → HTTP 请求 → 远程网关 → 远程 Agent
"""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import httpx

from ruyi_agent.config.loader import RemoteRef
from ruyi_agent.gateway._http_transport import (
    GatewayHTTPTransport,
    GatewayTransportHTTPStatusError,
    GatewayTransportInvalidJSONError,
    GatewayTransportInvalidPayloadError,
    GatewayTransportInvalidStreamError,
    GatewayTransportStreamError,
    gateway_bearer_auth_headers,
)
from ruyi_agent.gateway.sse import (
    GatewayTaskEvent,
)
from ruyi_agent.runtime.task_events import (
    MAX_SHORT_EVENT_TEXT_LENGTH,
    normalize_task_event_text,
)


class A2AClientError(Exception):
    """
    A2A 客户端统一异常类

    用于封装所有与远程网关通信相关的错误，包括：
    - 网络错误（连接失败、超时等）
    - HTTP 错误（4xx, 5xx）
    - 响应格式错误（无效 JSON、格式不符等）
    - 认证错误（token 缺失、无效等）

    Attributes:
        status_code: HTTP 状态码
        code: 错误代码（如 "upstream_gateway_error"）
        message: 错误消息
        details: 额外的错误详情（可选）
    """

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details


class A2AClient:
    """
    Agent-to-Agent 协议客户端

    负责与远程网关进行 HTTP 通信，实现跨网关的 agent 调用。

    主要功能：
    - create_task: 在远程网关创建任务
    - get_task: 查询远程任务状态
    - send_input: 向远程任务发送输入
    - cancel_task: 取消远程任务

    认证方式：
    - Bearer Token（从环境变量读取）

    错误处理：
    - 所有错误统一转换为 A2AClientError
    - 保留远程网关返回的错误信息

    Attributes:
        _timeout: HTTP 请求超时时间（秒）
        _transports: 自定义的 HTTP 传输层（用于测试或特殊场景）
    """

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        transports: dict[str, httpx.AsyncBaseTransport] | None = None,
    ) -> None:
        """
        初始化 A2A 客户端

        Args:
            timeout: HTTP 请求超时时间，默认 10 秒
            transports: 自定义传输层字典 {url: transport}，用于测试或特殊网络配置
        """
        self._timeout = timeout
        self._transports = transports if transports is not None else {}

    async def create_task(
        self,
        remote_ref: RemoteRef,
        *,
        input_content: str,
        metadata: dict[str, Any],
        attachments: list[dict[str, Any]] | None = None,
        webhook: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """
        在远程网关创建任务

        调用远程网关的 POST /agents/{agent_name}/tasks API。

        Args:
            remote_ref: 远程 agent 引用（包含 URL、agent 名称、认证信息）
            input_content: 任务输入内容
            metadata: 任务元数据（包含委托上下文等）
            webhook: 任务完成时的回调配置（可选）

        Returns:
            远程网关返回的任务信息（包含 task_id, status 等）

        Raises:
            A2AClientError: 网络错误、认证失败、远程网关错误等

        示例：
            response = await client.create_task(
                remote_ref,
                input_content="帮我分析这段代码",
                metadata={"delegation_context": "..."},
            )
            task_id = response["task_id"]
        """
        input_payload: dict[str, Any] = {"content": input_content}
        if attachments:
            input_payload["attachments"] = attachments
        payload: dict[str, Any] = {"input": input_payload, "metadata": metadata}
        if webhook is not None:
            payload["webhook"] = webhook
        return await self._request_json(
            remote_ref,
            "POST",
            f"agents/{remote_ref.remote_agent_name}/tasks",
            json=payload,
            idempotency_key=idempotency_key,
        )

    async def get_task(self, remote_ref: RemoteRef, *, task_id: str) -> dict[str, Any]:
        """
        查询远程任务状态

        调用远程网关的 GET /tasks/{task_id} API。

        Args:
            remote_ref: 远程 agent 引用
            task_id: 远程任务 ID

        Returns:
            任务状态信息（task_id, status, last_result, error 等）

        Raises:
            A2AClientError: 任务不存在、网络错误等
        """
        return await self._request_json(remote_ref, "GET", f"tasks/{task_id}")

    async def list_task_messages(
        self,
        remote_ref: RemoteRef,
        *,
        task_id: str,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Fetch one opaque page of the remote Task's public transcript."""

        params = {"limit": str(limit)}
        if cursor is not None:
            params["cursor"] = cursor
        return await self._request_json(
            remote_ref,
            "GET",
            f"tasks/{task_id}/messages",
            params=params,
        )

    @asynccontextmanager
    async def open_task_event_stream(
        self,
        remote_ref: RemoteRef,
        *,
        task_id: str,
        run_count: int,
        last_event_id: str | None,
    ) -> AsyncIterator[AsyncIterator[GatewayTaskEvent]]:
        """Open a downstream Task SSE stream and keep its response alive."""

        transport = self._http_transport(remote_ref)
        try:
            async with transport.stream_task_events(
                f"tasks/{task_id}/events",
                run_count=run_count,
                last_event_id=last_event_id,
            ) as events:
                yield _map_remote_task_event_errors(events, remote_ref)
        except GatewayTransportHTTPStatusError as exc:
            raise _task_event_response_error(remote_ref, exc) from exc
        except GatewayTransportInvalidStreamError as exc:
            label = "error" if exc.phase == "error_response" else "stream"
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message=(
                    f"Remote gateway for '{remote_ref.name}' returned "
                    f"an invalid Task event {label}"
                ),
            ) from exc
        except httpx.InvalidURL as exc:
            raise _remote_task_event_stream_error(remote_ref) from exc
        except (
            GatewayTransportInvalidJSONError,
            GatewayTransportInvalidPayloadError,
            GatewayTransportStreamError,
        ) as exc:
            raise _remote_task_event_stream_error(remote_ref) from exc

    async def send_input(
        self,
        remote_ref: RemoteRef,
        *,
        task_id: str,
        input_content: str,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """
        向远程任务发送新输入

        调用远程网关的 POST /tasks/{task_id}/input API。
        用于实现任务的交互式对话。

        Args:
            remote_ref: 远程 agent 引用
            task_id: 远程任务 ID
            input_content: 新的输入内容

        Returns:
            更新后的任务状态

        Raises:
            A2AClientError: 任务不存在、任务正在运行（无法接收输入）等
        """
        input_payload: dict[str, Any] = {"content": input_content}
        if attachments:
            input_payload["attachments"] = attachments
        return await self._request_json(
            remote_ref,
            "POST",
            f"tasks/{task_id}/input",
            json={"input": input_payload},
            idempotency_key=idempotency_key,
        )

    async def cancel_task(
        self,
        remote_ref: RemoteRef,
        *,
        task_id: str,
    ) -> dict[str, Any]:
        """
        取消远程任务

        调用远程网关的 POST /tasks/{task_id}/cancel API。

        Args:
            remote_ref: 远程 agent 引用
            task_id: 远程任务 ID

        Returns:
            更新后的任务状态（status 应为 "cancelled"）

        Raises:
            A2AClientError: 任务不存在、任务已完成（无法取消）等
        """
        return await self._request_json(remote_ref, "POST", f"tasks/{task_id}/cancel")

    async def submit_review_decision(
        self,
        remote_ref: RemoteRef,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """
        向远程任务提交 human review 决策。

        调用远程网关的 POST /tasks/{task_id}/reviews/{review_id}/decision API。
        """
        return await self._request_json(
            remote_ref,
            "POST",
            f"tasks/{task_id}/reviews/{review_id}/decision",
            json={"decisions": decisions},
        )

    async def _request_json(
        self,
        remote_ref: RemoteRef,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        """
        发送 HTTP 请求并返回 JSON 响应

        这是所有 API 调用的底层实现，负责：
        1. 构建 HTTP 请求（URL、headers、body）
        2. 发送请求并处理网络错误
        3. 解析 JSON 响应
        4. 统一错误处理和异常转换

        Args:
            remote_ref: 远程 agent 引用（包含 URL 和认证信息）
            method: HTTP 方法（GET, POST 等）
            path: API 路径（如 "tasks/123"）
            json: 请求体（JSON 格式，可选）

        Returns:
            远程网关返回的 JSON 响应（dict）

        Raises:
            A2AClientError: 所有错误都转换为此异常
                - 502: 网络错误、响应格式错误
                - 其他: 透传远程网关的错误码

        错误处理流程：
        1. 网络错误（连接失败、超时）→ 502 upstream_gateway_error
        2. 响应不是 JSON → 502 upstream_gateway_error
        3. 响应是 JSON 但格式错误 → 502 upstream_gateway_error
        4. 远程网关返回错误 → 透传状态码和错误信息
        """
        try:
            return await self._http_transport(remote_ref).request_json(
                method,
                path,
                params=params,
                json=json,
                idempotency_key=idempotency_key,
            )
        except (httpx.HTTPError, httpx.InvalidURL) as exc:
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message=f"Remote gateway request failed for '{remote_ref.name}'",
            ) from exc
        except GatewayTransportInvalidJSONError as exc:
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message=f"Remote gateway for '{remote_ref.name}' returned invalid JSON",
            ) from exc
        except GatewayTransportInvalidPayloadError as exc:
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message=(
                    f"Remote gateway for '{remote_ref.name}' returned an invalid "
                    "response payload"
                ),
            ) from exc
        except GatewayTransportHTTPStatusError as exc:
            raise _remote_http_status_error(remote_ref, exc) from exc

    def _http_transport(self, remote_ref: RemoteRef) -> GatewayHTTPTransport:
        return GatewayHTTPTransport(
            base_url=remote_ref.url,
            timeout=self._timeout,
            headers=self._build_headers(remote_ref),
            transport=self._transports.get(remote_ref.url),
        )

    def _build_headers(self, remote_ref: RemoteRef) -> dict[str, str]:
        """
        构建 HTTP 请求头

        主要处理认证信息：
        1. 从 remote_ref.auth 读取认证配置
        2. 目前只支持 Bearer Token 认证
        3. Token 从环境变量读取（安全考虑）

        Args:
            remote_ref: 远程 agent 引用

        Returns:
            HTTP 请求头字典

        Raises:
            A2AClientError: 认证配置错误
                - 不支持的认证类型
                - 缺少 token_env 配置
                - 环境变量未设置

        认证配置示例：
            remote_ref.auth = {
                "type": "bearer",
                "token_env": "REMOTE_GATEWAY_TOKEN"
            }
        """
        auth = remote_ref.auth

        # 如果没有配置认证，直接返回基础 headers
        if auth is None:
            return {}

        # auth 已在 TOML/RemoteRef 边界验证为 bearer 配置。
        token_env = auth.token_env

        # 从环境变量读取 token
        token = os.getenv(token_env)
        if not token:
            raise A2AClientError(
                status_code=503,
                code="runtime_unavailable",
                message=(
                    f"Remote ref '{remote_ref.name}' requires environment variable "
                    f"{token_env!r}"
                ),
            )

        return gateway_bearer_auth_headers(token)


def _remote_http_status_error(
    remote_ref: RemoteRef,
    exc: GatewayTransportHTTPStatusError,
) -> A2AClientError:
    error_payload = exc.payload.get("error") if isinstance(exc.payload, dict) else None
    if isinstance(error_payload, dict):
        return A2AClientError(
            status_code=exc.status_code,
            code=str(error_payload.get("code", "upstream_gateway_error")),
            message=str(
                error_payload.get(
                    "message",
                    f"Remote gateway request failed for '{remote_ref.name}'",
                )
            ),
            details=(
                error_payload.get("details")
                if isinstance(error_payload.get("details"), dict)
                else None
            ),
        )
    return A2AClientError(
        status_code=502,
        code="upstream_gateway_error",
        message=f"Remote gateway request failed for '{remote_ref.name}'",
    )


async def _map_remote_task_event_errors(
    events: AsyncIterator[GatewayTaskEvent],
    remote_ref: RemoteRef,
) -> AsyncIterator[GatewayTaskEvent]:
    try:
        async for event in events:
            yield event
    except GatewayTransportStreamError as exc:
        raise _remote_task_event_stream_error(remote_ref) from exc


def _remote_task_event_stream_error(remote_ref: RemoteRef) -> A2AClientError:
    return A2AClientError(
        status_code=502,
        code="upstream_gateway_error",
        message=f"Remote Task event stream failed for '{remote_ref.name}'",
    )


def _task_event_response_error(
    remote_ref: RemoteRef,
    exc: GatewayTransportHTTPStatusError,
) -> A2AClientError:
    error = exc.payload.get("error") if isinstance(exc.payload, dict) else None
    code = error.get("code") if isinstance(error, dict) else None
    raw_message = error.get("message") if isinstance(error, dict) else None
    message = (
        normalize_task_event_text(raw_message)[:MAX_SHORT_EVENT_TEXT_LENGTH]
        if isinstance(raw_message, str)
        else None
    )
    if (
        exc.status_code == 400
        and code == "invalid_request"
        and isinstance(message, str)
    ) or (
        exc.status_code == 409
        and code == "task_run_mismatch"
        and isinstance(message, str)
    ):
        return A2AClientError(
            status_code=exc.status_code,
            code=code,
            message=message,
            details=(
                _task_run_mismatch_details(error.get("details"))
                if exc.status_code == 409
                else None
            ),
        )
    return _remote_task_event_stream_error(remote_ref)


def _task_run_mismatch_details(value: Any) -> dict[str, int] | None:
    if not isinstance(value, dict):
        return None
    requested = value.get("requested_run_count")
    current = value.get("current_run_count")
    if (
        not isinstance(requested, int)
        or isinstance(requested, bool)
        or requested < 0
        or not isinstance(current, int)
        or isinstance(current, bool)
        or current < 0
    ):
        return None
    return {
        "requested_run_count": requested,
        "current_run_count": current,
    }
