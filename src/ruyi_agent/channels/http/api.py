"""
Gateway HTTP API - Agent-to-Agent 任务委托网关

这个模块实现了一个 HTTP 网关服务，用于管理多个 agent 之间的任务委托和路由。

核心职责：
1. 提供 RESTful API 供外部创建和管理 agent 任务
2. 路由任务到本地 agent 或远程 agent（通过 A2A 协议）
3. 管理委托上下文，防止循环委托和深度超限
4. 持久化任务路由信息，支持任务状态查询和 webhook 回调

数据流：
  外部请求 → FastAPI 路由 → GatewayService → GatewayRuntime → Agent 执行
                                    ↓
                              GatewayRouteStore (持久化路由信息)

关键概念：
- Local Agent: 在本地运行时执行的 agent
- Remote Ref: 通过 A2A 协议委托给远程网关的 agent 引用
- Delegation Context: 跟踪委托链路，防止循环和深度超限
- Task Route: 记录任务的路由信息（本地 task_id 到远程 upstream_task_id 的映射）
"""

from __future__ import annotations

import base64
import binascii
import secrets
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any, Callable, Literal, Protocol
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field, model_validator

from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import (
    AgentControl,
    MaxDelegationDepthError,
    MaxTasksPerRootError,
    RegisteredAgent,
    RemoteExecutorNotImplementedError,
    TaskAlreadyRunningError,
    TaskRecord,
    UnknownAgentTargetError,
    UnknownWorkerTaskError,
)
from ruyi_agent.runtime.delegation.context import (
    DelegationContext,
    DelegationContextDepthError,
    DelegationLoopError,
    InvalidDelegationContextError,
)
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore

MetadataScalar = str | int | float | bool | None
DEFAULT_ATTACHMENT_MAX_BYTES = 20 * 1024 * 1024
DEFAULT_ARTIFACT_MAX_BYTES = 50 * 1024 * 1024
ATTACHMENT_METADATA_KEY = "attachments"
ATTACHMENT_INBOX_SUBDIR = "inbox/gateway"
SAFE_ATTACHMENT_CHARS = set(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    ".-_"
)


class GatewayAPIError(Exception):
    """网关 API 统一异常类，用于将内部错误转换为标准 HTTP 错误响应"""

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


@dataclass(slots=True)
class TaskRouteRecord:
    """
    任务路由记录，持久化任务的路由信息

    用于记录任务如何被路由（本地或远程），以及远程任务的上游 ID 映射。
    这是网关实现任务追踪和状态同步的关键数据结构。

    Attributes:
        task_id: 本地任务 ID（网关分配的唯一标识）
        agent_name: 目标 agent 名称
        metadata: 任务元数据（包含委托上下文等信息）
        route_kind: 路由类型，"local" 或 "remote_ref"
        upstream_task_id: 远程任务的上游 ID（对于 remote_ref，这是远程网关返回的 task_id）
        webhook: 任务完成时的回调配置
    """

    task_id: str
    agent_name: str
    metadata: dict[str, MetadataScalar]
    route_kind: str
    upstream_task_id: str
    webhook: dict[str, MetadataScalar] | None = None


@dataclass(slots=True)
class PreparedInput:
    content: str
    attachment_metadata: list[dict[str, str]]


@dataclass(slots=True)
class GatewayArtifact:
    path: str
    filename: str
    content: bytes
    content_type: str


class AgentRefResponse(BaseModel):
    name: str
    kind: Literal["local", "remote_ref"]
    public: bool
    description: str
    is_default: bool


class AgentListResponse(BaseModel):
    items: list[AgentRefResponse]


class AttachmentInput(BaseModel):
    name: str = Field(min_length=1, max_length=255)
    data_base64: str = Field(min_length=1)
    content_type: str | None = Field(default=None, max_length=255)
    kind: Literal["image", "document", "audio", "video", "file"] = "file"


class TaskInput(BaseModel):
    content: str = ""
    attachments: list[AttachmentInput] = Field(default_factory=list, max_length=10)

    @model_validator(mode="after")
    def require_content_or_attachments(self) -> "TaskInput":
        if self.content.strip() or self.attachments:
            return self
        raise ValueError("input.content or input.attachments is required")


class CreateTaskRequest(BaseModel):
    input: TaskInput
    metadata: dict[str, MetadataScalar] = Field(default_factory=dict)
    webhook: dict[str, MetadataScalar] | None = None


class SendInputRequest(BaseModel):
    input: TaskInput


class ReviewDecisionInput(BaseModel):
    decisions: list[dict[str, Any]] = Field(min_length=1)


class TaskResponse(BaseModel):
    task_id: str
    agent_name: str
    status: Literal[
        "pending",
        "running",
        "waiting_for_human",
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    ]
    last_result: str | None
    error: str | None
    run_count: int
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, MetadataScalar]
    pending_review: dict[str, Any] | None = None


class TaskListResponse(BaseModel):
    items: list[TaskResponse]
    next_cursor: str | None


class ReviewResponse(BaseModel):
    review_id: str
    task_id: str
    thread_id: str | None
    agent_name: str
    route_kind: str
    status: Literal["pending"]
    action_requests: list[dict[str, Any]]
    review_configs: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime
    metadata: dict[str, MetadataScalar]


class ReviewListResponse(BaseModel):
    items: list[ReviewResponse]
    next_cursor: str | None


class ErrorEnvelope(BaseModel):
    error: dict[str, Any]


class TaskWebhookEvent(BaseModel):
    event_id: str
    event_type: str
    task_id: str
    agent_name: str
    status: Literal[
        "pending",
        "running",
        "waiting_for_human",
        "completed",
        "failed",
        "cancelled",
        "interrupted",
    ]
    last_result: str | None = None
    error: str | None = None
    run_count: int
    created_at: datetime
    updated_at: datetime


class ArtifactDownloadRequest(BaseModel):
    path: str = Field(min_length=1)


class GatewayRuntime(Protocol):
    """
    网关运行时接口协议

    定义了网关服务依赖的运行时能力，解耦业务逻辑和底层执行。
    实现类负责实际的 agent 管理、任务执行和远程通信。

    主要职责：
    - Agent 注册和查询
    - 任务生命周期管理（创建、输入、取消、刷新）
    - 委托上下文处理
    - 远程任务事件处理
    """

    def list_registered_agents_snapshot(self) -> list[RegisteredAgent]: ...

    def get_registered_agent(self, agent_name: str) -> RegisteredAgent: ...

    def get_task_record(self, task_id: str) -> TaskRecord: ...
    # ↑ 只定义签名，... 表示"这里不实现" Protocol之后好好看看
    def list_task_records(self) -> list[TaskRecord]: ...

    def list_pending_review_records(self) -> list[TaskRecord]: ...

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[Any]: ...

    def download_files(self, paths: list[str]) -> list[Any]: ...

    def get_workspace_root(self) -> str: ...

    def prepare_delegation_metadata(
        self,
        metadata: dict[str, MetadataScalar],
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]: ...

    async def spawn_task(
        self,
        agent_name: str,
        task: str,
        *,
        metadata: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        webhook: dict[str, Any] | None = None,
        delegation_context: DelegationContext | None = None,
    ) -> TaskRecord: ...

    async def send_task_input(
        self,
        task_id: str,
        message: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
    ) -> TaskRecord: ...

    async def cancel_task(self, task_id: str) -> TaskRecord: ...

    async def refresh_task(self, task_id: str) -> TaskRecord: ...

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskRecord: ...

    def get_task_by_review_id(self, review_id: str) -> TaskRecord: ...

    def ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None = None,
    ) -> TaskRecord: ...

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool: ...


class AgentControlGatewayRuntime:
    """
    AgentControl 的网关运行时适配器

    将 AgentControl 接口适配为 GatewayRuntime 协议，
    使得 GatewayService 可以使用 AgentControl 作为底层运行时。
    """

    def __init__(self, control: AgentControl) -> None:
        self._control = control

    def list_registered_agents_snapshot(self) -> list[RegisteredAgent]:
        return self._control.list_registered_agents_snapshot()

    def get_registered_agent(self, agent_name: str) -> RegisteredAgent:
        return self._control.get_registered_agent(agent_name)

    def get_task_record(self, task_id: str) -> TaskRecord:
        return self._control.get_task_record(task_id)

    def list_task_records(self) -> list[TaskRecord]:
        return self._control.list_task_records()

    def list_pending_review_records(self) -> list[TaskRecord]:
        return self._control.list_pending_review_records()

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[Any]:
        return self._control.upload_files(files)

    def download_files(self, paths: list[str]) -> list[Any]:
        return self._control.download_files(paths)

    def get_workspace_root(self) -> str:
        return self._control.workspace_root

    def prepare_delegation_metadata(
        self,
        metadata: dict[str, MetadataScalar],
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
        return self._control.prepare_delegation_metadata(metadata)

    async def spawn_task(
        self,
        agent_name: str,
        task: str,
        *,
        metadata: dict[str, Any] | None = None,
        attachments: list[dict[str, Any]] | None = None,
        webhook: dict[str, Any] | None = None,
        delegation_context: DelegationContext | None = None,
    ) -> TaskRecord:
        return await self._control.spawn_task(
            agent_name,
            task,
            metadata=metadata,
            attachments=attachments,
            webhook=webhook,
            delegation_context=delegation_context,
        )

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool:
        return await self._control.handle_remote_task_event(payload)

    async def send_task_input(
        self,
        task_id: str,
        message: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
    ) -> TaskRecord:
        return await self._control.send_task_input(
            task_id,
            message,
            attachments=attachments,
        )

    async def cancel_task(self, task_id: str) -> TaskRecord:
        return await self._control.cancel_task(task_id)

    async def refresh_task(self, task_id: str) -> TaskRecord:
        return await self._control.refresh_task(task_id)

    async def submit_review_decision(
        self,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskRecord:
        return await self._control.submit_review_decision(review_id, decisions)

    def get_task_by_review_id(self, review_id: str) -> TaskRecord:
        return self._control.get_task_by_review_id(review_id)

    def ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None = None,
    ) -> TaskRecord:
        return self._control.ensure_remote_task_record(
            agent_name=agent_name,
            task_id=task_id,
            upstream_task_id=upstream_task_id,
            webhook=webhook,
        )


class GatewayService:
    """
    网关服务核心业务逻辑层

    负责处理所有网关业务逻辑，包括：
    1. Agent 管理：列出和获取已注册的 agent
    2. 任务路由：根据 agent 类型（local/remote_ref）创建和路由任务
    3. 委托控制：通过 DelegationContext 防止循环委托和深度超限
    4. 任务生命周期：创建、查询、发送输入、取消任务
    5. Webhook 处理：接收和分发远程任务的状态更新

    数据流：
    - 创建任务：API → create_task → _spawn_local_task/_create_remote_task → Runtime
    - 查询任务：API → get_task → _get_task_route → _get_local_task/_refresh_route_task
    - 任务输入：API → send_input → _ensure_runtime_record_for_route → Runtime.send_task_input

    Attributes:
        _main_agent_name: 默认 agent 名称
        _agent_configs: Agent 配置字典 {agent_name: {kind, public, description, ...}}
        _runtime: 底层运行时实现
        _route_store: 任务路由信息持久化存储
        _remote_event_handlers: 额外的远程事件处理器列表
    """

    def __init__(
        self,
        *,
        main_agent_name: str,
        agent_configs: dict[str, dict[str, Any]],
        runtime: GatewayRuntime,
        route_store: GatewayRouteStore | None = None,
        remote_event_handlers: list[GatewayRuntime] | None = None,
        attachment_max_bytes: int = DEFAULT_ATTACHMENT_MAX_BYTES,
        artifact_max_bytes: int = DEFAULT_ARTIFACT_MAX_BYTES,
    ) -> None:
        self._main_agent_name = main_agent_name
        self._agent_configs = agent_configs
        self._runtime = runtime
        self._route_store = route_store or GatewayRouteStore(":memory:")
        self._remote_event_handlers = remote_event_handlers or []
        self._attachment_max_bytes = attachment_max_bytes
        self._artifact_max_bytes = artifact_max_bytes

    def list_agents(self) -> list[AgentRefResponse]:
        """
        列出所有 public 的 agent

        注意：只返回 public=true 的 agent，非 public 的 agent 不会出现在列表中。
        这是为了避免暴露内部 agent 给外部调用者。
        """
        return [
            self._build_agent_ref_response(agent_name, agent_config)
            for agent_name, agent_config in sorted(self._agent_configs.items())
            if agent_config["public"]  # 只返回 public 的 agent
        ]

    def get_agent(self, agent_name: str) -> AgentRefResponse:
        """获取指定 agent 的详细信息（不检查 public 状态）"""
        agent_config = self._get_agent_config(agent_name)
        return self._build_agent_ref_response(agent_name, agent_config)

    async def create_task(
        self,
        *,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None = None,
    ) -> TaskResponse:
        """
        创建新任务并路由到指定 agent

        核心流程：
        1. 验证 agent 存在且为 public
        2. 准备委托元数据（提取和验证 DelegationContext）
        3. 根据 agent 类型路由：
           - local: 在本地运行时创建任务
           - remote_ref: 通过 A2A 协议委托给远程网关
        4. 持久化任务路由信息到 route_store
        5. 返回任务响应

        Args:
            agent_name: 目标 agent 名称
            input_content: 任务输入内容
            metadata: 任务元数据（可能包含委托上下文）
            webhook: 任务完成时的回调配置

        Returns:
            TaskResponse: 创建的任务信息

        Raises:
            GatewayAPIError: agent 不存在、不是 public、委托深度超限等
        """
        agent_config = self._get_agent_config(agent_name)
        self._ensure_public(agent_name, agent_config)

        # 从 metadata 中提取委托上下文，验证委托链路合法性
        clean_metadata, delegation_context = self._prepare_delegation_metadata(metadata)

        kind = agent_config["kind"]
        if kind == "remote_ref":
            # 远程 agent：通过 A2A 协议委托给远程网关
            return await self._create_remote_task(
                agent_name=agent_name,
                input_content=input_content,
                attachments=attachments or [],
                metadata=clean_metadata,
                webhook=webhook,
                delegation_context=delegation_context,
            )

        prepared_input = await self._prepare_input_with_attachments(
            input_content,
            attachments or [],
            task_id=None,
        )
        route_metadata = self._metadata_with_attachments(
            clean_metadata,
            prepared_input.attachment_metadata,
        )

        # 本地 agent：在本地运行时创建任务
        record = await self._spawn_local_task(
            agent_name,
            prepared_input.content,
            webhook=webhook,
            delegation_context=delegation_context,
        )

        # 持久化路由信息，用于后续查询和状态同步
        await self._route_store.asave_route(
            TaskRouteRecord(
                task_id=record.task_id,
                agent_name=agent_name,
                metadata=dict(route_metadata),
                route_kind="local",
                upstream_task_id=record.task_id,  # 本地任务的 upstream_task_id 就是自己
                webhook=dict(webhook) if webhook is not None else None,
            )
        )
        return self._build_task_response(record, route_metadata)

    async def get_task(self, task_id: str) -> TaskResponse:
        """
        获取任务状态

        根据任务路由类型采取不同策略：
        - local: 直接从本地运行时读取
        - remote_ref: 主动调用远程网关刷新状态（因为远程状态可能已更新）
        """
        route = await self._get_task_route(task_id)
        if route.route_kind == "remote_ref":
            # 远程任务：主动刷新以获取最新状态
            record = await self._refresh_route_task(route)
            return self._build_task_response(record, route.metadata)
        # 本地任务：直接读取
        record = self._get_local_task(task_id)
        return self._build_task_response(record, route.metadata)

    async def send_input(
        self,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
    ) -> TaskResponse:
        """
        向运行中的任务发送新输入

        用于实现任务的交互式对话，任务必须处于非运行状态才能接收输入。
        """
        route = await self._get_task_route(task_id)
        if route.route_kind == "remote_ref":
            try:
                self._ensure_runtime_record_for_route(route)
                record = await self._runtime.send_task_input(
                    task_id,
                    input_content,
                    attachments=[
                        attachment.model_dump(mode="json")
                        for attachment in (attachments or [])
                    ],
                )
            except UnknownWorkerTaskError as exc:
                raise GatewayAPIError(
                    status_code=404,
                    code="task_not_found",
                    message=f"Task '{task_id}' does not exist",
                ) from exc
            except TaskAlreadyRunningError as exc:
                raise GatewayAPIError(
                    status_code=409,
                    code="task_already_running",
                    message=f"Task '{task_id}' has an active run, cannot send input",
                ) from exc
            except A2AClientError as exc:
                raise self._remote_gateway_error(exc) from exc
            except ValueError as exc:
                raise self._upstream_payload_error(str(exc)) from exc
            return self._build_task_response(record, route.metadata)

        prepared_input = await self._prepare_input_with_attachments(
            input_content,
            attachments or [],
            task_id=task_id,
        )
        route_metadata = self._metadata_with_attachments(
            route.metadata,
            prepared_input.attachment_metadata,
        )
        try:
            # 确保运行时有该任务的记录（对于远程任务可能需要重建）
            self._ensure_runtime_record_for_route(route)
            record = await self._runtime.send_task_input(
                task_id,
                prepared_input.content,
            )
        except UnknownWorkerTaskError as exc:
            raise GatewayAPIError(
                status_code=404,
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            ) from exc
        except TaskAlreadyRunningError as exc:
            raise GatewayAPIError(
                status_code=409,
                code="task_already_running",
                message=f"Task '{task_id}' has an active run, cannot send input",
            ) from exc
        except A2AClientError as exc:
            raise self._remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise self._upstream_payload_error(str(exc)) from exc
        if prepared_input.attachment_metadata:
            route.metadata = route_metadata
            await self._route_store.asave_route(route)
        return self._build_task_response(record, route_metadata)

    async def cancel_task(self, task_id: str) -> TaskResponse:
        """取消正在运行的任务"""
        route = await self._get_task_route(task_id)
        try:
            self._ensure_runtime_record_for_route(route)
            record = await self._runtime.cancel_task(task_id)
        except UnknownWorkerTaskError as exc:
            raise GatewayAPIError(
                status_code=404,
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            ) from exc
        except A2AClientError as exc:
            raise self._remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise self._upstream_payload_error(str(exc)) from exc
        return self._build_task_response(record, route.metadata)

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskResponse:
        route = await self._get_task_route(task_id)
        try:
            record_before = self._ensure_runtime_record_for_route(route)
            if not self._record_has_review(record_before, review_id):
                if route.route_kind == "remote_ref":
                    record_before = await self._refresh_route_task(route)
            if not self._record_has_review(record_before, review_id):
                raise GatewayAPIError(
                    status_code=404,
                    code="review_not_found",
                    message=f"Review '{review_id}' does not belong to task '{task_id}'",
                )
            mirrored_source_task_id = (record_before.pending_review or {}).get(
                "source_task_id"
            )
            review_record = await self._runtime.submit_review_decision(
                review_id,
                decisions,
            )
        except UnknownWorkerTaskError as exc:
            raise GatewayAPIError(
                status_code=404,
                code="review_not_found",
                message=f"Review '{review_id}' does not exist",
            ) from exc
        except TaskAlreadyRunningError as exc:
            raise GatewayAPIError(
                status_code=409,
                code="task_already_running",
                message=f"Task '{task_id}' has an active run, cannot resume review",
            ) from exc
        except A2AClientError as exc:
            raise self._remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise self._upstream_payload_error(str(exc)) from exc
        if review_record.task_id == task_id:
            return self._build_task_response(review_record, route.metadata)
        if mirrored_source_task_id == review_record.task_id:
            refreshed_root = self._ensure_runtime_record_for_route(route)
            return self._build_task_response(refreshed_root, route.metadata)
        if review_record.task_id != task_id:
            raise GatewayAPIError(
                status_code=409,
                code="review_task_mismatch",
                message=f"Review '{review_id}' does not belong to task '{task_id}'",
            )
        return self._build_task_response(review_record, route.metadata)

    def _record_has_review(self, record: TaskRecord, review_id: str) -> bool:
        pending_review = record.pending_review or {}
        return pending_review.get("review_id") == review_id

    async def download_artifact(self, path: str) -> GatewayArtifact:
        self._ensure_workspace_path(path, kind="Artifact")
        result = await self._run_in_thread(self._runtime.download_files, [path])
        if not result:
            raise self._artifact_not_found(path)
        item = result[0]
        error = getattr(item, "error", None)
        content = getattr(item, "content", None)
        if error or content is None:
            raise self._artifact_not_found(path)
        if len(content) > self._artifact_max_bytes:
            raise GatewayAPIError(
                status_code=413,
                code="artifact_too_large",
                message=f"Artifact exceeds max size: {path}",
            )
        filename = PurePosixPath(path).name or "artifact"
        return GatewayArtifact(
            path=path,
            filename=filename,
            content=content,
            content_type=_guess_content_type(filename),
        )

    async def list_tasks(
        self,
        *,
        agent_name: str | None,
        status: str | None,
        metadata_filters: dict[str, str],
        cursor: str | None,
        limit: int,
    ) -> TaskListResponse:
        """
        分页查询任务列表，支持多维度过滤

        查询流程：
        1. 从 route_store 获取所有任务路由
        2. 对每个路由获取最新任务状态（远程任务会主动刷新）
        3. 应用过滤条件（agent_name, status, metadata）
        4. 按更新时间倒序排序
        5. 分页返回

        Args:
            agent_name: 按 agent 名称过滤
            status: 按任务状态过滤
            metadata_filters: 按元数据过滤（key-value 精确匹配）
            cursor: 分页游标（base64 编码的 offset）
            limit: 每页数量（1-100）

        Returns:
            TaskListResponse: 任务列表和下一页游标
        """
        if limit <= 0 or limit > 100:
            raise GatewayAPIError(
                status_code=400,
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )

        offset = self._decode_cursor(cursor)
        items: list[TaskResponse] = []

        # 遍历所有路由，获取任务状态并应用过滤
        for route in await self._route_store.alist_routes():
            item = await self._get_task_for_listing(route)
            if item is None:  # 任务已被删除
                continue
            if agent_name is not None and route.agent_name != agent_name:
                continue
            if status is not None and item.status != status:
                continue
            if not self._metadata_matches(route.metadata, metadata_filters):
                continue
            items.append(item)

        # 按更新时间倒序排序（最新的在前）
        items.sort(key=lambda item: (item.updated_at, item.task_id), reverse=True)
        page = items[offset : offset + limit]
        next_cursor = None
        if offset + limit < len(items):
            next_cursor = self._encode_cursor(offset + limit)
        return TaskListResponse(items=page, next_cursor=next_cursor)

    async def list_reviews(
        self,
        *,
        cursor: str | None,
        limit: int,
    ) -> ReviewListResponse:
        if limit <= 0 or limit > 100:
            raise GatewayAPIError(
                status_code=400,
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )

        offset = self._decode_cursor(cursor)
        items: list[ReviewResponse] = []
        for route in await self._route_store.alist_routes():
            task = await self._get_task_for_listing(route)
            if task is None or task.pending_review is None:
                continue
            record = self._ensure_runtime_record_for_route(route)
            review = self._build_review_response(
                record,
                route.metadata,
            )
            if review is not None:
                items.append(review)

        items.sort(key=lambda item: (item.updated_at, item.review_id), reverse=True)
        page = items[offset : offset + limit]
        next_cursor = None
        if offset + limit < len(items):
            next_cursor = self._encode_cursor(offset + limit)
        return ReviewListResponse(items=page, next_cursor=next_cursor)

    async def get_review(self, review_id: str) -> ReviewResponse:
        for route in await self._route_store.alist_routes():
            task = await self._get_task_for_listing(route)
            if task is None or task.pending_review is None:
                continue
            if task.pending_review.get("review_id") != review_id:
                continue
            record = self._ensure_runtime_record_for_route(route)
            review = self._build_review_response(record, route.metadata)
            if review is not None:
                return review
        raise GatewayAPIError(
            status_code=404,
            code="review_not_found",
            message=f"Review '{review_id}' does not exist",
        )

    async def list_task_reviews(self, task_id: str) -> ReviewListResponse:
        route = await self._get_task_route(task_id)
        task = await self._get_task_for_listing(route)
        if task is None:
            raise GatewayAPIError(
                status_code=404,
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            )
        record = self._ensure_runtime_record_for_route(route)
        review = self._build_review_response(record, route.metadata)
        return ReviewListResponse(
            items=[review] if review is not None else [],
            next_cursor=None,
        )

    def _get_agent_config(self, agent_name: str) -> dict[str, Any]:
        try:
            return self._agent_configs[agent_name]
        except KeyError as exc:
            raise GatewayAPIError(
                status_code=404,
                code="agent_not_found",
                message=f"Agent '{agent_name}' does not exist",
            ) from exc

    def _ensure_public(self, agent_name: str, agent_config: dict[str, Any]) -> None:
        if agent_config["public"]:
            return
        raise GatewayAPIError(
            status_code=403,
            code="agent_not_public",
            message=f"Agent '{agent_name}' is not publicly callable",
        )

    async def _spawn_local_task(
        self,
        agent_name: str,
        input_content: str,
        *,
        webhook: dict[str, MetadataScalar] | None = None,
        delegation_context: DelegationContext | None = None,
    ) -> TaskRecord:
        """
        在本地运行时创建任务

        将内部异常转换为标准 GatewayAPIError，包括：
        - UnknownAgentTargetError: agent 未在运行时注册
        - MaxDelegationDepthError: 委托深度超限
        - MaxTasksPerRootError: 任务预算耗尽
        """
        try:
            return await self._runtime.spawn_task(
                agent_name,
                input_content,
                webhook=dict(webhook) if webhook is not None else None,
                delegation_context=delegation_context,
            )
        except UnknownAgentTargetError as exc:
            raise GatewayAPIError(
                status_code=503,
                code="runtime_unavailable",
                message=f"Local runtime is not configured for agent '{agent_name}'",
            ) from exc
        except RemoteExecutorNotImplementedError as exc:
            raise GatewayAPIError(
                status_code=422,
                code="remote_executor_not_implemented",
                message=str(exc),
            ) from exc
        except MaxDelegationDepthError as exc:
            raise self._delegation_depth_error(
                exc.current_depth, exc.max_depth
            ) from exc
        except MaxTasksPerRootError as exc:
            raise self._delegation_budget_error(exc) from exc

    async def _create_remote_task(
        self,
        *,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput],
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None = None,
        delegation_context: DelegationContext | None = None,
    ) -> TaskResponse:
        """
        创建远程任务并持久化路由信息

        流程：
        1. 通过运行时调用远程网关的 A2A API 创建任务
        2. 获取远程返回的 upstream_task_id
        3. 创建本地 task_id 到 upstream_task_id 的映射
        4. 持久化路由信息到 route_store

        这个映射关系用于：
        - 后续通过本地 task_id 查询远程任务状态
        - 接收远程 webhook 时反向查找本地 task_id
        """
        try:
            record = await self._runtime.spawn_task(
                agent_name,
                input_content,
                attachments=[
                    attachment.model_dump(mode="json")
                    for attachment in attachments
                ],
                metadata=dict(metadata),
                webhook=dict(webhook) if webhook is not None else None,
                delegation_context=delegation_context,
            )
        except UnknownAgentTargetError as exc:
            raise GatewayAPIError(
                status_code=503,
                code="runtime_unavailable",
                message=f"Runtime is not configured for agent '{agent_name}'",
            ) from exc
        except RemoteExecutorNotImplementedError as exc:
            raise GatewayAPIError(
                status_code=422,
                code="remote_executor_not_implemented",
                message=str(exc),
            ) from exc
        except MaxDelegationDepthError as exc:
            raise self._delegation_depth_error(
                exc.current_depth, exc.max_depth
            ) from exc
        except MaxTasksPerRootError as exc:
            raise self._delegation_budget_error(exc) from exc
        except A2AClientError as exc:
            raise self._remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise self._upstream_payload_error(str(exc)) from exc

        # 验证远程网关返回了 upstream_task_id
        if not record.upstream_task_id:
            raise self._upstream_payload_error(
                f"Remote ref '{agent_name}' returned no upstream task id"
            )

        # 持久化路由映射：本地 task_id → 远程 upstream_task_id
        route = TaskRouteRecord(
            task_id=record.task_id,
            agent_name=agent_name,
            metadata=dict(metadata),
            route_kind="remote_ref",
            upstream_task_id=record.upstream_task_id,
            webhook=dict(webhook) if webhook is not None else None,
        )
        await self._route_store.asave_route(route)
        return self._build_task_response(record, metadata)

    def _get_local_task(self, task_id: str) -> TaskRecord:
        try:
            return self._runtime.get_task_record(task_id)
        except UnknownWorkerTaskError as exc:
            raise GatewayAPIError(
                status_code=404,
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            ) from exc

    async def _get_task_route(self, task_id: str) -> TaskRouteRecord:
        route = await self._route_store.aget_route(task_id)
        if route is None:
            raise GatewayAPIError(
                status_code=404,
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            )
        return route

    def _prepare_delegation_metadata(
        self,
        metadata: dict[str, MetadataScalar],
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
        """
        从 metadata 中提取和验证委托上下文

        委托上下文用于跟踪任务委托链路，防止：
        - 循环委托（A → B → A）
        - 深度超限（委托层级过深）

        Returns:
            (clean_metadata, delegation_context): 清理后的元数据和提取的委托上下文
        """
        try:
            return self._runtime.prepare_delegation_metadata(metadata)
        except InvalidDelegationContextError as exc:
            raise GatewayAPIError(
                status_code=400,
                code="invalid_delegation_context",
                message=str(exc),
            ) from exc
        except DelegationLoopError as exc:
            raise GatewayAPIError(
                status_code=403,
                code="delegation_loop_detected",
                message=str(exc),
            ) from exc
        except DelegationContextDepthError as exc:
            raise self._delegation_depth_error(
                exc.current_depth,
                exc.max_depth,
            ) from exc

    def _ensure_runtime_record_for_route(
        self,
        route: TaskRouteRecord,
    ) -> TaskRecord:
        """
        确保运行时有该任务的记录

        对于远程任务，可能需要重建本地的任务记录（例如重启后）。
        这个方法会根据路由信息重新创建运行时的任务跟踪记录。
        """
        if route.route_kind != "remote_ref":
            return self._get_local_task(route.task_id)
        try:
            return self._runtime.ensure_remote_task_record(
                agent_name=route.agent_name,
                task_id=route.task_id,
                upstream_task_id=route.upstream_task_id,
                webhook=dict(route.webhook) if route.webhook is not None else None,
            )
        except UnknownAgentTargetError as exc:
            raise GatewayAPIError(
                status_code=503,
                code="runtime_unavailable",
                message=f"Runtime is not configured for agent '{route.agent_name}'",
            ) from exc
        except ValueError as exc:
            raise self._upstream_payload_error(str(exc)) from exc

    async def _refresh_route_task(self, route: TaskRouteRecord) -> TaskRecord:
        """
        刷新远程任务的状态

        主动调用远程网关的 API 获取最新任务状态，
        因为远程任务的状态可能在我们不知情的情况下发生变化。
        """
        self._ensure_runtime_record_for_route(route)
        try:
            return await self._runtime.refresh_task(route.task_id)
        except UnknownWorkerTaskError as exc:
            raise GatewayAPIError(
                status_code=404,
                code="task_not_found",
                message=f"Task '{route.task_id}' does not exist",
            ) from exc
        except A2AClientError as exc:
            raise self._remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise self._upstream_payload_error(str(exc)) from exc

    def _remote_gateway_error(self, exc: A2AClientError) -> GatewayAPIError:
        return GatewayAPIError(
            status_code=exc.status_code,
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )

    def _delegation_depth_error(
        self,
        current_depth: int,
        max_depth: int,
    ) -> GatewayAPIError:
        return GatewayAPIError(
            status_code=403,
            code="delegation_depth_exceeded",
            message=(
                "Delegation depth limit exceeded: "
                f"current_depth={current_depth} max_depth={max_depth}"
            ),
        )

    def _delegation_budget_error(self, exc: MaxTasksPerRootError) -> GatewayAPIError:
        return GatewayAPIError(
            status_code=429,
            code="delegation_budget_exhausted",
            message=(
                "Task budget exhausted: "
                f"root_task_id={exc.root_task_id} "
                f"current_count={exc.current_count} "
                f"max_tasks_per_root={exc.max_tasks_per_root}"
            ),
        )

    def _upstream_payload_error(self, message: str) -> GatewayAPIError:
        return GatewayAPIError(
            status_code=502,
            code="upstream_gateway_error",
            message=message,
        )

    async def _prepare_input_with_attachments(
        self,
        content: str,
        attachments: list[AttachmentInput],
        *,
        task_id: str | None,
    ) -> PreparedInput:
        if not attachments:
            return PreparedInput(content=content, attachment_metadata=[])

        workspace_root = self._normalized_workspace_root()
        if workspace_root is None:
            raise GatewayAPIError(
                status_code=503,
                code="runtime_unavailable",
                message="Runtime workspace root is not configured",
            )
        batch_id = task_id or str(uuid4())
        upload_items: list[tuple[str, bytes]] = []
        attachment_metadata: list[dict[str, str]] = []
        for index, attachment in enumerate(attachments, start=1):
            filename = _sanitize_attachment_name(attachment.name)
            try:
                content_bytes = base64.b64decode(
                    attachment.data_base64,
                    validate=True,
                )
            except (binascii.Error, ValueError) as exc:
                raise GatewayAPIError(
                    status_code=400,
                    code="invalid_attachment",
                    message=f"Attachment '{attachment.name}' is not valid base64",
                ) from exc
            if len(content_bytes) > self._attachment_max_bytes:
                raise GatewayAPIError(
                    status_code=413,
                    code="attachment_too_large",
                    message=f"Attachment '{attachment.name}' exceeds max size",
                )
            path = str(
                PurePosixPath(workspace_root)
                / ATTACHMENT_INBOX_SUBDIR
                / batch_id
                / f"{index:02d}-{filename}"
            )
            self._ensure_workspace_path(path, kind="Attachment upload")
            if not PurePosixPath(path).is_relative_to(
                PurePosixPath(workspace_root) / ATTACHMENT_INBOX_SUBDIR
            ):
                raise GatewayAPIError(
                    status_code=503,
                    code="runtime_unavailable",
                    message="Attachment upload path is outside the gateway inbox",
                )
            upload_items.append((path, content_bytes))
            attachment_metadata.append(
                {
                    "name": filename,
                    "path": path,
                    "content_type": attachment.content_type or "",
                    "kind": attachment.kind,
                }
            )

        upload_result = await self._run_in_thread(
            self._runtime.upload_files,
            upload_items,
        )
        if len(upload_result) != len(upload_items):
            raise GatewayAPIError(
                status_code=502,
                code="attachment_upload_failed",
                message="Runtime returned an incomplete attachment upload result",
            )
        for metadata, result in zip(attachment_metadata, upload_result, strict=False):
            error = getattr(result, "error", None)
            if error:
                raise GatewayAPIError(
                    status_code=502,
                    code="attachment_upload_failed",
                    message=f"Failed to upload attachment '{metadata['name']}': {error}",
                )

        return PreparedInput(
            content=self._append_attachment_context(content, attachment_metadata),
            attachment_metadata=attachment_metadata,
        )

    async def _run_in_thread(self, func: Callable[..., Any], *args: Any) -> Any:
        import asyncio

        return await asyncio.to_thread(func, *args)

    def _append_attachment_context(
        self,
        content: str,
        attachments: list[dict[str, str]],
    ) -> str:
        lines = ["", "Uploaded attachments:"]
        for attachment in attachments:
            details = [
                f"name={attachment['name']}",
                f"path={attachment['path']}",
            ]
            if attachment["content_type"]:
                details.append(f"content_type={attachment['content_type']}")
            details.append(f"kind={attachment['kind']}")
            lines.append(f"- {' | '.join(details)}")
        return content.rstrip() + "\n" + "\n".join(lines)

    def _metadata_with_attachments(
        self,
        metadata: dict[str, MetadataScalar],
        attachments: list[dict[str, str]],
    ) -> dict[str, MetadataScalar]:
        if not attachments:
            return dict(metadata)
        result = dict(metadata)
        compact = [
            "|".join(
                [
                    item["name"],
                    item["path"],
                    item.get("content_type", ""),
                    item["kind"],
                ]
            )
            for item in attachments
        ]
        result[ATTACHMENT_METADATA_KEY] = "\n".join(compact)
        return result

    def _artifact_not_found(self, path: str) -> GatewayAPIError:
        return GatewayAPIError(
            status_code=404,
            code="artifact_not_found",
            message=f"Artifact is not readable: {path}",
        )

    def _normalized_workspace_root(self) -> str | None:
        raw_root = self._runtime.get_workspace_root().strip()
        if not raw_root:
            return None
        if raw_root != "/":
            raw_root = raw_root.rstrip("/")
        root = PurePosixPath(raw_root)
        if not root.is_absolute() or ".." in root.parts:
            raise GatewayAPIError(
                status_code=503,
                code="runtime_unavailable",
                message="Runtime workspace root must be an absolute normalized path",
            )
        return str(root)

    def _ensure_workspace_path(self, path: str, *, kind: str) -> None:
        root_text = self._normalized_workspace_root()
        if root_text is None:
            raise GatewayAPIError(
                status_code=503,
                code="runtime_unavailable",
                message="Runtime workspace root is not configured",
            )
        candidate = PurePosixPath(path)
        if (
            not candidate.is_absolute()
            or ".." in candidate.parts
            or not candidate.is_relative_to(PurePosixPath(root_text))
        ):
            raise GatewayAPIError(
                status_code=403,
                code="workspace_path_forbidden",
                message=f"{kind} path is outside the runtime workspace",
            )

    def _build_agent_ref_response(
        self,
        agent_name: str,
        agent_config: dict[str, Any],
    ) -> AgentRefResponse:
        return AgentRefResponse(
            name=agent_config["name"],
            kind=agent_config["kind"],
            public=agent_config["public"],
            description=agent_config["description"],
            is_default=agent_name == self._main_agent_name,
        )

    def _build_task_response(
        self,
        record: TaskRecord,
        metadata: dict[str, MetadataScalar],
    ) -> TaskResponse:
        return TaskResponse(
            task_id=record.task_id,
            agent_name=record.agent_name,
            status=record.state,
            last_result=record.result,
            error=record.error,
            run_count=record.run_count,
            created_at=record.created_at,
            updated_at=record.updated_at,
            metadata=dict(metadata),
            pending_review=record.pending_review,
        )

    def _build_review_response(
        self,
        record: TaskRecord,
        metadata: dict[str, MetadataScalar],
    ) -> ReviewResponse | None:
        pending_review = record.pending_review
        if record.state != "waiting_for_human" or pending_review is None:
            return None
        review_id = pending_review.get("review_id")
        if not isinstance(review_id, str) or not review_id:
            return None
        raw_actions = pending_review.get("action_requests")
        raw_configs = pending_review.get("review_configs")
        return ReviewResponse(
            review_id=review_id,
            task_id=record.task_id,
            thread_id=record.thread_id,
            agent_name=record.agent_name,
            route_kind=record.route_kind,
            status="pending",
            action_requests=raw_actions if isinstance(raw_actions, list) else [],
            review_configs=raw_configs if isinstance(raw_configs, list) else [],
            created_at=record.updated_at,
            updated_at=record.updated_at,
            metadata=dict(metadata),
        )

    async def _get_task_for_listing(
        self,
        route: TaskRouteRecord,
    ) -> TaskResponse | None:
        if route.route_kind == "remote_ref":
            record = await self._refresh_route_task(route)
            return self._build_task_response(record, route.metadata)
        try:
            record = self._get_local_task(route.task_id)
        except GatewayAPIError:
            return None
        return self._build_task_response(record, route.metadata)

    async def handle_task_webhook(self, event: TaskWebhookEvent) -> dict[str, Any]:
        """
        处理远程任务的 webhook 事件

        Webhook 分发策略：
        1. 首先尝试交给主运行时处理（可能是直接委托的远程任务）
        2. 如果主运行时不认识这个任务，通过 upstream_task_id 反向查找路由
        3. 如果找到路由且是 remote_ref，重建运行时记录后再次尝试处理
        4. 最后遍历所有额外的事件处理器（用于多网关级联场景）

        这个设计支持多级网关级联：
        Gateway A → Gateway B → Gateway C
        当 C 完成任务时，webhook 会依次通知 B 和 A

        Returns:
            {"delivered": count}: 成功投递的处理器数量
        """
        payload = event.model_dump(mode="json")
        delivered = 0

        # 尝试主运行时处理
        if await self._runtime.handle_remote_task_event(payload):
            delivered += 1
        else:
            # 主运行时不认识，尝试通过 upstream_task_id 反向查找
            route = await self._route_store.aget_route_by_upstream_task_id(
                event.task_id
            )
            if route is not None and route.route_kind == "remote_ref":
                try:
                    # 重建运行时记录
                    self._ensure_runtime_record_for_route(route)
                except GatewayAPIError:
                    pass
                else:
                    if await self._runtime.handle_remote_task_event(payload):
                        delivered += 1

        # 分发给额外的事件处理器（多网关级联场景）
        for handler in self._remote_event_handlers:
            if handler is self._runtime:
                continue
            if await handler.handle_remote_task_event(payload):
                delivered += 1
        return {"delivered": delivered}

    def _encode_cursor(self, offset: int) -> str:
        return base64.urlsafe_b64encode(str(offset).encode("utf-8")).decode("ascii")

    def _decode_cursor(self, cursor: str | None) -> int:
        if cursor is None:
            return 0
        try:
            decoded = base64.urlsafe_b64decode(cursor.encode("ascii")).decode("utf-8")
            offset = int(decoded)
        except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
            raise GatewayAPIError(
                status_code=400,
                code="invalid_request",
                message="Query parameter 'cursor' is invalid",
            ) from exc
        if offset < 0:
            raise GatewayAPIError(
                status_code=400,
                code="invalid_request",
                message="Query parameter 'cursor' is invalid",
            )
        return offset

    def _metadata_matches(
        self,
        metadata: dict[str, MetadataScalar],
        filters: dict[str, str],
    ) -> bool:
        for key, expected in filters.items():
            actual = metadata.get(key)
            if self._stringify_metadata_value(actual) != expected:
                return False
        return True

    def _stringify_metadata_value(self, value: MetadataScalar) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        return str(value)


def _sanitize_attachment_name(name: str) -> str:
    basename = PurePosixPath(name.replace("\\", "/")).name.strip()
    if not basename or basename in {".", ".."}:
        basename = "attachment"
    sanitized = "".join(
        char if char in SAFE_ATTACHMENT_CHARS else "_"
        for char in basename
    ).strip("._")
    return sanitized or "attachment"


def _guess_content_type(filename: str) -> str:
    suffix = PurePosixPath(filename).suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".pdf": "application/pdf",
        ".txt": "text/plain",
        ".md": "text/markdown",
        ".csv": "text/csv",
        ".json": "application/json",
        ".html": "text/html",
        ".zip": "application/zip",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".ogg": "audio/ogg",
        ".mp4": "video/mp4",
    }.get(suffix, "application/octet-stream")


def attach_gateway_routes(
    app: FastAPI,
    *,
    service_getter: Callable[[Request], GatewayService],
    bearer_token: str,
) -> None:
    """
    将网关路由附加到 FastAPI 应用

    注册所有网关 API 端点和异常处理器。

    API 端点：
    - GET /agents - 列出所有 public agent
    - GET /agents/{agent_name} - 获取 agent 详情
    - POST /agents/{agent_name}/tasks - 创建任务
    - GET /tasks/{task_id} - 获取任务状态
    - POST /tasks/{task_id}/input - 发送任务输入
    - POST /tasks/{task_id}/cancel - 取消任务
    - GET /tasks - 查询任务列表
    - POST /webhooks/tasks - 接收远程任务 webhook

    所有端点都需要 Bearer Token 认证。

    Args:
        app: FastAPI 应用实例
        service_getter: 从请求获取 GatewayService 的函数
        bearer_token: API 认证令牌
    """

    @app.exception_handler(GatewayAPIError)
    async def handle_gateway_error(
        request: Request,
        exc: GatewayAPIError,
    ) -> JSONResponse:
        """将 GatewayAPIError 转换为标准 JSON 错误响应"""
        payload: dict[str, Any] = {
            "error": {
                "code": exc.code,
                "message": exc.message,
            }
        }
        if exc.details:
            payload["error"]["details"] = exc.details
        return JSONResponse(status_code=exc.status_code, content=payload)

    @app.exception_handler(Exception)
    async def handle_unexpected_error(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        """捕获所有未处理的异常，返回 500 错误"""
        return JSONResponse(
            status_code=500,
            content={
                "error": {
                    "code": "internal_error",
                    "message": "Internal gateway error",
                }
            },
        )

    async def require_bearer(
        authorization: str | None = Header(default=None),
    ) -> None:
        """验证 Bearer Token"""
        expected = f"Bearer {bearer_token}"
        if authorization is None or not secrets.compare_digest(authorization, expected):
            raise GatewayAPIError(
                status_code=401,
                code="unauthorized",
                message="Missing or invalid bearer token",
            )

    @app.get("/agents", response_model=AgentListResponse)
    async def list_agents(
        request: Request,
        _: None = Depends(require_bearer),
    ) -> AgentListResponse:
        service = service_getter(request)
        return AgentListResponse(items=service.list_agents())

    @app.get("/agents/{agent_name}", response_model=AgentRefResponse)
    async def get_agent(
        request: Request,
        agent_name: str,
        _: None = Depends(require_bearer),
    ) -> AgentRefResponse:
        service = service_getter(request)
        return service.get_agent(agent_name)

    @app.post(
        "/agents/{agent_name}/tasks", response_model=TaskResponse, status_code=201
    )
    async def create_task(
        request: Request,
        agent_name: str,
        payload: CreateTaskRequest,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        service = service_getter(request)
        task = await service.create_task(
            agent_name=agent_name,
            input_content=payload.input.content,
            attachments=payload.input.attachments,
            metadata=payload.metadata,
            webhook=payload.webhook,
        )
        return JSONResponse(
            status_code=201,
            headers={"Location": f"/tasks/{task.task_id}"},
            content=task.model_dump(mode="json"),
        )

    @app.get("/tasks/{task_id}", response_model=TaskResponse)
    async def get_task(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> TaskResponse:
        service = service_getter(request)
        return await service.get_task(task_id)

    @app.get("/reviews", response_model=ReviewListResponse)
    async def list_reviews(
        request: Request,
        _: None = Depends(require_bearer),
    ) -> ReviewListResponse:
        service = service_getter(request)
        return await service.list_reviews(
            cursor=request.query_params.get("cursor"),
            limit=int(request.query_params.get("limit", "20")),
        )

    @app.get("/reviews/{review_id}", response_model=ReviewResponse)
    async def get_review(
        request: Request,
        review_id: str,
        _: None = Depends(require_bearer),
    ) -> ReviewResponse:
        service = service_getter(request)
        return await service.get_review(review_id)

    @app.get("/tasks/{task_id}/reviews", response_model=ReviewListResponse)
    async def list_task_reviews(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> ReviewListResponse:
        service = service_getter(request)
        return await service.list_task_reviews(task_id)

    @app.post("/tasks/{task_id}/input", response_model=TaskResponse, status_code=202)
    async def send_input(
        request: Request,
        task_id: str,
        payload: SendInputRequest,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        service = service_getter(request)
        task = await service.send_input(
            task_id,
            payload.input.content,
            attachments=payload.input.attachments,
        )
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))

    @app.post("/artifacts/download")
    async def download_artifact(
        request: Request,
        payload: ArtifactDownloadRequest,
        _: None = Depends(require_bearer),
    ) -> Response:
        service = service_getter(request)
        artifact = await service.download_artifact(payload.path)
        headers = {
            "Content-Disposition": f'attachment; filename="{artifact.filename}"',
            "X-Artifact-Path": artifact.path,
        }
        return Response(
            content=artifact.content,
            media_type=artifact.content_type,
            headers=headers,
        )

    @app.post("/tasks/{task_id}/cancel", response_model=TaskResponse, status_code=202)
    async def cancel_task(
        request: Request,
        task_id: str,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        service = service_getter(request)
        task = await service.cancel_task(task_id)
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))

    @app.post(
        "/tasks/{task_id}/reviews/{review_id}/decision",
        response_model=TaskResponse,
        status_code=202,
    )
    async def submit_review_decision(
        request: Request,
        task_id: str,
        review_id: str,
        payload: ReviewDecisionInput,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        service = service_getter(request)
        task = await service.submit_review_decision(
            task_id=task_id,
            review_id=review_id,
            decisions=payload.decisions,
        )
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))

    @app.post("/webhooks/tasks", status_code=202)
    async def receive_task_webhook(
        request: Request,
        payload: TaskWebhookEvent,
        _: None = Depends(require_bearer),
    ) -> JSONResponse:
        service = service_getter(request)
        result = await service.handle_task_webhook(payload)
        return JSONResponse(status_code=202, content=result)

    @app.get("/tasks", response_model=TaskListResponse)
    async def list_tasks(
        request: Request,
        _: None = Depends(require_bearer),
    ) -> TaskListResponse:
        """
        查询任务列表

        支持的查询参数：
        - agent_name: 按 agent 名称过滤
        - status: 按任务状态过滤 (pending/running/completed/failed/cancelled)
        - metadata.{key}: 按元数据过滤，例如 metadata.user_id=123
        - cursor: 分页游标（从上一页的 next_cursor 获取）
        - limit: 每页数量，默认 20，最大 100
        """
        service = service_getter(request)
        # 提取 metadata.* 查询参数
        metadata_filters = {
            key.removeprefix("metadata."): value
            for key, value in request.query_params.items()
            if key.startswith("metadata.")
        }
        limit_raw = request.query_params.get("limit")
        limit = 20 if limit_raw is None else _parse_limit(limit_raw)
        return await service.list_tasks(
            agent_name=request.query_params.get("agent_name"),
            status=request.query_params.get("status"),
            metadata_filters=metadata_filters,
            cursor=request.query_params.get("cursor"),
            limit=limit,
        )


def create_gateway_app(*, service: GatewayService, bearer_token: str) -> FastAPI:
    """
    创建完整的网关 FastAPI 应用

    这是一个便捷函数，用于快速创建一个包含所有网关路由的应用。

    Args:
        service: GatewayService 实例
        bearer_token: API 认证令牌

    Returns:
        FastAPI: 配置好的应用实例
    """
    app = FastAPI(title="ruyi-agent Gateway")
    attach_gateway_routes(
        app,
        service_getter=lambda request: service,
        bearer_token=bearer_token,
    )
    return app


def _parse_limit(raw: str) -> int:
    try:
        return int(raw)
    except ValueError as exc:
        raise GatewayAPIError(
            status_code=400,
            code="invalid_request",
            message="Query parameter 'limit' must be an integer",
        ) from exc
