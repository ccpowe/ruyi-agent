"""
Gateway Task Module - Agent-to-Agent 任务委托与路由

这个模块实现与传输协议无关的 Gateway Task 业务，用于管理多个 agent 之间的任务委托和路由。

核心职责：
1. 提供 Gateway Task Interface 供 Adapter 创建和管理 agent 任务
2. 路由任务到本地 agent 或远程 agent（通过 A2A 协议）
3. 管理委托上下文，防止循环委托和深度超限
4. 持久化任务路由信息，支持任务状态查询和 webhook 回调

数据流：
  外部请求 → Adapter → GatewayTaskModule → AgentControl → Agent 执行
                              ↓
                        GatewayRouteStore

关键概念：
- Local Agent: 在本地运行时执行的 agent
- Remote Ref: 通过 A2A 协议委托给远程网关的 agent 引用
- Delegation Context: 跟踪委托链路，防止循环和深度超限
- Task Route: 记录任务的路由信息（本地 task_id 到远程 upstream_task_id 的映射）
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any
from uuid import uuid4

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import (
    AgentRefResponse,
    AttachmentInput,
    GatewayArtifact,
    PreparedInput,
    PublishedArtifactResponse,
    ReviewListResponse,
    ReviewResponse,
    TaskListResponse,
    TaskMessageListResponse,
    TaskMessageResponse,
    TaskMessageToolCallResponse,
    TaskResponse,
    TaskWebhookEvent,
)
from ruyi_agent.gateway.routing import TaskRouter
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.task_events import TaskStreamEvent
from ruyi_agent.runtime.delegation.context import DelegationContext
from ruyi_agent.task_models import (
    MetadataScalar,
    PublishedArtifact,
    TaskRecord,
    TaskRouteRecord,
)
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.gateway_command_store import (
    GatewayCommandClaim,
    GatewayCommandConflictError,
    GatewayCommandStore,
)

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
DEFAULT_GATEWAY_PRINCIPAL = "gateway-bearer"
COMMAND_WAIT_TIMEOUT_SECONDS = 30.0
DEFAULT_REMOTE_LISTING_CONCURRENCY = 8


@dataclass(frozen=True, slots=True)
class GatewayCommandOutcome:
    task: TaskResponse
    replayed: bool


class GatewayTaskModule:
    """
    网关服务核心业务逻辑层

    负责处理所有网关业务逻辑，包括：
    1. Agent 管理：列出和获取已注册的 agent
    2. 任务路由：根据 agent 类型（local/remote_ref）创建和路由任务
    3. 委托控制：通过 DelegationContext 防止循环委托和深度超限
    4. 任务生命周期：创建、查询、发送输入、取消任务
    5. Webhook 处理：接收和分发远程任务的状态更新

    数据流：
    - 创建任务：Adapter → create_task → TaskRouter → AgentControl
    - 查询任务：Adapter → get_task → TaskRouter.get_record
    - 任务输入：Adapter → send_input → TaskRouter.send_input

    Attributes:
        _main_agent_name: 默认 agent 名称
        _agent_configs: Agent 配置字典 {agent_name: {kind, public, description, ...}}
        _control: 唯一的 AgentControl 任务运行时
    """

    def __init__(
        self,
        *,
        main_agent_name: str,
        agent_configs: dict[str, dict[str, Any]],
        control: AgentControl,
        route_store: GatewayRouteStore | None = None,
        command_store: GatewayCommandStore | None = None,
        remote_event_handlers: list[AgentControl] | None = None,
        attachment_max_bytes: int = DEFAULT_ATTACHMENT_MAX_BYTES,
        artifact_max_bytes: int = DEFAULT_ARTIFACT_MAX_BYTES,
        unavailable_agents: dict[str, str] | None = None,
        remote_listing_concurrency: int = DEFAULT_REMOTE_LISTING_CONCURRENCY,
    ) -> None:
        if remote_listing_concurrency <= 0:
            raise ValueError("remote_listing_concurrency must be positive")
        self._main_agent_name = main_agent_name
        self._agent_configs = agent_configs
        self._control = control
        route_store = route_store or GatewayRouteStore(":memory:")
        self._router = TaskRouter(
            control=control,
            route_store=route_store,
            remote_event_handlers=remote_event_handlers,
        )
        self._command_store = command_store or GatewayCommandStore(":memory:")
        self._attachment_max_bytes = attachment_max_bytes
        self._artifact_max_bytes = artifact_max_bytes
        self._unavailable_agents = dict(unavailable_agents or {})
        self._remote_listing_concurrency = remote_listing_concurrency

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
        """获取指定 public agent 的详细信息。"""
        agent_config = self._get_agent_config(agent_name)
        self._ensure_public(agent_name, agent_config)
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
        outcome = await self.create_task_command(
            agent_name=agent_name,
            input_content=input_content,
            attachments=attachments,
            metadata=metadata,
            webhook=webhook,
            idempotency_key=None,
        )
        return outcome.task

    async def create_task_command(
        self,
        *,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None = None,
        idempotency_key: str | None,
        principal_id: str = DEFAULT_GATEWAY_PRINCIPAL,
    ) -> GatewayCommandOutcome:
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
            GatewayTaskError: agent 不存在、不是 public、委托深度超限等
        """
        _validate_idempotency_key(idempotency_key)
        normalized_attachments = list(attachments or [])
        if idempotency_key is None:
            task = await self._create_task_from_request(
                task_id=str(uuid4()),
                agent_name=agent_name,
                input_content=input_content,
                attachments=normalized_attachments,
                metadata=metadata,
                webhook=webhook,
                idempotency_key=None,
            )
            return GatewayCommandOutcome(task=task, replayed=False)

        request_hash = _command_request_hash(
            operation="create_task",
            target=agent_name,
            body={
                "input": {
                    "content": input_content,
                    "attachments": [
                        item.model_dump(mode="json") for item in normalized_attachments
                    ],
                },
                "metadata": metadata,
                "webhook": webhook,
            },
        )
        claim = await self._claim_command(
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            operation="create_task",
            target=agent_name,
            request_hash=request_hash,
            proposed_task_id=str(uuid4()),
        )
        if claim.status == "replay":
            return GatewayCommandOutcome(
                task=TaskResponse.model_validate_json(claim.response_json),
                replayed=True,
            )
        return await self._execute_claimed_command(
            claim,
            self._create_task_from_request(
                task_id=claim.task_id,
                agent_name=agent_name,
                input_content=input_content,
                attachments=normalized_attachments,
                metadata=metadata,
                webhook=webhook,
                idempotency_key=idempotency_key,
            ),
        )

    async def _create_task_from_request(
        self,
        *,
        task_id: str,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput],
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None,
        idempotency_key: str | None,
    ) -> TaskResponse:
        agent_config = self._get_agent_config(agent_name)
        self._ensure_public(agent_name, agent_config)
        self._ensure_available(agent_name)
        clean_metadata, delegation_context = self._router.prepare_delegation_metadata(
            metadata
        )
        return await self._create_task_effect(
            task_id=task_id,
            agent_name=agent_name,
            agent_config=agent_config,
            input_content=input_content,
            attachments=attachments,
            clean_metadata=clean_metadata,
            webhook=webhook,
            delegation_context=delegation_context,
            idempotency_key=idempotency_key,
        )

    async def _create_task_effect(
        self,
        *,
        task_id: str,
        agent_name: str,
        agent_config: dict[str, Any],
        input_content: str,
        attachments: list[AttachmentInput],
        clean_metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None,
        delegation_context: DelegationContext | None,
        idempotency_key: str | None,
    ) -> TaskResponse:
        kind = agent_config["kind"]
        if kind == "remote_ref":
            routed = await self._router.create_task(
                agent_name=agent_name,
                route_kind="remote_ref",
                input_content=input_content,
                attachments=[
                    attachment.model_dump(mode="json")
                    for attachment in attachments
                ],
                metadata=clean_metadata,
                webhook=webhook,
                delegation_context=delegation_context,
                task_id=task_id,
                idempotency_key=idempotency_key,
            )
            return self._build_task_response(routed.record, routed.route.metadata)

        prepared_input = await self._prepare_input_with_attachments(
            input_content,
            attachments,
            batch_id=task_id,
        )
        route_metadata = self._metadata_with_attachments(
            clean_metadata,
            prepared_input.attachment_metadata,
        )

        # 本地 agent：在本地运行时创建任务
        routed = await self._router.create_task(
            agent_name=agent_name,
            route_kind="local",
            input_content=prepared_input.content,
            metadata=route_metadata,
            webhook=webhook,
            delegation_context=delegation_context,
            task_id=task_id,
            idempotency_key=idempotency_key,
        )
        return self._build_task_response(routed.record, routed.route.metadata)

    async def get_task(self, task_id: str) -> TaskResponse:
        """
        获取任务状态

        根据任务路由类型采取不同策略：
        - local: 直接从本地运行时读取
        - remote_ref: 主动调用远程网关刷新状态（因为远程状态可能已更新）
        """
        route = await self._router.get_route(task_id)
        record = await self._router.get_record(route)
        return self._build_task_response(record, route.metadata)

    async def list_task_messages(
        self,
        task_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> TaskMessageListResponse:
        """Return one snapshot-consistent page of the public textual transcript."""

        if limit <= 0 or limit > 100:
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )
        route = await self._router.get_route(task_id)
        page = await self._router.list_task_messages(
            route,
            cursor=cursor,
            limit=limit,
        )
        return TaskMessageListResponse(
            task_id=page.task_id,
            items=[
                TaskMessageResponse(
                    sequence=item.sequence,
                    message_id=item.message_id,
                    role=item.role,
                    content=item.content,
                    name=item.name,
                    tool_call_id=item.tool_call_id,
                    tool_calls=[
                        TaskMessageToolCallResponse(
                            tool_call_id=call.tool_call_id,
                            name=call.name,
                            arguments=dict(call.arguments),
                        )
                        for call in item.tool_calls
                    ],
                    status=item.status,
                )
                for item in page.items
            ],
            next_cursor=page.next_cursor,
        )

    @asynccontextmanager
    async def open_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AsyncIterator[AsyncIterator[TaskStreamEvent]]:
        """Open one authenticated adapter-neutral fixed-run Task event stream."""

        if (
            not isinstance(run_count, int)
            or isinstance(run_count, bool)
            or run_count < 0
        ):
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'run_count' must be a non-negative integer",
            )
        route = await self._router.get_route(task_id)
        async with self._router.open_task_event_stream(
            route,
            run_count=run_count,
            last_event_id=last_event_id,
        ) as events:
            yield events

    async def send_input(
        self,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
    ) -> TaskResponse:
        outcome = await self.send_input_command(
            task_id=task_id,
            input_content=input_content,
            attachments=attachments,
            idempotency_key=None,
        )
        return outcome.task

    async def send_input_command(
        self,
        *,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput] | None = None,
        idempotency_key: str | None,
        principal_id: str = DEFAULT_GATEWAY_PRINCIPAL,
    ) -> GatewayCommandOutcome:
        """
        向运行中的任务发送新输入

        用于实现任务的交互式对话。运行中的本地 Task 会把输入放入 Task
        Mailbox，在下一次安全模型调用前注入；已 settled 的 Task 会被唤醒。
        """
        _validate_idempotency_key(idempotency_key)
        normalized_attachments = list(attachments or [])
        if idempotency_key is None:
            route = await self._router.get_route(task_id)
            task = await self._send_input_effect(
                route=route,
                input_content=input_content,
                attachments=normalized_attachments,
                batch_id=str(uuid4()),
                downstream_idempotency_key=None,
                mailbox_message_id=None,
            )
            return GatewayCommandOutcome(task=task, replayed=False)

        request_hash = _command_request_hash(
            operation="send_input",
            target=task_id,
            body={
                "input": {
                    "content": input_content,
                    "attachments": [
                        item.model_dump(mode="json") for item in normalized_attachments
                    ],
                }
            },
        )
        claim = await self._claim_command(
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            operation="send_input",
            target=task_id,
            request_hash=request_hash,
            proposed_task_id=task_id,
            proposed_mailbox_message_id=str(uuid4()),
        )
        if claim.status == "replay":
            return GatewayCommandOutcome(
                task=TaskResponse.model_validate_json(claim.response_json),
                replayed=True,
            )
        return await self._execute_claimed_command(
            claim,
            self._send_input_from_request(
                task_id=task_id,
                input_content=input_content,
                attachments=normalized_attachments,
                batch_id=claim.command_id,
                external_idempotency_key=idempotency_key,
                command_id=claim.command_id,
                mailbox_message_id=claim.mailbox_message_id or claim.command_id,
            ),
        )

    async def _send_input_from_request(
        self,
        *,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput],
        batch_id: str,
        external_idempotency_key: str,
        command_id: str,
        mailbox_message_id: str,
    ) -> TaskResponse:
        route = await self._router.get_route(task_id)
        downstream_key = (
            external_idempotency_key
            if route.route_kind == "remote_ref"
            else f"gateway-input:{command_id}"
        )
        return await self._send_input_effect(
            route=route,
            input_content=input_content,
            attachments=attachments,
            batch_id=batch_id,
            downstream_idempotency_key=downstream_key,
            mailbox_message_id=mailbox_message_id,
        )

    async def _send_input_effect(
        self,
        *,
        route: TaskRouteRecord,
        input_content: str,
        attachments: list[AttachmentInput],
        batch_id: str,
        downstream_idempotency_key: str | None,
        mailbox_message_id: str | None,
    ) -> TaskResponse:
        if route.route_kind == "remote_ref":
            record = await self._router.send_input(
                route,
                input_content,
                attachments=[
                    attachment.model_dump(mode="json")
                    for attachment in attachments
                ],
                idempotency_key=downstream_idempotency_key,
            )
            return self._build_task_response(record, route.metadata)

        prepared_input = await self._prepare_input_with_attachments(
            input_content,
            attachments,
            batch_id=batch_id,
        )
        route_metadata = self._metadata_with_attachments(
            route.metadata,
            prepared_input.attachment_metadata,
        )
        record = await self._router.send_input(
            route,
            prepared_input.content,
            idempotency_key=downstream_idempotency_key,
            mailbox_message_id=mailbox_message_id,
        )
        if prepared_input.attachment_metadata:
            route.metadata = route_metadata
            await self._router.save_route(route)
        return self._build_task_response(record, route_metadata)

    async def _claim_command(
        self,
        *,
        principal_id: str,
        idempotency_key: str,
        operation: str,
        target: str,
        request_hash: str,
        proposed_task_id: str,
        proposed_mailbox_message_id: str | None = None,
    ) -> GatewayCommandClaim:
        deadline = asyncio.get_running_loop().time() + COMMAND_WAIT_TIMEOUT_SECONDS
        while True:
            try:
                claim = await self._command_store.aclaim(
                    principal_id=principal_id,
                    idempotency_key=idempotency_key,
                    operation=operation,
                    target=target,
                    request_hash=request_hash,
                    proposed_task_id=proposed_task_id,
                    proposed_mailbox_message_id=proposed_mailbox_message_id,
                )
            except GatewayCommandConflictError as exc:
                raise GatewayTaskError(
                    code="idempotency_key_reused",
                    message=(
                        "Idempotency-Key was already used for a different request"
                    ),
                ) from exc
            if claim.status != "busy":
                return claim
            if asyncio.get_running_loop().time() >= deadline:
                raise GatewayTaskError(
                    code="idempotency_in_progress",
                    message="A request with this Idempotency-Key is still in progress",
                )
            await asyncio.sleep(0.02)

    async def _execute_claimed_command(
        self,
        claim: GatewayCommandClaim,
        effect: Awaitable[TaskResponse],
    ) -> GatewayCommandOutcome:
        claim_token = claim.claim_token
        if claim_token is None:
            raise RuntimeError("Acquired Gateway command has no claim token")
        try:
            task = await effect
            await self._command_store.acomplete(
                command_id=claim.command_id,
                claim_token=claim_token,
                response_json=task.model_dump_json(),
            )
        except BaseException:
            with suppress(BaseException):
                await asyncio.shield(
                    self._command_store.arelease(
                        command_id=claim.command_id,
                        claim_token=claim_token,
                    )
                )
            raise
        return GatewayCommandOutcome(task=task, replayed=False)

    async def cancel_task(self, task_id: str) -> TaskResponse:
        """取消正在运行的任务"""
        route = await self._router.get_route(task_id)
        record = await self._router.cancel(route)
        return self._build_task_response(record, route.metadata)

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskResponse:
        route = await self._router.get_route(task_id)
        record_before = self._router.ensure_record(route)
        if not self._record_has_review(record_before, review_id):
            if route.route_kind == "remote_ref":
                record_before = await self._router.get_record(route)
        if not self._record_has_review(record_before, review_id):
            raise GatewayTaskError(
                code="review_not_found",
                message=f"Review '{review_id}' does not belong to task '{task_id}'",
            )
        mirrored_source_task_id = (record_before.pending_review or {}).get(
            "source_task_id"
        )
        review_record = await self._router.submit_review(
            task_id=task_id,
            review_id=review_id,
            decisions=decisions,
        )
        if review_record.task_id == task_id:
            return self._build_task_response(review_record, route.metadata)
        if mirrored_source_task_id == review_record.task_id:
            refreshed_root = self._router.ensure_record(route)
            return self._build_task_response(refreshed_root, route.metadata)
        if review_record.task_id != task_id:
            raise GatewayTaskError(
                code="review_task_mismatch",
                message=f"Review '{review_id}' does not belong to task '{task_id}'",
            )
        return self._build_task_response(review_record, route.metadata)

    def _record_has_review(self, record: TaskRecord, review_id: str) -> bool:
        pending_review = record.pending_review or {}
        return pending_review.get("review_id") == review_id

    async def download_artifact(self, path: str) -> GatewayArtifact:
        self._ensure_workspace_path(path, kind="Artifact")
        result = await self._run_in_thread(self._control.download_files, [path])
        if not result:
            raise self._artifact_not_found(path)
        item = result[0]
        error = getattr(item, "error", None)
        content = getattr(item, "content", None)
        if error or content is None:
            raise self._artifact_not_found(path)
        if len(content) > self._artifact_max_bytes:
            raise GatewayTaskError(
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

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact:
        route = await self._router.get_route(task_id)
        record = await self._router.get_record(route)
        artifact = _find_artifact(record.artifacts, artifact_id)
        if artifact is None:
            raise self._artifact_not_found(artifact_id)
        downloaded = await self.download_artifact(artifact.path)
        return GatewayArtifact(
            path=artifact.path,
            filename=artifact.name,
            content=downloaded.content,
            content_type=artifact.content_type,
        )

    async def list_tasks(
        self,
        *,
        agent_name: str | None,
        status: str | None,
        metadata_filters: dict[str, str],
        cursor: str | None,
        limit: int,
        root_task_id: str | None = None,
    ) -> TaskListResponse:
        """
        分页查询任务列表，支持多维度过滤

        查询流程：
        1. 从 route_store 获取所有任务路由
        2. 按 Route 上的 agent_name 和 metadata 预过滤
        3. 以有界并发刷新剩余的远程任务
        4. 按最新状态和 root_task_id 过滤
        5. 按更新时间倒序排序并分页返回

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
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )

        offset = self._decode_cursor(cursor)
        routes = [
            route
            for route in await self._router.list_routes()
            if (agent_name is None or route.agent_name == agent_name)
            and self._metadata_matches(route.metadata, metadata_filters)
        ]
        items: list[TaskResponse] = []

        # Agent and metadata are durable Route fields, so avoid a remote refresh
        # when they already exclude the Task. Status and root identity belong to
        # the fresh Task snapshot and remain post-refresh filters.
        for _route, item in await self._collect_tasks_for_listing(routes):
            if item is None:  # 任务已被删除
                continue
            if status is not None and item.status != status:
                continue
            if root_task_id is not None and item.root_task_id != root_task_id:
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
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'limit' must be between 1 and 100",
            )

        offset = self._decode_cursor(cursor)
        items: list[ReviewResponse] = []
        routes = await self._router.list_routes()
        for route, task in await self._collect_tasks_for_listing(routes):
            if task is None or task.pending_review is None:
                continue
            record = self._router.ensure_record(route)
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
        routes = await self._router.list_routes()
        for route, task in await self._collect_tasks_for_listing(routes):
            if task is None or task.pending_review is None:
                continue
            if task.pending_review.get("review_id") != review_id:
                continue
            record = self._router.ensure_record(route)
            review = self._build_review_response(record, route.metadata)
            if review is not None:
                return review
        raise GatewayTaskError(
            code="review_not_found",
            message=f"Review '{review_id}' does not exist",
        )

    async def list_task_reviews(self, task_id: str) -> ReviewListResponse:
        route = await self._router.get_route(task_id)
        task = await self._get_task_for_listing(route)
        if task is None:
            raise GatewayTaskError(
                code="task_not_found",
                message=f"Task '{task_id}' does not exist",
            )
        record = self._router.ensure_record(route)
        review = self._build_review_response(record, route.metadata)
        return ReviewListResponse(
            items=[review] if review is not None else [],
            next_cursor=None,
        )

    def _get_agent_config(self, agent_name: str) -> dict[str, Any]:
        try:
            return self._agent_configs[agent_name]
        except KeyError as exc:
            raise GatewayTaskError(
                code="agent_not_found",
                message=f"Agent '{agent_name}' does not exist",
            ) from exc

    def _ensure_public(self, agent_name: str, agent_config: dict[str, Any]) -> None:
        if agent_config["public"]:
            return
        raise GatewayTaskError(
            code="agent_not_public",
            message=f"Agent '{agent_name}' is not publicly callable",
        )

    def _ensure_available(self, agent_name: str) -> None:
        reason = self._unavailable_agents.get(agent_name)
        if reason is None:
            return
        raise GatewayTaskError(
            code="agent_unavailable",
            message=f"Agent '{agent_name}' is unavailable: {reason}",
        )

    async def _prepare_input_with_attachments(
        self,
        content: str,
        attachments: list[AttachmentInput],
        *,
        batch_id: str,
    ) -> PreparedInput:
        if not attachments:
            return PreparedInput(content=content, attachment_metadata=[])

        workspace_root = self._normalized_workspace_root()
        if workspace_root is None:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message="Runtime workspace root is not configured",
            )
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
                raise GatewayTaskError(
                    code="invalid_attachment",
                    message=f"Attachment '{attachment.name}' is not valid base64",
                ) from exc
            if len(content_bytes) > self._attachment_max_bytes:
                raise GatewayTaskError(
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
                raise GatewayTaskError(
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
            self._control.upload_files,
            upload_items,
        )
        if len(upload_result) != len(upload_items):
            raise GatewayTaskError(
                code="attachment_upload_failed",
                message="Runtime returned an incomplete attachment upload result",
                kind="upstream_failure",
            )
        for metadata, result in zip(attachment_metadata, upload_result, strict=False):
            error = getattr(result, "error", None)
            if error:
                raise GatewayTaskError(
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

    def _artifact_not_found(self, path: str) -> GatewayTaskError:
        return GatewayTaskError(
            code="artifact_not_found",
            message=f"Artifact is not readable: {path}",
        )

    def _normalized_workspace_root(self) -> str | None:
        raw_root = self._control.workspace_root.strip()
        if not raw_root:
            return None
        if raw_root != "/":
            raw_root = raw_root.rstrip("/")
        root = PurePosixPath(raw_root)
        if not root.is_absolute() or ".." in root.parts:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message="Runtime workspace root must be an absolute normalized path",
            )
        return str(root)

    def _ensure_workspace_path(self, path: str, *, kind: str) -> None:
        root_text = self._normalized_workspace_root()
        if root_text is None:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message="Runtime workspace root is not configured",
            )
        candidate = PurePosixPath(path)
        if (
            not candidate.is_absolute()
            or ".." in candidate.parts
            or not candidate.is_relative_to(PurePosixPath(root_text))
        ):
            raise GatewayTaskError(
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
            available=agent_name not in self._unavailable_agents,
            unavailable_reason=self._unavailable_agents.get(agent_name),
        )

    def _build_task_response(
        self,
        record: TaskRecord,
        metadata: dict[str, MetadataScalar],
    ) -> TaskResponse:
        return TaskResponse(
            task_id=record.task_id,
            agent_name=record.agent_name,
            parent_task_id=record.parent_task_id,
            root_task_id=record.root_task_id,
            depth=record.depth,
            status=record.state,
            last_result=record.result,
            error=record.error,
            run_count=record.run_count,
            created_at=record.created_at,
            updated_at=record.updated_at,
            metadata=dict(metadata),
            pending_review=record.pending_review,
            artifacts=[
                _build_artifact_response(artifact)
                for artifact in record.artifacts
            ],
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
        try:
            record = await self._router.get_record(route)
        except GatewayTaskError:
            return None
        return self._build_task_response(record, route.metadata)

    async def _collect_tasks_for_listing(
        self,
        routes: list[TaskRouteRecord],
    ) -> list[tuple[TaskRouteRecord, TaskResponse | None]]:
        """Collect fresh Task snapshots with bounded remote concurrency.

        Local routes do not perform network I/O and are read immediately. A
        failed remote refresh keeps the historical listing behavior: that
        route is omitted by ``_get_task_for_listing`` while healthy Local and
        Remote Tasks remain available.
        """

        results: list[tuple[TaskRouteRecord, TaskResponse | None] | None] = [
            None
        ] * len(routes)
        semaphore = asyncio.Semaphore(self._remote_listing_concurrency)

        async def collect_remote(index: int, route: TaskRouteRecord) -> None:
            async with semaphore:
                results[index] = (route, await self._get_task_for_listing(route))

        remote_calls: list[Awaitable[None]] = []
        for index, route in enumerate(routes):
            if route.route_kind == "remote_ref":
                remote_calls.append(collect_remote(index, route))
            else:
                results[index] = (
                    route,
                    await self._get_task_for_listing(route),
                )
        if remote_calls:
            await asyncio.gather(*remote_calls)
        return [item for item in results if item is not None]

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
        delivered = await self._router.handle_remote_event(
            event.model_dump(mode="json")
        )
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
            raise GatewayTaskError(
                code="invalid_request",
                message="Query parameter 'cursor' is invalid",
            ) from exc
        if offset < 0:
            raise GatewayTaskError(
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


def _validate_idempotency_key(idempotency_key: str | None) -> None:
    if idempotency_key is None:
        return
    if not 1 <= len(idempotency_key) <= 255 or any(
        not 0x21 <= ord(char) <= 0x7E for char in idempotency_key
    ):
        raise GatewayTaskError(
            code="invalid_request",
            message=(
                "Idempotency-Key must contain 1-255 visible ASCII characters "
                "without whitespace"
            ),
        )


def _command_request_hash(
    *,
    operation: str,
    target: str,
    body: dict[str, Any],
) -> str:
    canonical = json.dumps(
        {
            "operation": operation,
            "target": target,
            "body": body,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


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


def _build_artifact_response(artifact: PublishedArtifact) -> PublishedArtifactResponse:
    return PublishedArtifactResponse(
        artifact_id=artifact.artifact_id,
        path=artifact.path,
        name=artifact.name,
        caption=artifact.caption,
        content_type=artifact.content_type,
        size=artifact.size,
        run_count=artifact.run_count,
    )


def _find_artifact(
    artifacts: list[PublishedArtifact],
    artifact_id: str,
) -> PublishedArtifact | None:
    return next(
        (artifact for artifact in artifacts if artifact.artifact_id == artifact_id),
        None,
    )
