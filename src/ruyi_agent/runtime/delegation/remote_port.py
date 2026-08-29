"""Remote Gateway transport port for delegated tasks.

RemoteTaskPort owns A2A lookup, refresh, message/event access, webhook relay,
and recovery of local proxy records. It does not decide delegation budgets or
local run lifecycle.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import Any, Literal, Protocol

from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.context import (
    DelegationContext,
    build_root_context,
    inject_context_metadata,
)
from ruyi_agent.runtime.delegation.contracts import (
    RemoteTaskIdentityMismatchError,
    UnknownWorkerTaskError,
    _validate_remote_task_identity,
    _validate_remote_task_state,
)
from ruyi_agent.runtime.delegation.registry import RemoteRefEntry
from ruyi_agent.task_models import TaskRecord

logger = logging.getLogger(__name__)

_AUTHORITATIVE_REJECTION_CODES = frozenset(
    {
        "agent_not_found",
        "agent_not_public",
        "idempotency_key_reused",
        "invalid_request",
        "review_not_found",
        "review_task_mismatch",
        "task_already_running",
        "task_not_found",
        "unauthorized",
    }
)


def _is_authoritative_rejection(exc: Exception) -> bool:
    return (
        isinstance(exc, A2AClientError)
        and 400 <= exc.status_code < 500
        and exc.code in _AUTHORITATIVE_REJECTION_CODES
    )


RemoteFailureDisposition = Literal[
    "not_dispatched",
    "remote_rejected",
    "outcome_unknown",
]


def _remote_failure_disposition(exc: Exception) -> RemoteFailureDisposition:
    if (
        isinstance(exc, A2AClientError)
        and exc.effect_boundary == "not_dispatched"
    ):
        return "not_dispatched"
    if _is_authoritative_rejection(exc):
        return "remote_rejected"
    return "outcome_unknown"


class RemoteTaskHost(Protocol):
    _a2a_client: Any
    _httpx: Any
    _max_delegation_depth: int
    _max_tasks_per_root: int
    _node_id: str
    _permission_default_profile: str
    _registry: Any
    _run_supervisor: Any
    _remote_poll_interval: float
    _remote_status_retry_attempts: int
    _task_manager: Any
    _webhook_token: str | None
    _webhook_url: str | None

    def _format_task_record(self, record: TaskRecord) -> str: ...
    def _is_settled_record(self, record: TaskRecord) -> bool: ...
    def _maybe_publish_settled_message(self, task_id: str) -> None: ...
    async def _send_settled_webhook(self, task_id: str) -> None: ...
    def _get_remote_entry_for_task(self, task_id: str) -> RemoteRefEntry: ...
    async def _refresh_remote_task(self, task_id: str) -> TaskRecord: ...


class RemoteTaskPort:
    """Isolate all remote-ref network effects behind one runtime port."""

    def __init__(self, control: RemoteTaskHost) -> None:
        self._control = control

    async def allocate_task(
        self,
        *,
        record: TaskRecord,
        entry: RemoteRefEntry,
        input_content: str,
        delegation_context: DelegationContext,
        metadata: dict[str, Any] | None,
        attachments: list[dict[str, Any]] | None,
        idempotency_key: str,
    ) -> TaskRecord:
        """Allocate one remote task and bind its upstream identity locally.

        An explicit pre-effect rejection settles the local proxy as failed.
        Transport loss, server failures, cancellation, and malformed responses
        retain a durable uncertain create intent for later reconciliation.
        """
        create_kwargs: dict[str, Any] = {
            "input_content": input_content,
            "metadata": inject_context_metadata(
                dict(metadata or {}), delegation_context
            ),
            "idempotency_key": idempotency_key,
        }
        if attachments:
            create_kwargs["attachments"] = attachments
        webhook = self._build_webhook_config()
        if webhook is not None:
            create_kwargs["webhook"] = webhook
        self._control._task_manager.begin_external_operation(
            record.task_id,
            operation="create",
            identity=idempotency_key,
            allow_replay=entry.ref.create_idempotency == "ruyi_gateway_v1",
        )
        try:
            payload = await self._control._a2a_client.create_task(
                entry.ref, **create_kwargs
            )
            upstream_task_id = payload.get("task_id")
            if not isinstance(upstream_task_id, str) or not upstream_task_id:
                raise ValueError(
                    f"Remote ref '{entry.name}' returned no task_id from remote gateway."
                )
            _validate_remote_task_state(record.task_id, payload)
        except asyncio.CancelledError:
            self._mark_external_outcome_uncertain(
                record.task_id,
                operation="create",
                identity=idempotency_key,
            )
            raise
        except Exception as exc:
            disposition = self._handle_remote_operation_failure(
                record.task_id,
                operation="create",
                identity=idempotency_key,
                exc=exc,
            )
            if disposition == "remote_rejected":
                self._control._task_manager.mark_failed(
                    record.task_id,
                    "Remote Gateway Task creation failed",
                )
                self._control._maybe_publish_settled_message(record.task_id)
            raise
        synced = self._control._task_manager.bind_and_sync_remote_task(
            record.task_id,
            upstream_task_id,
            payload,
        )
        if self._control._is_settled_record(synced):
            self._control._maybe_publish_settled_message(synced.task_id)
        return synced

    async def submit_review_decision(
        self,
        record: TaskRecord,
        *,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskRecord:
        """Forward a review decision and synchronize the local proxy record."""
        entry = self._get_remote_entry_for_task(record.task_id)
        upstream_task_id = record.upstream_task_id or record.task_id
        self._control._task_manager.begin_external_operation(
            record.task_id,
            operation="review",
            identity=review_id,
        )
        try:
            payload = await self._control._a2a_client.submit_review_decision(
                entry.ref,
                task_id=upstream_task_id,
                review_id=review_id,
                decisions=decisions,
            )
            _validate_remote_task_identity(
                record.task_id,
                upstream_task_id,
                payload,
            )
            _validate_remote_task_state(record.task_id, payload)
        except RemoteTaskIdentityMismatchError:
            # The durable request remains unresolved, but no field from a
            # different upstream Task may enter the local proxy record.
            raise
        except asyncio.CancelledError:
            self._mark_external_outcome_uncertain(
                record.task_id,
                operation="review",
                identity=review_id,
            )
            raise
        except Exception as exc:
            self._handle_remote_operation_failure(
                record.task_id,
                operation="review",
                identity=review_id,
                exc=exc,
            )
            raise
        return self._control._task_manager.sync_remote_task(record.task_id, payload)

    async def send_input(
        self,
        record: TaskRecord,
        *,
        message: str,
        attachments: list[dict[str, Any]] | None,
        idempotency_key: str | None,
    ) -> TaskRecord:
        """Forward input to one remote task and synchronize its proxy."""
        entry = self._get_remote_entry_for_task(record.task_id)
        upstream_task_id = record.upstream_task_id or record.task_id
        operation_identity = idempotency_key or f"ruyi-send:{uuid.uuid4().hex}"
        send_kwargs: dict[str, Any] = {
            "task_id": upstream_task_id,
            "input_content": message,
            "attachments": attachments,
            "idempotency_key": operation_identity,
        }
        self._control._task_manager.begin_external_operation(
            record.task_id,
            operation="send",
            identity=operation_identity,
            # Gateway input commands are idempotent under the forwarded key;
            # retrying that exact durable identity reconciles a lost response.
            allow_replay=True,
        )
        try:
            payload = await self._control._a2a_client.send_input(
                entry.ref, **send_kwargs
            )
            _validate_remote_task_identity(
                record.task_id,
                upstream_task_id,
                payload,
            )
            _validate_remote_task_state(record.task_id, payload)
        except RemoteTaskIdentityMismatchError:
            raise
        except asyncio.CancelledError:
            self._mark_external_outcome_uncertain(
                record.task_id,
                operation="send",
                identity=operation_identity,
            )
            raise
        except Exception as exc:
            self._handle_remote_operation_failure(
                record.task_id,
                operation="send",
                identity=operation_identity,
                exc=exc,
            )
            raise
        return self._control._task_manager.sync_remote_task(record.task_id, payload)

    async def cancel(self, record: TaskRecord) -> TaskRecord:
        """Cancel the active run represented by one remote proxy record."""
        entry = self._get_remote_entry_for_task(record.task_id)
        upstream_task_id = record.upstream_task_id or record.task_id
        operation_identity = upstream_task_id
        self._control._task_manager.begin_external_operation(
            record.task_id,
            operation="cancel",
            identity=operation_identity,
        )
        try:
            payload = await self._control._a2a_client.cancel_task(
                entry.ref,
                task_id=upstream_task_id,
            )
            _validate_remote_task_identity(
                record.task_id,
                upstream_task_id,
                payload,
            )
            _validate_remote_task_state(record.task_id, payload)
        except RemoteTaskIdentityMismatchError:
            raise
        except asyncio.CancelledError:
            self._mark_external_outcome_uncertain(
                record.task_id,
                operation="cancel",
                identity=operation_identity,
            )
            raise
        except Exception as exc:
            self._handle_remote_operation_failure(
                record.task_id,
                operation="cancel",
                identity=operation_identity,
                exc=exc,
            )
            raise
        return self._control._task_manager.sync_remote_task(record.task_id, payload)

    def _mark_external_outcome_uncertain(
        self,
        task_id: str,
        *,
        operation: str,
        identity: str,
    ) -> None:
        try:
            self._control._task_manager.mark_external_outcome_uncertain(
                task_id,
                operation=operation,
                identity=identity,
            )
        except Exception:
            logger.exception(
                "Failed to persist uncertain remote %s outcome for task %s",
                operation,
                task_id,
            )

    def _handle_remote_operation_failure(
        self,
        task_id: str,
        *,
        operation: str,
        identity: str,
        exc: Exception,
    ) -> RemoteFailureDisposition:
        """Persist the transport's explicit effect-boundary disposition."""

        disposition = _remote_failure_disposition(exc)
        if disposition != "outcome_unknown":
            self._control._task_manager.reject_external_operation(
                task_id,
                operation=operation,
                identity=identity,
            )
            return disposition
        self._mark_external_outcome_uncertain(
            task_id,
            operation=operation,
            identity=identity,
        )
        return disposition

    def _get_remote_entry_for_task(self, task_id: str) -> RemoteRefEntry:
        """
        获取远端任务对应的 remote_ref 注册项

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            远端引用注册项

        Raises:
            ValueError: task_id 对应的任务不是 remote_ref
        """
        record = self._control._task_manager.get_task(task_id)
        entry = self._control._registry.get_entry(record.agent_name)
        if not isinstance(entry, RemoteRefEntry):
            raise ValueError(f"Task '{task_id}' is not a remote_ref task")
        return entry

    async def _refresh_remote_task(self, task_id: str) -> TaskRecord:
        """
        查询一次远端任务状态并同步本地记录

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            同步后的任务记录

        Raises:
            A2AClientError: 远端请求失败
            ValueError: 远端 payload 不合法
        """
        entry = self._control._get_remote_entry_for_task(task_id)
        record = self._control._task_manager.get_task(task_id)
        upstream_task_id = record.upstream_task_id or task_id
        payload = await self._control._a2a_client.get_task(entry.ref, task_id=upstream_task_id)
        _validate_remote_task_identity(task_id, upstream_task_id, payload)
        return self._control._task_manager.sync_remote_task(task_id, payload)

    async def _refresh_remote_task_with_retries(self, task_id: str) -> TaskRecord:
        """
        带重试地刷新远端任务状态

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            同步后的任务记录

        Raises:
            A2AClientError: 多次重试后仍无法获取远端状态
        """
        permit = await self._control._run_supervisor.acquire_mutation()
        operation = None
        try:
            operation = await self._control._run_supervisor.promote_to_operation(
                permit
            )
            last_exc: A2AClientError | None = None
            for attempt in range(self._control._remote_status_retry_attempts):
                try:
                    return await self._control._refresh_remote_task(task_id)
                except A2AClientError as exc:
                    last_exc = exc
                    if attempt + 1 >= self._control._remote_status_retry_attempts:
                        raise
                    await asyncio.sleep(self._control._remote_poll_interval)
            if last_exc is not None:
                raise last_exc
            raise AssertionError("unreachable")
        finally:
            if operation is not None:
                await self._control._run_supervisor.cleanup_operation(operation)
            await self._control._run_supervisor.cleanup_mutation(permit)

    def _format_remote_status_unavailable(
        self,
        record: TaskRecord,
        exc: BaseException,
    ) -> str:
        """
        格式化远端状态暂不可用的工具返回文本

        Args:
            record: 本地最后已知任务记录
            exc: 最后一次远端查询异常

        Returns:
            包含最后已知状态和告警的文本
        """
        # 为什么保留最后已知状态：远端瞬时抖动不应让主 agent 误判为任务已经失败。
        return (
            f"{self._control._format_task_record(record)} | warning=remote_status_temporarily_unavailable "
            f"after {self._control._remote_status_retry_attempts} attempts: {exc}"
        )


    def _build_webhook_config(self) -> dict[str, Any] | None:
        """
        构造传给远端网关的 webhook 配置

        Returns:
            webhook 配置；未配置 webhook_url 时返回 None
        """
        if not self._control._webhook_url:
            return None
        webhook: dict[str, Any] = {"url": self._control._webhook_url}
        if self._control._webhook_token:
            webhook["token"] = self._control._webhook_token
        return webhook


    async def _send_settled_webhook(self, task_id: str) -> None:
        """
        发送 Task 当前 run 的 settled webhook

        Args:
            task_id: 当前 runtime 内部任务 ID
        """
        record = self._control._task_manager.get_task(task_id)
        if not record.webhook or not self._control._is_settled_record(record):
            return
        url = record.webhook.get("url")
        if not isinstance(url, str) or not url:
            return
        headers = {"Content-Type": "application/json"}
        token = record.webhook.get("token")
        if isinstance(token, str) and token:
            headers["Authorization"] = f"Bearer {token}"
        payload = {
            "event_id": str(uuid.uuid4()),
            "event_type": f"task.{record.state}",
            "task_id": record.task_id,
            "agent_name": record.agent_name,
            "status": record.state,
            "last_result": record.result,
            "error": record.error,
            "run_count": record.run_count,
            "created_at": record.created_at.isoformat(),
            "updated_at": record.updated_at.isoformat(),
        }
        try:
            async with self._control._httpx.AsyncClient(timeout=5.0) as client:
                await client.post(url, headers=headers, json=payload)
        except self._control._httpx.HTTPError:
            return

    async def handle_remote_task_event(self, payload: dict[str, Any]) -> bool:
        """
        处理远端任务 webhook 事件

        Args:
            payload: 远端网关推送的任务状态 payload

        Returns:
            找到并同步到本地任务返回 True；无法识别事件返回 False
        """
        permit = await self._control._run_supervisor.acquire_mutation()
        operation = None
        try:
            upstream_task_id = payload.get("task_id")
            if not isinstance(upstream_task_id, str) or not upstream_task_id:
                return False
            record = self._control._task_manager.find_by_upstream_task_id(
                upstream_task_id
            )
            if record is None:
                return False
            synced = self._control._task_manager.sync_remote_task(
                record.task_id, payload
            )
            if self._control._is_settled_record(synced):
                self._control._maybe_publish_settled_message(synced.task_id)
                operation = await self._control._run_supervisor.promote_to_operation(
                    permit
                )
                await self._control._send_settled_webhook(synced.task_id)
            return True
        finally:
            if operation is not None:
                await self._control._run_supervisor.cleanup_operation(operation)
            await self._control._run_supervisor.cleanup_mutation(permit)


    async def list_remote_task_messages(
        self,
        task_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Request one opaque message-history page for a remote-ref Task."""

        permit = await self._control._run_supervisor.acquire_mutation()
        operation = None
        try:
            record = self._control._task_manager.get_task(task_id)
            if record.route_kind != "remote_ref":
                raise ValueError(f"Task '{task_id}' is not a remote_ref task")
            entry = self._control._get_remote_entry_for_task(task_id)
            operation = await self._control._run_supervisor.promote_to_operation(
                permit
            )
            return await self._control._a2a_client.list_task_messages(
                entry.ref,
                task_id=record.upstream_task_id or task_id,
                cursor=cursor,
                limit=limit,
            )
        finally:
            if operation is not None:
                await self._control._run_supervisor.cleanup_operation(operation)
            await self._control._run_supervisor.cleanup_mutation(permit)

    def open_remote_task_event_stream(
        self,
        task_id: str,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AbstractAsyncContextManager[Any]:
        """Open the downstream SSE client boundary for a remote-ref Task."""

        @asynccontextmanager
        async def admitted_stream() -> Any:
            permit = await self._control._run_supervisor.acquire_mutation()
            operation = None
            try:
                record = self._control._task_manager.get_task(task_id)
                if record.route_kind != "remote_ref":
                    raise ValueError(f"Task '{task_id}' is not a remote_ref task")
                entry = self._control._get_remote_entry_for_task(task_id)
                operation = (
                    await self._control._run_supervisor.promote_to_operation(permit)
                )
                async with self._control._a2a_client.open_task_event_stream(
                    entry.ref,
                    task_id=record.upstream_task_id or task_id,
                    run_count=run_count,
                    last_event_id=last_event_id,
                ) as stream:
                    yield stream
            finally:
                if operation is not None:
                    await self._control._run_supervisor.cleanup_operation(operation)
                await self._control._run_supervisor.cleanup_mutation(permit)

        return admitted_stream()


    def ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None = None,
    ) -> TaskRecord:
        """
        确保远端代理任务在本地已有记录

        用于 webhook 或远端同步路径：如果本地还没有对应 task，则创建一个
        remote_ref TaskRecord；如果已有记录，则校验 agent 和 upstream 绑定。

        Args:
            agent_name: 远端引用名称
            task_id: 当前 runtime 内部任务 ID
            upstream_task_id: 远端网关上的任务 ID
            webhook: 当前 run settled 后的 webhook 配置

        Returns:
            已存在或新创建的任务记录

        Raises:
            ValueError: agent 类型、任务路由或 upstream_task_id 不匹配
        """
        return self._control._run_supervisor.mutate_now(
            lambda: self._ensure_remote_task_record(
                agent_name=agent_name,
                task_id=task_id,
                upstream_task_id=upstream_task_id,
                webhook=webhook,
            )
        )

    def _ensure_remote_task_record(
        self,
        *,
        agent_name: str,
        task_id: str,
        upstream_task_id: str,
        webhook: dict[str, Any] | None,
    ) -> TaskRecord:
        try:
            record = self._control._task_manager.get_task(task_id)
        except UnknownWorkerTaskError:
            entry = self._control._registry.get_entry(agent_name)
            if not isinstance(entry, RemoteRefEntry):
                raise ValueError(f"Agent target '{agent_name}' is not a remote_ref")
            self._control._registry.register_task(task_id, agent_name=agent_name)
            return self._control._task_manager.create_task_record(
                task_id,
                agent_name,
                parent_task_id=None,
                root_task_id=task_id,
                depth=1,
                route_kind="remote_ref",
                upstream_task_id=upstream_task_id,
                webhook=webhook,
                delegation_context=build_root_context(
                    node_id=self._control._node_id,
                    task_id=task_id,
                    max_depth=self._control._max_delegation_depth,
                    max_tasks_per_root=self._control._max_tasks_per_root,
                ),
                permission_profile=self._control._permission_default_profile,
            )
        if record.agent_name != agent_name:
            raise ValueError(
                f"Task '{task_id}' is registered for '{record.agent_name}', "
                f"not '{agent_name}'"
            )
        if record.route_kind != "remote_ref":
            raise ValueError(f"Task '{task_id}' is not a remote_ref task")
        if (
            record.upstream_task_id is None
            and record.external_outcome_uncertain
            and record.external_operation == "create"
        ):
            return self._control._task_manager.bind_uncertain_remote_task(
                task_id,
                upstream_task_id,
            )
        if record.upstream_task_id != upstream_task_id:
            raise ValueError(
                f"Task '{task_id}' is linked to upstream task "
                f"'{record.upstream_task_id}', not '{upstream_task_id}'"
            )
        if record.webhook is None and webhook is not None:
            return self._control._task_manager.set_remote_webhook_if_missing(
                task_id,
                webhook,
            )
        return record

    async def refresh_task(self, task_id: str) -> TaskRecord:
        """
        刷新任务状态

        本地任务直接返回当前记录；远端任务会调用远端网关同步最新状态。

        Args:
            task_id: 当前 runtime 内部任务 ID

        Returns:
            最新任务记录
        """
        record = self._control._task_manager.get_task(task_id)
        if record.route_kind == "remote_ref":
            return await self._control._refresh_remote_task_with_retries(task_id)
        return record
