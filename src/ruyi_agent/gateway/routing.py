"""Local and remote routing implementation for Gateway Tasks."""

from __future__ import annotations

import base64
import binascii
import json
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.sse import SSEProtocolError, task_stream_event_from_gateway
from ruyi_agent.gateway.models import MetadataScalar, TaskRouteRecord
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import (
    AgentControl,
    DurableTaskMailboxRequiredError,
    MaxDelegationDepthError,
    MaxTasksPerRootError,
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
from ruyi_agent.runtime.message_history import (
    TaskMessageHistoryUnavailableError,
    TaskMessagePage,
    TaskMessageSnapshotNotFoundError,
    project_task_messages,
    task_message_page_from_payload,
)
from ruyi_agent.runtime.task_events import (
    InvalidTaskEventCursorError,
    TaskEventsUnavailableError,
    TaskRunMismatchError,
    TaskStreamEvent,
)
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore

TASK_MESSAGE_CURSOR_VERSION = 1
MAX_TASK_MESSAGE_CURSOR_LENGTH = 4096
_FULL_STATE_TASK_EVENT_TYPES = {
    "task.snapshot",
    "task.created",
    "task.running",
    "task.review_requested",
    "task.completed",
    "task.failed",
    "task.cancelled",
    "task.interrupted",
}


@dataclass(slots=True)
class RoutedTask:
    record: TaskRecord
    route: TaskRouteRecord


class TaskRouter:
    """Hide local/remote routing, recovery, and runtime error translation."""

    def __init__(
        self,
        *,
        control: AgentControl,
        route_store: GatewayRouteStore,
        remote_event_handlers: list[AgentControl] | None = None,
    ) -> None:
        self._control = control
        self._route_store = route_store
        self._remote_event_handlers = remote_event_handlers or []

    def prepare_delegation_metadata(
        self,
        metadata: dict[str, MetadataScalar],
    ) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
        try:
            return self._control.prepare_delegation_metadata(metadata)
        except InvalidDelegationContextError as exc:
            raise GatewayTaskError(
                code="invalid_delegation_context",
                message=str(exc),
            ) from exc
        except DelegationLoopError as exc:
            raise GatewayTaskError(
                code="delegation_loop_detected",
                message=str(exc),
            ) from exc
        except DelegationContextDepthError as exc:
            raise _delegation_depth_error(exc.current_depth, exc.max_depth) from exc

    async def create_task(
        self,
        *,
        agent_name: str,
        route_kind: Literal["local", "remote_ref"],
        input_content: str,
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None,
        delegation_context: DelegationContext | None,
        attachments: list[dict[str, Any]] | None = None,
        task_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> RoutedTask:
        kwargs: dict[str, Any] = {
            "webhook": dict(webhook) if webhook is not None else None,
            "delegation_context": delegation_context,
        }
        if route_kind == "remote_ref":
            kwargs["attachments"] = attachments or []
            kwargs["metadata"] = dict(metadata)
        if task_id is not None:
            kwargs["task_id"] = task_id
        if idempotency_key is not None:
            kwargs["idempotency_key"] = idempotency_key
        try:
            record = await self._control.spawn_task(
                agent_name,
                input_content,
                **kwargs,
            )
        except UnknownAgentTargetError as exc:
            locality = "Local runtime" if route_kind == "local" else "Runtime"
            raise GatewayTaskError(
                code="runtime_unavailable",
                message=f"{locality} is not configured for agent '{agent_name}'",
            ) from exc
        except RemoteExecutorNotImplementedError as exc:
            raise GatewayTaskError(
                code="remote_executor_not_implemented",
                message=str(exc),
            ) from exc
        except MaxDelegationDepthError as exc:
            raise _delegation_depth_error(exc.current_depth, exc.max_depth) from exc
        except MaxTasksPerRootError as exc:
            raise _delegation_budget_error(exc) from exc
        except A2AClientError as exc:
            raise _remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise _upstream_payload_error(str(exc)) from exc

        if route_kind == "remote_ref" and not record.upstream_task_id:
            raise _upstream_payload_error(
                f"Remote ref '{agent_name}' returned no upstream task id"
            )
        route = TaskRouteRecord(
            task_id=record.task_id,
            agent_name=agent_name,
            metadata=dict(metadata),
            route_kind=route_kind,
            upstream_task_id=record.upstream_task_id or record.task_id,
            webhook=dict(webhook) if webhook is not None else None,
        )
        await self._route_store.asave_route(route)
        return RoutedTask(record=record, route=route)

    async def get_route(self, task_id: str) -> TaskRouteRecord:
        route = await self._route_store.aget_route(task_id)
        if route is not None:
            return route
        try:
            record = self._control.get_task_record(task_id)
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(task_id) from exc
        recovered = await self._recover_descendant_route(record, visited=set())
        if recovered is None:
            raise _task_not_found(task_id)
        return recovered

    async def list_routes(self) -> list[TaskRouteRecord]:
        records = sorted(
            self._control.list_persisted_task_records(),
            key=lambda record: (record.depth, record.created_at, record.task_id),
        )
        for record in records:
            if await self._route_store.aget_route(record.task_id) is not None:
                continue
            await self._recover_descendant_route(record, visited=set())
        return await self._route_store.alist_routes()

    async def save_route(self, route: TaskRouteRecord) -> None:
        await self._route_store.asave_route(route)

    def ensure_record(self, route: TaskRouteRecord) -> TaskRecord:
        if route.route_kind != "remote_ref":
            return self._get_local_record(route.task_id)
        try:
            return self._control.ensure_remote_task_record(
                agent_name=route.agent_name,
                task_id=route.task_id,
                upstream_task_id=route.upstream_task_id,
                webhook=dict(route.webhook) if route.webhook is not None else None,
            )
        except UnknownAgentTargetError as exc:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message=f"Runtime is not configured for agent '{route.agent_name}'",
            ) from exc
        except ValueError as exc:
            raise _upstream_payload_error(str(exc)) from exc

    async def get_record(
        self,
        route: TaskRouteRecord,
        *,
        refresh_remote: bool = True,
    ) -> TaskRecord:
        if route.route_kind != "remote_ref" or not refresh_remote:
            return self.ensure_record(route)
        self.ensure_record(route)
        try:
            return await self._control.refresh_task(route.task_id)
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(route.task_id) from exc
        except A2AClientError as exc:
            raise _remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise _upstream_payload_error(str(exc)) from exc

    async def list_task_messages(
        self,
        route: TaskRouteRecord,
        *,
        cursor: str | None,
        limit: int,
    ) -> TaskMessagePage:
        """Return one stable local snapshot page or proxy an opaque remote page."""

        self.ensure_record(route)
        if route.route_kind == "remote_ref":
            try:
                payload = await self._control.list_remote_task_messages(
                    route.task_id,
                    cursor=cursor,
                    limit=limit,
                )
            except A2AClientError as exc:
                raise _remote_message_history_error(exc) from exc
            except ValueError as exc:
                raise _upstream_payload_error(str(exc)) from exc
            try:
                page = task_message_page_from_payload(payload)
            except ValueError as exc:
                raise _upstream_payload_error(str(exc)) from exc
            if page.task_id != route.upstream_task_id:
                raise _upstream_payload_error(
                    "Remote Gateway returned a message page for the wrong task"
                )
            return TaskMessagePage(
                task_id=route.task_id,
                items=page.items,
                next_cursor=page.next_cursor,
            )

        checkpoint_id, offset = _decode_task_message_cursor(
            cursor,
            task_id=route.task_id,
        )
        try:
            snapshot = await self._control.get_local_task_message_snapshot(
                route.task_id,
                checkpoint_id=checkpoint_id,
            )
        except TaskMessageSnapshotNotFoundError as exc:
            raise _invalid_message_cursor() from exc
        except TaskMessageHistoryUnavailableError as exc:
            raise GatewayTaskError(
                code="task_history_unavailable",
                message=f"Message history for task '{route.task_id}' is unavailable",
            ) from exc

        items = project_task_messages(route.task_id, snapshot.messages)
        if cursor is not None and offset >= len(items):
            raise _invalid_message_cursor()
        page_items = items[offset : offset + limit]
        next_cursor = None
        if offset + limit < len(items):
            if snapshot.checkpoint_id is None:
                raise GatewayTaskError(
                    code="task_history_unavailable",
                    message=(
                        f"Message history for task '{route.task_id}' has no snapshot"
                    ),
                )
            next_cursor = _encode_task_message_cursor(
                task_id=route.task_id,
                checkpoint_id=snapshot.checkpoint_id,
                offset=offset + limit,
            )
        return TaskMessagePage(
            task_id=route.task_id,
            items=page_items,
            next_cursor=next_cursor,
        )

    @asynccontextmanager
    async def open_task_event_stream(
        self,
        route: TaskRouteRecord,
        *,
        run_count: int,
        last_event_id: str | None,
    ) -> AsyncIterator[AsyncIterator[TaskStreamEvent]]:
        """Open one local durable stream or a sanitized downstream proxy."""

        record = self.ensure_record(route)
        if route.route_kind == "remote_ref":
            try:
                downstream_context = self._control.open_remote_task_event_stream(
                    route.task_id,
                    run_count=run_count,
                    last_event_id=last_event_id,
                )
                async with downstream_context as downstream:

                    async def project_remote() -> AsyncIterator[TaskStreamEvent]:
                        first_event = True
                        pending_error: TaskStreamEvent | None = None
                        has_full_state = False
                        known_end_reason: str | None = None
                        try:
                            async for event in downstream:
                                projected = task_stream_event_from_gateway(
                                    event,
                                    expected_task_id=route.upstream_task_id,
                                    public_task_id=route.task_id,
                                    run_count=run_count,
                                )
                                if first_event:
                                    first_event = False
                                    if (
                                        last_event_id is None
                                        and projected.event_type != "task.snapshot"
                                    ):
                                        raise SSEProtocolError(
                                            "Fresh remote Task stream did not start "
                                            "with a snapshot"
                                        )
                                    if (
                                        last_event_id is not None
                                        and projected.event_type == "task.snapshot"
                                    ):
                                        raise SSEProtocolError(
                                            "Resumed remote Task stream unexpectedly "
                                            "started with a snapshot"
                                        )
                                elif projected.event_type == "task.snapshot":
                                    raise SSEProtocolError(
                                        "Remote Task stream contains an unexpected "
                                        "additional snapshot"
                                    )
                                if pending_error is not None:
                                    if (
                                        projected.event_type != "stream.end"
                                        or projected.data.get("reason") != "error"
                                    ):
                                        raise SSEProtocolError(
                                            "Remote stream.error was not followed by "
                                            "stream.end(reason=error)"
                                        )
                                    yield pending_error
                                    pending_error = None
                                    yield projected
                                    continue
                                if projected.event_type == "stream.error":
                                    pending_error = projected
                                    continue
                                if projected.event_type in _FULL_STATE_TASK_EVENT_TYPES:
                                    has_full_state = True
                                    known_end_reason = _public_state_end_reason(
                                        projected.data
                                    )
                                if (
                                    projected.event_type == "stream.end"
                                    and projected.data.get("reason") == "error"
                                ):
                                    raise SSEProtocolError(
                                        "Remote stream.end(reason=error) has no "
                                        "preceding stream.error"
                                    )
                                if projected.event_type == "stream.end":
                                    reason = projected.data.get("reason")
                                    if (
                                        has_full_state
                                        and reason != "superseded"
                                        and reason != known_end_reason
                                    ):
                                        raise SSEProtocolError(
                                            "Remote stream.end reason contradicts "
                                            "the latest Task state"
                                        )
                                yield projected
                        except SSEProtocolError as exc:
                            raise GatewayTaskError(
                                kind="upstream_failure",
                                code="upstream_gateway_error",
                                message="Remote Gateway returned an invalid Task event",
                            ) from exc

                    yield project_remote()
                return
            except A2AClientError as exc:
                raise _remote_task_events_error(exc) from exc

        try:
            subscription = self._control.open_local_task_event_stream(
                record.task_id,
                run_count=run_count,
                last_event_id=last_event_id,
            )
        except InvalidTaskEventCursorError as exc:
            raise GatewayTaskError(
                code="invalid_request",
                message="Header 'Last-Event-ID' is invalid",
            ) from exc
        except TaskRunMismatchError as exc:
            raise GatewayTaskError(
                code="task_run_mismatch",
                message=str(exc),
                details={"requested_run_count": exc.requested, "current_run_count": exc.current},
            ) from exc
        except TaskEventsUnavailableError as exc:
            raise GatewayTaskError(
                code="task_events_unavailable",
                message=f"Task events for '{route.task_id}' are unavailable",
            ) from exc
        try:
            yield subscription
        finally:
            await subscription.aclose()

    async def send_input(
        self,
        route: TaskRouteRecord,
        input_content: str,
        *,
        attachments: list[dict[str, Any]] | None = None,
        idempotency_key: str | None = None,
        mailbox_message_id: str | None = None,
    ) -> TaskRecord:
        self.ensure_record(route)
        try:
            return await self._control.send_task_input(
                route.task_id,
                input_content,
                attachments=attachments if route.route_kind == "remote_ref" else None,
                idempotency_key=idempotency_key,
                mailbox_message_id=mailbox_message_id,
            )
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(route.task_id) from exc
        except TaskAlreadyRunningError as exc:
            raise GatewayTaskError(
                code="task_already_running",
                message=(
                    f"Task '{route.task_id}' has an active run, cannot send input"
                ),
            ) from exc
        except DurableTaskMailboxRequiredError as exc:
            raise GatewayTaskError(
                code="idempotency_unavailable",
                message=str(exc),
            ) from exc
        except A2AClientError as exc:
            raise _remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise _upstream_payload_error(str(exc)) from exc

    async def cancel(self, route: TaskRouteRecord) -> TaskRecord:
        self.ensure_record(route)
        try:
            return await self._control.cancel_task(route.task_id)
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(route.task_id) from exc
        except A2AClientError as exc:
            raise _remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise _upstream_payload_error(str(exc)) from exc

    async def submit_review(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskRecord:
        try:
            return await self._control.submit_review_decision(review_id, decisions)
        except UnknownWorkerTaskError as exc:
            raise GatewayTaskError(
                code="review_not_found",
                message=f"Review '{review_id}' does not exist",
            ) from exc
        except TaskAlreadyRunningError as exc:
            raise GatewayTaskError(
                code="task_already_running",
                message=f"Task '{task_id}' has an active run, cannot resume review",
            ) from exc
        except A2AClientError as exc:
            raise _remote_gateway_error(exc) from exc
        except ValueError as exc:
            raise _upstream_payload_error(str(exc)) from exc

    async def handle_remote_event(self, payload: dict[str, Any]) -> int:
        delivered = 0
        if await self._control.handle_remote_task_event(payload):
            delivered += 1
        else:
            route = await self._route_store.aget_route_by_upstream_task_id(
                str(payload.get("task_id", ""))
            )
            if route is not None and route.route_kind == "remote_ref":
                try:
                    self.ensure_record(route)
                except GatewayTaskError:
                    pass
                else:
                    if await self._control.handle_remote_task_event(payload):
                        delivered += 1
        for handler in self._remote_event_handlers:
            if handler is self._control:
                continue
            if await handler.handle_remote_task_event(payload):
                delivered += 1
        return delivered

    def _get_local_record(self, task_id: str) -> TaskRecord:
        try:
            return self._control.get_task_record(task_id)
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(task_id) from exc

    async def _recover_descendant_route(
        self,
        record: TaskRecord,
        *,
        visited: set[str],
    ) -> TaskRouteRecord | None:
        if record.task_id in visited:
            return None
        visited.add(record.task_id)
        existing = await self._route_store.aget_route(record.task_id)
        if existing is not None:
            return existing

        ancestor_ids = [record.parent_task_id, record.root_task_id]
        ancestor_route: TaskRouteRecord | None = None
        for ancestor_id in ancestor_ids:
            if not ancestor_id or ancestor_id == record.task_id:
                continue
            ancestor_route = await self._route_store.aget_route(ancestor_id)
            if ancestor_route is not None:
                break
            try:
                ancestor_record = self._control.get_task_record(ancestor_id)
            except UnknownWorkerTaskError:
                continue
            ancestor_route = await self._recover_descendant_route(
                ancestor_record,
                visited=visited,
            )
            if ancestor_route is not None:
                break
        if ancestor_route is None:
            return None

        route = TaskRouteRecord(
            task_id=record.task_id,
            agent_name=record.agent_name,
            metadata=dict(ancestor_route.metadata),
            route_kind=record.route_kind,
            upstream_task_id=record.upstream_task_id or record.task_id,
            webhook=(
                dict(record.webhook)
                if record.webhook is not None
                else None
            ),
        )
        await self._route_store.asave_route(route)
        return route


def _public_state_end_reason(data: dict[str, Any]) -> str | None:
    if data.get("pending_review") or data.get("status") == "waiting_for_human":
        return "review_required"
    status = data.get("status")
    if status in {"completed", "failed", "cancelled", "interrupted"}:
        return str(status)
    return None


def _task_not_found(task_id: str) -> GatewayTaskError:
    return GatewayTaskError(
        code="task_not_found",
        message=f"Task '{task_id}' does not exist",
    )


def _remote_gateway_error(exc: A2AClientError) -> GatewayTaskError:
    return GatewayTaskError(
        kind="upstream_failure",
        code=exc.code,
        message=exc.message,
        details=exc.details,
    )


def _remote_message_history_error(exc: A2AClientError) -> GatewayTaskError:
    if exc.status_code == 400 and exc.code == "invalid_request":
        return GatewayTaskError(
            code="invalid_request",
            message=exc.message,
            details=exc.details,
        )
    return _remote_gateway_error(exc)


def _remote_task_events_error(exc: A2AClientError) -> GatewayTaskError:
    if (
        exc.status_code == 400 and exc.code == "invalid_request"
    ) or (
        exc.status_code == 409 and exc.code == "task_run_mismatch"
    ):
        return GatewayTaskError(
            code=exc.code,
            message=exc.message,
            details=exc.details,
        )
    return _remote_gateway_error(exc)


def _delegation_depth_error(
    current_depth: int,
    max_depth: int,
) -> GatewayTaskError:
    return GatewayTaskError(
        code="delegation_depth_exceeded",
        message=(
            "Delegation depth limit exceeded: "
            f"current_depth={current_depth} max_depth={max_depth}"
        ),
    )


def _delegation_budget_error(exc: MaxTasksPerRootError) -> GatewayTaskError:
    return GatewayTaskError(
        code="delegation_budget_exhausted",
        message=(
            "Task budget exhausted: "
            f"root_task_id={exc.root_task_id} "
            f"current_count={exc.current_count} "
            f"max_tasks_per_root={exc.max_tasks_per_root}"
        ),
    )


def _upstream_payload_error(message: str) -> GatewayTaskError:
    return GatewayTaskError(
        code="upstream_gateway_error",
        message=message,
    )


def _encode_task_message_cursor(
    *,
    task_id: str,
    checkpoint_id: str,
    offset: int,
) -> str:
    payload = json.dumps(
        {
            "checkpoint_id": checkpoint_id,
            "offset": offset,
            "task_id": task_id,
            "version": TASK_MESSAGE_CURSOR_VERSION,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")


def _decode_task_message_cursor(
    cursor: str | None,
    *,
    task_id: str,
) -> tuple[str | None, int]:
    if cursor is None:
        return None, 0
    if not cursor or len(cursor) > MAX_TASK_MESSAGE_CURSOR_LENGTH:
        raise _invalid_message_cursor()
    try:
        padding = b"=" * (-len(cursor) % 4)
        raw = base64.b64decode(
            cursor.encode("ascii") + padding,
            altchars=b"-_",
            validate=True,
        )
        payload = json.loads(raw.decode("utf-8"))
    except (ValueError, binascii.Error, UnicodeError) as exc:
        raise _invalid_message_cursor() from exc
    if not isinstance(payload, Mapping) or set(payload) != {
        "checkpoint_id",
        "offset",
        "task_id",
        "version",
    }:
        raise _invalid_message_cursor()
    version = payload.get("version")
    checkpoint_id = payload.get("checkpoint_id")
    bound_task_id = payload.get("task_id")
    offset = payload.get("offset")
    if (
        version != TASK_MESSAGE_CURSOR_VERSION
        or isinstance(version, bool)
        or not isinstance(checkpoint_id, str)
        or not checkpoint_id
        or len(checkpoint_id) > 512
        or bound_task_id != task_id
        or not isinstance(offset, int)
        or isinstance(offset, bool)
        or offset < 0
    ):
        raise _invalid_message_cursor()
    return checkpoint_id, offset


def _invalid_message_cursor() -> GatewayTaskError:
    return GatewayTaskError(
        code="invalid_request",
        message="Query parameter 'cursor' is invalid",
    )
