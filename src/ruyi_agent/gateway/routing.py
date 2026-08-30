"""Local and remote routing implementation for Gateway Tasks."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, Literal

from ruyi_agent.gateway.create_errors import (
    delegation_depth_error as _delegation_depth_error,
)
from ruyi_agent.gateway.create_route_workflow import (
    CreateRouteRequest,
    CreateRouteWorkflow,
    RouteRecordPort,
    RoutedTask,
)
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.message_cursors import (
    decode_task_message_cursor as _decode_task_message_cursor,
    encode_task_message_cursor as _encode_task_message_cursor,
    invalid_message_cursor as _invalid_message_cursor,
)
from ruyi_agent.gateway.public_errors import (
    public_remote_record,
    public_upstream_error,
    public_upstream_payload_error,
)
from ruyi_agent.gateway.route_reservations import (
    has_active_route_binding as _has_active_route_binding,
    has_durable_create_effect as _has_durable_create_effect,
)
from ruyi_agent.gateway_protocol.sse import (
    SSEProtocolError,
    task_stream_event_from_gateway,
)
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.delegation.contracts import (
    DurableTaskMailboxRequiredError,
    TaskAlreadyRunningError,
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
from ruyi_agent.task_models import (
    SETTLED_TASK_STATES,
    MetadataScalar,
    PendingReviewRecord,
    TaskRecord,
    TaskRouteRecord,
    parse_task_state,
)

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
        self._record_port = RouteRecordPort(control)
        self._create_route_workflow = CreateRouteWorkflow(control, route_store)

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

    def remote_create_idempotency_guaranteed(self, agent_name: str) -> bool:
        return self._create_route_workflow.remote_create_idempotency_guaranteed(
            agent_name
        )

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
        before_effect: Callable[[], Awaitable[None]] | None = None,
    ) -> RoutedTask:
        request = CreateRouteRequest(
            agent_name=agent_name,
            route_kind=route_kind,
            input_content=input_content,
            metadata=metadata,
            webhook=webhook,
            delegation_context=delegation_context,
            attachments=attachments,
            task_id=task_id,
            idempotency_key=idempotency_key,
            before_effect=before_effect,
        )
        return await self._create_route_workflow.run(request)

    async def get_route(self, task_id: str) -> TaskRouteRecord:
        route = await self._route_store.aget_route(task_id)
        if route is not None:
            return await self._create_route_workflow.reconcile_pending_create(route)
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
        return [
            await self._create_route_workflow.reconcile_pending_create(route)
            for route in await self._route_store.alist_routes()
        ]

    async def save_route(self, route: TaskRouteRecord) -> None:
        await self._route_store.asave_route(route)

    async def create_not_dispatched_is_durable(
        self,
        *,
        task_id: str,
        agent_name: str,
    ) -> bool:
        """Read raw cross-store evidence without reconciling the pending route."""

        return await self._route_store.acreate_not_dispatched_is_durable(
            task_id=task_id,
            agent_name=agent_name,
        )

    async def mark_create_outcome_uncertain(
        self,
        route: TaskRouteRecord,
        error: str,
    ) -> TaskRouteRecord:
        if route.route_state != "pending":
            return route
        return await self._route_store.atransition_route(
            route.task_id,
            route_state="uncertain",
            upstream_task_id=route.upstream_task_id,
            route_error=error,
        )

    def list_pending_reviews(
        self,
        *,
        root_task_id: str | None = None,
        task_id: str | None = None,
    ) -> list[PendingReviewRecord]:
        return self._control.list_pending_reviews(
            root_task_id=root_task_id,
            task_id=task_id,
        )

    def get_pending_review(self, review_id: str) -> PendingReviewRecord | None:
        try:
            return self._control.get_pending_review(review_id)
        except UnknownWorkerTaskError:
            return None

    def ensure_record(self, route: TaskRouteRecord) -> TaskRecord:
        return self._record_port.ensure(route)

    async def get_record(
        self,
        route: TaskRouteRecord,
        *,
        refresh_remote: bool = True,
    ) -> TaskRecord:
        return await self._record_port.read(route, refresh_remote=refresh_remote)

    async def list_task_messages(
        self,
        route: TaskRouteRecord,
        *,
        cursor: str | None,
        limit: int,
    ) -> TaskMessagePage:
        """Return one stable local snapshot page or proxy an opaque remote page."""

        await self.require_active_route(route)
        if route.route_kind == "remote_ref":
            try:
                payload = await self._control.list_remote_task_messages(
                    route.task_id,
                    cursor=cursor,
                    limit=limit,
                )
            except A2AClientError as exc:
                raise public_upstream_error(
                    exc,
                    operation="messages",
                    route=route,
                ) from exc
            except ValueError as exc:
                raise public_upstream_payload_error(
                    operation="messages",
                    route=route,
                ) from exc
            try:
                page = task_message_page_from_payload(payload)
            except ValueError as exc:
                raise public_upstream_payload_error(
                    operation="messages",
                    route=route,
                ) from exc
            if page.task_id != route.upstream_task_id:
                raise public_upstream_payload_error(
                    operation="messages",
                    route=route,
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

        await self.require_active_route(route)
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
                raise public_upstream_error(
                    exc,
                    operation="events",
                    route=route,
                ) from exc

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
                details={
                    "requested_run_count": exc.requested,
                    "current_run_count": exc.current,
                },
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
        await self.require_active_route(route)
        try:
            record = await self._control.send_task_input(
                route.task_id,
                input_content,
                attachments=(attachments if route.route_kind == "remote_ref" else None),
                idempotency_key=idempotency_key,
                mailbox_message_id=mailbox_message_id,
            )
            return (
                public_remote_record(record)
                if route.route_kind == "remote_ref"
                else record
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
            raise public_upstream_error(exc, operation="send", route=route) from exc
        except ValueError as exc:
            raise public_upstream_payload_error(
                operation="send",
                route=route,
            ) from exc

    async def cancel(self, route: TaskRouteRecord) -> TaskRecord:
        await self.require_active_route(route)
        try:
            record = await self._control.cancel_task(route.task_id)
            return (
                public_remote_record(record)
                if route.route_kind == "remote_ref"
                else record
            )
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(route.task_id) from exc
        except A2AClientError as exc:
            raise public_upstream_error(
                exc,
                operation="cancel",
                route=route,
            ) from exc
        except ValueError as exc:
            raise public_upstream_payload_error(
                operation="cancel",
                route=route,
            ) from exc

    async def submit_review(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> TaskRecord:
        try:
            record = await self._control.submit_review_decision(review_id, decisions)
            return (
                public_remote_record(record)
                if record.route_kind == "remote_ref"
                else record
            )
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
            raise public_upstream_error(
                exc,
                operation="review",
                task_id=task_id,
            ) from exc
        except ValueError as exc:
            raise public_upstream_payload_error(
                operation="review",
                task_id=task_id,
            ) from exc

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

    async def require_active_route(self, route: TaskRouteRecord) -> None:
        if _has_active_route_binding(route):
            return
        raise GatewayTaskError(
            code="task_route_unavailable",
            message=(
                f"Task '{route.task_id}' route is {route.route_state}; "
                "the requested operation cannot be routed safely"
            ),
            details={
                "task_id": route.task_id,
                "task_url": f"/tasks/{route.task_id}",
                "route_state": route.route_state,
            },
        )

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

        has_durable_effect = _has_durable_create_effect(
            record,
            route_kind=record.route_kind,
        )
        can_activate = ancestor_route.route_state == "active" and has_durable_effect
        route = TaskRouteRecord(
            task_id=record.task_id,
            agent_name=record.agent_name,
            metadata=dict(ancestor_route.metadata),
            route_kind=record.route_kind,
            upstream_task_id=(
                record.upstream_task_id
                if record.route_kind == "remote_ref"
                else record.task_id
            ),
            webhook=(dict(record.webhook) if record.webhook is not None else None),
            route_state="active" if can_activate else "uncertain",
            route_error=(
                None
                if can_activate
                else "Descendant route lacks an active ancestor or durable effect"
            ),
        )
        await self._route_store.asave_route(route)
        return route


def _public_state_end_reason(data: dict[str, Any]) -> str | None:
    if data.get("pending_review") or data.get("status") == "waiting_for_human":
        return "review_required"
    try:
        status = parse_task_state(data.get("status"))
    except ValueError:
        return None
    if status in SETTLED_TASK_STATES:
        return status
    return None


def _task_not_found(task_id: str) -> GatewayTaskError:
    return GatewayTaskError(
        code="task_not_found", message=f"Task '{task_id}' does not exist"
    )
