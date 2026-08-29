"""Local and remote routing implementation for Gateway Tasks."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Literal
from uuid import uuid4

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.message_cursors import (
    decode_task_message_cursor as _decode_task_message_cursor,
    encode_task_message_cursor as _encode_task_message_cursor,
    invalid_message_cursor as _invalid_message_cursor,
)
from ruyi_agent.gateway.route_reservations import (
    reservation_record,
    route_persistence_error,
    with_route_identity,
)
from ruyi_agent.gateway.sse import SSEProtocolError, task_stream_event_from_gateway
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import (
    AgentControl,
    DurableTaskMailboxRequiredError,
    MaxDelegationDepthError,
    MaxTasksPerRootError,
    RemoteExecutorNotImplementedError,
    TaskAlreadyRunningError,
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
        before_effect: Callable[[], Awaitable[None]] | None = None,
    ) -> RoutedTask:
        gateway_task_id = task_id or str(uuid4())
        reservation = TaskRouteRecord(
            task_id=gateway_task_id,
            agent_name=agent_name,
            metadata=dict(metadata),
            route_kind=route_kind,
            upstream_task_id=(gateway_task_id if route_kind == "local" else None),
            webhook=dict(webhook) if webhook is not None else None,
            route_state="pending",
        )
        try:
            reservation = await self._route_store.areserve_route(reservation)
        except Exception as exc:
            raise route_persistence_error(
                gateway_task_id,
                route_state="unpersisted",
                queryable=False,
                retryable=True,
                effect_outcome="not_started",
            ) from exc
        if reservation.route_state == "active":
            return RoutedTask(
                record=await self.get_record(reservation),
                route=reservation,
            )
        if reservation.route_state in {"failed", "uncertain"}:
            error = GatewayTaskError(
                code="task_creation_not_retryable",
                message=(
                    f"Gateway Task creation previously ended with route state "
                    f"'{reservation.route_state}'"
                ),
            )
            raise with_route_identity(
                error,
                reservation,
                task_id=gateway_task_id,
                retryable=False,
                effect_outcome=(
                    "uncertain"
                    if reservation.route_state == "uncertain"
                    else "not_started"
                ),
                downstream_idempotency_guaranteed=(
                    False if route_kind == "remote_ref" else None
                ),
            )

        kwargs: dict[str, Any] = {
            "webhook": dict(webhook) if webhook is not None else None,
            "delegation_context": delegation_context,
            "task_id": gateway_task_id,
        }
        if route_kind == "remote_ref":
            kwargs["attachments"] = attachments or []
            kwargs["metadata"] = dict(metadata)
        if idempotency_key is not None:
            kwargs["idempotency_key"] = idempotency_key
        if before_effect is not None:
            try:
                await before_effect()
            except Exception as exc:
                durable_route = await self._fail_reservation(
                    reservation,
                    "Gateway command effect boundary could not be persisted",
                    uncertain=False,
                )
                raise route_persistence_error(
                    gateway_task_id,
                    route_state=(
                        durable_route.route_state
                        if durable_route is not None
                        else "unknown"
                    ),
                    queryable=durable_route is not None,
                    retryable=False,
                    effect_outcome="not_started",
                ) from exc
        try:
            record = await self._control.spawn_task(
                agent_name,
                input_content,
                **kwargs,
            )
        except Exception as exc:
            error, uncertain, effect_outcome = _create_effect_error(
                exc,
                agent_name=agent_name,
                route_kind=route_kind,
            )
            durable_route = await self._fail_reservation(
                reservation,
                error,
                uncertain=uncertain,
            )
            raise with_route_identity(
                error,
                durable_route,
                task_id=gateway_task_id,
                retryable=False,
                route_state=reservation.route_state,
                effect_outcome=effect_outcome,
                downstream_idempotency_guaranteed=(
                    False if route_kind == "remote_ref" else None
                ),
            ) from exc

        if route_kind == "remote_ref" and not record.upstream_task_id:
            error = _upstream_payload_error(
                f"Remote ref '{agent_name}' returned no upstream task id"
            )
            durable_route = await self._fail_reservation(
                reservation,
                error,
                uncertain=True,
            )
            raise with_route_identity(
                error,
                durable_route,
                task_id=gateway_task_id,
                retryable=False,
                route_state=reservation.route_state,
                effect_outcome="uncertain",
                downstream_idempotency_guaranteed=False,
            )
        if not _has_durable_create_effect(record, route_kind=route_kind):
            error = GatewayTaskError(
                code="task_effect_not_durable",
                message="Gateway Task effect has no durable run or remote binding",
            )
            durable_route = await self._fail_reservation(
                reservation,
                error,
                uncertain=True,
            )
            raise with_route_identity(
                error,
                durable_route,
                task_id=gateway_task_id,
                retryable=False,
                route_state=reservation.route_state,
                effect_outcome="uncertain",
                downstream_idempotency_guaranteed=(
                    False if route_kind == "remote_ref" else None
                ),
            )
        try:
            route = await self._route_store.atransition_route(
                gateway_task_id,
                route_state="active",
                upstream_task_id=record.upstream_task_id or record.task_id,
                route_error=None,
            )
        except Exception as exc:
            with_route = TaskRouteRecord(
                task_id=reservation.task_id,
                agent_name=reservation.agent_name,
                metadata=dict(reservation.metadata),
                route_kind=reservation.route_kind,
                upstream_task_id=record.upstream_task_id or record.task_id,
                webhook=reservation.webhook,
                route_state="uncertain",
                route_error="Task effect completed but route activation failed",
            )
            durable_route = await self._fail_reservation(
                with_route,
                with_route.route_error,
                uncertain=True,
            )
            raise route_persistence_error(
                gateway_task_id,
                route_state=(
                    durable_route.route_state
                    if durable_route is not None
                    else "unknown"
                ),
                queryable=durable_route is not None,
                retryable=False,
                effect_outcome="completed",
            ) from exc
        return RoutedTask(record=record, route=route)

    async def _fail_reservation(
        self,
        route: TaskRouteRecord,
        error: BaseException | str,
        *,
        uncertain: bool,
    ) -> TaskRouteRecord | None:
        try:
            updated = await self._route_store.atransition_route(
                route.task_id,
                route_state="uncertain" if uncertain else "failed",
                upstream_task_id=route.upstream_task_id,
                route_error=str(error),
            )
        except Exception:
            try:
                return await self._route_store.aget_route(route.task_id)
            except Exception:
                return None
        route.upstream_task_id = updated.upstream_task_id
        route.route_state = updated.route_state
        route.route_error = updated.route_error
        return updated

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
        if route.route_state != "active":
            return reservation_record(route)
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
        if route.route_state != "active":
            return reservation_record(route)
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

        await self.require_active_route(route)
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
        await self.require_active_route(route)
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
        await self.require_active_route(route)
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

    async def require_active_route(self, route: TaskRouteRecord) -> None:
        if route.route_state == "active":
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
    try:
        status = parse_task_state(data.get("status"))
    except ValueError:
        return None
    if status in SETTLED_TASK_STATES:
        return status
    return None


def _task_not_found(task_id: str) -> GatewayTaskError:
    return GatewayTaskError(
        code="task_not_found",
        message=f"Task '{task_id}' does not exist",
    )


def _create_effect_error(
    exc: Exception,
    *,
    agent_name: str,
    route_kind: Literal["local", "remote_ref"],
) -> tuple[GatewayTaskError, bool, str]:
    """Translate create failures and identify result-uncertain effects."""

    if isinstance(exc, UnknownAgentTargetError):
        locality = "Local runtime" if route_kind == "local" else "Runtime"
        return (
            GatewayTaskError(
                code="runtime_unavailable",
                message=f"{locality} is not configured for agent '{agent_name}'",
            ),
            False,
            "not_started",
        )
    if isinstance(exc, RemoteExecutorNotImplementedError):
        return (
            GatewayTaskError(
                code="remote_executor_not_implemented",
                message=str(exc),
            ),
            False,
            "not_started",
        )
    if isinstance(exc, MaxDelegationDepthError):
        return (
            _delegation_depth_error(exc.current_depth, exc.max_depth),
            False,
            "not_started",
        )
    if isinstance(exc, MaxTasksPerRootError):
        return _delegation_budget_error(exc), False, "not_started"
    if isinstance(exc, A2AClientError):
        uncertain = exc.status_code >= 500
        return (
            _remote_gateway_error(exc),
            uncertain,
            "uncertain" if uncertain else "not_started",
        )
    if isinstance(exc, ValueError):
        uncertain = route_kind == "remote_ref"
        return (
            _upstream_payload_error(str(exc)),
            uncertain,
            "uncertain" if uncertain else "not_started",
        )
    return (
        GatewayTaskError(
            code="task_creation_failed",
            message="Gateway Task creation failed",
        ),
        True,
        "uncertain",
    )


def _has_durable_create_effect(
    record: TaskRecord,
    *,
    route_kind: Literal["local", "remote_ref"],
) -> bool:
    if route_kind == "remote_ref":
        return bool(record.upstream_task_id)
    return record.run_count > 0 and record.state != "pending"


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
