"""Durable Gateway Task route-creation workflow."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from uuid import uuid4

from ruyi_agent.gateway.create_errors import recover_create_effect_error
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.public_errors import (
    public_create_route_error,
    public_remote_record,
    public_upstream_error,
    public_upstream_payload_error,
)
from ruyi_agent.gateway.route_reservations import (
    create_evidence_policy,
    has_active_route_binding,
    has_durable_create_effect,
    reconcile_pending_create as _reconcile_pending_create,
    reservation_record,
    route_persistence_error,
    shield_durable_cleanup,
    with_route_identity,
)
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.delegation.contracts import (
    UnknownAgentTargetError,
    UnknownWorkerTaskError,
)
from ruyi_agent.runtime.delegation.context import DelegationContext
from ruyi_agent.storage.gateway_route_store import (
    GatewayCreateEvidence,
    GatewayRouteStore,
)
from ruyi_agent.task_models import (
    MetadataScalar,
    TaskRecord,
    TaskRouteKind,
    TaskRouteRecord,
)


Route = TaskRouteRecord


@dataclass(slots=True)
class RouteRecordPort:
    control: AgentControl

    def ensure(self, route: Route) -> TaskRecord:
        if not has_active_route_binding(route):
            return reservation_record(route)
        if route.route_kind != "remote_ref":
            try:
                return self.control.get_task_record(route.task_id)
            except UnknownWorkerTaskError as exc:
                raise _task_not_found(route.task_id) from exc
        try:
            return public_remote_record(
                self.control.ensure_remote_task_record(
                    agent_name=route.agent_name,
                    task_id=route.task_id,
                    upstream_task_id=route.upstream_task_id,
                    webhook=(
                        dict(route.webhook) if route.webhook is not None else None
                    ),
                )
            )
        except UnknownAgentTargetError as exc:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message=f"Runtime is not configured for agent '{route.agent_name}'",
            ) from exc
        except ValueError as exc:
            raise public_upstream_payload_error(operation="get", route=route) from exc

    async def read(self, route: Route, *, refresh_remote: bool = True) -> TaskRecord:
        if not has_active_route_binding(route):
            return reservation_record(route)
        if route.route_kind != "remote_ref" or not refresh_remote:
            return self.ensure(route)
        self.ensure(route)
        try:
            return public_remote_record(await self.control.refresh_task(route.task_id))
        except UnknownWorkerTaskError as exc:
            raise _task_not_found(route.task_id) from exc
        except A2AClientError as exc:
            raise public_upstream_error(exc, operation="get", route=route) from exc
        except ValueError as exc:
            raise public_upstream_payload_error(operation="get", route=route) from exc


@dataclass(slots=True)
class RoutedTask:
    record: TaskRecord
    route: Route


@dataclass(frozen=True, slots=True)
class CreateRouteRequest:
    agent_name: str
    route_kind: TaskRouteKind
    input_content: str
    metadata: dict[str, MetadataScalar]
    webhook: dict[str, MetadataScalar] | None
    delegation_context: DelegationContext | None
    attachments: list[dict[str, object]] | None = None
    task_id: str | None = None
    idempotency_key: str | None = None
    before_effect: Callable[[], Awaitable[None]] | None = None


class CreateRouteWorkflow:
    def __init__(self, control: AgentControl, route_store: GatewayRouteStore) -> None:
        self._control = control
        self._route_store = route_store
        self._record_port = RouteRecordPort(control)

    def remote_create_idempotency_guaranteed(self, agent_name: str) -> bool:
        try:
            return bool(self._control.remote_create_idempotency_guaranteed(agent_name))
        except UnknownAgentTargetError:
            return False

    async def run(self, request: CreateRouteRequest) -> RoutedTask:
        task_id = _task_id(request)
        remote_replay_safe = request.route_kind == "remote_ref" and bool(
            self.remote_create_idempotency_guaranteed(request.agent_name)
        )
        key_scope, replay_policy = create_evidence_policy(
            request.route_kind,
            external_key=request.idempotency_key is not None,
            remote_replay_safe=remote_replay_safe,
        )
        reservation = _reservation(request, task_id)
        reservation, evidence = await self._reserve(
            reservation,
            key_scope=key_scope,
            replay_policy=replay_policy,
        )
        if reservation.route_state == "active":
            return RoutedTask(
                record=await self._record_port.read(reservation),
                route=reservation,
            )
        if reservation.route_state in {"failed", "uncertain"}:
            raise _prior_creation_error(task_id, reservation)
        kwargs = _spawn_kwargs(request, task_id, remote_replay_safe)
        await self._effect_boundary(request.before_effect, reservation, task_id)
        record = await self._spawn(
            request, task_id, kwargs, reservation, remote_replay_safe
        )
        await self._validate_effect(request.route_kind, record, reservation, task_id)
        route = await self._activate(reservation, record, task_id)
        return RoutedTask(
            record=(
                public_remote_record(record)
                if request.route_kind == "remote_ref"
                else record
            ),
            route=route,
        )

    async def reconcile_pending_create(
        self, route: Route, *, live_interruption: bool = False
    ) -> Route:
        return await _reconcile_pending_create(
            route,
            route_store=self._route_store,
            get_local_record=self._control.get_task_record,
            live_interruption=live_interruption,
        )

    async def _reserve(
        self, route: Route, *, key_scope: str, replay_policy: str
    ) -> tuple[Route, GatewayCreateEvidence | None]:
        try:
            reserved = await self._route_store.areserve_route(
                route,
                create_key_scope=key_scope,
                create_replay_policy=replay_policy,
            )
            evidence = await self._route_store.aget_create_evidence(route.task_id)
        except Exception as exc:
            raise route_persistence_error(
                route.task_id,
                route_state="unpersisted",
                queryable=False,
                retryable=True,
                effect_outcome="not_started",
            ) from exc
        if evidence is None or evidence.effect_boundary != "reserved":
            reserved = await self.reconcile_pending_create(reserved)
        return reserved, evidence

    async def _effect_boundary(
        self,
        before_effect: Callable[[], Awaitable[None]] | None,
        reservation: Route,
        task_id: str,
    ) -> None:
        operations = (
            (before_effect, "Gateway command effect boundary could not be persisted"),
            (
                lambda: self._route_store.amark_create_effect_started(task_id),
                "Gateway create effect boundary could not be persisted",
            ),
        )
        for operation, message in operations:
            if operation is not None:
                await self._run_boundary(operation, reservation, task_id, message)

    async def _run_boundary(
        self,
        operation: Callable[[], Awaitable[object]],
        reservation: Route,
        task_id: str,
        message: str,
    ) -> None:
        try:
            await operation()
        except Exception as exc:
            await _boundary_failure(self, reservation, task_id, message, exc)
        except BaseException:
            await self._cleanup_interrupted(reservation)
            raise

    async def _cleanup_interrupted(self, reservation: Route) -> None:
        await shield_durable_cleanup(
            self.reconcile_pending_create(reservation, live_interruption=True)
        )

    async def _spawn(
        self,
        request: CreateRouteRequest,
        task_id: str,
        kwargs: dict[str, object],
        reservation: Route,
        remote_replay_safe: bool,
    ) -> TaskRecord:
        try:
            return await self._control.spawn_task(
                request.agent_name,
                request.input_content,
                **kwargs,
            )
        except Exception as exc:
            raise await recover_create_effect_error(
                exc,
                agent_name=request.agent_name,
                route_kind=request.route_kind,
                task_id=task_id,
                idempotency_key_present=request.idempotency_key is not None,
                remote_replay_safe=remote_replay_safe,
                reservation=reservation,
                route_store=self._route_store,
                fail_reservation=self._fail_reservation,
            ) from exc
        except BaseException:
            await self._cleanup_interrupted(reservation)
            raise

    async def _validate_effect(
        self,
        route_kind: TaskRouteKind,
        record: TaskRecord,
        reservation: Route,
        task_id: str,
    ) -> None:
        if route_kind == "remote_ref" and not record.upstream_task_id:
            error = public_upstream_payload_error(operation="create", task_id=task_id)
            await _reject_effect(self, error, reservation, task_id)
        elif not has_durable_create_effect(record, route_kind=route_kind):
            error = GatewayTaskError(
                code="task_effect_not_durable",
                message="Gateway Task effect has no durable run or remote binding",
            )
            await _reject_effect(self, error, reservation, task_id)

    async def _activate(
        self, reservation: Route, record: TaskRecord, task_id: str
    ) -> Route:
        try:
            return await self._route_store.atransition_route(
                task_id,
                route_state="active",
                upstream_task_id=record.upstream_task_id or record.task_id,
                route_error=None,
            )
        except Exception as exc:
            reservation.upstream_task_id = record.upstream_task_id or record.task_id
            durable_route = await self._fail_reservation(
                reservation,
                "Task effect completed but route activation failed",
                True,
            )
            raise route_persistence_error(
                task_id,
                route_state=durable_route.route_state if durable_route else "unknown",
                queryable=durable_route is not None,
                retryable=False,
                effect_outcome="completed",
            ) from exc

    async def _fail_reservation(
        self, route: Route, error: BaseException | str, uncertain: bool
    ) -> Route | None:
        try:
            updated = await self._route_store.atransition_route(
                route.task_id,
                route_state="uncertain" if uncertain else "failed",
                upstream_task_id=route.upstream_task_id,
                route_error=public_create_route_error(
                    error, route_kind=route.route_kind
                ),
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


def _reservation(request: CreateRouteRequest, task_id: str) -> Route:
    return TaskRouteRecord(
        task_id=task_id,
        agent_name=request.agent_name,
        metadata=dict(request.metadata),
        route_kind=request.route_kind,
        upstream_task_id=task_id if request.route_kind == "local" else None,
        webhook=dict(request.webhook) if request.webhook is not None else None,
        route_state="pending",
    )


def _task_id(request: CreateRouteRequest) -> str:
    return request.task_id if request.task_id is not None else str(uuid4())


def _spawn_kwargs(
    request: CreateRouteRequest, task_id: str, remote_replay_safe: bool
) -> dict[str, object]:
    kwargs: dict[str, object] = {
        "webhook": dict(request.webhook) if request.webhook is not None else None,
        "delegation_context": request.delegation_context,
        "task_id": task_id,
    }
    if request.route_kind == "remote_ref":
        kwargs["attachments"] = request.attachments or []
        kwargs["metadata"] = dict(request.metadata)
    if request.idempotency_key is not None:
        kwargs["idempotency_key"] = request.idempotency_key
    elif request.route_kind == "remote_ref" and remote_replay_safe:
        kwargs["idempotency_key"] = f"gateway-create:{task_id}"
    return kwargs


def _prior_creation_error(task_id: str, route: Route) -> GatewayTaskError:
    error = GatewayTaskError(
        code="task_creation_not_retryable",
        message=(
            "Gateway Task creation previously ended with route state "
            f"'{route.route_state}'"
        ),
    )
    return with_route_identity(
        error,
        route,
        task_id=task_id,
        retryable=False,
        effect_outcome="uncertain"
        if route.route_state == "uncertain"
        else "not_started",
    )


async def _reject_effect(
    workflow: CreateRouteWorkflow,
    error: GatewayTaskError,
    reservation: Route,
    task_id: str,
) -> None:
    durable_route = await workflow._fail_reservation(reservation, error, True)
    raise with_route_identity(
        error,
        durable_route,
        task_id=task_id,
        retryable=False,
        route_state=reservation.route_state,
        effect_outcome="uncertain",
    )


async def _boundary_failure(
    workflow: CreateRouteWorkflow,
    reservation: Route,
    task_id: str,
    message: str,
    cause: Exception,
) -> None:
    durable_route = await workflow._fail_reservation(reservation, message, False)
    raise route_persistence_error(
        task_id,
        route_state=durable_route.route_state if durable_route else "unknown",
        queryable=durable_route is not None,
        retryable=False,
        effect_outcome="not_started",
    ) from cause


def _task_not_found(task_id: str) -> GatewayTaskError:
    return GatewayTaskError(
        code="task_not_found", message=f"Task '{task_id}' does not exist"
    )
