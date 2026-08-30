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
    public_upstream_payload_error,
)
from ruyi_agent.gateway.route_reservations import (
    create_evidence_policy,
    has_durable_create_effect,
    reconcile_pending_create as _reconcile_pending_create,
    route_persistence_error,
    shield_durable_cleanup,
    with_route_identity,
)
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.delegation.contracts import UnknownAgentTargetError
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


AttachmentPayload = dict[str, object]
ActiveRecordReader = Callable[[TaskRouteRecord], Awaitable[TaskRecord]]
RemoteCreateCapability = Callable[[str], bool]


@dataclass(slots=True)
class RoutedTask:
    record: TaskRecord
    route: TaskRouteRecord


@dataclass(frozen=True, slots=True)
class CreateRouteRequest:
    agent_name: str
    route_kind: TaskRouteKind
    input_content: str
    metadata: dict[str, MetadataScalar]
    webhook: dict[str, MetadataScalar] | None
    delegation_context: DelegationContext | None
    attachments: list[AttachmentPayload] | None = None
    task_id: str | None = None
    idempotency_key: str | None = None
    before_effect: Callable[[], Awaitable[None]] | None = None


class CreateRouteWorkflow:
    """Reserve, execute, reconcile, and activate one Task route."""

    def __init__(self, *, control: AgentControl, route_store: GatewayRouteStore, active_record_reader: ActiveRecordReader | None = None, remote_create_capability: RemoteCreateCapability | None = None) -> None:  # fmt: skip
        self._control = control
        self._route_store = route_store
        self._active_record_reader = active_record_reader or (
            lambda route: _read_control_record(control, route)
        )
        self._remote_create_capability = (
            remote_create_capability or _never_remote_replay
        )

    def remote_create_idempotency_guaranteed(self, agent_name: str) -> bool:
        try:
            return bool(self._remote_create_capability(agent_name))
        except UnknownAgentTargetError:
            return False

    async def run(self, request: CreateRouteRequest) -> RoutedTask:
        task_id = _task_id(request)
        remote_replay_safe = _remote_replay_safe(self, request)
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
        if _needs_reconcile(evidence):
            reservation = await self.reconcile_pending_create(reservation)
        if reservation.route_state == "active":
            return RoutedTask(
                record=await self._active_record_reader(reservation),
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
            record=_public_record(record, request.route_kind), route=route
        )

    async def reconcile_pending_create(self, route: TaskRouteRecord, *, live_interruption: bool = False) -> TaskRouteRecord:  # fmt: skip
        return await _reconcile_pending_create(
            route,
            route_store=self._route_store,
            get_local_record=self._control.get_task_record,
            live_interruption=live_interruption,
        )

    async def _reserve(self, route: TaskRouteRecord, *, key_scope: str, replay_policy: str) -> tuple[TaskRouteRecord, GatewayCreateEvidence | None]:  # fmt: skip
        try:
            reserved = await self._route_store.areserve_route(
                route,
                create_key_scope=key_scope,
                create_replay_policy=replay_policy,
            )
            evidence = await self._route_store.aget_create_evidence(route.task_id)
        except Exception as exc:
            raise _persistence_error(
                route.task_id, "unpersisted", False, True, "not_started"
            ) from exc
        return reserved, evidence

    async def _effect_boundary(self, before_effect: Callable[[], Awaitable[None]] | None, reservation: TaskRouteRecord, task_id: str) -> None:  # fmt: skip
        if before_effect is not None:
            await self._run_boundary(
                before_effect,
                reservation,
                task_id,
                "Gateway command effect boundary could not be persisted",
            )
        await self._run_boundary(
            lambda: self._route_store.amark_create_effect_started(task_id),
            reservation,
            task_id,
            "Gateway create effect boundary could not be persisted",
        )

    async def _run_boundary(self, operation: Callable[[], Awaitable[object]], reservation: TaskRouteRecord, task_id: str, message: str) -> None:  # fmt: skip
        try:
            await operation()
        except Exception as exc:
            await _boundary_failure(self, reservation, task_id, message, exc)
        except BaseException:
            await self._cleanup_interrupted(reservation)
            raise

    async def _cleanup_interrupted(self, reservation: TaskRouteRecord) -> None:
        await shield_durable_cleanup(
            self.reconcile_pending_create(reservation, live_interruption=True)
        )

    async def _spawn(self, request: CreateRouteRequest, task_id: str, kwargs: dict[str, object], reservation: TaskRouteRecord, remote_replay_safe: bool) -> TaskRecord:  # fmt: skip
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

    async def _validate_effect(self, route_kind: TaskRouteKind, record: TaskRecord, reservation: TaskRouteRecord, task_id: str) -> None:  # fmt: skip
        if route_kind == "remote_ref" and not record.upstream_task_id:
            error = public_upstream_payload_error(operation="create", task_id=task_id)
            await _reject_effect(self, error, reservation, task_id)
        elif not has_durable_create_effect(record, route_kind=route_kind):
            error = GatewayTaskError(
                code="task_effect_not_durable",
                message="Gateway Task effect has no durable run or remote binding",
            )
            await _reject_effect(self, error, reservation, task_id)

    async def _activate(self, reservation: TaskRouteRecord, record: TaskRecord, task_id: str) -> TaskRouteRecord:  # fmt: skip
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
            raise _persistence_error(
                task_id,
                durable_route.route_state if durable_route else "unknown",
                durable_route is not None,
                False,
                "completed",
            ) from exc

    async def _fail_reservation(self, route: TaskRouteRecord, error: BaseException | str, uncertain: bool) -> TaskRouteRecord | None:  # fmt: skip
        try:
            updated = await self._route_store.atransition_route(
                route.task_id,
                route_state="uncertain" if uncertain else "failed",
                upstream_task_id=route.upstream_task_id,
                route_error=public_create_route_error(
                    error, route_kind=route.route_kind
                ),  # fmt: skip
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


def _reservation(request: CreateRouteRequest, task_id: str) -> TaskRouteRecord:
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


def _remote_replay_safe(
    workflow: CreateRouteWorkflow, request: CreateRouteRequest
) -> bool:
    if request.route_kind != "remote_ref":
        return False
    return workflow.remote_create_idempotency_guaranteed(request.agent_name)


def _needs_reconcile(evidence: GatewayCreateEvidence | None) -> bool:
    if evidence is None:
        return True
    return evidence.effect_boundary != "reserved"


def _spawn_kwargs(request: CreateRouteRequest, task_id: str, remote_replay_safe: bool) -> dict[str, object]:  # fmt: skip
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


def _prior_creation_error(task_id: str, route: TaskRouteRecord) -> GatewayTaskError:
    error = GatewayTaskError(code="task_creation_not_retryable", message=f"Gateway Task creation previously ended with route state '{route.route_state}'")  # fmt: skip
    return with_route_identity(
        error,
        route,
        task_id=task_id,
        retryable=False,
        effect_outcome="uncertain"
        if route.route_state == "uncertain"
        else "not_started",
    )


def _persistence_error(task_id: str, route_state: str, queryable: bool, retryable: bool, effect_outcome: str) -> GatewayTaskError:  # fmt: skip
    return route_persistence_error(task_id, route_state=route_state, queryable=queryable, retryable=retryable, effect_outcome=effect_outcome)  # fmt: skip


async def _reject_effect(workflow: CreateRouteWorkflow, error: GatewayTaskError, reservation: TaskRouteRecord, task_id: str) -> None:  # fmt: skip
    durable_route = await workflow._fail_reservation(reservation, error, True)
    raise with_route_identity(error, durable_route, task_id=task_id, retryable=False, route_state=reservation.route_state, effect_outcome="uncertain")  # fmt: skip


async def _boundary_failure(workflow: CreateRouteWorkflow, reservation: TaskRouteRecord, task_id: str, message: str, cause: Exception) -> None:  # fmt: skip
    durable_route = await workflow._fail_reservation(reservation, message, False)
    raise _persistence_error(task_id, durable_route.route_state if durable_route else "unknown", durable_route is not None, False, "not_started") from cause  # fmt: skip


def _public_record(record: TaskRecord, route_kind: TaskRouteKind) -> TaskRecord:
    return public_remote_record(record) if route_kind == "remote_ref" else record


async def _read_control_record(
    control: AgentControl, route: TaskRouteRecord
) -> TaskRecord:
    return control.get_task_record(route.task_id)


def _never_remote_replay(agent_name: str) -> bool:
    del agent_name
    return False
