"""Durable idempotent Gateway command orchestration."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ruyi_agent.gateway.application import GatewayApplicationContext
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import AttachmentInput, TaskResponse
from ruyi_agent.gateway.route_reservations import shield_durable_cleanup
from ruyi_agent.gateway.task_service import GatewayTaskService
from ruyi_agent.storage.gateway_command_store import (
    GatewayCommandClaim,
    GatewayCommandConflictError,
)
from ruyi_agent.task_models import MetadataScalar, TaskRouteRecord

DEFAULT_GATEWAY_PRINCIPAL = "gateway-bearer"
COMMAND_WAIT_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class GatewayCommandOutcome:
    task: TaskResponse
    replayed: bool


class GatewayCommandService:
    """Reserve, execute, and replay create/input Gateway Commands."""

    def __init__(
        self,
        context: GatewayApplicationContext,
        tasks: GatewayTaskService,
    ) -> None:
        self._context = context
        self._tasks = tasks

    async def create_task(
        self,
        *,
        agent_name: str,
        input_content: str,
        attachments: list[AttachmentInput] | None,
        metadata: dict[str, MetadataScalar],
        webhook: dict[str, MetadataScalar] | None,
        idempotency_key: str | None,
        principal_id: str = DEFAULT_GATEWAY_PRINCIPAL,
    ) -> GatewayCommandOutcome:
        validate_idempotency_key(idempotency_key)
        normalized = list(attachments or [])
        if idempotency_key is None:
            task = await self._tasks.create_effect(
                task_id=str(uuid4()),
                agent_name=agent_name,
                input_content=input_content,
                attachments=normalized,
                metadata=metadata,
                webhook=webhook,
                idempotency_key=None,
            )
            return GatewayCommandOutcome(task=task, replayed=False)

        request_hash = command_request_hash(
            operation="create_task",
            target=agent_name,
            body={
                "input": {
                    "content": input_content,
                    "attachments": [
                        item.model_dump(mode="json") for item in normalized
                    ],
                },
                "metadata": metadata,
                "webhook": webhook,
            },
        )
        claim = await self._claim(
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            operation="create_task",
            target=agent_name,
            request_hash=request_hash,
            proposed_task_id=str(uuid4()),
        )
        if claim.status == "replay":
            return await self._replayed_outcome(claim)
        if claim.status == "terminal":
            raise await self._terminal_error(claim, create_command=True)

        create_replay_safe = self._tasks.create_effect_replay_safe(agent_name)
        create_boundary_started = False

        async def mark_create_effect_started() -> None:
            nonlocal create_boundary_started
            if claim.claim_token is None:
                raise RuntimeError("Acquired Gateway command has no claim token")
            create_boundary_started = True
            await self._context.command_store.amark_effect_started(
                command_id=claim.command_id,
                claim_token=claim.claim_token,
                replay_safe=create_replay_safe,
            )

        return await self._execute(
            claim,
            lambda: self._tasks.create_effect(
                task_id=claim.task_id,
                agent_name=agent_name,
                input_content=input_content,
                attachments=normalized,
                metadata=metadata,
                webhook=webhook,
                idempotency_key=idempotency_key,
                before_effect=mark_create_effect_started,
            ),
            replay_safe=create_replay_safe,
            effect_boundary_is_managed=True,
            effect_boundary_started=lambda: create_boundary_started,
        )

    async def send_input(
        self,
        *,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput] | None,
        idempotency_key: str | None,
        principal_id: str = DEFAULT_GATEWAY_PRINCIPAL,
    ) -> GatewayCommandOutcome:
        validate_idempotency_key(idempotency_key)
        normalized = list(attachments or [])
        if idempotency_key is None:
            route = await self._context.router.get_route(task_id)
            task = await self._tasks.send_effect(
                route=route,
                input_content=input_content,
                attachments=normalized,
                batch_id=str(uuid4()),
                downstream_idempotency_key=None,
                mailbox_message_id=None,
            )
            return GatewayCommandOutcome(task=task, replayed=False)

        request_hash = command_request_hash(
            operation="send_input",
            target=task_id,
            body={
                "input": {
                    "content": input_content,
                    "attachments": [
                        item.model_dump(mode="json") for item in normalized
                    ],
                }
            },
        )
        claim = await self._claim(
            principal_id=principal_id,
            idempotency_key=idempotency_key,
            operation="send_input",
            target=task_id,
            request_hash=request_hash,
            proposed_task_id=task_id,
            proposed_mailbox_message_id=str(uuid4()),
        )
        if claim.status == "replay":
            return await self._replayed_outcome(claim)
        if claim.status == "terminal":
            raise await self._terminal_error(claim, create_command=False)
        return await self._execute(
            claim,
            lambda: self._send_claimed_input(
                task_id=task_id,
                input_content=input_content,
                attachments=normalized,
                external_idempotency_key=idempotency_key,
                claim=claim,
            ),
            replay_safe=True,
        )

    async def _send_claimed_input(
        self,
        *,
        task_id: str,
        input_content: str,
        attachments: list[AttachmentInput],
        external_idempotency_key: str,
        claim: GatewayCommandClaim,
    ) -> TaskResponse:
        """Resolve the route inside the command compensation boundary."""

        route = await self._context.router.get_route(task_id)
        downstream_key = (
            external_idempotency_key
            if route.route_kind == "remote_ref"
            else f"gateway-input:{claim.command_id}"
        )
        return await self._tasks.send_effect(
            route=route,
            input_content=input_content,
            attachments=attachments,
            batch_id=claim.command_id,
            downstream_idempotency_key=downstream_key,
            mailbox_message_id=claim.mailbox_message_id or claim.command_id,
        )

    async def _claim(
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
                claim = await self._context.command_store.aclaim(
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

    async def _execute(
        self,
        claim: GatewayCommandClaim,
        effect_factory: Callable[[], Awaitable[TaskResponse]],
        *,
        replay_safe: bool,
        effect_boundary_is_managed: bool = False,
        effect_boundary_started: Callable[[], bool] | None = None,
    ) -> GatewayCommandOutcome:
        if claim.claim_token is None:
            raise RuntimeError("Acquired Gateway command has no claim token")
        try:
            if not effect_boundary_is_managed:
                await self._context.command_store.amark_effect_started(
                    command_id=claim.command_id,
                    claim_token=claim.claim_token,
                    replay_safe=replay_safe,
                )
            task = await effect_factory()
            await self._context.command_store.acomplete(
                command_id=claim.command_id,
                claim_token=claim.claim_token,
                response_json=task.model_dump_json(),
            )
        except GatewayTaskError as exc:
            if _is_terminal_command_error(exc):
                with suppress(BaseException):
                    await shield_durable_cleanup(
                        self._context.command_store.afail(
                            command_id=claim.command_id,
                            claim_token=claim.claim_token,
                            error_json=_dump_gateway_error(exc),
                        )
                    )
            else:
                with suppress(BaseException):
                    await shield_durable_cleanup(
                        self._context.command_store.arelease(
                            command_id=claim.command_id,
                            claim_token=claim.claim_token,
                        )
                    )
            raise
        except BaseException:
            unsafe_started = (
                not replay_safe
                and effect_boundary_started is not None
                and effect_boundary_started()
            )
            if unsafe_started:
                uncertain = GatewayTaskError(
                    code="idempotency_outcome_uncertain",
                    message=(
                        "The Gateway command may have reached a non-idempotent "
                        "downstream service"
                    ),
                    details={
                        "task_id": claim.task_id,
                        "create_retryable": False,
                        "effect_outcome": "uncertain",
                    },
                )
                with suppress(BaseException):
                    await shield_durable_cleanup(
                        self._terminalize_unsafe_create(claim, uncertain)
                    )
            else:
                with suppress(BaseException):
                    await shield_durable_cleanup(
                        self._context.command_store.arelease(
                            command_id=claim.command_id,
                            claim_token=claim.claim_token,
                        )
                    )
            raise
        return GatewayCommandOutcome(task=task, replayed=False)

    async def _terminal_error(
        self,
        claim: GatewayCommandClaim,
        *,
        create_command: bool,
    ) -> GatewayTaskError:
        stored_error = _load_gateway_error(claim.error_json)
        try:
            route = await self._context.router.get_route(claim.task_id)
        except GatewayTaskError:
            return _public_terminal_error(
                stored_error,
                create_command=create_command,
                task_id=claim.task_id,
                route=None,
            )
        error = _public_terminal_error(
            stored_error,
            create_command=create_command,
            task_id=route.task_id,
            route=route,
        )
        if create_command and route.route_state == "pending":
            route = await self._context.router.mark_create_outcome_uncertain(
                route,
                error.message,
            )
            error = _public_terminal_error(
                stored_error,
                create_command=True,
                task_id=route.task_id,
                route=route,
            )
        return error

    async def _replayed_outcome(
        self,
        claim: GatewayCommandClaim,
    ) -> GatewayCommandOutcome:
        stored = TaskResponse.model_validate_json(claim.response_json)
        try:
            route = await self._context.router.get_route(claim.task_id)
            record = self._context.router.ensure_record(route)
        except (GatewayTaskError, ValueError):
            route = None
            record = None
        pending_review = _public_pending_review(
            stored.pending_review,
            task_id=claim.task_id,
        )
        updates: dict[str, object] = {
            "task_id": claim.task_id,
            "root_task_id": record.root_task_id if record is not None else claim.task_id,
            "parent_task_id": record.parent_task_id if record is not None else None,
            "agent_name": route.agent_name if route is not None else stored.agent_name,
            "metadata": dict(route.metadata) if route is not None else {},
            "pending_review": pending_review,
        }
        if route is not None and route.route_kind == "remote_ref":
            updates["artifacts"] = []
            if stored.error:
                updates["error"] = "Remote Gateway Task failed"
        return GatewayCommandOutcome(
            task=stored.model_copy(update=updates),
            replayed=True,
        )

    async def _terminalize_unsafe_create(
        self,
        claim: GatewayCommandClaim,
        error: GatewayTaskError,
    ) -> None:
        try:
            route = await self._context.router.get_route(claim.task_id)
            await self._context.router.mark_create_outcome_uncertain(
                route,
                error.message,
            )
        except Exception:
            pass
        if claim.claim_token is None:
            raise RuntimeError("Acquired Gateway command has no claim token")
        await self._context.command_store.afail(
            command_id=claim.command_id,
            claim_token=claim.claim_token,
            error_json=_dump_gateway_error(error),
        )

def validate_idempotency_key(idempotency_key: str | None) -> None:
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


def command_request_hash(
    *,
    operation: str,
    target: str,
    body: dict[str, Any],
) -> str:
    canonical = json.dumps(
        {"operation": operation, "target": target, "body": body},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _is_terminal_command_error(error: GatewayTaskError) -> bool:
    return bool(error.details and error.details.get("create_retryable") is False)


def _dump_gateway_error(error: GatewayTaskError) -> str:
    return json.dumps(
        {
            "kind": error.kind,
            "code": error.code,
            "message": error.message,
            "details": error.details,
        },
        ensure_ascii=False,
        sort_keys=True,
    )


def _load_gateway_error(payload: str | None) -> GatewayTaskError:
    if not payload:
        raise RuntimeError("Terminal Gateway command has no durable error")
    parsed = json.loads(payload)
    return GatewayTaskError(
        kind=parsed.get("kind"),
        code=str(parsed["code"]),
        message=str(parsed["message"]),
        details=parsed.get("details"),
    )


_SAFE_TERMINAL_CODES = {
    "agent_unavailable",
    "attachment_too_large",
    "delegation_budget_exhausted",
    "delegation_depth_exceeded",
    "idempotency_outcome_uncertain",
    "invalid_attachment",
    "invalid_delegation_context",
    "remote_executor_not_implemented",
    "route_persistence_failed",
    "runtime_unavailable",
    "task_creation_failed",
    "task_effect_not_durable",
    "task_route_unavailable",
    "upstream_gateway_error",
}


def _public_terminal_error(
    stored: GatewayTaskError,
    *,
    create_command: bool,
    task_id: str,
    route: TaskRouteRecord | None,
) -> GatewayTaskError:
    """Rebuild a legacy terminal command from public, authoritative fields."""

    code = (
        stored.code
        if stored.code in _SAFE_TERMINAL_CODES
        else "gateway_command_failed"
    )
    upstream = stored.kind == "upstream_failure" or code == "upstream_gateway_error"
    if code == "idempotency_outcome_uncertain":
        message = (
            "The previous Gateway command may have reached a non-idempotent "
            "downstream service"
        )
    elif upstream:
        message = (
            "Remote Gateway Task creation failed"
            if create_command
            else "Remote Gateway Task input failed"
        )
    elif code == "route_persistence_failed":
        message = "Gateway could not durably transition the Task route"
    elif code == "runtime_unavailable":
        message = "Gateway runtime is unavailable"
    elif code == "agent_unavailable":
        message = "Gateway Agent is unavailable"
    elif code.startswith("delegation_"):
        message = "Gateway Task creation was rejected by delegation policy"
    elif code == "task_effect_not_durable":
        message = "Gateway Task effect has no durable run or remote binding"
    else:
        message = (
            "Gateway Task creation failed"
            if create_command
            else "Gateway Task command failed"
        )
    details: dict[str, object] = {
        "task_id": task_id,
        "task_queryable": route is not None,
    }
    if route is not None:
        details.update(
            {
                "task_url": f"/tasks/{task_id}",
                "route_state": route.route_state,
            }
        )
    if create_command:
        details["create_retryable"] = False
        details["effect_outcome"] = (
            "completed"
            if route is not None and route.route_state == "active"
            else (
                "not_started"
                if route is not None and route.route_state == "failed"
                else "uncertain"
            )
        )
    return GatewayTaskError(
        kind="upstream_failure" if upstream else None,
        code=code,
        message=message,
        details=details,
    )


def _public_pending_review(
    pending_review: dict[str, Any] | None,
    *,
    task_id: str,
) -> dict[str, Any] | None:
    if pending_review is None:
        return None
    projected = {
        key: pending_review[key]
        for key in ("review_id", "action_requests", "review_configs")
        if key in pending_review
    }
    if "source_task_id" in pending_review:
        projected["source_task_id"] = task_id
    return projected
