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
from ruyi_agent.task_models import MetadataScalar

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
            return self._replayed_outcome(claim)
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
            return self._replayed_outcome(claim)
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
        error = _load_gateway_error(claim.error_json)
        try:
            route = await self._context.router.get_route(claim.task_id)
        except GatewayTaskError:
            return error
        details = dict(error.details or {})
        if create_command and route.route_state == "pending":
            route = await self._context.router.mark_create_outcome_uncertain(
                route,
                error.message,
            )
        details.update(
            {
                "task_id": route.task_id,
                "task_url": f"/tasks/{route.task_id}",
                "task_queryable": True,
                "route_state": route.route_state,
            }
        )
        if create_command:
            details["create_retryable"] = False
            if route.route_state == "active":
                details["effect_outcome"] = "completed"
            else:
                details.setdefault("effect_outcome", "uncertain")
        return GatewayTaskError(
            kind=error.kind,
            code=error.code,
            message=error.message,
            details=details,
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

    def _replayed_outcome(self, claim: GatewayCommandClaim) -> GatewayCommandOutcome:
        return GatewayCommandOutcome(
            task=TaskResponse.model_validate_json(claim.response_json),
            replayed=True,
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
