"""Durable idempotent Gateway command orchestration."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ruyi_agent.gateway.application import GatewayApplicationContext
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import AttachmentInput, TaskResponse
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
                    "attachments": [item.model_dump(mode="json") for item in normalized],
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
        return await self._execute(
            claim,
            self._tasks.create_effect(
                task_id=claim.task_id,
                agent_name=agent_name,
                input_content=input_content,
                attachments=normalized,
                metadata=metadata,
                webhook=webhook,
                idempotency_key=idempotency_key,
            ),
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
                    "attachments": [item.model_dump(mode="json") for item in normalized],
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
        return await self._execute(
            claim,
            self._send_claimed_input(
                task_id=task_id,
                input_content=input_content,
                attachments=normalized,
                external_idempotency_key=idempotency_key,
                claim=claim,
            ),
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
        effect: Awaitable[TaskResponse],
    ) -> GatewayCommandOutcome:
        if claim.claim_token is None:
            raise RuntimeError("Acquired Gateway command has no claim token")
        try:
            task = await effect
            await self._context.command_store.acomplete(
                command_id=claim.command_id,
                claim_token=claim.claim_token,
                response_json=task.model_dump_json(),
            )
        except BaseException:
            with suppress(BaseException):
                await asyncio.shield(
                    self._context.command_store.arelease(
                        command_id=claim.command_id,
                        claim_token=claim.claim_token,
                    )
                )
            raise
        return GatewayCommandOutcome(task=task, replayed=False)

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
