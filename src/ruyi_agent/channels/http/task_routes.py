"""Gateway Agent discovery and Task command/query HTTP routes."""

from __future__ import annotations

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse

from ruyi_agent.gateway.models import (
    AgentListResponse,
    AgentRefResponse,
    TaskListResponse,
    TaskMessageListResponse,
    TaskResponse,
    TaskWebhookEvent,
)

from .context import GatewayHttpContext
from .schemas import CreateTaskRequest, SendInputRequest


def attach_task_routes(app: FastAPI, context: GatewayHttpContext) -> None:
    @app.get("/agents", response_model=AgentListResponse)
    async def list_agents(
        request: Request,
        _: None = Depends(context.require_bearer),
    ) -> AgentListResponse:
        return AgentListResponse(items=context.service(request).list_agents())

    @app.get("/agents/{agent_name}", response_model=AgentRefResponse)
    async def get_agent(
        request: Request,
        agent_name: str,
        _: None = Depends(context.require_bearer),
    ) -> AgentRefResponse:
        return context.service(request).get_agent(agent_name)

    @app.post(
        "/agents/{agent_name}/tasks",
        response_model=TaskResponse,
        status_code=201,
    )
    async def create_task(
        request: Request,
        agent_name: str,
        payload: CreateTaskRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        _: None = Depends(context.require_bearer),
    ) -> JSONResponse:
        outcome = await context.service(request).create_task_command(
            agent_name=agent_name,
            input_content=payload.input.content,
            attachments=payload.input.attachments,
            metadata=payload.metadata,
            webhook=payload.webhook,
            idempotency_key=idempotency_key,
        )
        headers = {"Location": f"/tasks/{outcome.task.task_id}"}
        if outcome.replayed:
            headers["Idempotency-Replayed"] = "true"
        return JSONResponse(
            status_code=201,
            headers=headers,
            content=outcome.task.model_dump(mode="json"),
        )

    @app.get("/tasks/{task_id}", response_model=TaskResponse)
    async def get_task(
        request: Request,
        task_id: str,
        _: None = Depends(context.require_bearer),
    ) -> TaskResponse:
        return await context.service(request).get_task(task_id)

    @app.get("/tasks/{task_id}/messages", response_model=TaskMessageListResponse)
    async def list_task_messages(
        request: Request,
        task_id: str,
        _: None = Depends(context.require_bearer),
    ) -> TaskMessageListResponse:
        limit_raw = request.query_params.get("limit")
        limit = 20 if limit_raw is None else parse_limit(limit_raw)
        return await context.service(request).list_task_messages(
            task_id,
            cursor=request.query_params.get("cursor"),
            limit=limit,
        )

    @app.post("/tasks/{task_id}/input", response_model=TaskResponse, status_code=202)
    async def send_input(
        request: Request,
        task_id: str,
        payload: SendInputRequest,
        idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
        _: None = Depends(context.require_bearer),
    ) -> JSONResponse:
        outcome = await context.service(request).send_input_command(
            task_id=task_id,
            input_content=payload.input.content,
            attachments=payload.input.attachments,
            idempotency_key=idempotency_key,
        )
        headers = {"Idempotency-Replayed": "true"} if outcome.replayed else None
        return JSONResponse(
            status_code=202,
            headers=headers,
            content=outcome.task.model_dump(mode="json"),
        )

    @app.post("/tasks/{task_id}/cancel", response_model=TaskResponse, status_code=202)
    async def cancel_task(
        request: Request,
        task_id: str,
        _: None = Depends(context.require_bearer),
    ) -> JSONResponse:
        task = await context.service(request).cancel_task(task_id)
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))

    @app.post("/webhooks/tasks", status_code=202)
    async def receive_task_webhook(
        request: Request,
        payload: TaskWebhookEvent,
        _: None = Depends(context.require_bearer),
    ) -> JSONResponse:
        result = await context.service(request).handle_task_webhook(payload)
        return JSONResponse(status_code=202, content=result)

    @app.get("/tasks", response_model=TaskListResponse)
    async def list_tasks(
        request: Request,
        _: None = Depends(context.require_bearer),
    ) -> TaskListResponse:
        metadata_filters = {
            key.removeprefix("metadata."): value
            for key, value in request.query_params.items()
            if key.startswith("metadata.")
        }
        limit_raw = request.query_params.get("limit")
        limit = 20 if limit_raw is None else parse_limit(limit_raw)
        return await context.service(request).list_tasks(
            agent_name=request.query_params.get("agent_name"),
            status=request.query_params.get("status"),
            metadata_filters=metadata_filters,
            cursor=request.query_params.get("cursor"),
            limit=limit,
            root_task_id=request.query_params.get("root_task_id"),
        )


def parse_limit(raw: str) -> int:
    try:
        return int(raw)
    except ValueError as exc:
        from ruyi_agent.gateway.errors import GatewayTaskError

        raise GatewayTaskError(
            code="invalid_request",
            message="Query parameter 'limit' must be an integer",
        ) from exc
