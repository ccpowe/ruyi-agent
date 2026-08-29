"""Pending Review resource and decision HTTP routes."""

from __future__ import annotations

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from ruyi_agent.gateway.models import ReviewListResponse, ReviewResponse, TaskResponse

from .context import GatewayHttpContext
from .schemas import ReviewDecisionInput


def attach_review_routes(app: FastAPI, context: GatewayHttpContext) -> None:
    @app.get("/reviews", response_model=ReviewListResponse)
    async def list_reviews(
        request: Request,
        _: None = Depends(context.require_bearer),
    ) -> ReviewListResponse:
        return await context.service(request).list_reviews(
            cursor=request.query_params.get("cursor"),
            limit=int(request.query_params.get("limit", "20")),
        )

    @app.get("/reviews/{review_id}", response_model=ReviewResponse)
    async def get_review(
        request: Request,
        review_id: str,
        _: None = Depends(context.require_bearer),
    ) -> ReviewResponse:
        return await context.service(request).get_review(review_id)

    @app.get("/tasks/{task_id}/reviews", response_model=ReviewListResponse)
    async def list_task_reviews(
        request: Request,
        task_id: str,
        _: None = Depends(context.require_bearer),
    ) -> ReviewListResponse:
        return await context.service(request).list_task_reviews(task_id)

    @app.post(
        "/tasks/{task_id}/reviews/{review_id}/decision",
        response_model=TaskResponse,
        status_code=202,
    )
    async def submit_review_decision(
        request: Request,
        task_id: str,
        review_id: str,
        payload: ReviewDecisionInput,
        _: None = Depends(context.require_bearer),
    ) -> JSONResponse:
        task = await context.service(request).submit_review_decision(
            task_id=task_id,
            review_id=review_id,
            decisions=payload.decisions,
        )
        return JSONResponse(status_code=202, content=task.model_dump(mode="json"))
