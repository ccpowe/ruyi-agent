from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

import httpx

from ruyi_agent.channels.cli.approval_presenter import ApprovalPresenter
from ruyi_agent.channels.cli.event_adapter import (
    resume_command_from_decisions,
    stream_agent_events,
)
from ruyi_agent.runtime.agent_turn import normalize_agent_turn
from ruyi_agent.runtime.bootstrap import bootstrap_application
from ruyi_agent.channels.cli.commands import COMMAND_NAMES, CliState, SlashCommandHandler
from ruyi_agent.channels.cli.prompt import InteractivePrompt
from ruyi_agent.channels.cli.renderer import InteractiveRenderer
from ruyi_agent.control_plane.reviews import ReviewControl, runtime_decisions_from_review_payload


@dataclass(slots=True)
class RootTurnResult:
    agent_name: str
    thread_id: str
    content: str = ""
    interrupt_requests: list[dict[str, Any]] = field(default_factory=list)


@dataclass(slots=True)
class LocalRootRunner:
    get_agent: Any
    resolve_permission_profile: Any

    async def resume_review(
        self,
        *,
        agent_name: str,
        thread_id: str,
        decisions: list[dict[str, Any]],
    ) -> RootTurnResult:
        agent = self.get_agent(agent_name)
        config = _agent_config(
            agent_name=agent_name,
            thread_id=thread_id,
            resolve_permission_profile=self.resolve_permission_profile,
        )
        result = await agent.ainvoke(
            resume_command_from_decisions(decisions),
            config=config,
            version="v2",
        )
        outcome = await normalize_agent_turn(agent, config, result)
        return RootTurnResult(
            agent_name=agent_name,
            thread_id=thread_id,
            content=outcome.content,
            interrupt_requests=outcome.review_payloads,
        )


def _is_retryable_stream_error(exc: BaseException) -> bool:
    return isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ReadError,
            httpx.RemoteProtocolError,
            httpx.TimeoutException,
        ),
    )


def _agent_config(
    *,
    agent_name: str,
    thread_id: str,
    resolve_permission_profile: Any,
    resolve_pending_reviews: Any | None = None,
) -> dict[str, Any]:
    configurable = {
        "thread_id": thread_id,
        "agent_name": agent_name,
        "permission_profile": resolve_permission_profile(agent_name),
    }
    if resolve_pending_reviews is not None:
        configurable["resolve_pending_reviews"] = resolve_pending_reviews
    return {"configurable": configurable}


async def _stream_turn_once(
    *,
    agent: Any,
    payload: Any,
    config: dict[str, Any],
    renderer: InteractiveRenderer,
    resolve_task_reviews: Any | None = None,
) -> list[dict[str, Any]]:
    async for event in stream_agent_events(agent, payload, config):
        renderer.render_event(event)
        if resolve_task_reviews is not None:
            await resolve_task_reviews()
    outcome = await normalize_agent_turn(agent, config, {})
    return outcome.review_payloads


async def _run_agent_turn(
    *,
    runtime: Any,
    state: CliState,
    user_input: str,
    renderer: InteractiveRenderer,
    approval_presenter: ApprovalPresenter,
    review_control: ReviewControl,
    resolve_task_reviews: Any | None = None,
) -> None:
    agent = runtime.get_local_agent(state.agent_name)
    config = _agent_config(
        agent_name=state.agent_name,
        thread_id=state.thread_id,
        resolve_permission_profile=runtime.resolve_root_permission_profile,
        resolve_pending_reviews=resolve_task_reviews,
    )
    payload: Any = {"messages": [{"role": "user", "content": user_input}]}
    while True:
        interrupt_requests = await _stream_turn_once(
            agent=agent,
            payload=payload,
            config=config,
            renderer=renderer,
            resolve_task_reviews=resolve_task_reviews,
        )
        if not interrupt_requests:
            return
        if len(interrupt_requests) > 1:
            raise ValueError("Multiple simultaneous root reviews are not supported")
        registered_reviews = review_control.register_root_interrupts(
            agent_name=state.agent_name,
            thread_id=state.thread_id,
            interrupt_requests=interrupt_requests,
        )
        if not registered_reviews:
            raise ValueError("Root review interrupt could not be registered")
        review = registered_reviews[0]
        decisions = await approval_presenter.request_decisions(review)
        renderer.render_approval_summary(review, decisions)
        if decisions is None:
            renderer.info("Review remains pending. Use /reviews to continue it later.")
            return
        root_review = review_control.root_reviews.pop(review.review_id)
        payload = resume_command_from_decisions(
            runtime_decisions_from_review_payload(
                decisions,
                pending_review=root_review.pending_review,
            )
        )


async def _run_agent_turn_with_retry(
    *,
    runtime: Any,
    state: CliState,
    user_input: str,
    renderer: InteractiveRenderer,
    approval_presenter: ApprovalPresenter,
    review_control: ReviewControl,
    resolve_task_reviews: Any | None = None,
) -> None:
    attempts = 0
    max_attempts = 2
    while True:
        attempts += 1
        try:
            await _run_agent_turn(
                runtime=runtime,
                state=state,
                user_input=user_input,
                renderer=renderer,
                approval_presenter=approval_presenter,
                review_control=review_control,
                resolve_task_reviews=resolve_task_reviews,
            )
            return
        except Exception as exc:
            if attempts < max_attempts and _is_retryable_stream_error(exc):
                renderer.info(
                    f"{exc.__class__.__name__}: {exc}. Retrying once..."
                )
                await asyncio.sleep(0.5)
                continue
            raise


def _has_pending_task_review(
    review_control: ReviewControl,
    skipped_review_ids: set[str],
) -> bool:
    return any(
        review.task_id and review.review_id not in skipped_review_ids
        for review in review_control.list_pending_reviews()
    )


async def _wait_for_pending_task_review(
    review_control: ReviewControl,
    skipped_review_ids: set[str],
    *,
    poll_interval: float = 0.5,
) -> None:
    while not _has_pending_task_review(review_control, skipped_review_ids):
        await asyncio.sleep(poll_interval)


async def _read_prompt_or_wait_for_review(
    *,
    prompt: InteractivePrompt,
    state: CliState,
    review_control: ReviewControl,
    skipped_review_ids: set[str],
) -> str | None:
    input_task = asyncio.create_task(
        prompt.read(
            agent_name=state.agent_name,
            thread_id=state.thread_id,
        )
    )
    review_task = asyncio.create_task(
        _wait_for_pending_task_review(review_control, skipped_review_ids)
    )
    done, pending = await asyncio.wait(
        {input_task, review_task},
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
    for task in pending:
        with suppress(asyncio.CancelledError):
            await task
    if input_task in done:
        return await input_task
    if review_task in done:
        input_task.cancel()
        with suppress(asyncio.CancelledError):
            await input_task
        return None
    return None


async def run_interactive(
    *,
    agent_name: str | None = None,
    thread_id: str | None = None,
) -> None:
    renderer = InteractiveRenderer()
    approval_presenter = ApprovalPresenter(renderer)
    async with bootstrap_application() as runtime:
        state = CliState(
            agent_name=agent_name or runtime.main_agent_name,
            thread_id=thread_id or str(uuid4()),
        )
        root_runner = LocalRootRunner(
            get_agent=runtime.get_local_agent,
            resolve_permission_profile=runtime.resolve_root_permission_profile,
        )
        review_control = ReviewControl(
            control=runtime.worker_control,
            root_runner=root_runner,
        )

        async def resolve_reviews(
            *,
            include_root: bool = True,
            skipped_review_ids: set[str] | None = None,
        ) -> bool:
            attempted: set[str] = set(skipped_review_ids or ())
            handled = False
            while True:
                pending = [
                    review
                    for review in review_control.list_pending_reviews()
                    if review.review_id not in attempted
                    and (include_root or review.task_id)
                ]
                if not pending:
                    return handled
                pending.sort(key=lambda item: item.updated_at)
                review = pending[0]
                attempted.add(review.review_id)
                decisions = await approval_presenter.request_decisions(review)
                renderer.render_approval_summary(review, decisions)
                if decisions is None:
                    if skipped_review_ids is not None:
                        skipped_review_ids.add(review.review_id)
                    return handled
                result = await review_control.submit_decision(
                    review.review_id,
                    decisions,
                )
                handled = True
                if result.root_result is not None:
                    if result.root_result.content:
                        renderer.render_assistant_message(
                            result.root_result.content,
                            namespace=result.root_result.agent_name,
                        )
                    review_control.register_root_interrupts(
                        agent_name=result.root_result.agent_name,
                        thread_id=result.root_result.thread_id,
                        interrupt_requests=result.root_result.interrupt_requests,
                    )

        command_handler = SlashCommandHandler(
            runtime=runtime,
            state=state,
            renderer=renderer,
            review_control=review_control,
            resolve_reviews=resolve_reviews,
        )
        prompt = InteractivePrompt(command_names=COMMAND_NAMES)
        renderer.welcome(
            agent_name=state.agent_name,
            thread_id=state.thread_id,
            checkpoint_db=runtime.checkpoint_db,
        )

        skipped_idle_task_review_ids: set[str] = set()
        while True:
            try:
                maybe_user_input = await _read_prompt_or_wait_for_review(
                    prompt=prompt,
                    state=state,
                    review_control=review_control,
                    skipped_review_ids=skipped_idle_task_review_ids,
                )
                if maybe_user_input is None:
                    await resolve_reviews(
                        include_root=False,
                        skipped_review_ids=skipped_idle_task_review_ids,
                    )
                    continue
                user_input = maybe_user_input.strip()
            except EOFError:
                return
            except KeyboardInterrupt:
                renderer.info("Interrupted input.")
                continue

            if not user_input:
                continue
            if user_input.startswith("//"):
                user_input = user_input[1:]
            elif user_input.startswith("/"):
                result = await command_handler.execute(user_input)
                if result.should_exit:
                    return
                continue

            try:
                renderer.render_user_message(user_input)
                skipped_task_review_ids: set[str] = set()

                async def resolve_task_reviews() -> bool:
                    return await resolve_reviews(
                        include_root=False,
                        skipped_review_ids=skipped_task_review_ids,
                    )

                await _run_agent_turn_with_retry(
                    runtime=runtime,
                    state=state,
                    user_input=user_input,
                    renderer=renderer,
                    approval_presenter=approval_presenter,
                    review_control=review_control,
                    resolve_task_reviews=resolve_task_reviews,
                )
                await resolve_reviews()
            except asyncio.CancelledError:
                renderer.info("Turn interrupted.")
                continue
            except KeyboardInterrupt:
                renderer.info("Turn interrupted.")
                continue
            except Exception as exc:  # noqa: BLE001
                renderer.render_error(exc)
