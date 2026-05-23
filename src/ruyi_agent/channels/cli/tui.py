from __future__ import annotations

import asyncio
import json
import uuid
from dataclasses import dataclass, field
from typing import Any

import typer
from langgraph.types import Command
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.syntax import Syntax
from rich.table import Table

from ruyi_agent.runtime.bootstrap import bootstrap_application
from ruyi_agent.runtime.delegation.async_runtime import AgentControl, TaskRecord
from ruyi_agent.runtime.agent_turn import normalize_agent_turn
from ruyi_agent.control_plane.contracts import ReviewDecision, ReviewDecisionKind, ReviewSnapshot
from ruyi_agent.control_plane.reviews import ReviewControl

app = typer.Typer(help="Review-control TUI MVP.", invoke_without_command=True)
console = Console()


@dataclass(slots=True)
class RootTurnResult:
    agent_name: str
    thread_id: str
    content: str = ""
    interrupt_requests: list[dict[str, Any]] = field(default_factory=list)


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True)


@dataclass(slots=True)
class LocalRootRunner:
    get_agent: Any
    resolve_permission_profile: Any

    async def run_user_message(
        self,
        *,
        agent_name: str,
        thread_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
    ) -> RootTurnResult:
        return await self._run_payload(
            agent_name=agent_name,
            thread_id=thread_id,
            payload={"messages": [{"role": "user", "content": content}]},
        )

    async def resume_review(
        self,
        *,
        agent_name: str,
        thread_id: str,
        decisions: list[dict[str, Any]],
    ) -> RootTurnResult:
        return await self._run_payload(
            agent_name=agent_name,
            thread_id=thread_id,
            payload=Command(resume={"decisions": decisions}),
        )

    async def _run_payload(
        self,
        *,
        agent_name: str,
        thread_id: str,
        payload: Any,
    ) -> RootTurnResult:
        agent = self.get_agent(agent_name)
        config = {
            "configurable": {
                "thread_id": thread_id,
                "agent_name": agent_name,
                "permission_profile": self.resolve_permission_profile(agent_name),
            }
        }
        result = await agent.ainvoke(payload, config=config, version="v2")
        outcome = await normalize_agent_turn(agent, config, result)
        return RootTurnResult(
            agent_name=agent_name,
            thread_id=thread_id,
            content=outcome.content,
            interrupt_requests=outcome.review_payloads,
        )


def _read_json_object(prompt: str) -> dict[str, Any] | None:
    first_line = console.input(prompt)
    raw = first_line.strip()
    if not raw:
        return None
    lines = [first_line]
    while True:
        try:
            parsed = json.loads("\n".join(lines))
        except json.JSONDecodeError as exc:
            if len(lines) == 1:
                console.print("[dim]Paste remaining JSON lines. Use END to cancel.[/dim]")
            next_line = console.input()
            if next_line.strip() == "END":
                console.print(f"[red]invalid JSON:[/red] {exc}")
                return None
            lines.append(next_line)
            continue
        if not isinstance(parsed, dict):
            console.print("[red]edited args must be a JSON object[/red]")
            return None
        return parsed


def _render_root_result(result: RootTurnResult, review_control: ReviewControl) -> None:
    if result.content:
        console.print(Panel(result.content, title=result.agent_name))
    review_control.register_root_interrupts(
        agent_name=result.agent_name,
        thread_id=result.thread_id,
        interrupt_requests=result.interrupt_requests,
    )


def _render_task_record(record: TaskRecord) -> None:
    result = record.result or record.error or ""
    console.print(
        Panel(
            f"task_id={record.task_id}\n"
            f"agent={record.agent_name}\n"
            f"route={record.route_kind}\n"
            f"state={record.state}\n"
            f"runs={record.run_count}\n\n"
            f"{result}",
            title="Task",
        )
    )


def _render_review(review: ReviewSnapshot) -> None:
    console.print(
        Panel(
            f"review_id={review.review_id}\n"
            f"task_id={review.task_id or '<root>'}\n"
            f"agent={review.agent_name or ''}\n"
            f"risk={review.risk or ''}\n"
            f"reason={review.reason or ''}",
            title="Human Review Requested",
            style="cyan",
        )
    )
    for idx, action in enumerate(review.actions, start=1):
        header = (
            f"{idx}. {action.tool_name} | "
            f"allowed={','.join(item.value for item in action.allowed_decisions)}"
        )
        if action.description:
            console.print(Panel(action.description, title=header))
        else:
            console.print(Panel(header, title="Action"))
        console.print(Syntax(_json(action.args), "json", word_wrap=True))


def _read_review_decisions(review: ReviewSnapshot) -> list[ReviewDecision] | None:
    decisions: list[ReviewDecision] = []
    for action in review.actions:
        allowed = action.allowed_decisions or [
            ReviewDecisionKind.APPROVE,
            ReviewDecisionKind.REJECT,
        ]
        allowed_values = {item.value for item in allowed}
        while True:
            options = ["a=approve", "r=reject", "s=skip"]
            if "edit" in allowed_values:
                options.append("e=edit")
            console.print("[dim]" + ", ".join(options) + "[/dim]")
            choice = Prompt.ask(
                f"decision for {action.tool_name}",
                default="a",
                show_default=True,
            ).strip().lower()
            if choice in {"a", "approve"} and "approve" in allowed_values:
                decisions.append(
                    ReviewDecision(
                        action_id=action.action_id,
                        decision=ReviewDecisionKind.APPROVE,
                    )
                )
                break
            if choice in {"r", "reject"} and "reject" in allowed_values:
                message = Prompt.ask(
                    "reject reason",
                    default="User rejected the tool call.",
                    show_default=True,
                )
                decisions.append(
                    ReviewDecision(
                        action_id=action.action_id,
                        decision=ReviewDecisionKind.REJECT,
                        message=message,
                    )
                )
                break
            if choice in {"e", "edit"} and "edit" in allowed_values:
                edited_args = _read_json_object("edited args JSON: ")
                if edited_args is None:
                    continue
                decisions.append(
                    ReviewDecision(
                        action_id=action.action_id,
                        decision=ReviewDecisionKind.EDIT,
                        edited_args=edited_args,
                    )
                )
                break
            if choice in {"s", "skip"}:
                return None
            console.print(f"[red]unsupported decision:[/red] {choice}")
    return decisions


async def _resolve_reviews(review_control: ReviewControl) -> None:
    attempted: set[str] = set()
    while True:
        pending = [
            review
            for review in review_control.list_pending_reviews()
            if review.review_id not in attempted
        ]
        if not pending:
            return
        pending.sort(key=lambda item: item.updated_at)
        review = pending[0]
        attempted.add(review.review_id)
        _render_review(review)
        decisions = _read_review_decisions(review)
        if decisions is None:
            return
        has_reject = any(
            decision.decision == ReviewDecisionKind.REJECT
            for decision in decisions
        )
        result = await review_control.submit_decision(
            review.review_id,
            decisions,
        )
        if result.source == "root" and isinstance(result.root_result, RootTurnResult):
            _render_root_result(result.root_result, review_control)
        elif result.task_record is not None:
            _render_task_record(result.task_record)
        if has_reject:
            return


def _render_snapshot(control: AgentControl, review_control: ReviewControl) -> None:
    table = Table(title="Runtime Snapshot")
    table.add_column("task_id", overflow="fold")
    table.add_column("agent")
    table.add_column("route")
    table.add_column("status")
    table.add_column("thread")
    table.add_column("result", overflow="fold")
    for task in control.list_task_records():
        table.add_row(
            task.task_id,
            task.agent_name,
            task.route_kind,
            task.state,
            task.thread_id,
            task.result or task.error or "",
        )
    console.print(table)
    pending_reviews = review_control.list_pending_reviews()
    if pending_reviews:
        console.print(f"[cyan]pending_reviews={len(pending_reviews)}[/cyan]")


def _render_reviews(review_control: ReviewControl) -> None:
    pending_reviews = review_control.list_pending_reviews()
    if not pending_reviews:
        console.print("[dim]No pending reviews.[/dim]")
        return
    for review in pending_reviews:
        _render_review(review)


async def _run_chat(agent_name: str | None, thread_id: str | None) -> None:
    async with bootstrap_application() as runtime:
        selected_agent = agent_name or runtime.main_agent_name
        selected_thread = thread_id or str(uuid.uuid4())
        root_runner = LocalRootRunner(
            get_agent=runtime.get_local_agent,
            resolve_permission_profile=runtime.resolve_root_permission_profile,
            resolve_skill_config=runtime.resolve_root_skill_config,
        )
        review_control = ReviewControl(
            control=runtime.worker_control,
            root_runner=root_runner,
        )
        console.print(
            Panel(
                f"agent={selected_agent}\nthread_id={selected_thread}\n"
                "commands: /exit, /snapshot, /reviews",
                title="Review Control TUI",
            )
        )
        while True:
            user_input = Prompt.ask("you").strip()
            if not user_input:
                continue
            if user_input in {"/exit", "/quit", "exit", "quit"}:
                return
            if user_input == "/snapshot":
                _render_snapshot(runtime.worker_control, review_control)
                continue
            if user_input == "/reviews":
                _render_reviews(review_control)
                continue
            try:
                result = await root_runner.run_user_message(
                    agent_name=selected_agent,
                    thread_id=selected_thread,
                    content=user_input,
                    metadata={},
                )
                _render_root_result(result, review_control)
                await _resolve_reviews(review_control)
            except Exception as exc:  # noqa: BLE001
                console.print(f"[red]{exc.__class__.__name__}:[/red] {exc}")


@app.callback(invoke_without_command=True)
def chat(
    ctx: typer.Context,
    agent_name: str | None = typer.Option(None, "--agent", "-a"),
    thread_id: str | None = typer.Option(None, "--thread-id", "-t"),
) -> None:
    """Run the local review-control TUI MVP."""
    if ctx.invoked_subcommand is not None:
        return
    asyncio.run(_run_chat(agent_name, thread_id))


@app.command("chat")
def chat_command(
    agent_name: str | None = typer.Option(None, "--agent", "-a"),
    thread_id: str | None = typer.Option(None, "--thread-id", "-t"),
) -> None:
    """Run the local review-control TUI MVP."""
    asyncio.run(_run_chat(agent_name, thread_id))


if __name__ == "__main__":
    app()
