from __future__ import annotations

import json
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table

from ruyi_agent.control_plane.contracts import ReviewSnapshot
from ruyi_agent.runtime.events import RuntimeEvent, RuntimeEventKind


class InteractiveRenderer:
    def __init__(self, console: Console | None = None) -> None:
        self.console = console or Console()

    def clear(self) -> None:
        self.console.clear()

    def welcome(
        self,
        *,
        agent_name: str,
        thread_id: str,
        checkpoint_db: str,
    ) -> None:
        self.console.print(
            Panel(
                f"agent={agent_name}\n"
                f"thread_id={thread_id}\n"
                f"checkpoint_db={checkpoint_db}\n\n"
                "Type /help for commands. Ctrl-D exits on empty input.",
                title="Interactive CLI",
            )
        )

    def render_event(self, event: RuntimeEvent) -> None:
        if event.kind == RuntimeEventKind.CONTENT_UPDATE:
            self.render_assistant_message(event.text, namespace=event.namespace)
            return
        if event.kind == RuntimeEventKind.TOOL_CALL_STARTED:
            self.render_tool_call(event)
            return
        if event.kind == RuntimeEventKind.TOOL_RESULT:
            self.render_tool_result(event)
            return
        if event.kind == RuntimeEventKind.ERROR_OCCURRED:
            self.error(event.text)
            return
        if event.text:
            self.console.print(event.text)

    def render_user_message(self, text: str) -> None:
        self.console.print(Panel(text, title="You", style="blue"))

    def render_assistant_message(
        self,
        text: str,
        *,
        namespace: str | None = None,
    ) -> None:
        title = "Assistant"
        if namespace:
            title = f"Assistant | {namespace}"
        self.console.print(Panel(text, title=title, style="white"))

    def render_tool_call(self, event: RuntimeEvent) -> None:
        self.console.print(
            Panel(
                event.text,
                title=self._panel_title("Tool Call", event),
                style="cyan",
            )
        )

    def render_tool_result(self, event: RuntimeEvent) -> None:
        self.console.print(
            Panel(
                event.text,
                title=self._panel_title("Tool Result", event),
                style="green",
            )
        )

    def render_error(self, exc: BaseException) -> None:
        self.error(f"{exc.__class__.__name__}: {exc}")

    def error(self, text: str) -> None:
        self.console.print(f"[red]{text}[/red]")

    def info(self, text: str) -> None:
        self.console.print(f"[dim]{text}[/dim]")

    def render_agents(
        self,
        *,
        agent_names: list[str],
        current_agent: str,
        descriptions: dict[str, str],
    ) -> None:
        table = Table(title="Agents")
        table.add_column("agent")
        table.add_column("current")
        table.add_column("description", overflow="fold")
        for name in agent_names:
            table.add_row(
                name,
                "*" if name == current_agent else "",
                descriptions.get(name, ""),
            )
        self.console.print(table)

    def render_tasks(self, tasks: list[Any]) -> None:
        if not tasks:
            self.info("No tasks.")
            return
        table = Table(title="Tasks")
        table.add_column("task_id", overflow="fold")
        table.add_column("agent")
        table.add_column("route")
        table.add_column("state")
        table.add_column("thread", overflow="fold")
        table.add_column("result", overflow="fold")
        for task in tasks:
            table.add_row(
                task.task_id,
                task.agent_name,
                task.route_kind,
                task.state,
                task.thread_id,
                task.result or task.error or "",
            )
        self.console.print(table)

    def render_reviews(self, reviews: list[ReviewSnapshot]) -> None:
        if not reviews:
            self.info("No pending reviews.")
            return
        for review in reviews:
            self.render_review_prompt(review)

    def render_review_prompt(self, review: ReviewSnapshot) -> None:
        lines = [
            f"review_id={review.review_id}",
            f"source={self._review_source(review)}",
        ]
        if review.agent_name:
            lines.append(f"agent={review.agent_name}")
        if review.task_id:
            lines.append(f"task_id={review.task_id}")
        if review.reason:
            lines.append(f"reason={review.reason}")
        if review.risk:
            lines.append(f"risk={review.risk}")
        action_lines = []
        for idx, action in enumerate(review.actions, start=1):
            allowed = ",".join(item.value for item in action.allowed_decisions)
            suffix = f" [{allowed}]" if allowed else ""
            detail = action.reason or action.risk or action.description
            if detail:
                action_lines.append(f"{idx}. {action.tool_name}{suffix}: {detail}")
            else:
                action_lines.append(f"{idx}. {action.tool_name}{suffix}")
        if action_lines:
            lines.append("")
            lines.extend(action_lines)
        lines.append("")
        lines.append("a=approve, r=reject, s=skip, ?=details")
        if any(
            "edit" in {item.value for item in action.allowed_decisions}
            for action in review.actions
        ):
            lines[-1] += ", e=edit"
        self.console.print(
            Panel(
                "\n".join(lines),
                title="Approval Required",
                style="yellow",
            )
        )

    def render_review_details(self, review: ReviewSnapshot) -> None:
        self.console.print(
            Panel(
                f"review_id={review.review_id}\n"
                f"task_id={review.task_id or '<root>'}\n"
                f"agent={review.agent_name or ''}\n"
                f"risk={review.risk or ''}\n"
                f"reason={review.reason or ''}",
                title="Approval Details",
                style="yellow",
            )
        )
        for idx, action in enumerate(review.actions, start=1):
            allowed = ",".join(item.value for item in action.allowed_decisions)
            title = f"{idx}. {action.tool_name}"
            if allowed:
                title = f"{title} | allowed={allowed}"
            body = action.description or action.reason or action.risk or action.tool_name
            self.console.print(Panel(body, title=title))
            self.console.print(
                Syntax(
                    json.dumps(
                        action.args,
                        ensure_ascii=False,
                        indent=2,
                        sort_keys=True,
                    ),
                    "json",
                    word_wrap=True,
                )
            )

    def render_approval_summary(
        self,
        review: ReviewSnapshot,
        decisions: Any | None,
    ) -> None:
        tool_names = ", ".join(action.tool_name for action in review.actions) or "tool"
        source = self._review_source(review)
        if decisions is None:
            text = f"skipped {tool_names}; review remains pending ({source})"
        else:
            decision_names = ", ".join(
                getattr(decision.decision, "value", str(decision.decision))
                for decision in decisions
            )
            text = f"{decision_names} {tool_names} ({source})"
        self.console.print(Panel(text, title="Approval", style="yellow"))

    def status(
        self,
        *,
        agent_name: str,
        thread_id: str,
        checkpoint_db: str,
        task_count: int,
        pending_review_count: int,
    ) -> None:
        self.console.print(
            Panel(
                f"agent={agent_name}\n"
                f"thread_id={thread_id}\n"
                f"checkpoint_db={checkpoint_db}\n"
                f"tasks={task_count}\n"
                f"pending_reviews={pending_review_count}",
                title="Status",
            )
        )

    def help(self) -> None:
        self.console.print(
            Panel(
                "\n".join(
                    [
                        "/help                 show commands",
                        "/exit, /quit          exit",
                        "/clear                clear the terminal",
                        "/status               show current status",
                        "/agent                list agents",
                        "/agent <name>         switch agent",
                        "/thread               show current thread",
                        "/thread <id>          switch thread",
                        "/new                  create a new thread",
                        "/tasks                list tasks",
                        "/reviews              show and resolve pending reviews",
                        "",
                        "Use // at the start to send a literal slash message.",
                        "Ctrl-J or Alt-Enter inserts a newline. Ctrl-O opens editor.",
                    ]
                ),
                title="Help",
            )
        )

    def _panel_title(self, title: str, event: RuntimeEvent) -> str:
        if event.namespace:
            return f"{title} | {event.namespace}"
        return title

    def _review_source(self, review: ReviewSnapshot) -> str:
        if review.task_id:
            return f"task:{review.task_id[:8]}"
        return "root"
