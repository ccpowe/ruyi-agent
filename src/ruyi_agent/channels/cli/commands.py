from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ruyi_agent.channels.cli.renderer import InteractiveRenderer
from ruyi_agent.control_plane.reviews import ReviewControl


COMMAND_NAMES = [
    "/agent",
    "/clear",
    "/exit",
    "/help",
    "/new",
    "/quit",
    "/reviews",
    "/status",
    "/tasks",
    "/thread",
]


@dataclass(slots=True)
class CliState:
    agent_name: str
    thread_id: str


class CommandResult:
    def __init__(self, *, should_exit: bool = False) -> None:
        self.should_exit = should_exit


class SlashCommandHandler:
    def __init__(
        self,
        *,
        runtime: Any,
        state: CliState,
        renderer: InteractiveRenderer,
        review_control: ReviewControl,
        resolve_reviews: Any,
    ) -> None:
        self._runtime = runtime
        self._state = state
        self._renderer = renderer
        self._review_control = review_control
        self._resolve_reviews = resolve_reviews

    async def execute(self, raw: str) -> CommandResult:
        command, _, arg = raw.strip().partition(" ")
        arg = arg.strip()
        if command in {"/exit", "/quit"}:
            return CommandResult(should_exit=True)
        if command == "/help":
            self._renderer.help()
            return CommandResult()
        if command == "/clear":
            self._renderer.clear()
            return CommandResult()
        if command == "/new":
            self._state.thread_id = str(uuid4())
            self._renderer.info(f"Switched to new thread: {self._state.thread_id}")
            return CommandResult()
        if command == "/thread":
            if arg:
                self._state.thread_id = arg
                self._renderer.info(f"Switched thread: {self._state.thread_id}")
            else:
                self._renderer.info(f"Current thread: {self._state.thread_id}")
            return CommandResult()
        if command == "/agent":
            await self._agent(arg)
            return CommandResult()
        if command == "/tasks":
            self._renderer.render_tasks(self._runtime.worker_control.list_task_records())
            return CommandResult()
        if command == "/reviews":
            reviews = self._review_control.list_pending_reviews()
            if not reviews:
                self._renderer.render_reviews([])
                return CommandResult()
            await self._resolve_reviews()
            return CommandResult()
        if command == "/status":
            self._renderer.status(
                agent_name=self._state.agent_name,
                thread_id=self._state.thread_id,
                checkpoint_db=self._runtime.checkpoint_db,
                task_count=len(self._runtime.worker_control.list_task_records()),
                pending_review_count=len(self._review_control.list_pending_reviews()),
            )
            return CommandResult()
        self._renderer.error(f"Unknown command: {command}. Type /help.")
        return CommandResult()

    async def _agent(self, arg: str) -> None:
        descriptions = {
            name: str(self._runtime.agent_configs[name].get("description", ""))
            for name in self._runtime.list_local_agent_names()
        }
        if not arg:
            self._renderer.render_agents(
                agent_names=self._runtime.list_local_agent_names(),
                current_agent=self._state.agent_name,
                descriptions=descriptions,
            )
            return
        try:
            self._runtime.get_local_agent(arg)
        except ValueError as exc:
            self._renderer.render_error(exc)
            return
        self._state.agent_name = arg
        self._renderer.info(f"Switched agent: {arg}")
