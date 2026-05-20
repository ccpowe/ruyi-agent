from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ContextT, ResponseT
from langgraph.config import get_config


class TaskHydrationMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Load delegated task records owned by the currently active agent thread."""

    def __init__(self, load_tasks_for_thread: Callable[[str], None]) -> None:
        super().__init__()
        self._load_tasks_for_thread = load_tasks_for_thread

    def before_agent(self, state: object, runtime: Any) -> dict[str, Any] | None:
        self._load_current_thread()
        return None

    async def abefore_agent(
        self,
        state: object,
        runtime: Any,
    ) -> dict[str, Any] | None:
        self._load_current_thread()
        return None

    def _load_current_thread(self) -> None:
        try:
            config = get_config()
        except RuntimeError:
            return
        thread_id = (config.get("configurable") or {}).get("thread_id")
        if not isinstance(thread_id, str) or not thread_id:
            return
        self._load_tasks_for_thread(thread_id)
