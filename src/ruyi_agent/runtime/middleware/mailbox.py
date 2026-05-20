from __future__ import annotations

from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ContextT, ResponseT
from langchain_core.messages import HumanMessage
from langgraph.config import get_config

from ruyi_agent.runtime.mailbox.service import AgentMailbox, render_mailbox_messages


class MailboxMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Deliver queued inter-agent messages into the current thread before model calls."""

    def __init__(self, mailbox: AgentMailbox) -> None:
        super().__init__()
        self._mailbox = mailbox

    def before_model(self, state: object, runtime: Any) -> dict[str, Any] | None:
        return self._drain_current_thread()

    async def abefore_model(self, state: object, runtime: Any) -> dict[str, Any] | None:
        return self._drain_current_thread()

    def _drain_current_thread(self) -> dict[str, Any] | None:
        try:
            config = get_config()
        except RuntimeError:
            return None
        thread_id = (config.get("configurable") or {}).get("thread_id")
        if not isinstance(thread_id, str) or not thread_id:
            return None
        messages = self._mailbox.drain(thread_id)
        if not messages:
            return None
        return {
            "messages": [
                HumanMessage(
                    content=render_mailbox_messages(messages),
                    additional_kwargs={"source": "agent_mailbox"},
                )
            ]
        }
