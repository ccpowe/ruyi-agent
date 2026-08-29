# ruff: noqa: F401
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from ruyi_agent.channels.gateway_client import GatewayArtifact
from ruyi_agent.channels.feishu.adapter import (
    FeishuAdapter,
    FeishuEventStore,
    FeishuMention,
    FeishuMessage,
    build_feishu_identity_key,
    build_feishu_session_key,
    parse_feishu_message_event,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


class FakeGatewayClient:
    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.created: list[tuple[str, str, dict[str, str]]] = []
        self.sent: list[tuple[str, str]] = []
        self.idempotency_keys: list[str | None] = []
        self.agents: list[dict[str, Any]] = [
            {
                "name": "main",
                "kind": "local",
                "public": True,
                "description": "main agent",
                "is_default": True,
            },
            {
                "name": "research",
                "kind": "local",
                "public": True,
                "description": "research agent",
                "is_default": False,
            },
        ]
        self.submitted_reviews: list[dict[str, Any]] = []
        self.artifacts: dict[str, GatewayArtifact] = {}
        self.task_artifacts: dict[tuple[str, str], GatewayArtifact] = {}
        self.downloaded_artifacts: list[str] = []
        self._counter = 0

    async def list_agents(self) -> list[dict[str, Any]]:
        return self.agents

    async def list_tasks(
        self,
        *,
        agent_name: str | None = None,
        metadata: dict[str, str],
        limit: int = 1,
    ) -> list[dict[str, Any]]:
        items = list(self.tasks.values())
        if agent_name is not None:
            items = [item for item in items if item.get("agent_name") == agent_name]
        if metadata:
            items = [
                item
                for item in items
                if all(
                    str((item.get("metadata") or {}).get(key)) == value
                    for key, value in metadata.items()
                )
            ]
        return items[:limit]

    async def create_task(
        self,
        *,
        agent_name: str,
        content: str,
        metadata: dict[str, str],
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        del attachments
        self._counter += 1
        task_id = f"task-{self._counter}"
        task = {
            "task_id": task_id,
            "agent_name": agent_name,
            "status": "completed",
            "last_result": f"done: {content}",
            "error": None,
            "run_count": 1,
            "metadata": metadata,
        }
        self.created.append((agent_name, content, metadata))
        self.idempotency_keys.append(idempotency_key)
        self.tasks[task_id] = task
        return task

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        del attachments
        task = dict(self.tasks[task_id])
        task["status"] = "completed"
        task["last_result"] = f"done: {content}"
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[task_id] = task
        self.sent.append((task_id, content))
        self.idempotency_keys.append(idempotency_key)
        return task

    async def download_artifact(self, *, path: str) -> Any:
        self.downloaded_artifacts.append(path)
        if path not in self.artifacts:
            raise AssertionError(f"unexpected artifact download: {path}")
        return self.artifacts[path]

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact:
        artifact = self.task_artifacts.get((task_id, artifact_id))
        if artifact is None:
            raise AssertionError(f"unexpected artifact download: {task_id}/{artifact_id}")
        return artifact

    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        return self.tasks[task_id]

    async def submit_review_decision(
        self,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, Any]],
    ) -> dict[str, Any]:
        self.submitted_reviews.append(
            {
                "task_id": task_id,
                "review_id": review_id,
                "decisions": decisions,
            }
        )
        task = dict(self.tasks[task_id])
        task["status"] = "completed"
        task["pending_review"] = None
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[task_id] = task
        return task


class FakeFeishuClient:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []
        self.sent_markdown_messages: list[dict[str, Any]] = []
        self.sent_files: list[dict[str, Any]] = []
        self.added_reactions: list[dict[str, Any]] = []
        self.deleted_reactions: list[dict[str, Any]] = []
        self._reaction_counter = 0

    async def run(self, handler) -> None:
        del handler

    async def send_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
    ) -> None:
        self.sent_messages.append(
            {
                "chat_id": chat_id,
                "text": text,
                "reply_to_message_id": reply_to_message_id,
            }
        )

    async def send_markdown(
        self,
        *,
        chat_id: str,
        markdown: str,
        reply_to_message_id: str | None = None,
    ) -> None:
        self.sent_markdown_messages.append(
            {
                "chat_id": chat_id,
                "markdown": markdown,
                "reply_to_message_id": reply_to_message_id,
            }
        )

    async def send_file(
        self,
        *,
        chat_id: str,
        filename: str,
        content: bytes,
        reply_to_message_id: str | None = None,
    ) -> None:
        self.sent_files.append(
            {
                "chat_id": chat_id,
                "filename": filename,
                "content": content,
                "reply_to_message_id": reply_to_message_id,
            }
        )

    async def add_reaction(
        self,
        *,
        message_id: str,
        emoji_type: str,
    ) -> str:
        self._reaction_counter += 1
        reaction_id = f"reaction-{self._reaction_counter}"
        self.added_reactions.append(
            {
                "message_id": message_id,
                "emoji_type": emoji_type,
                "reaction_id": reaction_id,
            }
        )
        return reaction_id

    async def delete_reaction(
        self,
        *,
        message_id: str,
        reaction_id: str,
    ) -> None:
        self.deleted_reactions.append(
            {
                "message_id": message_id,
                "reaction_id": reaction_id,
            }
        )


def build_message(
    text: str,
    *,
    event_id: str = "event-1",
    message_id: str = "message-1",
    chat_id: str = "chat-1",
    user_id: str = "user-1",
    chat_type: str = "p2p",
    thread_id: str | None = None,
    mentions: list[FeishuMention] | None = None,
) -> FeishuMessage:
    return FeishuMessage(
        event_id=event_id,
        message_id=message_id,
        chat_id=chat_id,
        chat_type=chat_type,
        user_id=user_id,
        text=text,
        sender_open_id=f"open-{user_id}",
        thread_id=thread_id,
        mentions=mentions,
    )
