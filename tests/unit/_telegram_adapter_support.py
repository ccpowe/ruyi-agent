# ruff: noqa: F401
from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

import ruyi_agent.channels.telegram.adapter as telegram_adapter_module
from ruyi_agent.channels.gateway_client import GatewayClientError
from ruyi_agent.channels.telegram.adapter import (
    TelegramAdapter,
    TelegramAttachmentDownloadWarning,
    TelegramFallbackTransport,
    TelegramFallbackResolver,
    TelegramInboundAttachment,
    TelegramMessage,
    TelegramUpdateStore,
    UnsupportedTelegramChatTypeError,
    _looks_like_network_error,
    _format_telegram_markdown_v2,
    _split_telegram_message,
    build_telegram_session_key,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


class FakeGatewayClient:
    def __init__(self) -> None:
        self.tasks: dict[str, dict[str, Any]] = {}
        self.list_items: list[dict[str, Any]] = []
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
        self.created: list[tuple[str, str, dict[str, str], list[dict[str, str]] | None]] = []
        self.sent: list[tuple[str, str, list[dict[str, str]] | None]] = []
        self.idempotency_keys: list[str | None] = []
        self.submitted_reviews: list[dict[str, Any]] = []
        self.get_sequences: dict[str, list[dict[str, Any]]] = {}
        self.get_errors: dict[str, GatewayClientError] = {}
        self.artifacts: dict[str, TelegramInboundAttachment] = {}
        self.task_artifacts: dict[tuple[str, str], TelegramInboundAttachment] = {}
        self.list_calls: list[dict[str, str]] = []
        self.create_attempts = 0
        self.create_failures = 0
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
        self.list_calls.append(metadata)
        items = self.list_items
        if agent_name is not None:
            items = [item for item in items if item.get("agent_name") == agent_name]
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
        self.create_attempts += 1
        if self.create_failures > 0:
            self.create_failures -= 1
            raise RuntimeError("create failed")
        self._counter += 1
        task_id = f"task-{self._counter}"
        task = {
            "task_id": task_id,
            "agent_name": agent_name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "metadata": metadata,
        }
        self.created.append((agent_name, content, metadata, attachments))
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
        task = dict(self.tasks[task_id])
        task["status"] = "running"
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[task_id] = task
        self.sent.append((task_id, content, attachments))
        self.idempotency_keys.append(idempotency_key)
        return task

    async def download_artifact(self, *, path: str) -> TelegramInboundAttachment:
        artifact = self.artifacts.get(path)
        if artifact is None:
            raise GatewayClientError(
                status_code=404,
                code="artifact_not_found",
                message=f"missing artifact: {path}",
            )
        return artifact

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> TelegramInboundAttachment:
        artifact = self.task_artifacts.get((task_id, artifact_id))
        if artifact is None:
            raise GatewayClientError(
                status_code=404,
                code="artifact_not_found",
                message=f"missing artifact: {task_id}/{artifact_id}",
            )
        return artifact

    async def get_task(self, *, task_id: str) -> dict[str, Any]:
        error = self.get_errors.get(task_id)
        if error is not None:
            raise error
        sequence = self.get_sequences.get(task_id)
        if sequence:
            if len(sequence) > 1:
                task = sequence.pop(0)
            else:
                task = sequence[0]
            self.tasks[task_id] = task
            return task
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
        task["status"] = "running"
        task["pending_review"] = None
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[task_id] = task
        return task


class FakeTelegramClient:
    def __init__(self) -> None:
        self.sent_messages: list[dict[str, Any]] = []
        self.sent_photos: list[dict[str, Any]] = []
        self.sent_documents: list[dict[str, Any]] = []
        self.fail_markdown_once = False
        self.fail_all_messages = False
        self.fail_documents = False
        self.updates: list[TelegramMessage] = []
        self.get_updates_calls: list[int | None] = []

    async def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int,
    ) -> list[TelegramMessage]:
        self.get_updates_calls.append(offset)
        return [
            update
            for update in self.updates
            if offset is None or update.update_id >= offset
        ]

    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None:
        if self.fail_all_messages:
            raise RuntimeError("send failed")
        if self.fail_markdown_once and parse_mode == "MarkdownV2":
            self.fail_markdown_once = False
            raise RuntimeError("markdown parse failed")
        self.sent_messages.append(
            {
                "chat_id": chat_id,
                "text": text,
                "reply_to_message_id": reply_to_message_id,
                "parse_mode": parse_mode,
            }
        )

    async def send_photo(
        self,
        *,
        chat_id: int,
        filename: str,
        content: bytes,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None:
        self.sent_photos.append(
            {
                "chat_id": chat_id,
                "filename": filename,
                "content": content,
                "caption": caption,
                "reply_to_message_id": reply_to_message_id,
                "parse_mode": parse_mode,
            }
        )

    async def send_document(
        self,
        *,
        chat_id: int,
        filename: str,
        content: bytes,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None:
        if self.fail_documents:
            raise RuntimeError("document send failed")
        self.sent_documents.append(
            {
                "chat_id": chat_id,
                "filename": filename,
                "content": content,
                "caption": caption,
                "reply_to_message_id": reply_to_message_id,
                "parse_mode": parse_mode,
            }
        )


class FakeMermaidRenderer:
    async def render_png(self, source: str) -> bytes:
        return f"png:{source}".encode("utf-8")


class FakeFallbackResolver:
    def __init__(self) -> None:
        self.successes: list[str] = []
        self.failures: list[str] = []

    async def get_fallback_ips(self) -> list[str]:
        return ["149.154.167.220"]

    def mark_success(self, ip: str) -> None:
        self.successes.append(ip)

    def mark_failure(self, ip: str) -> None:
        self.failures.append(ip)


class FailsThenRecordsAsyncTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        assert isinstance(request.stream, httpx.AsyncByteStream)
        self.requests.append(request)
        if len(self.requests) == 1:
            raise httpx.ConnectError("temporary failure in name resolution", request=request)
        body = await request.aread()
        assert body == b'{"ok":true}'
        return httpx.Response(200, json={"ok": True}, request=request)


def build_message(
    text: str,
    *,
    update_id: int = 1,
    chat_id: int = 100,
    user_id: int = 200,
    chat_type: str = "private",
    message_thread_id: int | None = None,
    reply_to_message_id: int | None = None,
    attachments: list[TelegramInboundAttachment] | None = None,
    attachment_warnings: list[TelegramAttachmentDownloadWarning] | None = None,
) -> TelegramMessage:
    return TelegramMessage(
        update_id=update_id,
        chat_id=chat_id,
        user_id=user_id,
        text=text,
        message_id=300,
        chat_type=chat_type,
        message_thread_id=message_thread_id,
        reply_to_message_id=reply_to_message_id,
        attachments=attachments,
        attachment_warnings=attachment_warnings,
    )
