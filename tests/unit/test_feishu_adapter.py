from __future__ import annotations

import asyncio
import json
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
        self.tasks[task_id] = task
        return task

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        del attachments
        task = dict(self.tasks[task_id])
        task["status"] = "completed"
        task["last_result"] = f"done: {content}"
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[task_id] = task
        self.sent.append((task_id, content))
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


def test_parse_feishu_text_event_extracts_sender_and_mentions() -> None:
    payload = {
        "header": {"event_id": "event-1"},
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": "ou_1",
                    "user_id": "u_1",
                    "union_id": "on_1",
                }
            },
            "message": {
                "message_id": "om_1",
                "chat_id": "oc_1",
                "chat_type": "group",
                "message_type": "text",
                "content": json.dumps({"text": "@_bot hello"}),
                "mentions": [
                    {
                        "key": "@_bot",
                        "name": "Ruyi",
                        "id": {"open_id": "bot-open"},
                    }
                ],
            },
        },
    }

    message = parse_feishu_message_event(payload)

    assert message is not None
    assert message.event_id == "event-1"
    assert message.message_id == "om_1"
    assert message.user_id == "on_1"
    assert message.text == "@_bot hello"
    assert message.mentions == [
        FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")
    ]


def test_build_feishu_session_key_isolates_group_by_user() -> None:
    first = build_message("hello", chat_id="group-1", user_id="u1", chat_type="group")
    second = build_message("hello", chat_id="group-1", user_id="u2", chat_type="group")

    assert (
        build_feishu_session_key(first, agent_name="main")
        == "agent:main:feishu:group:group-1:user:u1"
    )
    assert (
        build_feishu_session_key(second, agent_name="main")
        == "agent:main:feishu:group:group-1:user:u2"
    )
    assert build_feishu_identity_key(first) == "feishu:group:group-1:user:u1"


def test_group_message_without_mention_is_ignored() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
        bot_open_id="bot-open",
    )

    asyncio.run(
        adapter.handle_message(
            build_message("hello", chat_id="group-1", chat_type="group")
        )
    )

    assert gateway.created == []
    assert feishu.sent_messages == []


def test_group_message_with_mention_is_ignored_without_bot_identity() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
    )
    mention = FeishuMention(key="@_other", name="Other", open_id="other-open")

    asyncio.run(
        adapter.handle_message(
            build_message(
                "@_other hello",
                chat_id="group-1",
                chat_type="group",
                mentions=[mention],
            )
        )
    )

    assert gateway.created == []
    assert feishu.sent_messages == []


def test_group_message_with_bot_mention_creates_user_scoped_task() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "@_bot hello",
                chat_id="group-1",
                chat_type="group",
                mentions=[mention],
            )
        )
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created == [
        (
            "main",
            "hello",
            {
                "channel": "feishu",
                "chat_id": "group-1",
                "user_id": "user-1",
                "chat_type": "group",
                "channel_session_key": "agent:main:feishu:group:group-1:user:user-1",
                "sender_open_id": "open-user-1",
            },
        )
    ]
    assert feishu.added_reactions == [
        {
            "message_id": "message-1",
            "emoji_type": "Typing",
            "reaction_id": "reaction-1",
        }
    ]
    assert feishu.deleted_reactions == [
        {"message_id": "message-1", "reaction_id": "reaction-1"}
    ]
    assert "done: hello" in feishu.sent_messages[0]["text"]


def test_ack_mode_message_keeps_legacy_text_ack() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        task_poll_interval=0.0,
        ack_mode="message",
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("hello"))
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert feishu.added_reactions == []
    assert feishu.sent_messages[0]["text"] == "已收到，task_id=task-1"
    assert "done: hello" in feishu.sent_messages[1]["text"]


def test_running_task_uses_reaction_instead_of_busy_message() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    session_store = ChannelSessionStore(":memory:")
    session_store.bind_session(
        session_key="agent:main:feishu:dm:chat-1",
        platform="feishu",
        agent_name="main",
        current_task_id="task-running",
        chat_id="chat-1",
        user_id="user-1",
        thread_id=None,
    )
    gateway.tasks["task-running"] = {
        "task_id": "task-running",
        "agent_name": "main",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 3,
        "metadata": {
            "channel": "feishu",
            "chat_id": "chat-1",
            "user_id": "user-1",
            "chat_type": "p2p",
            "channel_session_key": "agent:main:feishu:dm:chat-1",
        },
    }
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        session_store=session_store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "again",
                event_id="event-running",
                message_id="message-running",
            )
        )
        gateway.tasks["task-running"] = {
            **gateway.tasks["task-running"],
            "status": "completed",
            "last_result": "done: resumed running task",
        }
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert all("当前任务仍在处理中" not in item["text"] for item in feishu.sent_messages)
    assert feishu.added_reactions == [
        {
            "message_id": "message-running",
            "emoji_type": "Typing",
            "reaction_id": "reaction-1",
        }
    ]
    assert feishu.deleted_reactions == [
        {"message_id": "message-running", "reaction_id": "reaction-1"}
    ]
    assert feishu.sent_messages[-1]["text"].startswith("done: resumed running task")


def test_failed_task_swaps_processing_reaction_for_failure_reaction() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    session_store = ChannelSessionStore(":memory:")
    session_store.bind_session(
        session_key="agent:main:feishu:dm:chat-1",
        platform="feishu",
        agent_name="main",
        current_task_id="task-running",
        chat_id="chat-1",
        user_id="user-1",
        thread_id=None,
    )
    gateway.tasks["task-running"] = {
        "task_id": "task-running",
        "agent_name": "main",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 3,
        "metadata": {
            "channel": "feishu",
            "chat_id": "chat-1",
            "user_id": "user-1",
            "chat_type": "p2p",
            "channel_session_key": "agent:main:feishu:dm:chat-1",
        },
    }
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        session_store=session_store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "again",
                event_id="event-running",
                message_id="message-running",
            )
        )
        gateway.tasks["task-running"] = {
            **gateway.tasks["task-running"],
            "status": "failed",
            "error": "boom",
        }
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert feishu.added_reactions == [
        {
            "message_id": "message-running",
            "emoji_type": "Typing",
            "reaction_id": "reaction-1",
        },
        {
            "message_id": "message-running",
            "emoji_type": "CrossMark",
            "reaction_id": "reaction-2",
        },
    ]
    assert feishu.deleted_reactions == [
        {"message_id": "message-running", "reaction_id": "reaction-1"}
    ]
    assert "任务失败：boom" in feishu.sent_messages[-1]["text"]


def test_cancelled_task_clears_processing_reaction_without_failure_reaction() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    session_store = ChannelSessionStore(":memory:")
    session_store.bind_session(
        session_key="agent:main:feishu:dm:chat-1",
        platform="feishu",
        agent_name="main",
        current_task_id="task-running",
        chat_id="chat-1",
        user_id="user-1",
        thread_id=None,
    )
    gateway.tasks["task-running"] = {
        "task_id": "task-running",
        "agent_name": "main",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 3,
        "metadata": {
            "channel": "feishu",
            "chat_id": "chat-1",
            "user_id": "user-1",
            "chat_type": "p2p",
            "channel_session_key": "agent:main:feishu:dm:chat-1",
        },
    }
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        session_store=session_store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "again",
                event_id="event-running",
                message_id="message-running",
            )
        )
        gateway.tasks["task-running"] = {
            **gateway.tasks["task-running"],
            "status": "cancelled",
        }
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert feishu.added_reactions == [
        {
            "message_id": "message-running",
            "emoji_type": "Typing",
            "reaction_id": "reaction-1",
        }
    ]
    assert feishu.deleted_reactions == [
        {"message_id": "message-running", "reaction_id": "reaction-1"}
    ]
    assert "任务已取消。" in feishu.sent_messages[-1]["text"]


def test_strip_bot_mention_does_not_remove_embedded_text() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "literal@_bot @_bot hello",
                chat_id="group-1",
                chat_type="group",
                mentions=[mention],
            )
        )
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created[0][1] == "literal@_bot  hello"


def test_strip_bot_mention_handles_punctuation_boundary() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "@_bot, hello",
                chat_id="group-1",
                chat_type="group",
                mentions=[mention],
            )
        )
        await adapter.handle_message(
            build_message(
                "@_bot：again",
                event_id="event-2",
                message_id="message-2",
                chat_id="group-1",
                chat_type="group",
                mentions=[mention],
            )
        )
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created[0][1] == ", hello"
    assert gateway.sent == [("task-1", "：again")]


def test_group_messages_from_different_users_do_not_share_task() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "@_bot first",
                event_id="event-1",
                message_id="m1",
                chat_id="group-1",
                user_id="u1",
                chat_type="group",
                mentions=[mention],
            )
        )
        await adapter.handle_message(
            build_message(
                "@_bot second",
                event_id="event-2",
                message_id="m2",
                chat_id="group-1",
                user_id="u2",
                chat_type="group",
                mentions=[mention],
            )
        )
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert [item[1] for item in gateway.created] == ["first", "second"]
    assert gateway.created[0][2]["channel_session_key"].endswith(":user:u1")
    assert gateway.created[1][2]["channel_session_key"].endswith(":user:u2")


def test_event_store_deduplicates_repeated_event_id() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    event_store = FeishuEventStore(":memory:")
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        event_store=event_store,
        task_poll_interval=0.0,
    )
    message = build_message("hello", event_id="same-event")

    async def scenario() -> None:
        await adapter.handle_message(message)
        await adapter.handle_message(message)
        await adapter.wait_for_watchers()

    try:
        asyncio.run(scenario())
    finally:
        event_store.close()

    assert len(gateway.created) == 1


def test_event_store_allows_unprocessed_claim_after_lease_timeout(tmp_path) -> None:
    db_path = tmp_path / "feishu_events.sqlite3"
    message = build_message("hello", event_id="event-crash")
    first_store = FeishuEventStore(str(db_path), claim_timeout_seconds=0)
    try:
        assert first_store.claim_message(message)
    finally:
        first_store.close()

    second_store = FeishuEventStore(str(db_path), claim_timeout_seconds=0)
    try:
        assert second_store.claim_message(message)
        second_store.mark_processed(message)
        assert not second_store.claim_message(message)
    finally:
        second_store.close()


def test_session_recovery_uses_thread_scoped_session_key() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")
    first_adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )

    async def create_two_threads() -> None:
        await first_adapter.handle_message(
            build_message(
                "@_bot first",
                event_id="event-1",
                message_id="m1",
                chat_id="group-1",
                user_id="u1",
                chat_type="group",
                thread_id="thread-1",
                mentions=[mention],
            )
        )
        await first_adapter.handle_message(
            build_message(
                "@_bot second",
                event_id="event-2",
                message_id="m2",
                chat_id="group-1",
                user_id="u1",
                chat_type="group",
                thread_id="thread-2",
                mentions=[mention],
            )
        )
        await first_adapter.wait_for_watchers()

    asyncio.run(create_two_threads())
    recovered_adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
        event_store=FeishuEventStore(":memory:"),
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )

    async def recover_thread_one() -> None:
        await recovered_adapter.handle_message(
            build_message(
                "@_bot again",
                event_id="event-3",
                message_id="m3",
                chat_id="group-1",
                user_id="u1",
                chat_type="group",
                thread_id="thread-1",
                mentions=[mention],
            )
        )
        await recovered_adapter.wait_for_watchers()

    asyncio.run(recover_thread_one())

    assert gateway.sent == [("task-1", "again")]


def test_review_command_without_session_does_not_guess_other_thread_task() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")
    gateway.tasks["task-other"] = {
        "task_id": "task-other",
        "agent_name": "main",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "feishu",
            "chat_id": "group-1",
            "user_id": "u1",
            "chat_type": "group",
            "channel_session_key": (
                "agent:main:feishu:group:group-1:thread:thread-2:user:u1"
            ),
            "message_thread_id": "thread-2",
        },
        "pending_review": {
            "review_id": "review-other",
            "action_requests": [{"name": "execute", "args": {"command": "date"}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
        event_store=FeishuEventStore(":memory:"),
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )

    asyncio.run(
        adapter.handle_message(
            build_message(
                "@_bot y",
                event_id="event-review",
                message_id="m-review",
                chat_id="group-1",
                user_id="u1",
                chat_type="group",
                thread_id="thread-1",
                mentions=[mention],
            )
        )
    )

    assert gateway.submitted_reviews == []
    assert feishu.sent_messages[-1]["text"] == "没有可审批的任务。"


def test_review_command_without_session_finds_non_default_agent_by_session_key() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    mention = FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")
    gateway.tasks["task-review"] = {
        "task_id": "task-review",
        "agent_name": "research",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "feishu",
            "chat_id": "group-1",
            "user_id": "u1",
            "chat_type": "group",
            "channel_session_key": (
                "agent:research:feishu:group:group-1:thread:thread-1:user:u1"
            ),
            "message_thread_id": "thread-1",
        },
        "pending_review": {
            "review_id": "review-research",
            "action_requests": [{"name": "execute", "args": {"command": "date"}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    session_store = ChannelSessionStore(":memory:")
    session_store.bind_session(
        session_key="feishu:group:group-1:thread:thread-1:user:u1",
        platform="feishu",
        agent_name="research",
        current_task_id="",
        chat_id="group-1",
        user_id="u1",
        thread_id="thread-1",
    )
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        session_store=session_store,
        event_store=FeishuEventStore(":memory:"),
        require_mention=True,
        bot_open_id="bot-open",
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "@_bot y",
                event_id="event-review-research",
                message_id="m-review-research",
                chat_id="group-1",
                user_id="u1",
                chat_type="group",
                thread_id="thread-1",
                mentions=[mention],
            )
        )
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.submitted_reviews == [
        {
            "task_id": "task-review",
            "review_id": "review-research",
            "decisions": [{"type": "approve"}],
        }
    ]
    assert feishu.added_reactions[:2] == [
        {
            "message_id": "m-review-research",
            "emoji_type": "CheckMark",
            "reaction_id": "reaction-1",
        },
        {
            "message_id": "m-review-research",
            "emoji_type": "Typing",
            "reaction_id": "reaction-2",
        },
    ]
    assert feishu.deleted_reactions == [
        {"message_id": "m-review-research", "reaction_id": "reaction-2"}
    ]
    assert all("审批已提交" not in item["text"] for item in feishu.sent_messages)


def test_send_message_renders_markdown_with_interactive_card() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
    )

    asyncio.run(
        adapter._send_message(
            chat_id="chat-1",
            text="## Summary\n\n- **done**\n- [link](https://example.com)",
            reply_to_message_id="message-1",
        )
    )

    assert feishu.sent_messages == []
    assert feishu.sent_markdown_messages == [
        {
            "chat_id": "chat-1",
            "markdown": "## Summary\n\n- **done**\n- [link](https://example.com)",
            "reply_to_message_id": "message-1",
        }
    ]


def test_send_message_falls_back_to_text_when_markdown_send_fails() -> None:
    class FailingMarkdownFeishuClient(FakeFeishuClient):
        async def send_markdown(
            self,
            *,
            chat_id: str,
            markdown: str,
            reply_to_message_id: str | None = None,
        ) -> None:
            del chat_id, markdown, reply_to_message_id
            raise RuntimeError("bad card")

    gateway = FakeGatewayClient()
    feishu = FailingMarkdownFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
    )

    asyncio.run(adapter._send_message(chat_id="chat-1", text="## Summary"))

    assert feishu.sent_messages == [
        {
            "chat_id": "chat-1",
            "text": "## Summary",
            "reply_to_message_id": None,
        }
    ]


def test_send_message_does_not_parse_media_reference(tmp_path: Path) -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    doc_path = tmp_path / "report.html"
    doc_path.write_bytes(b"<html>report</html>")
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(
        adapter._send_message(
            chat_id="chat-1",
            text=f"文件如下：\nMEDIA:{doc_path}",
            reply_to_message_id="message-1",
        )
    )

    assert feishu.sent_messages == [
        {
            "chat_id": "chat-1",
            "text": f"文件如下：\nMEDIA:{doc_path}",
            "reply_to_message_id": "message-1",
        }
    ]
    assert feishu.sent_files == []


def test_adapter_sends_published_artifacts_on_terminal_task() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    task = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "completed",
        "last_result": "done",
        "error": None,
        "run_count": 2,
        "metadata": {},
        "artifacts": [
            {
                "artifact_id": "art_1",
                "path": "/workspace/out/report.html",
                "name": "report.html",
                "caption": "Report",
                "content_type": "text/html",
                "size": 13,
                "run_count": 2,
            }
        ],
    }
    gateway.task_artifacts[("task-7", "art_1")] = GatewayArtifact(
        kind="file",
        filename="report.html",
        content_type="text/html",
        content=b"<html></html>",
    )
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        ack_mode="off",
    )

    asyncio.run(adapter._send_terminal_if_needed(chat_id="chat-1", task=task))

    assert feishu.sent_messages[0]["text"] == "done\n\ntask_id=task-7"
    assert feishu.sent_files == [
        {
            "chat_id": "chat-1",
            "filename": "report.html",
            "content": b"<html></html>",
            "reply_to_message_id": None,
        }
    ]

