from __future__ import annotations

from _feishu_adapter_support import (
    ChannelSessionStore,
    FakeFeishuClient,
    FakeGatewayClient,
    FeishuAdapter,
    FeishuEventStore,
    FeishuMention,
    asyncio,
    build_message,
)


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
