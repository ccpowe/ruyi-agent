from __future__ import annotations

from _telegram_adapter_support import (
    ChannelSessionStore,
    FakeGatewayClient,
    FakeTelegramClient,
    GatewayClientError,
    TelegramAdapter,
    TelegramAttachmentDownloadWarning,
    TelegramInboundAttachment,
    asyncio,
    build_message,
)


def test_adapter_sends_inbound_attachment_to_gateway() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
    )
    attachment = TelegramInboundAttachment(
        kind="document",
        filename="report.txt",
        content_type="text/plain",
        content=b"hello",
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "please read",
                attachments=[attachment],
            )
        )
        task_id = next(iter(gateway.tasks))
        gateway.get_sequences[task_id] = [
            gateway.tasks[task_id],
            {
                **gateway.tasks[task_id],
                "status": "completed",
                "last_result": "done",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert len(gateway.created) == 1
    _, content, _, attachments = gateway.created[0]
    assert content == "please read"
    assert attachments == [
        {
            "name": "report.txt",
            "content_type": "text/plain",
            "kind": "document",
            "data_base64": "aGVsbG8=",
        }
    ]


def test_adapter_injects_inbound_attachment_download_warning() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "",
                attachment_warnings=[
                    TelegramAttachmentDownloadWarning(
                        kind="document",
                        filename="report.pdf",
                        error="download failed",
                    )
                ],
            )
        )
        task_id = next(iter(gateway.tasks))
        gateway.get_sequences[task_id] = [
            gateway.tasks[task_id],
            {
                **gateway.tasks[task_id],
                "status": "completed",
                "last_result": "done",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert len(gateway.created) == 1
    _, content, _, attachments = gateway.created[0]
    assert attachments is None
    assert "Telegram 附件下载失败" in content
    assert "report.pdf" in content
    assert "download failed" in content


def test_adapter_creates_new_task_and_sends_final_result() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("hello"))
        task_id = next(iter(gateway.tasks))
        gateway.get_sequences[task_id] = [
            gateway.tasks[task_id],
            {
                **gateway.tasks[task_id],
                "status": "completed",
                "last_result": "done: hello",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created == [
        (
            "main",
            "hello",
            {
                "channel": "telegram",
                "chat_id": "100",
                "user_id": "200",
                "chat_type": "private",
                "channel_session_key": "agent:main:telegram:dm:100",
            },
            None,
        )
    ]
    assert telegram.sent_messages[0]["text"].startswith("已收到，task\\_id\\=task\\-1")
    assert "done: hello" in telegram.sent_messages[1]["text"]
    assert telegram.sent_messages[0]["parse_mode"] == "MarkdownV2"



def test_adapter_continues_existing_task_when_not_running() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.tasks["task-9"] = {
        "task_id": "task-9",
        "agent_name": "main",
        "status": "completed",
        "last_result": "old",
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
    }
    gateway.list_items = [gateway.tasks["task-9"]]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("follow up"))
        gateway.get_sequences["task-9"] = [
            gateway.tasks["task-9"],
            {
                **gateway.tasks["task-9"],
                "status": "completed",
                "last_result": "done: follow up",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.list_calls == [
        {"channel": "telegram", "chat_id": "100", "user_id": "200"}
    ]
    assert gateway.sent == [("task-9", "follow up", None)]
    assert telegram.sent_messages[0]["text"].startswith("已收到，task\\_id\\=task\\-9")
    assert "done: follow up" in telegram.sent_messages[1]["text"]


def test_adapter_legacy_fallback_uses_old_metadata_only() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.tasks["task-9"] = {
        "task_id": "task-9",
        "agent_name": "main",
        "status": "completed",
        "last_result": "old",
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
    }
    gateway.list_items = [gateway.tasks["task-9"]]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "follow up",
                message_thread_id=10,
                reply_to_message_id=20,
            )
        )
        gateway.get_sequences["task-9"] = [
            gateway.tasks["task-9"],
            {
                **gateway.tasks["task-9"],
                "status": "completed",
                "last_result": "done: follow up",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.list_calls == [
        {"channel": "telegram", "chat_id": "100", "user_id": "200"}
    ]
    assert gateway.sent == [("task-9", "follow up", None)]
    assert gateway.created == []


def test_adapter_unbinds_missing_session_task_and_falls_back() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="missing-task",
        chat_id="100",
        user_id="200",
    )
    gateway.get_errors["missing-task"] = GatewayClientError(
        status_code=404,
        code="task_not_found",
        message="missing",
    )
    gateway.tasks["task-9"] = {
        "task_id": "task-9",
        "agent_name": "main",
        "status": "completed",
        "last_result": "old",
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
    }
    gateway.list_items = [gateway.tasks["task-9"]]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("follow up"))
        gateway.get_sequences["task-9"] = [
            gateway.tasks["task-9"],
            {
                **gateway.tasks["task-9"],
                "status": "completed",
                "last_result": "done: follow up",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.sent == [("task-9", "follow up", None)]
    assert store.get_session("agent:main:telegram:dm:100").current_task_id == "task-9"


def test_adapter_continues_session_store_task_without_listing() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-9"] = {
        "task_id": "task-9",
        "agent_name": "main",
        "status": "completed",
        "last_result": "old",
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-9",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("follow up"))
        gateway.get_sequences["task-9"] = [
            gateway.tasks["task-9"],
            {
                **gateway.tasks["task-9"],
                "status": "completed",
                "last_result": "done: follow up",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.list_items == []
    assert gateway.sent == [("task-9", "follow up", None)]
    assert "done: follow up" in telegram.sent_messages[1]["text"]


def test_adapter_restores_watcher_when_existing_task_is_running() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    running_task = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
    }
    gateway.tasks["task-7"] = running_task
    gateway.list_items = [running_task]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
        terminal_review_grace_checks=0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("hello again"))
        assert adapter._has_active_watcher(task_id="task-7", run_count=1)
        gateway.tasks["task-7"] = {
            **running_task,
            "status": "completed",
            "last_result": "done after watcher recovery",
        }
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created == []
    assert gateway.sent == []
    assert "当前任务仍在处理中" in telegram.sent_messages[0]["text"]
    assert "done after watcher recovery" in telegram.sent_messages[1]["text"]


def test_adapter_reports_pending_review_instead_of_sending_input() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
        "pending_review": {
            "review_id": "review-1",
            "action_requests": [
                {
                    "name": "execute",
                    "args": {"command": "python -V"},
                }
            ],
            "review_configs": [
                {
                    "action_name": "execute",
                    "allowed_decisions": ["approve", "reject"],
                }
            ],
        },
    }
    gateway.list_items = [gateway.tasks["task-7"]]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(adapter.handle_message(build_message("hello again")))

    assert gateway.created == []
    assert gateway.sent == []
    assert "任务等待人工审批" in telegram.sent_messages[0]["text"]
    assert "/approve review\\-1" in telegram.sent_messages[0]["text"]


def test_adapter_approves_pending_review_and_watches_task() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
        "pending_review": {
            "review_id": "review-1",
            "action_requests": [{"name": "execute", "args": {}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    gateway.list_items = [gateway.tasks["task-7"]]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/approve review-1"))
        gateway.get_sequences["task-7"] = [
            gateway.tasks["task-7"],
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "done after approval",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.submitted_reviews == [
        {
            "task_id": "task-7",
            "review_id": "review-1",
            "decisions": [{"type": "approve"}],
        }
    ]
    assert "审批已提交" in telegram.sent_messages[0]["text"]
    assert "done after approval" in telegram.sent_messages[1]["text"]


def test_adapter_approves_pending_review_from_session_store() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
        "pending_review": {
            "review_id": "review-1",
            "action_requests": [{"name": "execute", "args": {}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-7",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/approve review-1"))
        gateway.get_sequences["task-7"] = [
            gateway.tasks["task-7"],
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "done after approval",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.submitted_reviews == [
        {
            "task_id": "task-7",
            "review_id": "review-1",
            "decisions": [{"type": "approve"}],
        }
    ]
    assert store.get_session("agent:main:telegram:dm:100").current_task_id == "task-7"
    assert "done after approval" in telegram.sent_messages[1]["text"]


def test_adapter_approves_current_review_with_short_y() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
        "pending_review": {
            "review_id": "review-1",
            "action_requests": [{"name": "execute", "args": {}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-7",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("y"))
        gateway.get_sequences["task-7"] = [
            gateway.tasks["task-7"],
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "done after approval",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.submitted_reviews == [
        {
            "task_id": "task-7",
            "review_id": "review-1",
            "decisions": [{"type": "approve"}],
        }
    ]
    assert "审批已提交" in telegram.sent_messages[0]["text"]


def test_adapter_approves_current_review_without_review_id() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "waiting_for_human",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
        "pending_review": {
            "review_id": "review-1",
            "action_requests": [{"name": "execute", "args": {}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-7",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/approve"))
        gateway.get_sequences["task-7"] = [
            gateway.tasks["task-7"],
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "done after approval",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.submitted_reviews == [
        {
            "task_id": "task-7",
            "review_id": "review-1",
            "decisions": [{"type": "approve"}],
        }
    ]


def test_adapter_reports_pending_review_even_when_status_is_running() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
        "pending_review": {
            "review_id": "review-1",
            "action_requests": [{"name": "execute", "args": {}}],
            "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-7",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.001,
        terminal_review_grace_checks=0,
    )

    asyncio.run(adapter.handle_message(build_message("继续处理")))

    assert "任务等待人工审批" in telegram.sent_messages[0]["text"]
    assert "review\\_id\\=review\\-1" in telegram.sent_messages[0]["text"]


def test_watcher_reports_delayed_mirrored_review_after_terminal_status() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
        terminal_review_grace_checks=2,
    )

    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "running",
        "last_result": None,
        "error": None,
        "run_count": 1,
        "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
        "pending_review": None,
    }

    async def scenario() -> None:
        adapter._ensure_watcher(task_id="task-7", chat_id=100, run_count=1)
        gateway.get_sequences["task-7"] = [
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "root done first",
                "pending_review": None,
            },
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "root done first",
                "pending_review": {
                    "review_id": "review-1",
                    "source_task_id": "child-1",
                    "action_requests": [{"name": "execute", "args": {}}],
                    "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
                },
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert "root done first" in telegram.sent_messages[0]["text"]
    assert "任务等待人工审批" in telegram.sent_messages[1]["text"]
    assert "review\\_id\\=review\\-1" in telegram.sent_messages[1]["text"]


def test_parse_review_command_accepts_group_bot_suffix() -> None:
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=FakeTelegramClient(),
        default_agent_name="main",
    )

    assert adapter._parse_review_command("/approve@my_bot review-1") == {
        "type": "approve",
        "review_id": "review-1",
    }
    assert adapter._parse_review_command("/reject@my_bot review-1 because") == {
        "type": "reject",
        "review_id": "review-1",
        "message": "because",
    }


def test_new_command_forces_new_task() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.list_items = [
        {
            "task_id": "task-7",
            "agent_name": "main",
            "status": "completed",
            "last_result": "old",
            "error": None,
            "run_count": 1,
            "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
        }
    ]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/new fresh task"))
        task_id = next(iter(gateway.tasks))
        gateway.get_sequences[task_id] = [
            gateway.tasks[task_id],
            {
                **gateway.tasks[task_id],
                "status": "completed",
                "last_result": "done: fresh task",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created[0][1] == "fresh task"
    assert gateway.sent == []


def test_new_command_rebinds_existing_session_store_task() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "main",
        "status": "completed",
        "last_result": "old",
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-7",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/new fresh task"))
        new_task_id = "task-1"
        gateway.get_sequences[new_task_id] = [
            gateway.tasks[new_task_id],
            {
                **gateway.tasks[new_task_id],
                "status": "completed",
                "last_result": "done: fresh task",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created[0][1] == "fresh task"
    assert gateway.sent == []
    assert store.get_session("agent:main:telegram:dm:100").current_task_id == "task-1"


def test_agent_command_switches_agent_and_starts_new_session() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/agent research"))
        await adapter.handle_message(build_message("fresh question"))
        gateway.get_sequences["task-1"] = [
            gateway.tasks["task-1"],
            {
                **gateway.tasks["task-1"],
                "status": "completed",
                "last_result": "done",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert store.get_session("telegram:dm:100").agent_name == "research"
    assert gateway.created[0][0] == "research"
    assert gateway.created[0][1] == "fresh question"
    assert (
        gateway.created[0][2]["channel_session_key"]
        == "agent:research:telegram:dm:100"
    )


def test_agent_command_can_switch_and_create_initial_task() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/agent research first prompt"))
        gateway.get_sequences["task-1"] = [
            gateway.tasks["task-1"],
            {
                **gateway.tasks["task-1"],
                "status": "completed",
                "last_result": "done",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert gateway.created[0][0] == "research"
    assert gateway.created[0][1] == "first prompt"
    assert store.get_session("telegram:dm:100").agent_name == "research"
    assert store.get_session("agent:research:telegram:dm:100").current_task_id == "task-1"


def test_agent_command_lists_agent_names_as_inline_code() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.agents = [
        {
            "name": "background_research",
            "kind": "local",
            "public": True,
            "description": "research agent",
            "is_default": False,
        }
    ]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="background_research",
        session_store=ChannelSessionStore(":memory:"),
    )

    asyncio.run(adapter.handle_message(build_message("/agent")))

    assert "`background_research`" in telegram.sent_messages[0]["text"]
    assert "backgroundresearch" not in telegram.sent_messages[0]["text"]


def test_agent_command_accepts_name_when_underscore_was_omitted() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.agents = [
        {
            "name": "background_research",
            "kind": "local",
            "public": True,
            "description": "research agent",
            "is_default": False,
        }
    ]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
    )

    asyncio.run(adapter.handle_message(build_message("/agent backgroundresearch")))

    assert store.get_session("telegram:dm:100").agent_name == "background_research"


def test_resume_lists_recent_telegram_sessions() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.list_items = [
        {
            "task_id": "task-7",
            "agent_name": "main",
            "status": "completed",
            "last_result": "old result",
            "error": None,
            "run_count": 1,
            "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
        }
    ]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
    )

    asyncio.run(adapter.handle_message(build_message("/resume")))

    assert "task\\_id\\=task\\-7" in telegram.sent_messages[0]["text"]
    assert "old result" in telegram.sent_messages[0]["text"]


def test_resume_restores_completed_task_and_continues_on_next_message() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "research",
        "status": "completed",
        "last_result": "old result",
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:research:telegram:dm:100",
        },
    }
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("/resume task-7"))
        await adapter.handle_message(build_message("continue"))
        gateway.get_sequences["task-7"] = [
            gateway.tasks["task-7"],
            {
                **gateway.tasks["task-7"],
                "status": "completed",
                "last_result": "new result",
            },
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert store.get_session("telegram:dm:100").agent_name == "research"
    assert store.get_session("agent:research:telegram:dm:100").current_task_id == "task-7"
    assert gateway.sent == [("task-7", "continue", None)]


def test_resume_rejects_task_from_different_telegram_user() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.tasks["task-7"] = {
        "task_id": "task-7",
        "agent_name": "research",
        "status": "completed",
        "last_result": "old result",
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "999",
            "chat_type": "private",
            "channel_session_key": "agent:research:telegram:dm:100",
        },
    }
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=ChannelSessionStore(":memory:"),
    )

    asyncio.run(adapter.handle_message(build_message("/resume task-7")))

    assert "不能恢复不属于当前 Telegram 会话的 task" in telegram.sent_messages[0]["text"]


def test_group_users_are_routed_to_independent_sessions() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message("first", chat_id=-100, user_id=200, chat_type="group")
        )
        await adapter.handle_message(
            build_message(
                "second",
                update_id=2,
                chat_id=-100,
                user_id=201,
                chat_type="group",
            )
        )
        gateway.get_sequences["task-1"] = [
            gateway.tasks["task-1"],
            {**gateway.tasks["task-1"], "status": "completed", "last_result": "first"},
        ]
        gateway.get_sequences["task-2"] = [
            gateway.tasks["task-2"],
            {**gateway.tasks["task-2"], "status": "completed", "last_result": "second"},
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert len(gateway.created) == 2
    assert (
        store.get_session("agent:main:telegram:group:-100:user:200").current_task_id
        == "task-1"
    )
    assert (
        store.get_session("agent:main:telegram:group:-100:user:201").current_task_id
        == "task-2"
    )


def test_adapter_sends_both_terminal_messages_for_back_to_back_followups() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    gateway.tasks["task-9"] = {
        "task_id": "task-9",
        "agent_name": "main",
        "status": "completed",
        "last_result": "old",
        "error": None,
        "run_count": 1,
        "metadata": {
            "channel": "telegram",
            "chat_id": "100",
            "user_id": "200",
            "chat_type": "private",
            "channel_session_key": "agent:main:telegram:dm:100",
        },
    }
    store.bind_session(
        session_key="agent:main:telegram:dm:100",
        platform="telegram",
        agent_name="main",
        current_task_id="task-9",
        chat_id="100",
        user_id="200",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(build_message("first"))
        gateway.tasks["task-9"] = {
            **gateway.tasks["task-9"],
            "status": "completed",
            "run_count": 2,
            "last_result": "done: first",
        }
        await adapter.handle_message(build_message("second", update_id=2))
        gateway.get_sequences["task-9"] = [
            {
                **gateway.tasks["task-9"],
                "status": "completed",
                "run_count": 3,
                "last_result": "done: second",
            }
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    terminal_messages = [
        item["text"] for item in telegram.sent_messages if "done:" in item["text"]
    ]
    assert len(terminal_messages) == 2
    assert "done: first" in terminal_messages[0]
    assert "done: second" in terminal_messages[1]


def test_supergroup_topics_are_routed_to_independent_sessions() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    store = ChannelSessionStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        session_store=store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        await adapter.handle_message(
            build_message(
                "first",
                chat_id=-100,
                user_id=200,
                chat_type="supergroup",
                message_thread_id=10,
            )
        )
        await adapter.handle_message(
            build_message(
                "second",
                update_id=2,
                chat_id=-100,
                user_id=200,
                chat_type="supergroup",
                message_thread_id=11,
            )
        )
        gateway.get_sequences["task-1"] = [
            gateway.tasks["task-1"],
            {**gateway.tasks["task-1"], "status": "completed", "last_result": "first"},
        ]
        gateway.get_sequences["task-2"] = [
            gateway.tasks["task-2"],
            {**gateway.tasks["task-2"], "status": "completed", "last_result": "second"},
        ]
        await adapter.wait_for_watchers()

    asyncio.run(scenario())

    assert len(gateway.created) == 2
    assert (
        store.get_session(
            "agent:main:telegram:supergroup:-100:thread:10:user:200"
        ).current_task_id
        == "task-1"
    )
    assert (
        store.get_session(
            "agent:main:telegram:supergroup:-100:thread:11:user:200"
        ).current_task_id
        == "task-2"
    )


def test_adapter_rejects_unsupported_chat_type_without_creating_task() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        task_poll_interval=0.0,
    )

    asyncio.run(adapter.handle_message(build_message("hello", chat_type="channel")))

    assert gateway.created == []
    assert gateway.sent == []
    assert "暂不支持 Telegram chat\\_type\\='channel'" in telegram.sent_messages[0]["text"]
