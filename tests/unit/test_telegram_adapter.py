from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx

from ruyi_agent.channels.telegram.adapter import (
    GatewayClientError,
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
        self.submitted_reviews: list[dict[str, Any]] = []
        self.get_sequences: dict[str, list[dict[str, Any]]] = {}
        self.get_errors: dict[str, GatewayClientError] = {}
        self.artifacts: dict[str, TelegramInboundAttachment] = {}
        self.list_calls: list[dict[str, str]] = []
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
    ) -> dict[str, Any]:
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
        self.tasks[task_id] = task
        return task

    async def send_input(
        self,
        *,
        task_id: str,
        content: str,
        attachments: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        task = dict(self.tasks[task_id])
        task["status"] = "running"
        task["run_count"] = int(task["run_count"]) + 1
        self.tasks[task_id] = task
        self.sent.append((task_id, content, attachments))
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

    async def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int,
    ) -> list[TelegramMessage]:
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


def test_build_telegram_session_key_uses_dm_chat_id() -> None:
    message = build_message("hello", chat_id=100, user_id=200)

    assert (
        build_telegram_session_key(message, agent_name="main")
        == "agent:main:telegram:dm:100"
    )


def test_telegram_fallback_resolver_prefers_configured_and_sticky_ips() -> None:
    resolver = TelegramFallbackResolver(fallback_ips=["1.1.1.1"])

    async def scenario() -> None:
        first = await resolver.get_fallback_ips()
        resolver.mark_success("2.2.2.2")
        second = await resolver.get_fallback_ips()
        assert first[0] == "1.1.1.1"
        assert second[:2] == ["2.2.2.2", "1.1.1.1"]

    asyncio.run(scenario())


def test_looks_like_network_error_detects_dns_failure_text() -> None:
    assert _looks_like_network_error(
        RuntimeError("[Errno -3] Temporary failure in name resolution")
    )


def test_build_telegram_session_key_isolates_group_by_user() -> None:
    first = build_message("hello", chat_id=-100, user_id=200, chat_type="group")
    second = build_message("hello", chat_id=-100, user_id=201, chat_type="group")

    assert (
        build_telegram_session_key(first, agent_name="main")
        == "agent:main:telegram:group:-100:user:200"
    )
    assert (
        build_telegram_session_key(second, agent_name="main")
        == "agent:main:telegram:group:-100:user:201"
    )


def test_build_telegram_session_key_isolates_supergroup_topic_by_thread() -> None:
    first = build_message(
        "hello",
        chat_id=-100,
        user_id=200,
        chat_type="supergroup",
        message_thread_id=10,
    )
    second = build_message(
        "hello",
        chat_id=-100,
        user_id=200,
        chat_type="supergroup",
        message_thread_id=11,
    )

    assert (
        build_telegram_session_key(first, agent_name="main")
        == "agent:main:telegram:supergroup:-100:thread:10:user:200"
    )
    assert (
        build_telegram_session_key(second, agent_name="main")
        == "agent:main:telegram:supergroup:-100:thread:11:user:200"
    )


def test_build_telegram_session_key_rejects_channel_chat_type() -> None:
    message = build_message("hello", chat_type="channel")

    try:
        build_telegram_session_key(message, agent_name="main")
    except UnsupportedTelegramChatTypeError:
        return
    raise AssertionError("Expected UnsupportedTelegramChatTypeError")


def test_fallback_transport_rebuilds_async_stream_for_fallback_request() -> None:
    base_transport = FailsThenRecordsAsyncTransport()
    resolver = FakeFallbackResolver()
    transport = TelegramFallbackTransport(
        resolver=resolver,  # type: ignore[arg-type]
        base_transport=base_transport,
    )
    request = httpx.Request(
        "POST",
        "https://api.telegram.org/bot-token/getUpdates",
        content=b'{"ok":true}',
    )

    response = asyncio.run(transport.handle_async_request(request))

    assert response.status_code == 200
    assert len(base_transport.requests) == 2
    assert base_transport.requests[1].url.host == "149.154.167.220"
    assert base_transport.requests[1].headers["Host"] == "api.telegram.org"
    assert resolver.successes == ["149.154.167.220"]


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


def test_poll_once_deduplicates_repeated_update_id() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    telegram.updates = [build_message("hello", update_id=10)]
    update_store = TelegramUpdateStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        offset = await adapter.poll_once(offset=None)
        assert offset == 11
        offset = await adapter.poll_once(offset=10)
        assert offset == 11

    asyncio.run(scenario())

    assert len(gateway.created) == 1
    assert gateway.sent == []
    update_store.close()


def test_poll_once_advances_offset_when_reply_send_fails() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    telegram.fail_all_messages = True
    telegram.updates = [build_message("hello", update_id=10)]
    update_store = TelegramUpdateStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
        task_poll_interval=0.0,
    )

    offset = asyncio.run(adapter.poll_once(offset=None))

    assert offset == 11
    assert len(gateway.created) == 1
    update_store.close()


def test_format_telegram_markdown_v2_converts_common_markdown() -> None:
    text = (
        "# Title\n"
        "**bold** and *italic* and ~~strike~~\n"
        "[link](https://example.com/test?a=1)\n"
        "> quote\n"
        "```python\nprint('hi')\n```\n"
    )

    formatted = _format_telegram_markdown_v2(text)

    assert "*Title*" in formatted
    assert "*bold*" in formatted
    assert "_italic_" in formatted
    assert "~strike~" in formatted
    assert "[link](https://example.com/test?a=1)" in formatted
    assert "> quote" in formatted
    assert "```python" in formatted


def test_format_telegram_markdown_v2_rewrites_pipe_tables() -> None:
    formatted = _format_telegram_markdown_v2(
        "| Name | Value |\n| --- | --- |\n| Foo | Bar |\n"
    )

    assert "*Foo*" in formatted
    assert "\\- Value: Bar" in formatted


def test_format_telegram_markdown_v2_escapes_snake_case_plain_text() -> None:
    formatted = _format_telegram_markdown_v2("background_research and main_agent")

    assert "background\\_research" in formatted
    assert "main\\_agent" in formatted


def test_format_telegram_markdown_v2_preserves_snake_case_inline_code() -> None:
    formatted = _format_telegram_markdown_v2("Use `/agent background_research`")

    assert "`/agent background_research`" in formatted


def test_split_telegram_message_chunks_long_text() -> None:
    chunks = _split_telegram_message("a" * 5000, limit=4096)

    assert len(chunks) == 2
    assert sum(len(chunk) for chunk in chunks) == 5000


def test_adapter_falls_back_to_plain_text_when_markdown_send_fails() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    telegram.fail_markdown_once = True
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(
        adapter._send_message(
            chat_id=100,
            text="**bold**",
        )
    )

    assert telegram.sent_messages[0]["parse_mode"] is None
    assert telegram.sent_messages[0]["text"] == "bold"


def test_adapter_plain_text_fallback_removes_mdv2_escapes() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    telegram.fail_markdown_once = True
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(
        adapter._send_message(
            chat_id=100,
            text="**今日新闻**\n---\n- A+B = C",
        )
    )

    assert telegram.sent_messages[0]["parse_mode"] is None
    assert "\\-" not in telegram.sent_messages[0]["text"]
    assert "今日新闻" in telegram.sent_messages[0]["text"]
    assert "- A+B = C" in telegram.sent_messages[0]["text"]


def test_adapter_extracts_mermaid_block_as_photo_attachment() -> None:
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        mermaid_renderer=FakeMermaidRenderer(),
    )

    asyncio.run(
        adapter._send_message(
            chat_id=100,
            text="before\n```mermaid\ngraph TD\nA-->B\n```\nafter",
        )
    )

    assert "before" in telegram.sent_messages[0]["text"]
    assert "Mermaid diagram 1" in telegram.sent_messages[0]["text"]
    assert "after" in telegram.sent_messages[0]["text"]
    assert len(telegram.sent_photos) == 1
    assert telegram.sent_photos[0]["filename"] == "mermaid_1.png"
    assert telegram.sent_photos[0]["content"] == b"png:graph TD\nA-->B"


def test_help_command_lists_telegram_commands() -> None:
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(adapter.handle_message(build_message("/help")))

    text = telegram.sent_messages[0]["text"]
    assert "`/help`" in text
    assert "`/agent <agent_name>`" in text
    assert "`/resume <task_id>`" in text
    assert "`/approve <review_id>`" in text


def test_adapter_keeps_tables_inline_instead_of_csv_attachment() -> None:
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(
        adapter._send_message(
            chat_id=100,
            text="summary\n| Name | Value |\n| --- | --- |\n| Foo | Bar |\n",
        )
    )

    assert "summary" in telegram.sent_messages[0]["text"]
    assert "*Foo*" in telegram.sent_messages[0]["text"]
    assert "\\- Value: Bar" in telegram.sent_messages[0]["text"]
    assert telegram.sent_documents == []


def test_adapter_sends_media_image_as_photo(tmp_path: Path) -> None:
    image_path = tmp_path / "chart.png"
    image_path.write_bytes(b"png-bytes")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(
        adapter._send_message(
            chat_id=100,
            text=f"see this\nMEDIA:{image_path}\ndone",
        )
    )

    assert "Attachment: chart\\.png" in telegram.sent_messages[0]["text"]
    assert len(telegram.sent_photos) == 1
    assert telegram.sent_photos[0]["filename"] == "chart.png"
    assert telegram.sent_photos[0]["content"] == b"png-bytes"
    assert telegram.sent_documents == []


def test_adapter_sends_media_document(tmp_path: Path) -> None:
    doc_path = tmp_path / "slides.pptx"
    doc_path.write_bytes(b"pptx-bytes")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{doc_path}"))

    assert len(telegram.sent_documents) == 1
    assert telegram.sent_documents[0]["filename"] == "slides.pptx"
    assert telegram.sent_documents[0]["content"] == b"pptx-bytes"
    assert telegram.sent_documents[0]["parse_mode"] is None


def test_adapter_downloads_media_document_from_gateway_when_not_local(tmp_path: Path) -> None:
    gateway = FakeGatewayClient()
    gateway.artifacts["/home/daytona/snake.html"] = TelegramInboundAttachment(
        kind="file",
        filename="snake.html",
        content_type="text/html",
        content=b"artifact",
    )
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text="MEDIA:/home/daytona/snake.html"))

    assert len(telegram.sent_documents) == 1
    assert telegram.sent_documents[0]["filename"] == "snake.html"
    assert telegram.sent_documents[0]["content"] == b"artifact"
    assert "missing file" not in telegram.sent_messages[0]["text"]


def test_adapter_sends_backticked_media_reference(tmp_path: Path) -> None:
    doc_path = tmp_path / "README.md"
    doc_path.write_bytes(b"readme")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"`MEDIA:{doc_path}`"))

    assert len(telegram.sent_documents) == 1
    assert telegram.sent_documents[0]["filename"] == "README.md"
    assert telegram.sent_messages[0]["text"] == "\\[Attachment: README\\.md\\]"


def test_adapter_ignores_inline_media_reference(tmp_path: Path) -> None:
    doc_path = tmp_path / "README.md"
    doc_path.write_bytes(b"readme")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"see MEDIA:{doc_path}"))

    assert telegram.sent_documents == []
    assert "MEDIA:" in telegram.sent_messages[0]["text"]


def test_adapter_ignores_media_reference_inside_code_block(tmp_path: Path) -> None:
    doc_path = tmp_path / "README.md"
    doc_path.write_bytes(b"readme")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(
        adapter._send_message(
            chat_id=100,
            text=f"```\nMEDIA:{doc_path}\n```",
        )
    )

    assert telegram.sent_documents == []
    assert "MEDIA:" in telegram.sent_messages[0]["text"]


def test_adapter_reports_document_send_failure(tmp_path: Path) -> None:
    doc_path = tmp_path / "README.md"
    doc_path.write_bytes(b"readme")
    telegram = FakeTelegramClient()
    telegram.fail_documents = True
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{doc_path}"))

    assert telegram.sent_documents == []
    assert len(telegram.sent_messages) == 2
    assert telegram.sent_messages[1]["parse_mode"] is None
    assert "文件发送失败：README.md" in telegram.sent_messages[1]["text"]
    assert "document send failed" in telegram.sent_messages[1]["text"]


def test_adapter_reports_missing_media_file(tmp_path: Path) -> None:
    missing_path = tmp_path / "missing.csv"
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{missing_path}"))

    assert "文件不存在或不可访问" in telegram.sent_messages[0]["text"]
    assert "missing\\.csv" in telegram.sent_messages[0]["text"]
    assert telegram.sent_photos == []
    assert telegram.sent_documents == []


def test_adapter_rejects_media_outside_media_root(tmp_path: Path) -> None:
    media_root = tmp_path / "root"
    outside_root = tmp_path / "outside"
    media_root.mkdir()
    outside_root.mkdir()
    outside_file = outside_root / "secret.txt"
    outside_file.write_text("secret")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=media_root,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{outside_file}"))

    assert "文件不存在或不可访问" in telegram.sent_messages[0]["text"]
    assert telegram.sent_documents == []


def test_adapter_rejects_media_symlink_escape(tmp_path: Path) -> None:
    media_root = tmp_path / "root"
    outside_root = tmp_path / "outside"
    media_root.mkdir()
    outside_root.mkdir()
    outside_file = outside_root / "secret.txt"
    outside_file.write_text("secret")
    symlink_path = media_root / "secret-link.txt"
    symlink_path.symlink_to(outside_file)
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=media_root,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{symlink_path}"))

    assert "文件不存在或不可访问" in telegram.sent_messages[0]["text"]
    assert telegram.sent_documents == []


def test_adapter_rejects_media_file_over_size_limit(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv("TELEGRAM_MEDIA_MAX_BYTES", "4")
    doc_path = tmp_path / "large.txt"
    doc_path.write_bytes(b"12345")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
        media_root=tmp_path,
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{doc_path}"))

    assert "文件不存在或不可访问" in telegram.sent_messages[0]["text"]
    assert telegram.sent_documents == []


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


def test_adapter_rejects_message_when_existing_task_is_running() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    gateway.list_items = [
        {
            "task_id": "task-7",
            "agent_name": "main",
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "metadata": {"channel": "telegram", "chat_id": "100", "user_id": "200"},
        }
    ]
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(adapter.handle_message(build_message("hello again")))

    assert gateway.created == []
    assert gateway.sent == []
    assert "当前任务仍在处理中" in telegram.sent_messages[0]["text"]


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
            build_message("second", chat_id=-100, user_id=201, chat_type="group")
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
