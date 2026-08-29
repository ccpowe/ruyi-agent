from __future__ import annotations

from _telegram_adapter_support import (
    FakeGatewayClient,
    FakeMermaidRenderer,
    FakeTelegramClient,
    Path,
    TelegramAdapter,
    TelegramInboundAttachment,
    _format_telegram_markdown_v2,
    _split_telegram_message,
    asyncio,
    build_message,
)


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


def test_adapter_does_not_parse_media_references(tmp_path: Path) -> None:
    doc_path = tmp_path / "slides.pptx"
    doc_path.write_bytes(b"pptx-bytes")
    telegram = FakeTelegramClient()
    adapter = TelegramAdapter(
        gateway_client=FakeGatewayClient(),
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(adapter._send_message(chat_id=100, text=f"MEDIA:{doc_path}"))

    assert telegram.sent_photos == []
    assert telegram.sent_documents == []
    assert "MEDIA:" in telegram.sent_messages[0]["text"]


def test_adapter_sends_published_artifacts_on_terminal_task() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
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
    gateway.task_artifacts[("task-7", "art_1")] = TelegramInboundAttachment(
        kind="file",
        filename="report.html",
        content_type="text/html",
        content=b"<html></html>",
    )
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
    )

    asyncio.run(adapter._send_terminal_if_needed(chat_id=100, task=task))

    assert telegram.sent_messages[0]["text"] == "done\n\ntask\\_id\\=task\\-7"
    assert telegram.sent_documents == [
        {
            "chat_id": 100,
            "filename": "report.html",
            "content": b"<html></html>",
            "caption": "Report",
            "reply_to_message_id": None,
            "parse_mode": None,
        }
    ]
