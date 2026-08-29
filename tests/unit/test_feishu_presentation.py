from __future__ import annotations

from ruyi_agent.channels.gateway_dto import GatewayTask

from _feishu_adapter_support import (
    FakeFeishuClient,
    FakeGatewayClient,
    FeishuAdapter,
    GatewayArtifact,
    Path,
    asyncio,
)


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
    gateway.tasks["task-7"] = task

    async def scenario() -> None:
        parsed = GatewayTask.model_validate(task)
        await adapter._delivery.ensure_terminal_delivery(
            task=parsed,
            session_key="feishu:chat:chat-1",
            chat_id="chat-1",
            task_id=parsed.task_id,
            run_count=parsed.run_count,
            hooks=adapter._delivery_hooks(
                chat_id="chat-1",
                key=(parsed.task_id, parsed.run_count),
            ),
        )
        await adapter.close()

    asyncio.run(scenario())

    assert feishu.sent_messages[0]["text"] == "done\n\ntask_id=task-7"
    assert feishu.sent_files == [
        {
            "chat_id": "chat-1",
            "filename": "report.html",
            "content": b"<html></html>",
            "reply_to_message_id": None,
        }
    ]
