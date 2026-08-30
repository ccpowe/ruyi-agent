from __future__ import annotations

import asyncio

import httpx
import pytest

import ruyi_agent.channels.telegram.client as telegram_client_module
from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.channels.gateway_client import GatewayArtifact
from ruyi_agent.gateway_protocol.dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.media import MediaLimitError, validate_content_length
from ruyi_agent.channels.feishu.delivery import FeishuArtifactDelivery
from ruyi_agent.channels.telegram.delivery import TelegramArtifactDelivery
from ruyi_agent.channels.telegram.client import TelegramBotAPIClient
from ruyi_agent.channels.telegram.network import TelegramAPIError


class ChunkStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes) -> None:
        self.chunks = chunks

    async def __aiter__(self):
        for chunk in self.chunks:
            yield chunk

    async def aclose(self) -> None:
        return None


@pytest.mark.parametrize(
    ("headers", "values"),
    [
        ({"content-length": "not-a-number"}, None),
        ({"content-length": "1, 2"}, None),
        ({"content-length": "1"}, ["1", "1"]),
    ],
)
def test_content_length_rejects_illegal_or_repeated_values(
    headers: dict[str, str], values: list[str] | None
) -> None:
    with pytest.raises(MediaLimitError, match="invalid Content-Length"):
        validate_content_length(headers, max_bytes=10, values=values)


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"content-length": "1"},
    ],
)
def test_gateway_download_enforces_streamed_limit_without_trusting_header(
    headers: dict[str, str],
) -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers=headers,
            stream=ChunkStream(b"ab", b"cd"),
        )

    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(handler),
        max_download_bytes=3,
    )

    with pytest.raises(MediaLimitError, match="exceeds 3 bytes"):
        asyncio.run(
            client.download_task_artifact(
                task_id="task-1",
                artifact_id="artifact-1",
            )
        )


def test_gateway_download_buffers_no_more_than_limit_plus_one() -> None:
    yielded = 0

    class HugeChunk(httpx.AsyncByteStream):
        async def __aiter__(self):
            nonlocal yielded
            yielded += 1
            yield b"x" * 1000
            yielded += 1000
            yield b"unreachable"

        async def aclose(self) -> None:
            return None

    client = GatewayHTTPClient(
        base_url="http://gateway.test",
        bearer_token="token",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, stream=HugeChunk())
        ),
        max_download_bytes=3,
    )

    with pytest.raises(MediaLimitError):
        asyncio.run(client.download_artifact(path="/workspace/huge.bin"))
    assert yielded == 1


@pytest.mark.parametrize(
    "headers",
    [
        {},
        {"content-length": "1"},
        [("content-length", "1"), ("content-length", "1")],
    ],
)
def test_telegram_inbound_download_enforces_same_hard_limit(
    monkeypatch: pytest.MonkeyPatch,
    headers: dict[str, str] | list[tuple[str, str]],
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/getFile"):
            return httpx.Response(
                200,
                json={"ok": True, "result": {"file_path": "docs/file.bin"}},
            )
        return httpx.Response(
            200,
            headers=headers,
            stream=ChunkStream(b"ab", b"cd"),
        )

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        telegram_client_module,
        "TelegramFallbackTransport",
        lambda *, resolver: transport,
    )
    client = TelegramBotAPIClient(bot_token="token", media_max_bytes=3)

    with pytest.raises(TelegramAPIError, match="media download rejected"):
        asyncio.run(client._download_file("file-1"))


def test_telegram_inbound_download_accepts_exact_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/getFile"):
            return httpx.Response(
                200,
                json={"ok": True, "result": {"file_path": "docs/file.bin"}},
            )
        return httpx.Response(200, stream=ChunkStream(b"a", b"bc"))

    transport = httpx.MockTransport(handler)
    monkeypatch.setattr(
        telegram_client_module,
        "TelegramFallbackTransport",
        lambda *, resolver: transport,
    )
    client = TelegramBotAPIClient(bot_token="token", media_max_bytes=3)

    assert asyncio.run(client._download_file("file-1")) == b"abc"


@pytest.mark.parametrize("platform", ["telegram", "feishu"])
def test_platform_artifact_delivery_enforces_limit_with_fake_gateway(
    platform: str,
) -> None:
    class Gateway:
        async def download_task_artifact(
            self, *, task_id: str, artifact_id: str
        ) -> GatewayArtifact:
            assert (task_id, artifact_id) == ("task-1", "artifact-1")
            return GatewayArtifact(
                kind="file",
                filename="report.bin",
                content_type="application/octet-stream",
                content=b"four",
            )

    class Telegram:
        def __init__(self) -> None:
            self.messages: list[str] = []
            self.files = 0

        async def send_message(self, *, text: str, **_: object) -> None:
            self.messages.append(text)

        async def send_photo(self, **_: object) -> None:
            self.files += 1

        async def send_document(self, **_: object) -> None:
            self.files += 1

    class Feishu:
        def __init__(self) -> None:
            self.messages: list[str] = []
            self.files = 0

        async def send_message(self, *, text: str, **_: object) -> None:
            self.messages.append(text)

        async def send_file(self, **_: object) -> None:
            self.files += 1

    task = GatewayTask.model_validate(
        {"task_id": "task-1", "status": "completed", "run_count": 1}
    )
    artifact = GatewayPublishedArtifact.model_validate(
        {
            "artifact_id": "artifact-1",
            "path": "/workspace/report.bin",
            "name": "report.bin",
            "content_type": "application/octet-stream",
            "size": 4,
            "run_count": 1,
        }
    )
    if platform == "telegram":
        transport = Telegram()
        delivery = TelegramArtifactDelivery(
            gateway_client=Gateway(),  # type: ignore[arg-type]
            telegram_client=transport,  # type: ignore[arg-type]
            max_bytes=3,
        )
        asyncio.run(delivery.send(task, artifact, chat_id=100))
    else:
        transport = Feishu()
        delivery = FeishuArtifactDelivery(
            gateway_client=Gateway(),  # type: ignore[arg-type]
            feishu_client=transport,  # type: ignore[arg-type]
            max_bytes=3,
        )
        asyncio.run(delivery.send(task, artifact, chat_id="chat-1"))
    assert transport.files == 0
    assert len(transport.messages) == 1
    assert "exceeds 3 bytes" in transport.messages[0]
