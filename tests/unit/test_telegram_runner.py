from __future__ import annotations

import asyncio

import ruyi_agent.channels.telegram.adapter as telegram_adapter_module
import ruyi_agent.channels.telegram.runner as telegram_runner
from ruyi_agent.channels.telegram.client import TelegramBotAPIClient


def test_telegram_runner_wires_media_limit_to_all_downloaders(
    monkeypatch, tmp_path
) -> None:
    captured: dict[str, object] = {}

    class AdapterProbe:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def run_forever(self) -> None:
            captured["ran"] = True

    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "bot-token")
    monkeypatch.setenv("TELEGRAM_MEDIA_MAX_BYTES", "4321")
    monkeypatch.setenv("TELEGRAM_SESSION_DB", str(tmp_path / "sessions.sqlite3"))
    monkeypatch.setenv("TELEGRAM_UPDATE_DB", str(tmp_path / "updates.sqlite3"))
    monkeypatch.setattr(telegram_adapter_module, "TelegramAdapter", AdapterProbe)

    asyncio.run(telegram_runner.run_telegram_adapter())

    assert captured["media_max_bytes"] == 4321
    assert captured["gateway_client"]._max_download_bytes == 4321
    telegram_client = captured["telegram_client"]
    assert isinstance(telegram_client, TelegramBotAPIClient)
    assert telegram_client._media_max_bytes == 4321
    assert captured["ran"] is True
