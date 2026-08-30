from __future__ import annotations

import asyncio
from dataclasses import replace

import ruyi_agent.channels.telegram.adapter as telegram_adapter_module
import ruyi_agent.channels.telegram.runner as telegram_runner
from ruyi_agent.channels.telegram.client import TelegramBotAPIClient
from ruyi_agent.config.runtime_settings import configure_runtime_environment


def test_telegram_runner_wires_media_limit_to_all_downloaders(
    monkeypatch, tmp_path
) -> None:
    captured: dict[str, object] = {}

    class AdapterProbe:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

        async def run_forever(self) -> None:
            captured["ran"] = True

    env = {"RUYI_HOME": str(tmp_path / ".ruyi_agent")}
    base_settings = configure_runtime_environment(
        workspace=tmp_path,
        env=env,
        init_templates=True,
    )
    telegram_settings = replace(
        base_settings.channels.telegram,
        bot_token="bot-token",
        media_max_bytes=4321,
        session_db=tmp_path / "sessions.sqlite3",
        update_db=tmp_path / "updates.sqlite3",
    )
    settings = replace(
        base_settings,
        channels=replace(base_settings.channels, telegram=telegram_settings),
    )
    monkeypatch.setattr(telegram_adapter_module, "TelegramAdapter", AdapterProbe)

    asyncio.run(telegram_runner.run_telegram_adapter(settings))

    assert captured["media_max_bytes"] == 4321
    assert captured["gateway_client"]._max_download_bytes == 4321
    telegram_client = captured["telegram_client"]
    assert isinstance(telegram_client, TelegramBotAPIClient)
    assert telegram_client._media_max_bytes == 4321
    assert captured["ran"] is True
