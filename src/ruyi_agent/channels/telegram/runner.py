from __future__ import annotations

import os
from pathlib import Path

from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.channels.telegram.client import TelegramBotAPIClient
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


DEFAULT_GATEWAY_BEARER_TOKEN = "dev-token"
DEFAULT_CHANNEL_SESSION_DB = "data/channel_sessions.sqlite3"


async def run_telegram_adapter() -> None:
    from ruyi_agent.channels.telegram.adapter import (
        TelegramAdapter,
        TelegramUpdateStore,
    )

    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    gateway_base_url = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
    gateway_bearer_token = (
        os.getenv("GATEWAY_BEARER_TOKEN") or DEFAULT_GATEWAY_BEARER_TOKEN
    )
    default_agent_name = os.getenv("TELEGRAM_DEFAULT_AGENT", "main")
    session_db_path = os.getenv(
        "TELEGRAM_SESSION_DB",
        os.getenv("CHANNEL_SESSION_DB", DEFAULT_CHANNEL_SESSION_DB),
    )
    update_db_path = os.getenv(
        "TELEGRAM_UPDATE_DB",
        str(Path(session_db_path).expanduser().with_name("telegram_updates.sqlite3")),
    )
    poll_timeout = int(os.getenv("TELEGRAM_POLL_TIMEOUT", "30"))
    session_store = ChannelSessionStore(session_db_path)
    update_store = TelegramUpdateStore(update_db_path)
    try:
        adapter = TelegramAdapter(
            gateway_client=GatewayHTTPClient(
                base_url=gateway_base_url,
                bearer_token=gateway_bearer_token,
            ),
            telegram_client=TelegramBotAPIClient(
                bot_token=bot_token,
                timeout=float(
                    os.getenv("TELEGRAM_API_TIMEOUT", str(poll_timeout + 10))
                ),
                default_parse_mode=os.getenv(
                    "TELEGRAM_MESSAGE_PARSE_MODE",
                    "MarkdownV2",
                ),
            ),
            default_agent_name=default_agent_name,
            session_store=session_store,
            update_store=update_store,
            poll_timeout=poll_timeout,
            task_poll_interval=float(os.getenv("TELEGRAM_TASK_POLL_INTERVAL", "2")),
            terminal_review_grace_checks=int(
                os.getenv("TELEGRAM_TERMINAL_REVIEW_GRACE_CHECKS", "3")
            ),
            message_parse_mode=os.getenv("TELEGRAM_MESSAGE_PARSE_MODE", "MarkdownV2"),
        )
        await adapter.run_forever()
    finally:
        update_store.close()
        session_store.close()
