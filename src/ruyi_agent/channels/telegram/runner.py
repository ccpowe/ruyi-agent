from __future__ import annotations

import os
from pathlib import Path

from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.channels.telegram.client import (
    DEFAULT_TELEGRAM_MEDIA_MAX_BYTES,
    TelegramBotAPIClient,
)
from ruyi_agent.storage.channel_delivery_store import ChannelDeliveryStore
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
    media_max_bytes = int(
        os.getenv("TELEGRAM_MEDIA_MAX_BYTES", str(DEFAULT_TELEGRAM_MEDIA_MAX_BYTES))
    )
    session_store = ChannelSessionStore(session_db_path)
    delivery_store = ChannelDeliveryStore(session_db_path)
    update_store = TelegramUpdateStore(update_db_path)
    adapter: TelegramAdapter | None = None
    try:
        adapter = TelegramAdapter(
            gateway_client=GatewayHTTPClient(
                base_url=gateway_base_url,
                bearer_token=gateway_bearer_token,
                max_download_bytes=media_max_bytes,
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
                media_max_bytes=media_max_bytes,
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
            media_max_bytes=media_max_bytes,
            delivery_store=delivery_store,
        )
        await adapter.run_forever()
    finally:
        if adapter is not None and hasattr(adapter, "close"):
            await adapter.close()
        update_store.close()
        delivery_store.close()
        session_store.close()
