from __future__ import annotations

from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.channels.telegram.client import (
    TelegramBotAPIClient,
)
from ruyi_agent.storage.channel_delivery_store import ChannelDeliveryStore
from ruyi_agent.storage.channel_session_store import ChannelSessionStore
from ruyi_agent.config.runtime_settings import (
    RuntimeSettings,
    configure_runtime_environment,
)


DEFAULT_GATEWAY_BEARER_TOKEN = "dev-token"
DEFAULT_CHANNEL_SESSION_DB = "data/channel_sessions.sqlite3"


async def run_telegram_adapter(settings: RuntimeSettings | None = None) -> None:
    from ruyi_agent.channels.telegram.adapter import (
        TelegramAdapter,
        TelegramUpdateStore,
    )

    active_settings = settings or configure_runtime_environment()
    telegram = active_settings.channels.telegram
    bot_token = telegram.bot_token
    if not bot_token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    gateway_base_url = active_settings.gateway.base_url
    gateway_bearer_token = active_settings.gateway.bearer_token
    default_agent_name = telegram.default_agent
    session_db_path = str(telegram.session_db)
    update_db_path = str(telegram.update_db)
    poll_timeout = telegram.poll_timeout
    media_max_bytes = telegram.media_max_bytes
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
                timeout=telegram.api_timeout,
                default_parse_mode=telegram.message_parse_mode,
                media_max_bytes=media_max_bytes,
                fallback_ips=telegram.fallback_ips,
            ),
            default_agent_name=default_agent_name,
            session_store=session_store,
            update_store=update_store,
            poll_timeout=poll_timeout,
            task_poll_interval=telegram.task_poll_interval,
            terminal_review_grace_checks=telegram.terminal_review_grace_checks,
            message_parse_mode=telegram.message_parse_mode,
            media_max_bytes=media_max_bytes,
            kroki_base_url=telegram.kroki_base_url,
            delivery_store=delivery_store,
        )
        await adapter.run_forever()
    finally:
        if adapter is not None and hasattr(adapter, "close"):
            await adapter.close()
        update_store.close()
        delivery_store.close()
        session_store.close()
