from __future__ import annotations

from ruyi_agent.channels.feishu.client import (
    FeishuSDKClient,
)
from ruyi_agent.channels.feishu.receipts import FeishuEventStore
from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.storage.channel_delivery_store import ChannelDeliveryStore
from ruyi_agent.storage.channel_session_store import ChannelSessionStore
from ruyi_agent.config.runtime_settings import (
    RuntimeSettings,
)


DEFAULT_GATEWAY_BEARER_TOKEN = "dev-token"
DEFAULT_CHANNEL_SESSION_DB = "data/channel_sessions.sqlite3"


async def run_feishu_adapter(settings: RuntimeSettings) -> None:
    from ruyi_agent.channels.feishu.adapter import FeishuAdapter

    if not isinstance(settings, RuntimeSettings):
        raise TypeError("run_feishu_adapter requires RuntimeSettings")
    feishu = settings.channels.feishu
    app_id = feishu.app_id
    app_secret = feishu.app_secret
    if not app_id:
        raise SystemExit("Missing FEISHU_APP_ID")
    if not app_secret:
        raise SystemExit("Missing FEISHU_APP_SECRET")
    gateway_base_url = settings.gateway.base_url
    gateway_bearer_token = settings.gateway.bearer_token
    default_agent_name = feishu.default_agent
    session_db_path = str(feishu.session_db)
    event_db_path = str(feishu.event_db)
    require_mention = feishu.require_mention
    group_policy = feishu.group_policy
    bot_open_id = feishu.bot_open_id
    bot_user_id = feishu.bot_user_id
    bot_union_id = feishu.bot_union_id
    bot_name = feishu.bot_name
    media_max_bytes = feishu.media_max_bytes
    session_store = ChannelSessionStore(session_db_path)
    delivery_store = ChannelDeliveryStore(session_db_path)
    event_store = FeishuEventStore(event_db_path)
    adapter: FeishuAdapter | None = None
    try:
        adapter = FeishuAdapter(
            gateway_client=GatewayHTTPClient(
                base_url=gateway_base_url,
                bearer_token=gateway_bearer_token,
                max_download_bytes=media_max_bytes,
            ),
            feishu_client=FeishuSDKClient(
                app_id=app_id,
                app_secret=app_secret,
                domain=feishu.domain,
                timeout=feishu.api_timeout,
            ),
            default_agent_name=default_agent_name,
            session_store=session_store,
            event_store=event_store,
            require_mention=require_mention,
            group_policy=group_policy,
            allowed_users=set(feishu.allowed_users),
            allowed_groups=set(feishu.allowed_groups),
            bot_open_id=bot_open_id,
            bot_user_id=bot_user_id,
            bot_union_id=bot_union_id,
            bot_name=bot_name,
            task_poll_interval=feishu.task_poll_interval,
            terminal_review_grace_checks=feishu.terminal_review_grace_checks,
            media_max_bytes=media_max_bytes,
            delivery_store=delivery_store,
            ack_mode=feishu.ack_mode,
            reactions_enabled=feishu.reactions,
            processing_reaction=feishu.processing_reaction,
            approval_reaction=feishu.approval_reaction,
            failure_reaction=feishu.failure_reaction,
        )
        await adapter.run_forever()
    finally:
        if adapter is not None and hasattr(adapter, "close"):
            await adapter.close()
        event_store.close()
        delivery_store.close()
        session_store.close()
