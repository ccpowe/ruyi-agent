from __future__ import annotations

import os
from pathlib import Path

from ruyi_agent.channels.feishu.client import (
    DEFAULT_FEISHU_MEDIA_MAX_BYTES,
    FeishuSDKClient,
)
from ruyi_agent.channels.feishu.receipts import FeishuEventStore
from ruyi_agent.channels.gateway_client import GatewayHTTPClient
from ruyi_agent.storage.channel_delivery_store import ChannelDeliveryStore
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


DEFAULT_GATEWAY_BEARER_TOKEN = "dev-token"
DEFAULT_CHANNEL_SESSION_DB = "data/channel_sessions.sqlite3"


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


async def run_feishu_adapter() -> None:
    from ruyi_agent.channels.feishu.adapter import FeishuAdapter

    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_id:
        raise SystemExit("Missing FEISHU_APP_ID")
    if not app_secret:
        raise SystemExit("Missing FEISHU_APP_SECRET")
    connection_mode = os.getenv("FEISHU_CONNECTION_MODE", "websocket").strip().lower()
    if connection_mode != "websocket":
        raise SystemExit("Only FEISHU_CONNECTION_MODE=websocket is supported for now")
    gateway_base_url = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
    gateway_bearer_token = (
        os.getenv("GATEWAY_BEARER_TOKEN") or DEFAULT_GATEWAY_BEARER_TOKEN
    )
    default_agent_name = os.getenv("FEISHU_DEFAULT_AGENT", "main")
    session_db_path = os.getenv(
        "FEISHU_SESSION_DB",
        os.getenv("CHANNEL_SESSION_DB", DEFAULT_CHANNEL_SESSION_DB),
    )
    event_db_path = os.getenv(
        "FEISHU_EVENT_DB",
        str(Path(session_db_path).expanduser().with_name("feishu_events.sqlite3")),
    )
    require_mention = _env_bool("FEISHU_REQUIRE_MENTION", default=True)
    group_policy = os.getenv("FEISHU_GROUP_POLICY", "disabled").strip().lower()
    bot_open_id = os.getenv("FEISHU_BOT_OPEN_ID") or None
    bot_user_id = os.getenv("FEISHU_BOT_USER_ID") or None
    bot_union_id = os.getenv("FEISHU_BOT_UNION_ID") or None
    bot_name = os.getenv("FEISHU_BOT_NAME") or None
    media_max_bytes = int(
        os.getenv("FEISHU_MEDIA_MAX_BYTES", str(DEFAULT_FEISHU_MEDIA_MAX_BYTES))
    )
    if (
        require_mention
        and group_policy != "disabled"
        and not any([bot_open_id, bot_user_id, bot_union_id, bot_name])
    ):
        raise SystemExit(
            "Missing Feishu bot identity for group mention checks. Set one of "
            "FEISHU_BOT_OPEN_ID, FEISHU_BOT_USER_ID, FEISHU_BOT_UNION_ID, "
            "FEISHU_BOT_NAME, or set FEISHU_GROUP_POLICY=disabled for DM-only use."
        )
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
                domain=os.getenv("FEISHU_DOMAIN", "feishu"),
                timeout=float(os.getenv("FEISHU_API_TIMEOUT", "10")),
            ),
            default_agent_name=default_agent_name,
            session_store=session_store,
            event_store=event_store,
            require_mention=require_mention,
            group_policy=group_policy,
            allowed_users=set(_env_list("FEISHU_ALLOWED_USERS")),
            allowed_groups=set(_env_list("FEISHU_ALLOWED_GROUPS")),
            bot_open_id=bot_open_id,
            bot_user_id=bot_user_id,
            bot_union_id=bot_union_id,
            bot_name=bot_name,
            task_poll_interval=float(os.getenv("FEISHU_TASK_POLL_INTERVAL", "2")),
            terminal_review_grace_checks=int(
                os.getenv("FEISHU_TERMINAL_REVIEW_GRACE_CHECKS", "3")
            ),
            media_max_bytes=media_max_bytes,
            delivery_store=delivery_store,
            ack_mode=os.getenv("FEISHU_ACK_MODE", "reaction"),
            reactions_enabled=_env_bool("FEISHU_REACTIONS", default=True),
            processing_reaction=os.getenv("FEISHU_PROCESSING_REACTION", "Typing"),
            approval_reaction=os.getenv("FEISHU_APPROVAL_REACTION", "CheckMark"),
            failure_reaction=os.getenv("FEISHU_FAILURE_REACTION", "CrossMark"),
        )
        await adapter.run_forever()
    finally:
        if adapter is not None and hasattr(adapter, "close"):
            await adapter.close()
        event_store.close()
        delivery_store.close()
        session_store.close()
