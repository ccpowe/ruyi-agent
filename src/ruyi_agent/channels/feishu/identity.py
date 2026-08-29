from __future__ import annotations

import re

from ruyi_agent.channels.feishu.client import FeishuMessage


def _is_feishu_group_chat(chat_type: str) -> bool:
    return chat_type not in {"p2p", "private", "dm"}


def _strip_mention_token(text: str, token: str) -> str:
    if not token:
        return text
    pattern = re.compile(rf"(?<!\S){re.escape(token)}(?=$|[\s,.:;!?，。：；！？])")
    return pattern.sub("", text)


def build_feishu_session_key(
    message: FeishuMessage,
    *,
    agent_name: str,
) -> str:
    if not _is_feishu_group_chat(message.chat_type):
        return f"agent:{agent_name}:feishu:dm:{message.chat_id}"
    thread_part = (
        f":thread:{message.thread_id}" if message.thread_id is not None else ""
    )
    return (
        f"agent:{agent_name}:feishu:group:"
        f"{message.chat_id}{thread_part}:user:{message.user_id}"
    )


def build_feishu_identity_key(message: FeishuMessage) -> str:
    if not _is_feishu_group_chat(message.chat_type):
        return f"feishu:dm:{message.chat_id}"
    thread_part = (
        f":thread:{message.thread_id}" if message.thread_id is not None else ""
    )
    return f"feishu:group:{message.chat_id}{thread_part}:user:{message.user_id}"
