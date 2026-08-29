from __future__ import annotations

from ruyi_agent.channels.telegram.client import TelegramMessage
from ruyi_agent.channels.telegram.network import UnsupportedTelegramChatTypeError


def build_telegram_session_key(
    message: TelegramMessage,
    *,
    agent_name: str,
) -> str:
    if message.chat_type == "private":
        return f"agent:{agent_name}:telegram:dm:{message.chat_id}"
    if message.chat_type in {"group", "supergroup"}:
        thread_part = (
            f":thread:{message.message_thread_id}"
            if message.message_thread_id is not None
            else ""
        )
        return (
            f"agent:{agent_name}:telegram:{message.chat_type}:"
            f"{message.chat_id}{thread_part}:user:{message.user_id}"
        )
    raise UnsupportedTelegramChatTypeError(
        f"Unsupported Telegram chat_type: {message.chat_type!r}"
    )


def build_telegram_identity_key(message: TelegramMessage) -> str:
    if message.chat_type == "private":
        return f"telegram:dm:{message.chat_id}"
    if message.chat_type in {"group", "supergroup"}:
        thread_part = (
            f":thread:{message.message_thread_id}"
            if message.message_thread_id is not None
            else ""
        )
        return (
            f"telegram:{message.chat_type}:"
            f"{message.chat_id}{thread_part}:user:{message.user_id}"
        )
    raise UnsupportedTelegramChatTypeError(
        f"Unsupported Telegram chat_type: {message.chat_type!r}"
    )
