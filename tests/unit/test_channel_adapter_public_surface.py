from __future__ import annotations


def star_import(module_name: str) -> dict[str, object]:
    namespace: dict[str, object] = {}
    exec(f"from {module_name} import *", namespace)
    return namespace


def test_telegram_adapter_preserves_historical_star_import_surface() -> None:
    exported = star_import("ruyi_agent.channels.telegram.adapter")

    assert {
        "KrokiMermaidRenderer",
        "MermaidRenderError",
        "TelegramAPIError",
        "TelegramAttachment",
        "TelegramClient",
        "TelegramNetworkError",
        "TelegramPollResult",
        "UnsupportedTelegramChatTypeError",
    } <= exported.keys()


def test_feishu_adapter_preserves_historical_star_import_surface() -> None:
    exported = star_import("ruyi_agent.channels.feishu.adapter")

    assert {
        "FeishuAttachment",
        "FeishuClient",
        "FeishuReactionReceipt",
    } <= exported.keys()
