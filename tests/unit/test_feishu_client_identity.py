from __future__ import annotations

import asyncio
from dataclasses import replace

from _feishu_adapter_support import (
    FeishuMention,
    build_feishu_identity_key,
    build_feishu_session_key,
    build_message,
    json,
    parse_feishu_message_event,
)
import ruyi_agent.channels.feishu.adapter as feishu_adapter_module
import ruyi_agent.channels.feishu.runner as feishu_runner
from ruyi_agent.channels.feishu.client import FeishuSDKClient
from ruyi_agent.config.runtime_settings import configure_runtime_environment


def test_parse_feishu_text_event_extracts_sender_and_mentions() -> None:
    payload = {
        "header": {"event_id": "event-1"},
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": "ou_1",
                    "user_id": "u_1",
                    "union_id": "on_1",
                }
            },
            "message": {
                "message_id": "om_1",
                "chat_id": "oc_1",
                "chat_type": "group",
                "message_type": "text",
                "content": json.dumps({"text": "@_bot hello"}),
                "mentions": [
                    {
                        "key": "@_bot",
                        "name": "Ruyi",
                        "id": {"open_id": "bot-open"},
                    }
                ],
            },
        },
    }

    message = parse_feishu_message_event(payload)

    assert message is not None
    assert message.event_id == "event-1"
    assert message.message_id == "om_1"
    assert message.user_id == "on_1"
    assert message.text == "@_bot hello"
    assert message.mentions == [
        FeishuMention(key="@_bot", name="Ruyi", open_id="bot-open")
    ]


def test_build_feishu_session_key_isolates_group_by_user() -> None:
    first = build_message("hello", chat_id="group-1", user_id="u1", chat_type="group")
    second = build_message("hello", chat_id="group-1", user_id="u2", chat_type="group")

    assert (
        build_feishu_session_key(first, agent_name="main")
        == "agent:main:feishu:group:group-1:user:u1"
    )
    assert (
        build_feishu_session_key(second, agent_name="main")
        == "agent:main:feishu:group:group-1:user:u2"
    )
    assert build_feishu_identity_key(first) == "feishu:group:group-1:user:u1"


def test_feishu_sdk_client_standard_constructor_keeps_lazy_client() -> None:
    client = FeishuSDKClient(
        app_id="app-id",
        app_secret="app-secret",
        domain="lark",
        timeout=7.5,
    )

    assert client._app_id == "app-id"
    assert client._app_secret == "app-secret"
    assert client._domain == "lark"
    assert client._timeout == 7.5
    assert client._client is None


def test_feishu_sdk_client_builds_and_caches_sdk_client_lazily() -> None:
    calls: list[tuple[object, ...]] = []
    sdk_client = object()

    class Builder:
        def app_id(self, value: str):
            calls.append(("app_id", value))
            return self

        def app_secret(self, value: str):
            calls.append(("app_secret", value))
            return self

        def domain(self, value: str):
            calls.append(("domain", value))
            return self

        def timeout(self, value: float):
            calls.append(("timeout", value))
            return self

        def build(self):
            calls.append(("build",))
            return sdk_client

    builder = Builder()

    class ClientFactory:
        @staticmethod
        def builder() -> Builder:
            calls.append(("builder",))
            return builder

    class DomainProbe:
        Feishu = "feishu-domain"

    class Lark:
        Client = ClientFactory

    Lark.Domain = DomainProbe

    client = FeishuSDKClient(
        app_id="app-id",
        app_secret="app-secret",
        timeout=7.5,
    )

    assert client._client is None
    assert client._get_client(Lark) is sdk_client
    assert client._get_client(Lark) is sdk_client
    assert calls == [
        ("builder",),
        ("app_id", "app-id"),
        ("app_secret", "app-secret"),
        ("domain", "feishu-domain"),
        ("timeout", 7.5),
        ("build",),
    ]


def test_feishu_runner_wires_real_sdk_client_factory(monkeypatch, tmp_path) -> None:
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
    feishu_settings = replace(
        base_settings.channels.feishu,
        app_id="runner-app",
        app_secret="runner-secret",
        domain="lark",
        api_timeout=4.5,
        group_policy="disabled",
        media_max_bytes=1234,
        session_db=tmp_path / "sessions.sqlite3",
        event_db=tmp_path / "events.sqlite3",
    )
    settings = replace(
        base_settings,
        channels=replace(base_settings.channels, feishu=feishu_settings),
    )
    monkeypatch.setattr(feishu_adapter_module, "FeishuAdapter", AdapterProbe)

    asyncio.run(feishu_runner.run_feishu_adapter(settings))

    sdk_client = captured["feishu_client"]
    assert isinstance(sdk_client, FeishuSDKClient)
    assert sdk_client._app_id == "runner-app"
    assert sdk_client._app_secret == "runner-secret"
    assert sdk_client._domain == "lark"
    assert sdk_client._timeout == 4.5
    assert sdk_client._client is None
    assert captured["default_agent_name"] == "main"
    assert captured["media_max_bytes"] == 1234
    assert captured["gateway_client"]._max_download_bytes == 1234
    assert captured["ran"] is True
