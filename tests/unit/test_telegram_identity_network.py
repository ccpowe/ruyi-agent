from __future__ import annotations

from _telegram_adapter_support import (
    FailsThenRecordsAsyncTransport,
    FakeFallbackResolver,
    TelegramFallbackResolver,
    TelegramFallbackTransport,
    UnsupportedTelegramChatTypeError,
    _looks_like_network_error,
    asyncio,
    build_message,
    build_telegram_session_key,
    httpx,
)


def test_build_telegram_session_key_uses_dm_chat_id() -> None:
    message = build_message("hello", chat_id=100, user_id=200)

    assert (
        build_telegram_session_key(message, agent_name="main")
        == "agent:main:telegram:dm:100"
    )


def test_telegram_fallback_resolver_prefers_configured_and_sticky_ips() -> None:
    resolver = TelegramFallbackResolver(fallback_ips=["1.1.1.1"])

    async def scenario() -> None:
        first = await resolver.get_fallback_ips()
        resolver.mark_success("2.2.2.2")
        second = await resolver.get_fallback_ips()
        assert first[0] == "1.1.1.1"
        assert second[:2] == ["2.2.2.2", "1.1.1.1"]

    asyncio.run(scenario())


def test_looks_like_network_error_detects_dns_failure_text() -> None:
    assert _looks_like_network_error(
        RuntimeError("[Errno -3] Temporary failure in name resolution")
    )


def test_build_telegram_session_key_isolates_group_by_user() -> None:
    first = build_message("hello", chat_id=-100, user_id=200, chat_type="group")
    second = build_message("hello", chat_id=-100, user_id=201, chat_type="group")

    assert (
        build_telegram_session_key(first, agent_name="main")
        == "agent:main:telegram:group:-100:user:200"
    )
    assert (
        build_telegram_session_key(second, agent_name="main")
        == "agent:main:telegram:group:-100:user:201"
    )


def test_build_telegram_session_key_isolates_supergroup_topic_by_thread() -> None:
    first = build_message(
        "hello",
        chat_id=-100,
        user_id=200,
        chat_type="supergroup",
        message_thread_id=10,
    )
    second = build_message(
        "hello",
        chat_id=-100,
        user_id=200,
        chat_type="supergroup",
        message_thread_id=11,
    )

    assert (
        build_telegram_session_key(first, agent_name="main")
        == "agent:main:telegram:supergroup:-100:thread:10:user:200"
    )
    assert (
        build_telegram_session_key(second, agent_name="main")
        == "agent:main:telegram:supergroup:-100:thread:11:user:200"
    )


def test_build_telegram_session_key_rejects_channel_chat_type() -> None:
    message = build_message("hello", chat_type="channel")

    try:
        build_telegram_session_key(message, agent_name="main")
    except UnsupportedTelegramChatTypeError:
        return
    raise AssertionError("Expected UnsupportedTelegramChatTypeError")


def test_fallback_transport_rebuilds_async_stream_for_fallback_request() -> None:
    base_transport = FailsThenRecordsAsyncTransport()
    resolver = FakeFallbackResolver()
    transport = TelegramFallbackTransport(
        resolver=resolver,  # type: ignore[arg-type]
        base_transport=base_transport,
    )
    request = httpx.Request(
        "POST",
        "https://api.telegram.org/bot-token/getUpdates",
        content=b'{"ok":true}',
    )

    response = asyncio.run(transport.handle_async_request(request))

    assert response.status_code == 200
    assert len(base_transport.requests) == 2
    assert base_transport.requests[1].url.host == "149.154.167.220"
    assert base_transport.requests[1].headers["Host"] == "api.telegram.org"
    assert resolver.successes == ["149.154.167.220"]
