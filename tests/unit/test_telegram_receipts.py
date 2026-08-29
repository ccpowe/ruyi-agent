from __future__ import annotations

from _telegram_adapter_support import (
    UTC,
    ChannelSessionStore,
    FakeGatewayClient,
    FakeTelegramClient,
    Path,
    TelegramAdapter,
    TelegramUpdateStore,
    asyncio,
    build_message,
    datetime,
    sqlite3,
    telegram_adapter_module,
)


def test_poll_once_deduplicates_repeated_update_id() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    telegram.updates = [build_message("hello", update_id=10)]
    update_store = TelegramUpdateStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        offset = await adapter.poll_once(offset=None)
        assert offset == 11
        offset = await adapter.poll_once(offset=10)
        assert offset == 11

    asyncio.run(scenario())

    assert len(gateway.created) == 1
    assert gateway.idempotency_keys == ["telegram:update:10"]
    assert gateway.sent == []
    update_store.close()


def test_update_store_migrates_old_schema_and_reclaims_expired_lease(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "telegram_updates.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE telegram_processed_updates (
                update_id INTEGER PRIMARY KEY,
                chat_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                processed_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO telegram_processed_updates (
                update_id, chat_id, message_id, first_seen_at, processed_at
            ) VALUES (10, '100', '300', '2026-01-01T00:00:00+00:00', NULL)
            """
        )

    update = build_message("hello", update_id=10)
    store = TelegramUpdateStore(str(db_path), claim_timeout_seconds=0)
    try:
        with sqlite3.connect(db_path) as conn:
            columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(telegram_processed_updates)"
                ).fetchall()
            }
        assert {"claimed_at", "claim_token"}.issubset(columns)

        first_claim = store.claim_update_result(update)
        assert first_claim.status == "claimed"
        assert first_claim.claimed_at is not None
        assert first_claim.claim_token is not None

        reclaimed = store.claim_update_result(update)
        assert reclaimed.status == "claimed"
        assert reclaimed.claimed_at is not None
        assert reclaimed.claim_token is not None
        assert store.mark_processed(10, claim_token=reclaimed.claim_token)
        assert store.claim_update_result(update).status == "processed"
        assert not store.claim_update(update)
    finally:
        store.close()


def test_update_store_release_makes_unprocessed_update_claimable() -> None:
    update = build_message("hello", update_id=10)
    store = TelegramUpdateStore(":memory:")
    try:
        claim = store.claim_update_result(update)
        assert claim.status == "claimed"
        assert claim.claim_token is not None
        assert store.claim_update_result(update).status == "busy"

        assert store.release_claim(10, claim_token=claim.claim_token)
        reclaimed = store.claim_update_result(update)
        assert reclaimed.status == "claimed"
    finally:
        store.close()


def test_update_store_stale_owner_cannot_finish_reclaimed_lease(monkeypatch) -> None:
    fixed_now = datetime(2026, 8, 29, 12, 0, tzinfo=UTC)
    monkeypatch.setattr(telegram_adapter_module, "_utc_now", lambda: fixed_now)
    update = build_message("hello", update_id=10)
    store = TelegramUpdateStore(":memory:", claim_timeout_seconds=0)
    try:
        stale_claim = store.claim_update_result(update)
        current_claim = store.claim_update_result(update)

        assert stale_claim.claimed_at == current_claim.claimed_at
        assert stale_claim.claim_token is not None
        assert current_claim.claim_token is not None
        assert stale_claim.claim_token != current_claim.claim_token
        assert not store.mark_processed(10, claim_token=stale_claim.claim_token)
        assert not store.release_claim(10, claim_token=stale_claim.claim_token)
        assert store.mark_processed(10, claim_token=current_claim.claim_token)
    finally:
        store.close()


def test_poll_once_retries_when_gateway_fails_before_effect() -> None:
    gateway = FakeGatewayClient()
    gateway.create_failures = 1
    telegram = FakeTelegramClient()
    telegram.updates = [build_message("hello", update_id=10)]
    update_store = TelegramUpdateStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
        task_poll_interval=0.0,
    )

    async def scenario() -> None:
        offset = await adapter.poll_once(offset=None)
        assert offset is None
        offset = await adapter.poll_once(offset=offset)
        assert offset == 11
        gateway.tasks["task-1"] = {
            **gateway.tasks["task-1"],
            "status": "completed",
            "last_result": "done",
        }
        await adapter.wait_for_watchers()

    try:
        asyncio.run(scenario())
    finally:
        update_store.close()

    assert gateway.create_attempts == 2
    assert len(gateway.created) == 1


def test_poll_once_reuses_turn_receipt_after_reply_failure_and_restart(
    tmp_path: Path,
) -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    telegram.fail_all_messages = True
    telegram.updates = [build_message("hello", update_id=10)]
    update_db_path = tmp_path / "telegram_updates.sqlite3"
    session_db_path = tmp_path / "channel_sessions.sqlite3"
    first_update_store = TelegramUpdateStore(str(update_db_path))
    first_session_store = ChannelSessionStore(str(session_db_path))
    first_adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=first_update_store,
        session_store=first_session_store,
        task_poll_interval=0.0,
    )

    try:
        offset = asyncio.run(first_adapter.poll_once(offset=None))
        assert offset is None
    finally:
        first_update_store.close()
        first_session_store.close()

    telegram.fail_all_messages = False
    second_update_store = TelegramUpdateStore(str(update_db_path))
    second_session_store = ChannelSessionStore(str(session_db_path))
    second_adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=second_update_store,
        session_store=second_session_store,
        task_poll_interval=0.0,
    )
    try:
        offset = asyncio.run(second_adapter.poll_once(offset=offset))
        assert offset == 11
    finally:
        second_update_store.close()
        second_session_store.close()

    assert gateway.create_attempts == 1
    assert len(gateway.created) == 1
    assert gateway.idempotency_keys == ["telegram:update:10"]


def test_poll_once_stops_batch_without_advancing_past_failed_update() -> None:
    gateway = FakeGatewayClient()
    gateway.create_failures = 1
    telegram = FakeTelegramClient()
    first = build_message("/start", update_id=10)
    failed = build_message("second", update_id=11)
    skipped = build_message("third", update_id=12)
    telegram.updates = [first, failed, skipped]
    update_store = TelegramUpdateStore(":memory:")
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
        task_poll_interval=0.0,
    )

    try:
        offset = asyncio.run(adapter.poll_once(offset=10))
        assert offset == 11
        assert gateway.create_attempts == 1
        assert update_store.claim_update_result(first).status == "processed"
        assert update_store.claim_update(skipped)
    finally:
        update_store.close()


def test_poll_once_does_not_advance_past_update_with_live_claim() -> None:
    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    update = build_message("hello", update_id=10)
    telegram.updates = [update]
    update_store = TelegramUpdateStore(":memory:")
    claim = update_store.claim_update_result(update)
    assert claim.status == "claimed"
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
        task_poll_interval=0.0,
    )

    try:
        offset = asyncio.run(adapter.poll_once(offset=10))
        assert offset == 10
        assert gateway.create_attempts == 0
    finally:
        update_store.close()


def test_run_forever_backs_off_when_update_has_live_claim(monkeypatch) -> None:
    class StopPolling(Exception):
        pass

    gateway = FakeGatewayClient()
    telegram = FakeTelegramClient()
    update = build_message("hello", update_id=10)
    telegram.updates = [update]
    update_store = TelegramUpdateStore(":memory:")
    assert update_store.claim_update(update)
    adapter = TelegramAdapter(
        gateway_client=gateway,
        telegram_client=telegram,
        default_agent_name="main",
        update_store=update_store,
    )
    sleep_delays: list[float] = []

    async def stop_after_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        raise StopPolling

    monkeypatch.setattr(telegram_adapter_module.asyncio, "sleep", stop_after_sleep)

    async def scenario() -> None:
        try:
            await adapter.run_forever()
        except StopPolling:
            return
        raise AssertionError("run_forever did not back off after a live claim")

    try:
        asyncio.run(scenario())
    finally:
        update_store.close()

    assert telegram.get_updates_calls == [None]
    assert sleep_delays == [
        telegram_adapter_module.TELEGRAM_CLAIM_RETRY_DELAY_SECONDS
    ]
