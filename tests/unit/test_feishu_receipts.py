from __future__ import annotations

from _feishu_adapter_support import (
    Any,
    FakeFeishuClient,
    FakeGatewayClient,
    FeishuAdapter,
    FeishuEventStore,
    FeishuMessage,
    ThreadPoolExecutor,
    asyncio,
    build_message,
    sqlite3,
    threading,
)


def test_event_store_deduplicates_repeated_event_id() -> None:
    gateway = FakeGatewayClient()
    feishu = FakeFeishuClient()
    event_store = FeishuEventStore(":memory:")
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=feishu,
        default_agent_name="main",
        event_store=event_store,
        task_poll_interval=0.0,
    )
    message = build_message("hello", event_id="same-event")

    async def scenario() -> None:
        await adapter.handle_message(message)
        await adapter.handle_message(message)
        await adapter.wait_for_watchers()

    try:
        asyncio.run(scenario())
    finally:
        event_store.close()

    assert len(gateway.created) == 1
    assert gateway.idempotency_keys == ["feishu:event:same-event"]


def test_event_store_allows_unprocessed_claim_after_lease_timeout(tmp_path) -> None:
    db_path = tmp_path / "feishu_events.sqlite3"
    message = build_message("hello", event_id="event-crash")
    first_store = FeishuEventStore(str(db_path), claim_timeout_seconds=0)
    try:
        first_claim = first_store.claim_message_result(message)
        assert first_claim.status == "claimed"
        assert first_claim.claim_token is not None
    finally:
        first_store.close()

    second_store = FeishuEventStore(str(db_path), claim_timeout_seconds=0)
    try:
        second_claim = second_store.claim_message_result(message)
        assert second_claim.status == "claimed"
        assert second_claim.event_key == "event-crash"
        assert second_claim.claim_token is not None
        assert second_claim.claim_token != first_claim.claim_token
        assert second_store.mark_processed(
            second_claim.event_key,
            claim_token=second_claim.claim_token,
        )
        assert not second_store.claim_message(message)
    finally:
        second_store.close()


def test_event_store_claim_is_atomic_across_independent_connections(tmp_path) -> None:
    db_path = tmp_path / "feishu_events.sqlite3"
    message = build_message("hello", event_id="event-race")
    first_store = FeishuEventStore(str(db_path))
    second_store = FeishuEventStore(str(db_path))
    barrier = threading.Barrier(2)

    def claim(store: FeishuEventStore):
        barrier.wait()
        return store.claim_message_result(message)

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            first_future = executor.submit(claim, first_store)
            second_future = executor.submit(claim, second_store)
            claims = [first_future.result(), second_future.result()]

        assert sorted(claim.status for claim in claims) == ["busy", "claimed"]
        winning_claim = next(claim for claim in claims if claim.status == "claimed")
        assert winning_claim.event_key == "event-race"
        assert winning_claim.claim_token is not None
    finally:
        first_store.close()
        second_store.close()


def test_event_store_stale_owner_cannot_finish_or_release_reclaimed_lease(
    tmp_path,
) -> None:
    db_path = tmp_path / "feishu_events.sqlite3"
    message = build_message("hello", event_id="event-stale")
    stale_store = FeishuEventStore(str(db_path), claim_timeout_seconds=0)
    current_store = FeishuEventStore(str(db_path), claim_timeout_seconds=0)
    try:
        stale_claim = stale_store.claim_message_result(message)
        current_claim = current_store.claim_message_result(message)

        assert stale_claim.event_key == current_claim.event_key == "event-stale"
        assert stale_claim.claim_token is not None
        assert current_claim.claim_token is not None
        assert stale_claim.claim_token != current_claim.claim_token
        assert not stale_store.mark_processed(
            stale_claim.event_key,
            claim_token=stale_claim.claim_token,
        )
        assert not stale_store.release_claim(
            stale_claim.event_key,
            claim_token=stale_claim.claim_token,
        )
        assert current_store.mark_processed(
            current_claim.event_key,
            claim_token=current_claim.claim_token,
        )
    finally:
        stale_store.close()
        current_store.close()


def test_event_store_migrates_claim_token_without_losing_existing_rows(
    tmp_path,
) -> None:
    db_path = tmp_path / "feishu_events.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE feishu_processed_events (
                event_key TEXT PRIMARY KEY,
                chat_id TEXT NOT NULL,
                message_id TEXT NOT NULL,
                first_seen_at TEXT NOT NULL,
                claimed_at TEXT,
                processed_at TEXT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO feishu_processed_events (
                event_key, chat_id, message_id, first_seen_at, claimed_at, processed_at
            ) VALUES (?, 'chat-1', ?, '2026-08-29T00:00:00+00:00', ?, ?)
            """,
            [
                (
                    "event-processed",
                    "message-processed",
                    None,
                    "2026-08-29T00:01:00+00:00",
                ),
                (
                    "event-claimed",
                    "message-claimed",
                    "2026-08-29T00:02:00+00:00",
                    None,
                ),
            ],
        )

    store = FeishuEventStore(str(db_path))
    try:
        with sqlite3.connect(db_path) as conn:
            columns = {
                row[1]
                for row in conn.execute(
                    "PRAGMA table_info(feishu_processed_events)"
                ).fetchall()
            }
            rows = conn.execute(
                """
                SELECT event_key, message_id, claimed_at, processed_at
                FROM feishu_processed_events
                ORDER BY event_key
                """
            ).fetchall()

        assert "claim_token" in columns
        assert rows == [
            (
                "event-claimed",
                "message-claimed",
                "2026-08-29T00:02:00+00:00",
                None,
            ),
            (
                "event-processed",
                "message-processed",
                None,
                "2026-08-29T00:01:00+00:00",
            ),
        ]
        assert (
            store.claim_message_result(
                build_message(
                    "done",
                    event_id="event-processed",
                    message_id="message-processed",
                )
            ).status
            == "processed"
        )
        migrated_claim = store.claim_message_result(
            build_message(
                "retry",
                event_id="event-claimed",
                message_id="message-claimed",
            )
        )
        assert migrated_claim.status == "claimed"
        assert migrated_claim.claim_token is not None
    finally:
        store.close()


def test_adapter_releases_failed_handler_claim_and_allows_retry() -> None:
    class FailingOnceGatewayClient(FakeGatewayClient):
        def __init__(self) -> None:
            super().__init__()
            self.create_attempts = 0

        async def create_task(
            self,
            *,
            agent_name: str,
            content: str,
            metadata: dict[str, str],
            attachments: list[dict[str, str]] | None = None,
            idempotency_key: str | None = None,
        ) -> dict[str, Any]:
            self.create_attempts += 1
            if self.create_attempts == 1:
                raise RuntimeError("transient gateway failure")
            return await super().create_task(
                agent_name=agent_name,
                content=content,
                metadata=metadata,
                attachments=attachments,
                idempotency_key=idempotency_key,
            )

    gateway = FailingOnceGatewayClient()
    event_store = FeishuEventStore(":memory:")
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=FakeFeishuClient(),
        default_agent_name="main",
        event_store=event_store,
        task_poll_interval=0.0,
    )
    message = build_message("hello", event_id="event-retry")

    async def scenario() -> None:
        try:
            await adapter.handle_message(message)
        except RuntimeError as exc:
            assert str(exc) == "transient gateway failure"
        else:
            raise AssertionError("Expected the first delivery to fail")
        await adapter.handle_message(message)
        await adapter.handle_message(message)
        await adapter.wait_for_watchers()

    try:
        asyncio.run(scenario())
    finally:
        event_store.close()

    assert gateway.create_attempts == 2
    assert len(gateway.created) == 1


def test_release_failure_does_not_replace_original_handler_error() -> None:
    class ReleaseFailureStore(FeishuEventStore):
        async def arelease_claim(
            self,
            event_key: str,
            *,
            claim_token: str,
        ) -> bool:
            del event_key, claim_token
            raise RuntimeError("release failed")

    class HandlerFailureAdapter(FeishuAdapter):
        async def _handle_claimed_message(self, message: FeishuMessage) -> None:
            del message
            raise LookupError("original handler failure")

    event_store = ReleaseFailureStore(":memory:")
    adapter = HandlerFailureAdapter(
        gateway_client=FakeGatewayClient(),
        feishu_client=FakeFeishuClient(),
        default_agent_name="main",
        event_store=event_store,
    )
    message = build_message("hello", event_id="event-release-failure")

    try:
        try:
            asyncio.run(adapter.handle_message(message))
        except LookupError as exc:
            assert str(exc) == "original handler failure"
        else:
            raise AssertionError("Expected the original handler failure")
        assert event_store.claim_message_result(message).status == "busy"
    finally:
        event_store.close()


def test_cancelled_handler_releases_claim_despite_repeated_cancellation() -> None:
    class BlockingReleaseStore(FeishuEventStore):
        def __init__(self) -> None:
            super().__init__(":memory:")
            self.release_started = asyncio.Event()
            self.release_allowed = asyncio.Event()
            self.release_finished = asyncio.Event()

        async def arelease_claim(
            self,
            event_key: str,
            *,
            claim_token: str,
        ) -> bool:
            self.release_started.set()
            await self.release_allowed.wait()
            released = await super().arelease_claim(
                event_key,
                claim_token=claim_token,
            )
            self.release_finished.set()
            return released

    class BlockingHandlerAdapter(FeishuAdapter):
        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.handler_started = asyncio.Event()
            self.handler_allowed = asyncio.Event()

        async def _handle_claimed_message(self, message: FeishuMessage) -> None:
            del message
            self.handler_started.set()
            await self.handler_allowed.wait()

    event_store = BlockingReleaseStore()
    adapter = BlockingHandlerAdapter(
        gateway_client=FakeGatewayClient(),
        feishu_client=FakeFeishuClient(),
        default_agent_name="main",
        event_store=event_store,
    )
    message = build_message("hello", event_id="event-cancelled")

    async def scenario() -> None:
        handling = asyncio.create_task(adapter.handle_message(message))
        await adapter.handler_started.wait()
        handling.cancel()
        await event_store.release_started.wait()
        handling.cancel()
        try:
            await handling
        except asyncio.CancelledError:
            pass
        else:
            raise AssertionError("Expected handler cancellation")

        event_store.release_allowed.set()
        await asyncio.wait_for(event_store.release_finished.wait(), timeout=1.0)
        reclaimed = event_store.claim_message_result(message)
        assert reclaimed.status == "claimed"
        assert reclaimed.event_key is not None
        assert reclaimed.claim_token is not None
        assert event_store.release_claim(
            reclaimed.event_key,
            claim_token=reclaimed.claim_token,
        )

    try:
        asyncio.run(scenario())
    finally:
        event_store.close()


def test_adapter_reports_lost_lease_instead_of_succeeding() -> None:
    class MarkFailureStore(FeishuEventStore):
        async def amark_processed(
            self,
            event_key: str,
            *,
            claim_token: str,
        ) -> bool:
            del event_key, claim_token
            return False

    event_store = MarkFailureStore(":memory:")
    feishu = FakeFeishuClient()
    adapter = FeishuAdapter(
        gateway_client=FakeGatewayClient(),
        feishu_client=feishu,
        default_agent_name="main",
        event_store=event_store,
    )

    try:
        try:
            asyncio.run(adapter.handle_message(build_message("/start")))
        except RuntimeError as exc:
            assert "lease was lost" in str(exc)
        else:
            raise AssertionError("Expected a lost lease error")
    finally:
        event_store.close()

    assert len(feishu.sent_messages) == 1


def test_filtered_and_empty_messages_are_still_deduplicated() -> None:
    event_store = FeishuEventStore(":memory:")
    gateway = FakeGatewayClient()
    adapter = FeishuAdapter(
        gateway_client=gateway,
        feishu_client=FakeFeishuClient(),
        default_agent_name="main",
        event_store=event_store,
        require_mention=True,
        bot_open_id="bot-open",
    )
    filtered = build_message(
        "hello",
        event_id="event-filtered",
        message_id="message-filtered",
        chat_id="group-1",
        chat_type="group",
    )
    empty = build_message(
        "   ",
        event_id="event-empty",
        message_id="message-empty",
    )

    async def scenario() -> None:
        await adapter.handle_message(filtered)
        await adapter.handle_message(empty)

    try:
        asyncio.run(scenario())
        assert event_store.claim_message_result(filtered).status == "processed"
        assert event_store.claim_message_result(empty).status == "processed"
    finally:
        event_store.close()

    assert gateway.created == []
