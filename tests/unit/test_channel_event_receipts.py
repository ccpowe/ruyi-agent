from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest

from ruyi_agent.channels.event_receipts import (
    ChannelEventReceipt,
    ChannelEventReceiptSchema,
    ChannelEventReceiptStore,
)


SCHEMA = ChannelEventReceiptSchema(
    table_name="test_channel_events",
    key_column="event_key",
    key_sql_type="TEXT",
)


def receipt(event_key: str, *, channel_id: str = "chat-1") -> ChannelEventReceipt:
    return ChannelEventReceipt(
        event_key=event_key,
        channel_id=channel_id,
        message_id="message-1",
    )


def test_receipt_store_owns_claim_busy_and_processed_transitions() -> None:
    store = ChannelEventReceiptStore(":memory:", schema=SCHEMA)
    try:
        claimed = store.claim(receipt("event-1"))
        assert claimed.status == "claimed"
        assert claimed.claim_token

        busy = store.claim(receipt("event-1"))
        assert busy.status == "busy"

        assert store.mark_processed(
            "event-1",
            claim_token=claimed.claim_token or "",
        )
        assert store.claim(receipt("event-1")).status == "processed"
    finally:
        store.close()


def test_reclaimed_lease_fences_stale_owner(tmp_path) -> None:
    db_path = tmp_path / "events.sqlite3"
    first = ChannelEventReceiptStore(
        str(db_path),
        schema=SCHEMA,
        claim_timeout_seconds=0,
    )
    second = ChannelEventReceiptStore(
        str(db_path),
        schema=SCHEMA,
        claim_timeout_seconds=0,
    )
    now = datetime(2026, 8, 29, tzinfo=UTC)
    try:
        old_claim = first.claim(receipt("event-1"), now=now)
        new_claim = second.claim(receipt("event-1"), now=now)
        assert old_claim.claim_token != new_claim.claim_token
        assert not first.release(
            "event-1",
            claim_token=old_claim.claim_token or "",
        )
        assert not first.mark_processed(
            "event-1",
            claim_token=old_claim.claim_token or "",
            now=now,
        )
        assert second.mark_processed(
            "event-1",
            claim_token=new_claim.claim_token or "",
            now=now,
        )
    finally:
        second.close()
        first.close()


def test_failed_claim_insert_rolls_back_transaction() -> None:
    store = ChannelEventReceiptStore(":memory:", schema=SCHEMA)
    try:
        invalid = ChannelEventReceipt(
            event_key="event-1",
            channel_id=None,  # type: ignore[arg-type]
            message_id="message-1",
        )
        with pytest.raises(sqlite3.IntegrityError):
            store.claim(invalid)

        recovered = store.claim(receipt("event-1"))
        assert recovered.status == "claimed"
    finally:
        store.close()


@pytest.mark.parametrize(
    "identifier",
    ["bad-name", "bad table", 'bad"name', "1bad", "表名"],
)
def test_receipt_schema_rejects_untrusted_sql_identifiers(identifier: str) -> None:
    with pytest.raises(ValueError, match="Invalid SQLite identifier"):
        ChannelEventReceiptSchema(
            table_name=identifier,
            key_column="event_key",
            key_sql_type="TEXT",
        )
