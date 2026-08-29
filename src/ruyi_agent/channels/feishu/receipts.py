from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Literal

from ruyi_agent.channels.event_receipts import (
    ChannelEventReceipt,
    ChannelEventReceiptSchema,
    ChannelEventReceiptStore,
)
from ruyi_agent.channels.feishu.client import FeishuMessage


FEISHU_RECEIPT_SCHEMA = ChannelEventReceiptSchema(
    table_name="feishu_processed_events",
    key_column="event_key",
    key_sql_type="TEXT",
)


@dataclass(frozen=True, slots=True)
class FeishuEventClaim:
    status: Literal["claimed", "processed", "busy"]
    event_key: str | None = None
    claimed_at: str | None = None
    claim_token: str | None = None


def _feishu_event_key(message: FeishuMessage) -> str | None:
    return message.event_id or message.message_id or None


class FeishuEventStore:
    """Feishu-compatible facade over the shared Channel receipt lease store."""

    def __init__(self, db_path: str, *, claim_timeout_seconds: float = 300.0) -> None:
        self._store = ChannelEventReceiptStore(
            db_path,
            schema=FEISHU_RECEIPT_SCHEMA,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    def claim_message(self, message: FeishuMessage) -> bool:
        return self.claim_message_result(message).status == "claimed"

    def claim_message_result(self, message: FeishuMessage) -> FeishuEventClaim:
        event_key = _feishu_event_key(message)
        if not event_key:
            return FeishuEventClaim(status="claimed")
        claim = self._store.claim(
            ChannelEventReceipt(
                event_key=event_key,
                channel_id=message.chat_id,
                message_id=message.message_id,
            )
        )
        return FeishuEventClaim(
            status=claim.status,
            event_key=str(claim.event_key),
            claimed_at=claim.claimed_at,
            claim_token=claim.claim_token,
        )

    def mark_processed(self, event_key: str, *, claim_token: str) -> bool:
        return self._store.mark_processed(event_key, claim_token=claim_token)

    def release_claim(self, event_key: str, *, claim_token: str) -> bool:
        return self._store.release(event_key, claim_token=claim_token)

    async def aclaim_message(self, message: FeishuMessage) -> bool:
        return await asyncio.to_thread(self.claim_message, message)

    async def aclaim_message_result(
        self,
        message: FeishuMessage,
    ) -> FeishuEventClaim:
        return await asyncio.to_thread(self.claim_message_result, message)

    async def amark_processed(self, event_key: str, *, claim_token: str) -> bool:
        return await asyncio.to_thread(
            self.mark_processed,
            event_key,
            claim_token=claim_token,
        )

    async def arelease_claim(self, event_key: str, *, claim_token: str) -> bool:
        return await asyncio.to_thread(
            self.release_claim,
            event_key,
            claim_token=claim_token,
        )

    def close(self) -> None:
        self._store.close()
