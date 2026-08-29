from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from ruyi_agent.channels.event_receipts import (
    ChannelEventReceipt,
    ChannelEventReceiptSchema,
    ChannelEventReceiptStore,
)
from ruyi_agent.channels.telegram.client import TelegramMessage


TELEGRAM_RECEIPT_SCHEMA = ChannelEventReceiptSchema(
    table_name="telegram_processed_updates",
    key_column="update_id",
    key_sql_type="INTEGER",
)


@dataclass(frozen=True, slots=True)
class TelegramUpdateClaim:
    status: Literal["claimed", "processed", "busy"]
    claimed_at: str | None = None
    claim_token: str | None = None


class TelegramUpdateStore:
    """Telegram-compatible facade over the shared Channel receipt lease store."""

    def __init__(
        self,
        db_path: str,
        *,
        claim_timeout_seconds: float = 300.0,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._clock = clock
        self._store = ChannelEventReceiptStore(
            db_path,
            schema=TELEGRAM_RECEIPT_SCHEMA,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    def claim_update(self, update: TelegramMessage) -> bool:
        return self.claim_update_result(update).status == "claimed"

    def claim_update_result(self, update: TelegramMessage) -> TelegramUpdateClaim:
        claim = self._store.claim(
            ChannelEventReceipt(
                event_key=update.update_id,
                channel_id=str(update.chat_id),
                message_id=str(update.message_id),
            ),
            now=self._clock() if self._clock is not None else None,
        )
        return TelegramUpdateClaim(
            status=claim.status,
            claimed_at=claim.claimed_at,
            claim_token=claim.claim_token,
        )

    def mark_processed(self, update_id: int, *, claim_token: str) -> bool:
        return self._store.mark_processed(
            update_id,
            claim_token=claim_token,
            now=self._clock() if self._clock is not None else None,
        )

    def release_claim(self, update_id: int, *, claim_token: str) -> bool:
        return self._store.release(update_id, claim_token=claim_token)

    async def aclaim_update(self, update: TelegramMessage) -> bool:
        return await asyncio.to_thread(self.claim_update, update)

    async def aclaim_update_result(
        self,
        update: TelegramMessage,
    ) -> TelegramUpdateClaim:
        return await asyncio.to_thread(self.claim_update_result, update)

    async def amark_processed(self, update_id: int, *, claim_token: str) -> bool:
        return await asyncio.to_thread(
            self.mark_processed,
            update_id,
            claim_token=claim_token,
        )

    async def arelease_claim(self, update_id: int, *, claim_token: str) -> bool:
        return await asyncio.to_thread(
            self.release_claim,
            update_id,
            claim_token=claim_token,
        )

    def close(self) -> None:
        self._store.close()
