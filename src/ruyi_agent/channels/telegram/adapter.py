from __future__ import annotations

import asyncio
import base64
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ruyi_agent.channels.gateway_client import GatewayTaskClient
from ruyi_agent.channels.gateway_dto import GatewayTask
from ruyi_agent.channels.adapter_lifecycle import ChannelAdapterLifecycle
from ruyi_agent.channels.media import warn_deprecated_media_root
from ruyi_agent.channels.presentation import (
    ChannelDeliveryCoordinator,
    ChannelDeliveryHooks,
    delivery_session_key,
)
from ruyi_agent.channels.task_watch import TERMINAL_TASK_STATES, TaskWatchManager
from ruyi_agent.channels.telegram.client import (
    DEFAULT_TELEGRAM_MEDIA_MAX_BYTES,
    KrokiMermaidRenderer,
    MermaidRenderError,
    TelegramAttachment,
    TelegramAttachmentDownloadWarning,
    TelegramBotAPIClient,
    TelegramClient,
    TelegramInboundAttachment,
    TelegramMessage,
    _gateway_attachment_kind,
)
from ruyi_agent.channels.telegram.delivery import TelegramArtifactDelivery
from ruyi_agent.channels.telegram.formatting import (
    _escape_mdv2,
    _format_telegram_markdown_v2,
    _split_telegram_message,
    _strip_mdv2,
)
from ruyi_agent.channels.telegram.identity import (
    build_telegram_identity_key,
    build_telegram_session_key,
)
from ruyi_agent.channels.telegram.network import (
    TelegramAPIError,
    TelegramFallbackResolver,
    TelegramFallbackTransport,
    TelegramNetworkError,
    UnsupportedTelegramChatTypeError,
    _looks_like_network_error,
)
from ruyi_agent.channels.telegram.presentation import extract_telegram_attachments
from ruyi_agent.channels.telegram.receipts import (
    TelegramUpdateClaim,
    TelegramUpdateStore as _TelegramUpdateStore,
)
from ruyi_agent.channels.turn import (
    AgentCommandTurn,
    ChannelTurnHandler,
    InboundTurn,
    ResumeCommandTurn,
    ReviewTurn,
    parse_review_command,
)
from ruyi_agent.storage.channel_delivery_store import (
    ChannelDeliveryIntent,
    ChannelDeliveryStore,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


__all__ = [
    "KrokiMermaidRenderer",
    "MermaidRenderError",
    "TelegramAdapter",
    "TelegramAPIError",
    "TelegramAttachment",
    "TelegramAttachmentDownloadWarning",
    "TelegramBotAPIClient",
    "TelegramClient",
    "TelegramFallbackResolver",
    "TelegramFallbackTransport",
    "TelegramInboundAttachment",
    "TelegramMessage",
    "TelegramNetworkError",
    "TelegramPollResult",
    "TelegramUpdateClaim",
    "TelegramUpdateStore",
    "UnsupportedTelegramChatTypeError",
    "_format_telegram_markdown_v2",
    "_looks_like_network_error",
    "_split_telegram_message",
    "build_telegram_identity_key",
    "build_telegram_session_key",
    "run_telegram_adapter",
]


TELEGRAM_CLAIM_RETRY_DELAY_SECONDS = 1.0


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _telegram_help_text() -> str:
    return "\n".join(
        [
            "可用命令：",
            "`/help` - 查看命令说明",
            "`/start` - 检查 Telegram adapter 是否已连接",
            "`/new <message>` - 在当前 agent 下开启新会话",
            "`/agent` - 查看可切换的 public agent",
            "`/agent <agent_name>` - 切换直连 agent，并开启该 agent 的新会话",
            "`/agent <agent_name> <message>` - 切换 agent 后直接创建新会话",
            "`/resume` - 展示最近会话",
            "`/resume <task_id>` - 恢复指定会话，已完成 task 也可续聊",
            "`/approve <review_id>` - 批准指定审批项",
            "`/reject <review_id> [reason]` - 拒绝指定审批项",
            "`y` / `yes` / `/yes` - 批准当前待审批项",
            "`n` / `no` / `/no` - 拒绝当前待审批项",
        ]
    )


class TelegramUpdateStore(_TelegramUpdateStore):
    """Preserve the adapter clock seam while sharing lease persistence."""

    def __init__(
        self,
        db_path: str,
        *,
        claim_timeout_seconds: float = 300.0,
    ) -> None:
        super().__init__(
            db_path,
            claim_timeout_seconds=claim_timeout_seconds,
            clock=lambda: _utc_now(),
        )


@dataclass(frozen=True, slots=True)
class TelegramPollResult:
    next_offset: int | None
    retry_after_delay: bool = False


class TelegramAdapter:
    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        telegram_client: TelegramClient,
        default_agent_name: str,
        session_store: ChannelSessionStore | None = None,
        update_store: TelegramUpdateStore | None = None,
        poll_timeout: int = 30,
        task_poll_interval: float = 2.0,
        terminal_review_grace_checks: int = 3,
        message_parse_mode: str | None = "MarkdownV2",
        mermaid_renderer: KrokiMermaidRenderer | None = None,
        media_max_bytes: int = DEFAULT_TELEGRAM_MEDIA_MAX_BYTES,
        delivery_store: ChannelDeliveryStore | None = None,
        media_root: object | None = None,
    ) -> None:
        warn_deprecated_media_root(media_root)
        self._gateway_client = gateway_client
        self._telegram_client = telegram_client
        self._default_agent_name = default_agent_name
        self._session_store = session_store or ChannelSessionStore(":memory:")
        self._turn_handler = ChannelTurnHandler(
            gateway_client=self._gateway_client,
            session_store=self._session_store,
        )
        self._update_store = update_store or TelegramUpdateStore(":memory:")
        self._poll_timeout = poll_timeout
        self._task_watch = TaskWatchManager(
            gateway_client=self._gateway_client,
            poll_interval=task_poll_interval,
            terminal_review_grace_checks=terminal_review_grace_checks,
        )
        self._delivery_store = delivery_store or ChannelDeliveryStore(
            self._session_store.db_path
        )
        self._owns_delivery_store = delivery_store is None
        self._delivery = ChannelDeliveryCoordinator(
            task_watch=self._task_watch,
            store=self._delivery_store,
            platform="telegram",
            lease_seconds=max(30.0, task_poll_interval * 3.0),
        )
        self._message_parse_mode = message_parse_mode
        self._mermaid_renderer = mermaid_renderer or KrokiMermaidRenderer(
            base_url=os.getenv("KROKI_BASE_URL", "https://kroki.io")
        )
        if media_max_bytes <= 0:
            raise ValueError("Telegram media_max_bytes must be positive")
        self._artifact_delivery = TelegramArtifactDelivery(
            gateway_client=self._gateway_client,
            telegram_client=self._telegram_client,
            max_bytes=media_max_bytes,
        )
        self._lifecycle = ChannelAdapterLifecycle(adapter_name="TelegramAdapter")

    async def start(self) -> int:
        return await self._lifecycle.start(
            lambda: self._delivery.recover(self._recovery_hooks)
        )

    async def close(self) -> None:
        await self._lifecycle.close(self._shutdown)

    async def _shutdown(self) -> None:
        await self._delivery.close()
        if self._owns_delivery_store:
            self._delivery_store.close()

    async def _send_message(
        self,
        *,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
    ) -> None:
        text, attachments = await self._extract_attachments(text)
        parse_mode = self._message_parse_mode
        if text.strip():
            formatted_text = text
            if parse_mode == "MarkdownV2":
                formatted_text = _format_telegram_markdown_v2(text)
            chunks = _split_telegram_message(formatted_text)
            total = len(chunks)
            for index, chunk in enumerate(chunks, start=1):
                chunk_text = chunk
                if total > 1:
                    prefix = f"({index}/{total})\n"
                    if parse_mode == "MarkdownV2":
                        prefix = _escape_mdv2(prefix)
                    chunk_text = prefix + chunk_text
                try:
                    await self._telegram_client.send_message(
                        chat_id=chat_id,
                        text=chunk_text,
                        reply_to_message_id=reply_to_message_id if index == 1 else None,
                        parse_mode=parse_mode,
                    )
                except Exception:
                    fallback = (
                        _strip_mdv2(chunk_text)
                        if parse_mode == "MarkdownV2"
                        else chunk_text
                    )
                    await self._telegram_client.send_message(
                        chat_id=chat_id,
                        text=fallback,
                        reply_to_message_id=reply_to_message_id if index == 1 else None,
                        parse_mode=None,
                    )
            reply_to_message_id = None

        for attachment in attachments:
            caption = attachment.caption
            if attachment.kind == "photo":
                try:
                    await self._telegram_client.send_photo(
                        chat_id=chat_id,
                        filename=attachment.filename,
                        content=attachment.content,
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode=None,
                    )
                except Exception as exc:
                    await self._send_attachment_error(
                        chat_id=chat_id,
                        filename=attachment.filename,
                        error=exc,
                    )
            elif attachment.kind == "document":
                try:
                    await self._telegram_client.send_document(
                        chat_id=chat_id,
                        filename=attachment.filename,
                        content=attachment.content,
                        caption=caption,
                        reply_to_message_id=reply_to_message_id,
                        parse_mode=None,
                    )
                except Exception as exc:
                    await self._send_attachment_error(
                        chat_id=chat_id,
                        filename=attachment.filename,
                        error=exc,
                    )
            reply_to_message_id = None

    async def _send_attachment_error(
        self,
        *,
        chat_id: int,
        filename: str,
        error: BaseException,
    ) -> None:
        await self._telegram_client.send_message(
            chat_id=chat_id,
            text=f"文件发送失败：{filename}\n{error}",
            parse_mode=None,
        )

    async def _extract_attachments(
        self,
        text: str,
    ) -> tuple[str, list[TelegramAttachment]]:
        return await extract_telegram_attachments(
            text,
            mermaid_renderer=self._mermaid_renderer,
        )

    async def _send_attachment(
        self,
        *,
        chat_id: int,
        attachment: TelegramAttachment,
    ) -> None:
        if attachment.kind == "photo":
            try:
                await self._telegram_client.send_photo(
                    chat_id=chat_id,
                    filename=attachment.filename,
                    content=attachment.content,
                    caption=attachment.caption,
                    parse_mode=None,
                )
            except Exception as exc:
                await self._send_attachment_error(
                    chat_id=chat_id,
                    filename=attachment.filename,
                    error=exc,
                )
            return
        try:
            await self._telegram_client.send_document(
                chat_id=chat_id,
                filename=attachment.filename,
                content=attachment.content,
                caption=attachment.caption,
                parse_mode=None,
            )
        except Exception as exc:
            await self._send_attachment_error(
                chat_id=chat_id,
                filename=attachment.filename,
                error=exc,
            )

    async def run_forever(self) -> None:
        await self.start()
        offset: int | None = None
        network_failures = 0
        try:
            while True:
                try:
                    poll_result = await self._poll_once_result(offset=offset)
                    offset = poll_result.next_offset
                    network_failures = 0
                    if poll_result.retry_after_delay:
                        await asyncio.sleep(TELEGRAM_CLAIM_RETRY_DELAY_SECONDS)
                except TelegramNetworkError as exc:
                    network_failures += 1
                    delay = min(60, 5 * (2 ** (network_failures - 1)))
                    print(
                        f"[telegram warning] network error: {exc}. "
                        f"retrying in {delay}s",
                        flush=True,
                    )
                    await asyncio.sleep(delay)
                except TelegramAPIError as exc:
                    print(f"[telegram warning] {exc}", flush=True)
                    await asyncio.sleep(1)
        finally:
            await self.close()

    async def poll_once(self, *, offset: int | None) -> int | None:
        result = await self._poll_once_result(offset=offset)
        return result.next_offset

    async def _poll_once_result(self, *, offset: int | None) -> TelegramPollResult:
        updates = await self._telegram_client.get_updates(
            offset=offset,
            timeout=self._poll_timeout,
        )
        next_offset = offset
        for update in sorted(updates, key=lambda item: item.update_id):
            claim = await self._update_store.aclaim_update_result(update)
            if claim.status == "processed":
                candidate = update.update_id + 1
                next_offset = (
                    candidate if next_offset is None else max(next_offset, candidate)
                )
                continue
            if claim.status == "busy":
                return TelegramPollResult(
                    next_offset=next_offset,
                    retry_after_delay=True,
                )
            if claim.claim_token is None:
                raise RuntimeError("Claimed Telegram update is missing its owner token")
            try:
                await self.handle_message(update)
            except Exception as exc:
                print(
                    "[telegram warning] update handling failed: "
                    f"update_id={update.update_id} error={exc}",
                    flush=True,
                )
                await self._update_store.arelease_claim(
                    update.update_id,
                    claim_token=claim.claim_token,
                )
                return TelegramPollResult(
                    next_offset=next_offset,
                    retry_after_delay=True,
                )
            marked = await self._update_store.amark_processed(
                update.update_id,
                claim_token=claim.claim_token,
            )
            if not marked:
                return TelegramPollResult(
                    next_offset=next_offset,
                    retry_after_delay=True,
                )
            candidate = update.update_id + 1
            next_offset = (
                candidate if next_offset is None else max(next_offset, candidate)
            )
        return TelegramPollResult(next_offset=next_offset)

    async def handle_message(self, message: TelegramMessage) -> None:
        text = message.text.strip()
        inbound_attachments = self._build_gateway_attachments(message)
        if message.attachment_warnings:
            warning_text = self._format_attachment_download_warnings(
                message.attachment_warnings
            )
            text = f"{text}\n\n{warning_text}" if text else warning_text
        if not text and not inbound_attachments:
            return
        if text == "/start":
            await self._send_message(
                chat_id=message.chat_id,
                text="已连接到 Gateway。直接发消息即可开始对话，使用 /new 可开启新会话。",
                reply_to_message_id=message.message_id,
            )
            return

        if text == "/help":
            await self._send_message(
                chat_id=message.chat_id,
                text=_telegram_help_text(),
                reply_to_message_id=message.message_id,
            )
            return

        try:
            identity_key = build_telegram_identity_key(message)
            active_agent_name = await self._get_active_agent_name(identity_key)
        except UnsupportedTelegramChatTypeError:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"暂不支持 Telegram chat_type={message.chat_type!r}。",
                reply_to_message_id=message.message_id,
            )
            return

        if text == "/agent" or text.startswith("/agent "):
            await self._handle_agent_command(
                message=message,
                identity_key=identity_key,
                active_agent_name=active_agent_name,
                text=text,
            )
            return

        if text == "/resume" or text.startswith("/resume "):
            await self._handle_resume_command(
                message=message,
                identity_key=identity_key,
                text=text,
            )
            return

        force_new = False
        if text == "/new":
            await self._send_message(
                chat_id=message.chat_id,
                text="请在 /new 后面附带首条消息，例如：/new 帮我总结这个仓库。",
                reply_to_message_id=message.message_id,
            )
            return
        if text.startswith("/new "):
            force_new = True
            text = text.removeprefix("/new ").strip()
            if not text:
                return

        try:
            session_key = build_telegram_session_key(
                message,
                agent_name=active_agent_name,
            )
        except UnsupportedTelegramChatTypeError:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"暂不支持 Telegram chat_type={message.chat_type!r}。",
                reply_to_message_id=message.message_id,
            )
            return

        metadata = self._build_message_metadata(message, session_key=session_key)
        review_command = self._parse_review_command(text)
        if review_command is not None:
            await self._handle_review_command(
                message=message,
                metadata=metadata,
                command=review_command,
            )
            return

        async def before_continue(task: GatewayTask) -> None:
            if task.status not in TERMINAL_TASK_STATES:
                return
            await self._delivery.ensure_terminal_delivery(
                task=task,
                session_key=session_key,
                chat_id=str(message.chat_id),
                task_id=task.task_id,
                run_count=task.run_count,
                hooks=self._delivery_hooks(message.chat_id),
            )

        outcome = await self._turn_handler.handle(
            InboundTurn(
                platform="telegram",
                session_key=session_key,
                agent_name=active_agent_name,
                content=text,
                metadata=metadata,
                fallback_metadata=self._legacy_lookup_metadata(message),
                chat_id=str(message.chat_id),
                user_id=str(message.user_id),
                thread_id=(
                    str(message.message_thread_id)
                    if message.message_thread_id is not None
                    else None
                ),
                attachments=inbound_attachments,
                force_new=force_new,
                idempotency_key=f"telegram:update:{message.update_id}",
            ),
            before_continue=before_continue,
        )
        if outcome.kind == "pending_review":
            await self._send_message(
                chat_id=message.chat_id,
                text=self._format_review_message(outcome.task),
                reply_to_message_id=message.message_id,
            )
            return
        if outcome.kind == "active":
            task_id = outcome.task.task_id
            self._ensure_watcher(
                task_id=task_id,
                chat_id=message.chat_id,
                run_count=outcome.task.run_count,
                session_key=delivery_session_key(
                    outcome.task,
                    platform="telegram",
                    chat_id=str(message.chat_id),
                ),
            )
            await self._send_message(
                chat_id=message.chat_id,
                text=(f"当前任务仍在处理中，请稍后再试。task_id={task_id}"),
                reply_to_message_id=message.message_id,
            )
            return
        task = outcome.task
        task_id = task.task_id
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=task.run_count,
            session_key=delivery_session_key(
                task,
                platform="telegram",
                chat_id=str(message.chat_id),
            ),
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=f"已收到，task_id={task_id}",
            reply_to_message_id=message.message_id,
        )

    async def wait_for_watchers(self) -> None:
        await self._delivery.wait()

    def _legacy_lookup_metadata(self, message: TelegramMessage) -> dict[str, str]:
        return {
            "channel": "telegram",
            "chat_id": str(message.chat_id),
            "user_id": str(message.user_id),
        }

    async def _get_active_agent_name(self, identity_key: str) -> str:
        session = await self._session_store.aget_session(identity_key)
        if session is None or not session.agent_name:
            return self._default_agent_name
        return session.agent_name

    async def _handle_agent_command(
        self,
        *,
        message: TelegramMessage,
        identity_key: str,
        active_agent_name: str,
        text: str,
    ) -> None:
        result = await self._turn_handler.handle_agent_command(
            AgentCommandTurn(
                platform="telegram",
                identity_key=identity_key,
                active_agent_name=active_agent_name,
                text=text,
                chat_id=str(message.chat_id),
                user_id=str(message.user_id),
                thread_id=(
                    str(message.message_thread_id)
                    if message.message_thread_id is not None
                    else None
                ),
                session_key_for_agent=lambda agent_name: build_telegram_session_key(
                    message, agent_name=agent_name
                ),
                metadata_for_session=lambda session_key: self._build_message_metadata(
                    message, session_key=session_key
                ),
                idempotency_key=f"telegram:update:{message.update_id}",
            )
        )
        if result.kind == "started" and result.task is not None:
            task_id = result.task.task_id
            self._ensure_watcher(
                task_id=task_id,
                chat_id=message.chat_id,
                run_count=result.task.run_count,
                session_key=delivery_session_key(
                    result.task,
                    platform="telegram",
                    chat_id=str(message.chat_id),
                ),
            )
        await self._send_message(
            chat_id=message.chat_id,
            text=result.message,
            reply_to_message_id=message.message_id,
        )

    async def _handle_resume_command(
        self,
        *,
        message: TelegramMessage,
        identity_key: str,
        text: str,
    ) -> None:
        result = await self._turn_handler.handle_resume_command(
            ResumeCommandTurn(
                platform="telegram",
                platform_label="Telegram",
                identity_key=identity_key,
                default_agent_name=self._default_agent_name,
                text=text,
                fallback_metadata=self._legacy_lookup_metadata(message),
                chat_id=str(message.chat_id),
                user_id=str(message.user_id),
                thread_id=(
                    str(message.message_thread_id)
                    if message.message_thread_id is not None
                    else None
                ),
                task_belongs_to_turn=lambda task: self._task_belongs_to_message(
                    task,
                    message,
                ),
                session_key_for_agent=lambda agent_name: build_telegram_session_key(
                    message, agent_name=agent_name
                ),
            )
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=result.message,
            reply_to_message_id=message.message_id,
        )

    def _build_message_metadata(
        self,
        message: TelegramMessage,
        *,
        session_key: str,
    ) -> dict[str, str]:
        metadata = {
            "channel": "telegram",
            "chat_id": str(message.chat_id),
            "user_id": str(message.user_id),
            "chat_type": message.chat_type,
            "channel_session_key": session_key,
        }
        if message.message_thread_id is not None:
            metadata["message_thread_id"] = str(message.message_thread_id)
        if message.reply_to_message_id is not None:
            metadata["reply_to_message_id"] = str(message.reply_to_message_id)
        return metadata

    def _build_gateway_attachments(
        self,
        message: TelegramMessage,
    ) -> list[dict[str, str]] | None:
        attachments = message.attachments or []
        if not attachments:
            return None
        return [
            {
                "name": attachment.filename,
                "content_type": attachment.content_type or "",
                "kind": _gateway_attachment_kind(attachment.kind),
                "data_base64": base64.b64encode(attachment.content).decode("ascii"),
            }
            for attachment in attachments
        ]

    def _format_attachment_download_warnings(
        self,
        warnings: list[TelegramAttachmentDownloadWarning],
    ) -> str:
        lines = ["Telegram 附件下载失败："]
        for warning in warnings:
            lines.append(
                f"- {warning.filename} kind={warning.kind} error={warning.error}"
            )
        return "\n".join(lines)

    def _task_belongs_to_message(
        self,
        task: GatewayTask,
        message: TelegramMessage,
    ) -> bool:
        metadata = task.metadata
        if metadata.get("channel") != "telegram":
            return False
        if str(metadata.get("chat_id")) != str(message.chat_id):
            return False
        if str(metadata.get("user_id")) != str(message.user_id):
            return False
        if message.message_thread_id is not None:
            return str(metadata.get("message_thread_id")) == str(
                message.message_thread_id
            )
        return metadata.get("message_thread_id") in {None, ""}

    def _ensure_watcher(
        self,
        *,
        task_id: str,
        chat_id: int,
        run_count: int,
        session_key: str | None = None,
    ) -> None:
        self._delivery.ensure_delivery(
            session_key=session_key or f"telegram:chat:{chat_id}",
            chat_id=str(chat_id),
            task_id=task_id,
            run_count=run_count,
            hooks=self._delivery_hooks(chat_id),
        )

    def _delivery_hooks(self, chat_id: int) -> ChannelDeliveryHooks:
        async def send_review(task: GatewayTask) -> None:
            await self._send_message(
                chat_id=chat_id,
                text=self._format_review_message(task),
            )

        async def send_terminal_message(task: GatewayTask) -> None:
            await self._send_message(
                chat_id=chat_id,
                text=self._delivery.terminal_presenter.format(task),
            )

        return ChannelDeliveryHooks(
            send_review=send_review,
            send_terminal_message=send_terminal_message,
            send_artifact=lambda task, artifact: self._artifact_delivery.send(
                task,
                artifact,
                chat_id=chat_id,
            ),
        )

    def _recovery_hooks(self, intent: ChannelDeliveryIntent) -> ChannelDeliveryHooks:
        return self._delivery_hooks(int(intent.chat_id))

    def _parse_review_command(self, text: str) -> dict[str, Any] | None:
        return parse_review_command(text)

    async def _handle_review_command(
        self,
        *,
        message: TelegramMessage,
        metadata: dict[str, str],
        command: dict[str, Any],
    ) -> None:
        session_key = metadata["channel_session_key"]
        result = await self._turn_handler.handle_review(
            ReviewTurn(
                platform="telegram",
                session_key=session_key,
                default_agent_name=self._default_agent_name,
                fallback_metadata=self._legacy_lookup_metadata(message),
                fallback_agent_name=self._default_agent_name,
                chat_id=str(message.chat_id),
                user_id=str(message.user_id),
                thread_id=(
                    str(message.message_thread_id)
                    if message.message_thread_id is not None
                    else None
                ),
                command=command,
            )
        )
        if result.kind != "submitted" or result.task is None:
            await self._send_message(
                chat_id=message.chat_id,
                text=result.message,
                reply_to_message_id=message.message_id,
            )
            return
        task = result.task
        task_id = task.task_id
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=task.run_count,
            session_key=delivery_session_key(
                task,
                platform="telegram",
                chat_id=str(message.chat_id),
            ),
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=result.message,
            reply_to_message_id=message.message_id,
        )

    def _format_review_message(self, task: GatewayTask) -> str:
        return self._delivery.review_presenter.format(task)

async def run_telegram_adapter() -> None:
    from ruyi_agent.channels.telegram.runner import run_telegram_adapter as run

    await run()
