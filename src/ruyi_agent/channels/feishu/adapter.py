from __future__ import annotations

import asyncio
import re
from contextlib import suppress
from pathlib import Path
from typing import Any

from ruyi_agent.channels.feishu.client import (
    DEFAULT_FEISHU_MEDIA_MAX_BYTES,
    FEISHU_ACK_MODES,
    FeishuAPIError,
    FeishuAttachment,
    FeishuClient,
    FeishuMention,
    FeishuMessage,
    FeishuReactionReceipt,
    FeishuSDKClient,
    UnsupportedFeishuChatTypeError,
    _consume_cleanup_result,
    _current_run_artifacts,
    _feishu_help_text,
    _looks_like_markdown,
    _split_feishu_text,
    parse_feishu_message_event,
)
from ruyi_agent.channels.feishu.identity import (
    _is_feishu_group_chat,
    _strip_mention_token,
    build_feishu_identity_key,
    build_feishu_session_key,
)
from ruyi_agent.channels.feishu.receipts import FeishuEventClaim, FeishuEventStore
from ruyi_agent.channels.gateway_client import GatewayArtifact, GatewayTaskClient
from ruyi_agent.channels.gateway_dto import GatewayTask
from ruyi_agent.channels.presentation import ChannelDeliveryCoordinator
from ruyi_agent.channels.task_watch import TERMINAL_TASK_STATES, TaskWatchManager
from ruyi_agent.channels.turn import (
    AgentCommandTurn,
    ChannelTurnHandler,
    InboundTurn,
    ResumeCommandTurn,
    ReviewTurn,
    parse_review_command,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


__all__ = [
    "FeishuAdapter",
    "FeishuAPIError",
    "FeishuEventClaim",
    "FeishuEventStore",
    "FeishuMention",
    "FeishuMessage",
    "FeishuSDKClient",
    "UnsupportedFeishuChatTypeError",
    "build_feishu_identity_key",
    "build_feishu_session_key",
    "parse_feishu_message_event",
    "run_feishu_adapter",
]


class FeishuAdapter:
    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        feishu_client: FeishuClient,
        default_agent_name: str,
        session_store: ChannelSessionStore | None = None,
        event_store: FeishuEventStore | None = None,
        require_mention: bool = True,
        group_policy: str = "open",
        allowed_users: set[str] | None = None,
        allowed_groups: set[str] | None = None,
        bot_open_id: str | None = None,
        bot_user_id: str | None = None,
        bot_union_id: str | None = None,
        bot_name: str | None = None,
        task_poll_interval: float = 2.0,
        terminal_review_grace_checks: int = 3,
        media_root: str | Path | None = None,
        media_max_bytes: int = DEFAULT_FEISHU_MEDIA_MAX_BYTES,
        ack_mode: str = "reaction",
        reactions_enabled: bool = True,
        processing_reaction: str = "Typing",
        approval_reaction: str = "CheckMark",
        failure_reaction: str = "CrossMark",
    ) -> None:
        self._gateway_client = gateway_client
        self._feishu_client = feishu_client
        self._default_agent_name = default_agent_name
        self._session_store = session_store or ChannelSessionStore(":memory:")
        self._turn_handler = ChannelTurnHandler(
            gateway_client=self._gateway_client,
            session_store=self._session_store,
        )
        self._event_store = event_store or FeishuEventStore(":memory:")
        self._require_mention = require_mention
        self._group_policy = group_policy
        self._allowed_users = allowed_users or set()
        self._allowed_groups = allowed_groups or set()
        self._bot_open_id = bot_open_id
        self._bot_user_id = bot_user_id
        self._bot_union_id = bot_union_id
        self._bot_name = bot_name
        self._task_watch = TaskWatchManager(
            gateway_client=self._gateway_client,
            poll_interval=task_poll_interval,
            terminal_review_grace_checks=terminal_review_grace_checks,
        )
        self._delivery = ChannelDeliveryCoordinator(task_watch=self._task_watch)
        normalized_ack_mode = ack_mode.strip().lower()
        self._ack_mode = (
            normalized_ack_mode
            if normalized_ack_mode in FEISHU_ACK_MODES
            else "reaction"
        )
        self._reactions_enabled = reactions_enabled
        self._processing_reaction = processing_reaction
        self._approval_reaction = approval_reaction
        self._failure_reaction = failure_reaction
        del media_root, media_max_bytes
        self._delivered_terminal_runs = (
            self._delivery.terminal_presenter.delivered_run_counts
        )
        self._task_reactions: dict[tuple[str, int], list[FeishuReactionReceipt]] = {}

    async def run_forever(self) -> None:
        await self._feishu_client.run(self.handle_message)

    async def handle_message(self, message: FeishuMessage) -> None:
        claim = await self._event_store.aclaim_message_result(message)
        if claim.status != "claimed":
            return
        if claim.event_key is None:
            await self._handle_claimed_message(message)
            return
        if claim.claim_token is None:
            raise RuntimeError("Claimed Feishu event is missing its owner token")
        try:
            await self._handle_claimed_message(message)
        except BaseException:
            with suppress(BaseException):
                release_task = asyncio.create_task(
                    self._event_store.arelease_claim(
                        claim.event_key,
                        claim_token=claim.claim_token,
                    )
                )
                release_task.add_done_callback(_consume_cleanup_result)
                await asyncio.shield(release_task)
            raise
        marked = await self._event_store.amark_processed(
            claim.event_key,
            claim_token=claim.claim_token,
        )
        if not marked:
            raise RuntimeError(
                "Feishu event lease was lost before processing could be recorded: "
                f"event_key={claim.event_key}"
            )

    async def _handle_claimed_message(self, message: FeishuMessage) -> None:
        if not self._is_allowed_message(message):
            return
        text = self._normalize_inbound_text(message)
        if not text:
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
                text=_feishu_help_text(),
                reply_to_message_id=message.message_id,
            )
            return

        try:
            identity_key = build_feishu_identity_key(message)
            active_agent_name = await self._get_active_agent_name(identity_key)
        except UnsupportedFeishuChatTypeError:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"暂不支持 Feishu chat_type={message.chat_type!r}。",
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
            session_key = build_feishu_session_key(
                message, agent_name=active_agent_name
            )
        except UnsupportedFeishuChatTypeError:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"暂不支持 Feishu chat_type={message.chat_type!r}。",
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
            run_count = task.run_count
            if self._has_active_watcher(
                task_id=task.task_id,
                run_count=run_count,
            ):
                await self._send_terminal_if_needed(
                    chat_id=message.chat_id,
                    task=task,
                )

        outcome = await self._turn_handler.handle(
            InboundTurn(
                platform="feishu",
                session_key=session_key,
                agent_name=active_agent_name,
                content=text,
                metadata=metadata,
                fallback_metadata=self._session_lookup_metadata(session_key),
                chat_id=message.chat_id,
                user_id=message.user_id,
                thread_id=message.thread_id,
                force_new=force_new,
                idempotency_key=(
                    f"feishu:event:{message.event_id or message.message_id}"
                ),
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
            run_count = outcome.task.run_count
            await self._ack_running_task(
                message=message,
                task=outcome.task,
            )
            self._ensure_watcher(
                task_id=task_id,
                chat_id=message.chat_id,
                run_count=run_count,
            )
            return
        task = outcome.task
        task_id = task.task_id
        await self._ack_task_accepted(
            task_id=task_id,
            run_count=task.run_count,
            message=message,
            fallback_text=f"已收到，task_id={task_id}",
        )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=task.run_count,
        )

    def _is_allowed_message(self, message: FeishuMessage) -> bool:
        if self._allowed_users and not {
            message.user_id,
            message.sender_open_id or "",
            message.sender_user_id or "",
            message.sender_union_id or "",
        }.intersection(self._allowed_users):
            return False
        if not _is_feishu_group_chat(message.chat_type):
            return True
        if self._group_policy == "disabled":
            return False
        if (
            self._group_policy == "allowlist"
            and message.chat_id not in self._allowed_groups
        ):
            return False
        if self._require_mention and not self._mentions_self(message):
            return False
        return True

    def _normalize_inbound_text(self, message: FeishuMessage) -> str:
        text = message.text.strip()
        if _is_feishu_group_chat(message.chat_type) and self._require_mention:
            text = self._strip_self_mentions(text, message.mentions or [])
        return text.strip()

    def _mentions_self(self, message: FeishuMessage) -> bool:
        mentions = message.mentions or []
        if not mentions or not self._has_bot_identity():
            return False
        for mention in mentions:
            if self._mention_matches_self(mention):
                return True
        return False

    def _has_bot_identity(self) -> bool:
        return any(
            [self._bot_open_id, self._bot_user_id, self._bot_union_id, self._bot_name]
        )

    def _mention_matches_self(self, mention: FeishuMention) -> bool:
        if self._bot_open_id and mention.open_id == self._bot_open_id:
            return True
        if self._bot_user_id and mention.user_id == self._bot_user_id:
            return True
        if self._bot_union_id and mention.union_id == self._bot_union_id:
            return True
        if self._bot_name and mention.name == self._bot_name:
            return True
        return False

    def _strip_self_mentions(self, text: str, mentions: list[FeishuMention]) -> str:
        for mention in mentions:
            if not self._mention_matches_self(mention):
                continue
            if mention.key:
                text = _strip_mention_token(text, mention.key)
            if mention.name:
                text = _strip_mention_token(text, f"@{mention.name}")
                text = re.sub(
                    rf"<at\b[^>]*>{re.escape(mention.name)}</at>",
                    "",
                    text,
                )
        return text

    async def _ack_task_accepted(
        self,
        *,
        task_id: str,
        run_count: int,
        message: FeishuMessage,
        fallback_text: str,
    ) -> None:
        if self._ack_mode == "message":
            await self._send_message(
                chat_id=message.chat_id,
                text=fallback_text,
                reply_to_message_id=message.message_id,
            )
            return
        if self._ack_mode == "reaction":
            await self._add_task_processing_reaction(
                task_id=task_id,
                run_count=run_count,
                message_id=message.message_id,
            )

    async def _ack_running_task(
        self,
        *,
        message: FeishuMessage,
        task: GatewayTask,
    ) -> None:
        task_id = task.task_id
        if self._ack_mode == "message":
            await self._send_message(
                chat_id=message.chat_id,
                text=f"当前任务仍在处理中，请稍后再试。task_id={task_id}",
                reply_to_message_id=message.message_id,
            )
            return
        if self._ack_mode == "reaction":
            await self._add_task_processing_reaction(
                task_id=task_id,
                run_count=task.run_count,
                message_id=message.message_id,
            )

    async def _add_task_processing_reaction(
        self,
        *,
        task_id: str,
        run_count: int,
        message_id: str,
    ) -> None:
        receipt = await self._add_message_reaction(
            message_id=message_id,
            emoji_type=self._processing_reaction,
        )
        if receipt is None:
            return
        key = (task_id, run_count)
        self._task_reactions.setdefault(key, []).append(receipt)

    async def _add_message_reaction(
        self,
        *,
        message_id: str,
        emoji_type: str,
    ) -> FeishuReactionReceipt | None:
        if not self._reactions_enabled or not message_id or not emoji_type:
            return None
        add_reaction = getattr(self._feishu_client, "add_reaction", None)
        if add_reaction is None:
            return None
        try:
            reaction_id = await add_reaction(
                message_id=message_id,
                emoji_type=emoji_type,
            )
        except Exception as exc:
            print(
                f"[feishu warning] add reaction failed: message_id={message_id} "
                f"emoji_type={emoji_type} error={exc}",
                flush=True,
            )
            return None
        return FeishuReactionReceipt(
            message_id=message_id,
            reaction_id=reaction_id,
            emoji_type=emoji_type,
        )

    async def _clear_task_reactions(self, key: tuple[str, int]) -> None:
        receipts = self._task_reactions.pop(key, [])
        for receipt in receipts:
            await self._delete_reaction(receipt)

    async def _complete_task_reactions(
        self,
        *,
        key: tuple[str, int],
        status: str,
    ) -> None:
        receipts = self._task_reactions.pop(key, [])
        should_mark_failure = status == "failed"
        for receipt in receipts:
            await self._delete_reaction(receipt)
            if should_mark_failure:
                await self._add_message_reaction(
                    message_id=receipt.message_id,
                    emoji_type=self._failure_reaction,
                )

    async def _delete_reaction(self, receipt: FeishuReactionReceipt) -> None:
        if not self._reactions_enabled or not receipt.reaction_id:
            return
        delete_reaction = getattr(self._feishu_client, "delete_reaction", None)
        if delete_reaction is None:
            return
        try:
            await delete_reaction(
                message_id=receipt.message_id,
                reaction_id=receipt.reaction_id,
            )
        except Exception as exc:
            print(
                f"[feishu warning] delete reaction failed: "
                f"message_id={receipt.message_id} "
                f"reaction_id={receipt.reaction_id} error={exc}",
                flush=True,
            )

    async def _send_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
    ) -> None:
        text, attachments = await self._extract_attachments(text)
        chunks = _split_feishu_text(text)
        total = len(chunks)
        if text.strip():
            for index, chunk in enumerate(chunks, start=1):
                chunk_text = chunk
                if total > 1:
                    chunk_text = f"({index}/{total})\n{chunk}"
                current_reply_to = reply_to_message_id if index == 1 else None
                if _looks_like_markdown(chunk_text):
                    try:
                        await self._feishu_client.send_markdown(
                            chat_id=chat_id,
                            markdown=chunk_text,
                            reply_to_message_id=current_reply_to,
                        )
                    except Exception:
                        await self._feishu_client.send_message(
                            chat_id=chat_id,
                            text=chunk_text,
                            reply_to_message_id=current_reply_to,
                        )
                else:
                    await self._feishu_client.send_message(
                        chat_id=chat_id,
                        text=chunk_text,
                        reply_to_message_id=current_reply_to,
                    )
            reply_to_message_id = None

        for attachment in attachments:
            try:
                await self._feishu_client.send_file(
                    chat_id=chat_id,
                    filename=attachment.filename,
                    content=attachment.content,
                    reply_to_message_id=reply_to_message_id,
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
        chat_id: str,
        filename: str,
        error: BaseException,
    ) -> None:
        await self._feishu_client.send_message(
            chat_id=chat_id,
            text=f"文件发送失败：{filename}\n{error}",
        )

    async def _extract_attachments(
        self, text: str
    ) -> tuple[str, list[FeishuAttachment]]:
        stripped = re.sub(r"\n{3,}", "\n\n", text).strip()
        return stripped, []

    def _attachment_from_gateway_artifact(
        self, artifact: GatewayArtifact
    ) -> FeishuAttachment:
        return FeishuAttachment(
            filename=artifact.filename or "artifact",
            content=artifact.content,
        )

    async def _send_task_artifacts(
        self,
        *,
        chat_id: str,
        task: GatewayTask,
    ) -> None:
        task_id = task.task_id
        run_count = task.run_count
        for artifact in _current_run_artifacts(task, run_count):
            artifact_id = artifact.artifact_id
            filename = artifact.name or artifact_id
            try:
                downloaded = await self._gateway_client.download_task_artifact(
                    task_id=task_id,
                    artifact_id=artifact_id,
                )
            except Exception as exc:
                await self._send_attachment_error(
                    chat_id=chat_id,
                    filename=filename,
                    error=exc,
                )
                continue
            attachment = self._attachment_from_gateway_artifact(downloaded)
            try:
                await self._feishu_client.send_file(
                    chat_id=chat_id,
                    filename=attachment.filename,
                    content=attachment.content,
                )
            except Exception as exc:
                await self._send_attachment_error(
                    chat_id=chat_id,
                    filename=attachment.filename,
                    error=exc,
                )

    async def wait_for_watchers(self) -> None:
        await self._delivery.wait()

    def _legacy_lookup_metadata(self, message: FeishuMessage) -> dict[str, str]:
        return {
            "channel": "feishu",
            "chat_id": message.chat_id,
            "user_id": message.user_id,
        }

    def _session_lookup_metadata(self, session_key: str) -> dict[str, str]:
        return {
            "channel_session_key": session_key,
        }

    async def _get_active_agent_name(self, identity_key: str) -> str:
        session = await self._session_store.aget_session(identity_key)
        if session is None or not session.agent_name:
            return self._default_agent_name
        return session.agent_name

    async def _handle_agent_command(
        self,
        *,
        message: FeishuMessage,
        identity_key: str,
        active_agent_name: str,
        text: str,
    ) -> None:
        result = await self._turn_handler.handle_agent_command(
            AgentCommandTurn(
                platform="feishu",
                identity_key=identity_key,
                active_agent_name=active_agent_name,
                text=text,
                chat_id=message.chat_id,
                user_id=message.user_id,
                thread_id=message.thread_id,
                session_key_for_agent=lambda agent_name: build_feishu_session_key(
                    message, agent_name=agent_name
                ),
                metadata_for_session=lambda session_key: self._build_message_metadata(
                    message, session_key=session_key
                ),
                idempotency_key=(
                    f"feishu:event:{message.event_id or message.message_id}"
                ),
            )
        )
        if result.kind != "started" or result.task is None:
            await self._send_message(
                chat_id=message.chat_id,
                text=result.message,
                reply_to_message_id=message.message_id,
            )
            return
        task = result.task
        task_id = task.task_id
        await self._ack_task_accepted(
            task_id=task_id,
            run_count=task.run_count,
            message=message,
            fallback_text=result.message,
        )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=task.run_count,
        )

    async def _handle_resume_command(
        self,
        *,
        message: FeishuMessage,
        identity_key: str,
        text: str,
    ) -> None:
        result = await self._turn_handler.handle_resume_command(
            ResumeCommandTurn(
                platform="feishu",
                platform_label="Feishu",
                identity_key=identity_key,
                default_agent_name=self._default_agent_name,
                text=text,
                fallback_metadata=self._legacy_lookup_metadata(message),
                chat_id=message.chat_id,
                user_id=message.user_id,
                thread_id=message.thread_id,
                task_belongs_to_turn=lambda task: self._task_belongs_to_message(
                    task,
                    message,
                ),
                session_key_for_agent=lambda agent_name: build_feishu_session_key(
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
        message: FeishuMessage,
        *,
        session_key: str,
    ) -> dict[str, str]:
        metadata = {
            "channel": "feishu",
            "chat_id": message.chat_id,
            "user_id": message.user_id,
            "chat_type": message.chat_type,
            "channel_session_key": session_key,
        }
        if message.sender_open_id:
            metadata["sender_open_id"] = message.sender_open_id
        if message.sender_user_id:
            metadata["sender_user_id"] = message.sender_user_id
        if message.sender_union_id:
            metadata["sender_union_id"] = message.sender_union_id
        if message.thread_id is not None:
            metadata["message_thread_id"] = message.thread_id
        return metadata

    def _task_belongs_to_message(
        self,
        task: GatewayTask,
        message: FeishuMessage,
    ) -> bool:
        metadata = task.metadata
        if metadata.get("channel") != "feishu":
            return False
        if str(metadata.get("chat_id")) != message.chat_id:
            return False
        if str(metadata.get("user_id")) != message.user_id:
            return False
        if message.thread_id is not None:
            return str(metadata.get("message_thread_id")) == message.thread_id
        return metadata.get("message_thread_id") in {None, ""}

    def _ensure_watcher(self, *, task_id: str, chat_id: str, run_count: int) -> None:
        key = (task_id, run_count)

        async def on_pending_review(task: GatewayTask) -> None:
            await self._clear_task_reactions(key)
            await self._send_message(
                chat_id=chat_id,
                text=self._format_review_message(task),
            )

        async def on_superseded(_: GatewayTask) -> None:
            await self._clear_task_reactions(key)

        async def on_error(_: Exception) -> None:
            await self._complete_task_reactions(key=key, status="failed")

        self._delivery.ensure_watch(
            task_id=task_id,
            run_count=run_count,
            on_pending_review=on_pending_review,
            on_terminal=lambda task: self._send_terminal_if_needed(
                chat_id=chat_id,
                task=task,
            ),
            on_superseded=on_superseded,
            on_error=on_error,
        )

    def _parse_review_command(self, text: str) -> dict[str, Any] | None:
        return parse_review_command(text)

    async def _handle_review_command(
        self,
        *,
        message: FeishuMessage,
        metadata: dict[str, str],
        command: dict[str, Any],
    ) -> None:
        session_key = metadata["channel_session_key"]
        result = await self._turn_handler.handle_review(
            ReviewTurn(
                platform="feishu",
                session_key=session_key,
                default_agent_name=self._default_agent_name,
                fallback_metadata=self._session_lookup_metadata(session_key),
                fallback_agent_name=None,
                chat_id=message.chat_id,
                user_id=message.user_id,
                thread_id=message.thread_id,
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
        if self._ack_mode == "message":
            await self._send_message(
                chat_id=message.chat_id,
                text=result.message,
                reply_to_message_id=message.message_id,
            )
        elif self._ack_mode == "reaction":
            await self._add_message_reaction(
                message_id=message.message_id,
                emoji_type=self._approval_reaction,
            )
            await self._add_task_processing_reaction(
                task_id=task_id,
                run_count=task.run_count,
                message_id=message.message_id,
            )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=task.run_count,
        )

    def _format_review_message(self, task: GatewayTask) -> str:
        return self._delivery.review_presenter.format(task)

    def _has_active_watcher(self, *, task_id: str, run_count: int) -> bool:
        return self._delivery.is_active(task_id=task_id, run_count=run_count)

    async def _send_terminal_if_needed(
        self,
        *,
        chat_id: str,
        task: GatewayTask | dict[str, Any],
    ) -> None:
        async def send_message(terminal_task: GatewayTask) -> None:
            await self._send_message(
                chat_id=chat_id,
                text=self._delivery.terminal_presenter.format(terminal_task),
            )

        async def send_artifacts(terminal_task: GatewayTask) -> None:
            await self._send_task_artifacts(chat_id=chat_id, task=terminal_task)

        async def clear_duplicate(terminal_task: GatewayTask) -> None:
            await self._clear_task_reactions(
                (terminal_task.task_id, terminal_task.run_count)
            )

        async def complete_reactions(terminal_task: GatewayTask) -> None:
            await self._complete_task_reactions(
                key=(terminal_task.task_id, terminal_task.run_count),
                status=terminal_task.status,
            )

        await self._delivery.terminal_presenter.present(
            task,
            send_message=send_message,
            send_artifacts=send_artifacts,
            on_duplicate=clear_duplicate,
            on_delivered=complete_reactions,
        )


async def run_feishu_adapter() -> None:
    from ruyi_agent.channels.feishu.runner import run_feishu_adapter as run

    await run()
