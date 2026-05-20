from __future__ import annotations

import asyncio
import io
import json
import os
import re
import sqlite3
import threading
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from ruyi_agent.channels.gateway_client import (
    GatewayArtifact,
    GatewayClientError,
    GatewayHTTPClient,
    GatewayTaskClient,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


RUNNING_STATES = {"pending", "running"}
TERMINAL_STATES = {"completed", "failed", "cancelled", "interrupted"}
FEISHU_TEXT_CHUNK_LIMIT = 4000
DEFAULT_FEISHU_MEDIA_MAX_BYTES = 30 * 1024 * 1024
FEISHU_ACK_MODES = {"reaction", "message", "off"}
MEDIA_TAG_PATTERN = re.compile(
    r"(?m)^(?P<indent>\s*)`?MEDIA:(?P<path>[^`\s]+)`?\s*$"
)


@dataclass(slots=True)
class FeishuMention:
    key: str
    name: str | None = None
    open_id: str | None = None
    user_id: str | None = None
    union_id: str | None = None


@dataclass(slots=True)
class FeishuMessage:
    event_id: str
    message_id: str
    chat_id: str
    chat_type: str
    user_id: str
    text: str
    sender_open_id: str | None = None
    sender_user_id: str | None = None
    sender_union_id: str | None = None
    thread_id: str | None = None
    mentions: list[FeishuMention] | None = None


@dataclass(slots=True)
class FeishuAttachment:
    filename: str
    content: bytes


@dataclass(slots=True)
class FeishuReactionReceipt:
    message_id: str
    reaction_id: str | None
    emoji_type: str


class FeishuClient(Protocol):
    async def run(
        self,
        handler: Callable[[FeishuMessage], Awaitable[None]],
    ) -> None: ...

    async def send_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
    ) -> None: ...

    async def send_markdown(
        self,
        *,
        chat_id: str,
        markdown: str,
        reply_to_message_id: str | None = None,
    ) -> None: ...

    async def send_file(
        self,
        *,
        chat_id: str,
        filename: str,
        content: bytes,
        reply_to_message_id: str | None = None,
    ) -> None: ...

    async def add_reaction(
        self,
        *,
        message_id: str,
        emoji_type: str,
    ) -> str | None: ...

    async def delete_reaction(
        self,
        *,
        message_id: str,
        reaction_id: str,
    ) -> None: ...


class FeishuAPIError(Exception):
    pass


class UnsupportedFeishuChatTypeError(ValueError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _env_bool(name: str, *, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _single_line_preview(text: str, *, limit: int = 80) -> str:
    preview = re.sub(r"\s+", " ", text).strip()
    if len(preview) <= limit:
        return preview
    return f"{preview[: limit - 3]}..."


def _feishu_help_text() -> str:
    return "\n".join(
        [
            "可用命令：",
            "`/help` - 查看命令说明",
            "`/start` - 检查 Feishu adapter 是否已连接",
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


def _split_feishu_text(text: str, limit: int = FEISHU_TEXT_CHUNK_LIMIT) -> list[str]:
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if len(current) + len(line) <= limit:
            current += line
            continue
        if current:
            chunks.append(current)
            current = ""
        if len(line) <= limit:
            current = line
            continue
        for char in line:
            if len(current) + len(char) > limit and current:
                chunks.append(current)
                current = char
            else:
                current += char
    if current:
        chunks.append(current)
    return chunks


def _looks_like_markdown(text: str) -> bool:
    if re.search(r"(?m)^\s{0,3}#{1,6}\s+\S", text):
        return True
    if re.search(r"(?m)^\s*(?:[-*+]|\d+\.)\s+\S", text):
        return True
    if re.search(r"(?m)^\s*\|.+\|\s*$", text):
        return True
    return any(token in text for token in ("```", "**", "__", "`", "]("))


def _build_feishu_markdown_card(markdown: str) -> dict[str, Any]:
    return {
        "config": {"wide_screen_mode": True},
        "elements": [
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": markdown,
                },
            }
        ],
    }


class FeishuEventStore:
    def __init__(self, db_path: str, *, claim_timeout_seconds: float = 300.0) -> None:
        self._db_path = db_path
        self._claim_timeout_seconds = max(0.0, claim_timeout_seconds)
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self._init_db()

    def claim_message(self, message: FeishuMessage) -> bool:
        event_key = message.event_id or message.message_id
        if not event_key:
            return True
        now_dt = datetime.now(UTC)
        now = now_dt.isoformat()
        with self._lock:
            row = self._conn.execute(
                """
                SELECT processed_at, claimed_at
                FROM feishu_processed_events
                WHERE event_key = ?
                """,
                (event_key,),
            ).fetchone()
            if row is None:
                self._conn.execute(
                    """
                    INSERT INTO feishu_processed_events (
                        event_key,
                        chat_id,
                        message_id,
                        first_seen_at,
                        claimed_at,
                        processed_at
                    ) VALUES (?, ?, ?, ?, ?, NULL)
                    """,
                    (event_key, message.chat_id, message.message_id, now, now),
                )
                self._conn.commit()
                return True
            processed_at, claimed_at = row
            if processed_at:
                return False
            if not self._claim_expired(claimed_at, now_dt):
                return False
            cursor = self._conn.execute(
                """
                UPDATE feishu_processed_events
                SET chat_id = ?, message_id = ?, claimed_at = ?
                WHERE event_key = ? AND processed_at IS NULL
                """,
                (message.chat_id, message.message_id, now, event_key),
            )
            self._conn.commit()
            return cursor.rowcount > 0

    def mark_processed(self, message: FeishuMessage) -> None:
        event_key = message.event_id or message.message_id
        if not event_key:
            return
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE feishu_processed_events
                SET processed_at = ?
                WHERE event_key = ?
                """,
                (now, event_key),
            )
            self._conn.commit()

    async def aclaim_message(self, message: FeishuMessage) -> bool:
        return await asyncio.to_thread(self.claim_message, message)

    async def amark_processed(self, message: FeishuMessage) -> None:
        await asyncio.to_thread(self.mark_processed, message)

    def _ensure_parent_dir(self) -> None:
        if self._db_path == ":memory:":
            return
        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS feishu_processed_events (
                    event_key TEXT PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    claimed_at TEXT,
                    processed_at TEXT
                )
                """
            )
            columns = {
                row[1]
                for row in self._conn.execute(
                    "PRAGMA table_info(feishu_processed_events)"
                ).fetchall()
            }
            if "claimed_at" not in columns:
                self._conn.execute(
                    "ALTER TABLE feishu_processed_events ADD COLUMN claimed_at TEXT"
                )
            self._conn.commit()

    def _claim_expired(self, claimed_at: str | None, now: datetime) -> bool:
        if not claimed_at:
            return True
        try:
            claimed_at_dt = datetime.fromisoformat(claimed_at)
        except ValueError:
            return True
        if claimed_at_dt.tzinfo is None:
            claimed_at_dt = claimed_at_dt.replace(tzinfo=UTC)
        return (now - claimed_at_dt).total_seconds() >= self._claim_timeout_seconds

    def close(self) -> None:
        with self._lock:
            self._conn.close()


class FeishuSDKClient:
    def __init__(
        self,
        *,
        app_id: str,
        app_secret: str,
        domain: str = "feishu",
        timeout: float = 10.0,
    ) -> None:
        self._app_id = app_id
        self._app_secret = app_secret
        self._domain = domain
        self._timeout = timeout
        self._client: Any | None = None

    async def run(
        self,
        handler: Callable[[FeishuMessage], Awaitable[None]],
    ) -> None:
        loop = asyncio.get_running_loop()
        await asyncio.to_thread(self._start_websocket, loop, handler)

    async def send_message(
        self,
        *,
        chat_id: str,
        text: str,
        reply_to_message_id: str | None = None,
    ) -> None:
        await self._send_message_content(
            chat_id=chat_id,
            msg_type="text",
            content=json.dumps({"text": text}, ensure_ascii=False),
            reply_to_message_id=reply_to_message_id,
        )

    async def send_markdown(
        self,
        *,
        chat_id: str,
        markdown: str,
        reply_to_message_id: str | None = None,
    ) -> None:
        await self._send_message_content(
            chat_id=chat_id,
            msg_type="interactive",
            content=json.dumps(
                _build_feishu_markdown_card(markdown),
                ensure_ascii=False,
            ),
            reply_to_message_id=reply_to_message_id,
        )

    async def send_file(
        self,
        *,
        chat_id: str,
        filename: str,
        content: bytes,
        reply_to_message_id: str | None = None,
    ) -> None:
        file_key = await self._upload_file(filename=filename, content=content)
        await self._send_message_content(
            chat_id=chat_id,
            msg_type="file",
            content=json.dumps({"file_key": file_key}, ensure_ascii=False),
            reply_to_message_id=reply_to_message_id,
        )

    async def add_reaction(
        self,
        *,
        message_id: str,
        emoji_type: str,
    ) -> str | None:
        lark = _import_lark_oapi()
        create_request_cls, create_body_cls, _, emoji_cls = _import_lark_reaction_types()
        client = self._get_client(lark)
        request = (
            create_request_cls.builder()
            .message_id(message_id)
            .request_body(
                create_body_cls.builder()
                .reaction_type(emoji_cls.builder().emoji_type(emoji_type).build())
                .build()
            )
            .build()
        )
        response = await client.im.v1.message_reaction.acreate(request)
        self._ensure_success(response, action=f"add {emoji_type} reaction")
        data = getattr(response, "data", None)
        reaction_id = getattr(data, "reaction_id", None)
        return reaction_id if isinstance(reaction_id, str) and reaction_id else None

    async def delete_reaction(
        self,
        *,
        message_id: str,
        reaction_id: str,
    ) -> None:
        lark = _import_lark_oapi()
        _, _, delete_request_cls, _ = _import_lark_reaction_types()
        client = self._get_client(lark)
        request = (
            delete_request_cls.builder()
            .message_id(message_id)
            .reaction_id(reaction_id)
            .build()
        )
        response = await client.im.v1.message_reaction.adelete(request)
        self._ensure_success(response, action="delete reaction")

    async def _send_message_content(
        self,
        *,
        chat_id: str,
        msg_type: str,
        content: str,
        reply_to_message_id: str | None = None,
    ) -> None:
        lark = _import_lark_oapi()
        create_request_cls, create_body_cls, reply_request_cls, reply_body_cls = (
            _import_lark_message_types()
        )
        client = self._get_client(lark)
        if reply_to_message_id:
            request = (
                reply_request_cls.builder()
                .message_id(reply_to_message_id)
                .request_body(
                    reply_body_cls.builder()
                    .msg_type(msg_type)
                    .content(content)
                    .reply_in_thread(True)
                    .build()
                )
                .build()
            )
            response = await client.im.v1.message.areply(request)
        else:
            request = (
                create_request_cls.builder()
                .receive_id_type("chat_id")
                .request_body(
                    create_body_cls.builder()
                    .receive_id(chat_id)
                    .msg_type(msg_type)
                    .content(content)
                    .build()
                )
                .build()
            )
            response = await client.im.v1.message.acreate(request)
        self._ensure_success(response, action=f"send {msg_type} message")

    async def _upload_file(self, *, filename: str, content: bytes) -> str:
        lark = _import_lark_oapi()
        create_file_request_cls, create_file_body_cls = _import_lark_file_types()
        client = self._get_client(lark)
        request = (
            create_file_request_cls.builder()
            .request_body(
                create_file_body_cls.builder()
                .file_type("stream")
                .file_name(filename)
                .file(io.BytesIO(content))
                .build()
            )
            .build()
        )
        response = await client.im.v1.file.acreate(request)
        self._ensure_success(response, action="upload file")
        data = getattr(response, "data", None)
        file_key = getattr(data, "file_key", None)
        if not isinstance(file_key, str) or not file_key:
            raise FeishuAPIError("Feishu upload file succeeded without file_key")
        return file_key

    def _ensure_success(self, response: Any, *, action: str) -> None:
        success = response.success() if hasattr(response, "success") else False
        if not success:
            code = getattr(response, "code", "")
            msg = getattr(response, "msg", "")
            raise FeishuAPIError(f"Feishu {action} failed: code={code} msg={msg}")

    def _start_websocket(
        self,
        loop: asyncio.AbstractEventLoop,
        handler: Callable[[FeishuMessage], Awaitable[None]],
    ) -> None:
        lark = _import_lark_oapi()

        def on_message(data: Any) -> None:
            try:
                payload = _sdk_object_to_dict(data)
                message = parse_feishu_message_event(payload)
            except Exception as exc:
                print(f"[feishu warning] failed to parse event: {exc}", flush=True)
                return
            if message is None:
                return
            future = asyncio.run_coroutine_threadsafe(handler(message), loop)

            def log_failure(done: asyncio.Future[Any]) -> None:
                try:
                    done.result()
                except Exception as exc:
                    print(f"[feishu warning] event handling failed: {exc}", flush=True)

            future.add_done_callback(log_failure)

        dispatcher = (
            lark.EventDispatcherHandler.builder("", "")
            .register_p2_im_message_receive_v1(on_message)
            .build()
        )
        ws_client = lark.ws.Client(
            self._app_id,
            self._app_secret,
            event_handler=dispatcher,
            log_level=getattr(lark.LogLevel, "INFO", None),
            domain=_resolve_lark_domain(lark, self._domain) or "https://open.feishu.cn",
        )
        ws_client.start()

    def _get_client(self, lark: Any) -> Any:
        if self._client is not None:
            return self._client
        builder = lark.Client.builder().app_id(self._app_id).app_secret(self._app_secret)
        domain = _resolve_lark_domain(lark, self._domain)
        if domain is not None and hasattr(builder, "domain"):
            builder = builder.domain(domain)
        if hasattr(builder, "timeout"):
            builder = builder.timeout(self._timeout)
        self._client = builder.build()
        return self._client


def _import_lark_oapi() -> Any:
    try:
        import lark_oapi as lark
    except ImportError as exc:
        raise FeishuAPIError(
            "Missing lark-oapi dependency. Run `uv sync` after installing project deps."
        ) from exc
    return lark


def _import_lark_message_types() -> tuple[Any, Any, Any, Any]:
    try:
        from lark_oapi.api.im.v1 import (
            CreateMessageRequest,
            CreateMessageRequestBody,
            ReplyMessageRequest,
            ReplyMessageRequestBody,
        )
    except ImportError as exc:
        raise FeishuAPIError("Installed lark-oapi does not expose im.v1 message API") from exc
    return (
        CreateMessageRequest,
        CreateMessageRequestBody,
        ReplyMessageRequest,
        ReplyMessageRequestBody,
    )


def _import_lark_file_types() -> tuple[Any, Any]:
    try:
        from lark_oapi.api.im.v1 import CreateFileRequest, CreateFileRequestBody
    except ImportError as exc:
        raise FeishuAPIError("Installed lark-oapi does not expose im.v1 file API") from exc
    return CreateFileRequest, CreateFileRequestBody


def _import_lark_reaction_types() -> tuple[Any, Any, Any, Any]:
    try:
        from lark_oapi.api.im.v1 import (
            CreateMessageReactionRequest,
            CreateMessageReactionRequestBody,
            DeleteMessageReactionRequest,
            Emoji,
        )
    except ImportError as exc:
        raise FeishuAPIError(
            "Installed lark-oapi does not expose im.v1 reaction API"
        ) from exc
    return (
        CreateMessageReactionRequest,
        CreateMessageReactionRequestBody,
        DeleteMessageReactionRequest,
        Emoji,
    )


def _resolve_lark_domain(lark: Any, domain: str) -> Any | None:
    domain_class = getattr(lark, "Domain", None)
    if domain_class is None:
        if domain.lower() == "lark":
            return "https://open.larksuite.com"
        return "https://open.feishu.cn"
    if domain.lower() == "lark":
        return getattr(domain_class, "Lark", None) or getattr(domain_class, "LARK", None)
    return getattr(domain_class, "Feishu", None) or getattr(domain_class, "FEISHU", None)


def _sdk_object_to_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        lark = _import_lark_oapi()
        payload = json.loads(lark.JSON.marshal(value))
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass
    if hasattr(value, "model_dump"):
        payload = value.model_dump()
        if isinstance(payload, dict):
            return payload
    if hasattr(value, "to_dict"):
        payload = value.to_dict()
        if isinstance(payload, dict):
            return payload
    payload = getattr(value, "__dict__", {})
    return payload if isinstance(payload, dict) else {}


def parse_feishu_message_event(payload: dict[str, Any]) -> FeishuMessage | None:
    event = payload.get("event")
    if not isinstance(event, dict):
        event = payload
    message = event.get("message")
    if not isinstance(message, dict):
        return None
    sender = event.get("sender")
    sender_id = sender.get("sender_id") if isinstance(sender, dict) else {}
    if not isinstance(sender_id, dict):
        sender_id = {}

    header = payload.get("header")
    if not isinstance(header, dict):
        header = {}

    message_id = _string_value(message.get("message_id"))
    chat_id = _string_value(message.get("chat_id"))
    if not message_id or not chat_id:
        return None

    sender_open_id = _string_value(sender_id.get("open_id"))
    sender_user_id = _string_value(sender_id.get("user_id"))
    sender_union_id = _string_value(sender_id.get("union_id"))
    user_id = sender_union_id or sender_user_id or sender_open_id
    if not user_id:
        return None

    msg_type = _string_value(message.get("message_type"))
    text = _extract_feishu_text(msg_type, message.get("content")).strip()
    mentions = _parse_mentions(message.get("mentions"))
    return FeishuMessage(
        event_id=_string_value(header.get("event_id")) or _string_value(payload.get("event_id")),
        message_id=message_id,
        chat_id=chat_id,
        chat_type=_string_value(message.get("chat_type")) or "p2p",
        user_id=user_id,
        text=text,
        sender_open_id=sender_open_id,
        sender_user_id=sender_user_id,
        sender_union_id=sender_union_id,
        thread_id=_string_value(message.get("thread_id"))
        or _string_value(message.get("root_id")),
        mentions=mentions,
    )


def _string_value(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _parse_mentions(value: Any) -> list[FeishuMention]:
    if not isinstance(value, list):
        return []
    mentions: list[FeishuMention] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        mention_id = item.get("id")
        if not isinstance(mention_id, dict):
            mention_id = {}
        mentions.append(
            FeishuMention(
                key=_string_value(item.get("key")),
                name=_string_value(item.get("name")) or None,
                open_id=_string_value(mention_id.get("open_id")) or None,
                user_id=_string_value(mention_id.get("user_id")) or None,
                union_id=_string_value(mention_id.get("union_id")) or None,
            )
        )
    return mentions


def _extract_feishu_text(message_type: str, raw_content: Any) -> str:
    content = raw_content
    if isinstance(raw_content, str):
        try:
            content = json.loads(raw_content)
        except json.JSONDecodeError:
            return raw_content
    if not isinstance(content, dict):
        return ""
    if message_type == "text":
        text = content.get("text")
        return text if isinstance(text, str) else ""
    if message_type == "post":
        fragments: list[str] = []
        _collect_post_text(content.get("content"), fragments)
        return "".join(fragments)
    text = content.get("text")
    return text if isinstance(text, str) else ""


def _collect_post_text(value: Any, fragments: list[str]) -> None:
    if isinstance(value, list):
        for item in value:
            _collect_post_text(item, fragments)
        return
    if not isinstance(value, dict):
        return
    tag = value.get("tag")
    if tag in {"text", "a"} and isinstance(value.get("text"), str):
        fragments.append(value["text"])
    elif tag == "at":
        name = value.get("user_name") or value.get("text")
        if isinstance(name, str):
            fragments.append(f"@{name}")
    for child_key in ("content", "children"):
        if child_key in value:
            _collect_post_text(value[child_key], fragments)


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
        self._event_store = event_store or FeishuEventStore(":memory:")
        self._require_mention = require_mention
        self._group_policy = group_policy
        self._allowed_users = allowed_users or set()
        self._allowed_groups = allowed_groups or set()
        self._bot_open_id = bot_open_id
        self._bot_user_id = bot_user_id
        self._bot_union_id = bot_union_id
        self._bot_name = bot_name
        self._task_poll_interval = task_poll_interval
        self._terminal_review_grace_checks = max(0, terminal_review_grace_checks)
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
        self._media_root = (
            Path(media_root)
            if media_root is not None
            else Path.cwd()
        ).expanduser().resolve()
        self._media_max_bytes = max(0, media_max_bytes)
        self._watchers: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._delivered_terminal_runs: dict[str, int] = {}
        self._task_reactions: dict[tuple[str, int], list[FeishuReactionReceipt]] = {}

    async def run_forever(self) -> None:
        await self._feishu_client.run(self.handle_message)

    async def handle_message(self, message: FeishuMessage) -> None:
        claimed = await self._event_store.aclaim_message(message)
        if not claimed:
            return
        await self._handle_claimed_message(message)
        await self._event_store.amark_processed(message)

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
            session_key = build_feishu_session_key(message, agent_name=active_agent_name)
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

        latest_task = None if force_new else await self._find_session_task(session_key)
        if latest_task is None and not force_new:
            latest_task = await self._find_latest_task(
                self._session_lookup_metadata(session_key),
                agent_name=active_agent_name,
            )
            if latest_task is not None:
                await self._bind_session(
                    session_key=session_key,
                    task_id=str(latest_task["task_id"]),
                    message=message,
                    agent_name=active_agent_name,
                )
        if latest_task is not None and self._task_has_pending_review(latest_task):
            await self._send_message(
                chat_id=message.chat_id,
                text=self._format_review_message(latest_task),
                reply_to_message_id=message.message_id,
            )
            return
        if latest_task is not None and latest_task["status"] in RUNNING_STATES:
            task_id = str(latest_task["task_id"])
            run_count = self._task_run_count(latest_task)
            await self._ack_running_task(
                message=message,
                task=latest_task,
            )
            self._ensure_watcher(
                task_id=task_id,
                chat_id=message.chat_id,
                run_count=run_count,
            )
            return
        if latest_task is not None and latest_task["status"] in TERMINAL_STATES:
            current_run_count = self._task_run_count(latest_task)
            if self._has_active_watcher(
                task_id=str(latest_task["task_id"]),
                run_count=current_run_count,
            ):
                await self._send_terminal_if_needed(
                    chat_id=message.chat_id,
                    task=latest_task,
                )

        if latest_task is None:
            task = await self._gateway_client.create_task(
                agent_name=active_agent_name,
                content=text,
                metadata=metadata,
            )
        else:
            task = await self._gateway_client.send_input(
                task_id=latest_task["task_id"],
                content=text,
            )
        task_id = str(task["task_id"])
        await self._bind_session(
            session_key=session_key,
            task_id=task_id,
            message=message,
            agent_name=active_agent_name,
        )
        await self._ack_task_accepted(
            task_id=task_id,
            run_count=self._task_run_count(task),
            message=message,
            fallback_text=f"已收到，task_id={task_id}",
        )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=self._task_run_count(task),
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
        if self._group_policy == "allowlist" and message.chat_id not in self._allowed_groups:
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
        task: dict[str, Any],
    ) -> None:
        task_id = str(task["task_id"])
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
                run_count=self._task_run_count(task),
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

    async def _extract_attachments(self, text: str) -> tuple[str, list[FeishuAttachment]]:
        stripped, attachments, warnings = await self._extract_media_attachments(text)
        if warnings:
            warning_text = "\n".join(warnings)
            stripped = f"{stripped}\n\n{warning_text}" if stripped.strip() else warning_text
        stripped = re.sub(r"\n{3,}", "\n\n", stripped).strip()
        return stripped, attachments

    async def _extract_media_attachments(
        self,
        text: str,
    ) -> tuple[str, list[FeishuAttachment], list[str]]:
        attachments: list[FeishuAttachment] = []
        warnings: list[str] = []
        lines: list[str] = []
        in_code_fence = False
        for line in text.splitlines(keepends=True):
            if line.lstrip().startswith("```"):
                in_code_fence = not in_code_fence
                lines.append(line)
                continue
            if in_code_fence:
                lines.append(line)
                continue
            line_ending = ""
            content = line
            if line.endswith("\r\n"):
                content = line[:-2]
                line_ending = "\r\n"
            elif line.endswith("\n"):
                content = line[:-1]
                line_ending = "\n"
            match = MEDIA_TAG_PATTERN.match(content)
            if match is None:
                lines.append(line)
                continue
            raw_path = match.group("path").rstrip(".,;)")
            attachment = await self._build_media_attachment(raw_path)
            if attachment is None:
                warnings.append(f"文件不存在、超过大小限制或不可访问：{raw_path}")
                lines.append(f"[missing file: {raw_path}]{line_ending}")
            else:
                attachments.append(attachment)
                lines.append(f"[Attachment: {attachment.filename}]{line_ending}")
        return "".join(lines), attachments, warnings

    async def _build_media_attachment(self, raw_path: str) -> FeishuAttachment | None:
        try:
            candidate = Path(raw_path).expanduser()
            if not candidate.is_absolute():
                return None
            resolved = candidate.resolve(strict=False)
            if not resolved.is_relative_to(self._media_root):
                return await self._download_gateway_artifact(raw_path)
            if not resolved.is_file():
                return await self._download_gateway_artifact(raw_path)
            if resolved.stat().st_size > self._media_max_bytes:
                return None
            content = resolved.read_bytes()
        except OSError:
            return await self._download_gateway_artifact(raw_path)
        return FeishuAttachment(
            filename=resolved.name,
            content=content,
        )

    async def _download_gateway_artifact(self, raw_path: str) -> FeishuAttachment | None:
        try:
            artifact = await self._gateway_client.download_artifact(path=raw_path)
        except Exception:
            return None
        attachment = self._attachment_from_gateway_artifact(artifact)
        if len(attachment.content) > self._media_max_bytes:
            return None
        return attachment

    def _attachment_from_gateway_artifact(self, artifact: GatewayArtifact) -> FeishuAttachment:
        return FeishuAttachment(
            filename=artifact.filename or "artifact",
            content=artifact.content,
        )

    async def wait_for_watchers(self) -> None:
        active = [task for task in self._watchers.values() if not task.done()]
        if active:
            await asyncio.gather(*active)

    async def _find_latest_task(
        self,
        metadata: dict[str, str],
        *,
        agent_name: str | None = None,
        use_default_agent: bool = True,
        limit: int = 1,
    ) -> dict[str, Any] | None:
        effective_agent_name = agent_name
        if effective_agent_name is None and use_default_agent:
            effective_agent_name = self._default_agent_name
        items = await self._gateway_client.list_tasks(
            agent_name=effective_agent_name,
            metadata=metadata,
            limit=limit,
        )
        return items[0] if items else None

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

    async def _set_active_agent_name(
        self,
        *,
        identity_key: str,
        agent_name: str,
        message: FeishuMessage,
    ) -> None:
        await self._session_store.abind_session(
            session_key=identity_key,
            platform="feishu",
            agent_name=agent_name,
            current_task_id="",
            chat_id=message.chat_id,
            user_id=message.user_id,
            thread_id=message.thread_id,
        )

    async def _handle_agent_command(
        self,
        *,
        message: FeishuMessage,
        identity_key: str,
        active_agent_name: str,
        text: str,
    ) -> None:
        parts = text.split(maxsplit=2)
        agents = await self._gateway_client.list_agents()
        public_agent_names = {
            str(agent.get("name"))
            for agent in agents
            if agent.get("public") is True and agent.get("name")
        }
        if len(parts) == 1:
            lines = [f"当前 agent：`{active_agent_name}`", "", "可用 agents："]
            for agent in sorted(agents, key=lambda item: str(item.get("name", ""))):
                if agent.get("public") is not True:
                    continue
                name = str(agent.get("name", ""))
                marker = " *" if name == active_agent_name else ""
                description = str(agent.get("description") or "")
                suffix = f" - {description}" if description else ""
                lines.append(f"- `{name}`{marker}{suffix}")
            lines.append("")
            lines.append("切换：`/agent <agent_name>`")
            await self._send_message(
                chat_id=message.chat_id,
                text="\n".join(lines),
                reply_to_message_id=message.message_id,
            )
            return
        requested_agent_name = self._resolve_agent_name(parts[1], public_agent_names)
        if requested_agent_name is None:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"未知或不可用 agent：{parts[1]}。使用 /agent 查看列表。",
                reply_to_message_id=message.message_id,
            )
            return
        await self._set_active_agent_name(
            identity_key=identity_key,
            agent_name=requested_agent_name,
            message=message,
        )
        agent_session_key = build_feishu_session_key(
            message,
            agent_name=requested_agent_name,
        )
        await self._session_store.aunbind_session(agent_session_key)
        initial_message = parts[2].strip() if len(parts) > 2 else ""
        if not initial_message:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"已切换到 agent={requested_agent_name}。发送消息即可开始新会话。",
                reply_to_message_id=message.message_id,
            )
            return
        task = await self._gateway_client.create_task(
            agent_name=requested_agent_name,
            content=initial_message,
            metadata=self._build_message_metadata(message, session_key=agent_session_key),
        )
        task_id = str(task["task_id"])
        await self._bind_session(
            session_key=agent_session_key,
            task_id=task_id,
            message=message,
            agent_name=requested_agent_name,
        )
        await self._ack_task_accepted(
            task_id=task_id,
            run_count=self._task_run_count(task),
            message=message,
            fallback_text=(
                f"已切换到 agent={requested_agent_name}，"
                f"并创建新会话 task_id={task_id}"
            ),
        )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=self._task_run_count(task),
        )

    def _resolve_agent_name(
        self,
        requested_agent_name: str,
        public_agent_names: set[str],
    ) -> str | None:
        if requested_agent_name in public_agent_names:
            return requested_agent_name
        normalized = requested_agent_name.replace("_", "")
        matches = [
            agent_name
            for agent_name in public_agent_names
            if agent_name.replace("_", "") == normalized
        ]
        if len(matches) == 1:
            return matches[0]
        return None

    async def _handle_resume_command(
        self,
        *,
        message: FeishuMessage,
        identity_key: str,
        text: str,
    ) -> None:
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            items = await self._gateway_client.list_tasks(
                agent_name=None,
                metadata=self._legacy_lookup_metadata(message),
                limit=10,
            )
            if not items:
                await self._send_message(
                    chat_id=message.chat_id,
                    text="暂无可恢复会话。",
                    reply_to_message_id=message.message_id,
                )
                return
            lines = ["最近会话："]
            for item in items:
                task_id = str(item.get("task_id", ""))
                agent_name = str(item.get("agent_name", ""))
                status = str(item.get("status", ""))
                result = str(item.get("last_result") or item.get("error") or "")
                preview = _single_line_preview(result) if result else ""
                suffix = f"\n   {preview}" if preview else ""
                lines.append(
                    f"- task_id={task_id} agent={agent_name} status={status}{suffix}"
                )
            lines.append("")
            lines.append("恢复：/resume <task_id>")
            await self._send_message(
                chat_id=message.chat_id,
                text="\n".join(lines),
                reply_to_message_id=message.message_id,
            )
            return
        task_id = parts[1].strip()
        if not task_id:
            return
        try:
            task = await self._gateway_client.get_task(task_id=task_id)
        except GatewayClientError as exc:
            if exc.status_code in {404, 410}:
                await self._send_message(
                    chat_id=message.chat_id,
                    text=f"没有找到会话：{task_id}",
                    reply_to_message_id=message.message_id,
                )
                return
            raise
        if not self._task_belongs_to_message(task, message):
            await self._send_message(
                chat_id=message.chat_id,
                text=f"不能恢复不属于当前 Feishu 会话的 task：{task_id}",
                reply_to_message_id=message.message_id,
            )
            return
        agent_name = str(task.get("agent_name") or self._default_agent_name)
        await self._set_active_agent_name(
            identity_key=identity_key,
            agent_name=agent_name,
            message=message,
        )
        session_key = build_feishu_session_key(message, agent_name=agent_name)
        await self._bind_session(
            session_key=session_key,
            task_id=task_id,
            message=message,
            agent_name=agent_name,
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=f"已恢复 agent={agent_name} task_id={task_id}。继续发送消息即可续聊。",
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
        task: dict[str, Any],
        message: FeishuMessage,
    ) -> bool:
        metadata = task.get("metadata")
        if not isinstance(metadata, dict):
            return False
        if metadata.get("channel") != "feishu":
            return False
        if str(metadata.get("chat_id")) != message.chat_id:
            return False
        if str(metadata.get("user_id")) != message.user_id:
            return False
        if message.thread_id is not None:
            return str(metadata.get("message_thread_id")) == message.thread_id
        return metadata.get("message_thread_id") in {None, ""}

    async def _find_session_task(self, session_key: str) -> dict[str, Any] | None:
        session = await self._session_store.aget_session(session_key)
        if session is None or not session.current_task_id:
            return None
        try:
            return await self._gateway_client.get_task(task_id=session.current_task_id)
        except GatewayClientError as exc:
            if exc.status_code in {401, 403, 404, 410}:
                await self._session_store.aunbind_session(session_key)
                return None
            raise

    async def _bind_session(
        self,
        *,
        session_key: str,
        task_id: str,
        message: FeishuMessage,
        agent_name: str | None = None,
    ) -> None:
        await self._session_store.abind_session(
            session_key=session_key,
            platform="feishu",
            agent_name=agent_name or self._default_agent_name,
            current_task_id=task_id,
            chat_id=message.chat_id,
            user_id=message.user_id,
            thread_id=message.thread_id,
        )

    def _ensure_watcher(self, *, task_id: str, chat_id: str, run_count: int) -> None:
        key = (task_id, run_count)
        existing = self._watchers.get(key)
        if existing is not None and not existing.done():
            return
        self._watchers[key] = asyncio.create_task(
            self._watch_task(
                task_id=task_id,
                chat_id=chat_id,
                expected_run_count=run_count,
            )
        )

    async def _watch_task(
        self,
        *,
        task_id: str,
        chat_id: str,
        expected_run_count: int,
    ) -> None:
        terminal_sent = False
        terminal_review_grace_checks_remaining = self._terminal_review_grace_checks
        key = (task_id, expected_run_count)
        try:
            while True:
                task = await self._gateway_client.get_task(task_id=task_id)
                current_run_count = self._task_run_count(task)
                if current_run_count > expected_run_count:
                    await self._clear_task_reactions(key)
                    return
                if self._task_has_pending_review(task):
                    await self._clear_task_reactions(key)
                    await self._send_message(
                        chat_id=chat_id,
                        text=self._format_review_message(task),
                    )
                    return
                status = task["status"]
                if status in TERMINAL_STATES:
                    if not terminal_sent:
                        await self._send_terminal_if_needed(chat_id=chat_id, task=task)
                        terminal_sent = True
                    if terminal_review_grace_checks_remaining <= 0:
                        return
                    terminal_review_grace_checks_remaining -= 1
                await asyncio.sleep(self._task_poll_interval)
        except Exception:
            await self._complete_task_reactions(key=key, status="failed")
            raise
        finally:
            self._watchers.pop(key, None)

    def _parse_review_command(self, text: str) -> dict[str, Any] | None:
        parts = text.split(maxsplit=2)
        if not parts:
            return None
        command = self._normalize_command_token(parts[0])
        if command in {"y", "yes", "/yes", "/approve"}:
            payload: dict[str, Any] = {"type": "approve"}
            if len(parts) >= 2:
                payload["review_id"] = parts[1]
            return payload
        if command in {"n", "no", "/no", "/reject"}:
            payload = {"type": "reject"}
            if len(parts) >= 2 and command == "/reject":
                payload["review_id"] = parts[1]
            if len(parts) > 2 and command == "/reject":
                payload["message"] = parts[2]
            return payload
        return None

    def _normalize_command_token(self, token: str) -> str:
        command = token.lower()
        if not command.startswith("/"):
            return command
        return command.split("@", 1)[0]

    async def _handle_review_command(
        self,
        *,
        message: FeishuMessage,
        metadata: dict[str, str],
        command: dict[str, Any],
    ) -> None:
        session_key = metadata["channel_session_key"]
        latest_task = await self._find_session_task(session_key)
        if latest_task is None:
            latest_task = await self._find_latest_task(
                self._session_lookup_metadata(session_key),
                use_default_agent=False,
            )
            if latest_task is not None:
                await self._bind_session(
                    session_key=session_key,
                    task_id=str(latest_task["task_id"]),
                    message=message,
                    agent_name=str(latest_task.get("agent_name") or self._default_agent_name),
                )
        if latest_task is None:
            await self._send_message(
                chat_id=message.chat_id,
                text="没有可审批的任务。",
                reply_to_message_id=message.message_id,
            )
            return
        pending_review = latest_task.get("pending_review")
        if not isinstance(pending_review, dict):
            await self._send_message(
                chat_id=message.chat_id,
                text="当前任务没有待审批项。",
                reply_to_message_id=message.message_id,
            )
            return
        review_id = str(command.get("review_id") or pending_review.get("review_id") or "")
        if not review_id:
            await self._send_message(
                chat_id=message.chat_id,
                text="待审批任务缺少 review_id。",
                reply_to_message_id=message.message_id,
            )
            return
        if command.get("review_id") is not None and pending_review.get("review_id") != review_id:
            await self._send_message(
                chat_id=message.chat_id,
                text=f"没有找到待审批 review：{review_id}",
                reply_to_message_id=message.message_id,
            )
            return
        decision: dict[str, Any] = {"type": command["type"]}
        if command.get("type") == "reject" and command.get("message"):
            decision["message"] = command["message"]
        task = await self._gateway_client.submit_review_decision(
            task_id=str(latest_task["task_id"]),
            review_id=review_id,
            decisions=[decision],
        )
        task_id = str(task["task_id"])
        await self._bind_session(
            session_key=session_key,
            task_id=task_id,
            message=message,
            agent_name=str(task.get("agent_name") or self._default_agent_name),
        )
        if self._ack_mode == "message":
            await self._send_message(
                chat_id=message.chat_id,
                text=f"审批已提交，task_id={task_id}",
                reply_to_message_id=message.message_id,
            )
        elif self._ack_mode == "reaction":
            await self._add_message_reaction(
                message_id=message.message_id,
                emoji_type=self._approval_reaction,
            )
            await self._add_task_processing_reaction(
                task_id=task_id,
                run_count=self._task_run_count(task),
                message_id=message.message_id,
            )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=self._task_run_count(task),
        )

    def _format_review_message(self, task: dict[str, Any]) -> str:
        task_id = task.get("task_id")
        pending_review = task.get("pending_review")
        if not isinstance(pending_review, dict):
            return f"任务等待审批，但缺少审批详情。\n\ntask_id={task_id}"
        review_id = pending_review.get("review_id")
        actions = pending_review.get("action_requests")
        configs = pending_review.get("review_configs")
        action_lines: list[str] = []
        if isinstance(actions, list):
            config_list = configs if isinstance(configs, list) else []
            for index, action in enumerate(actions, start=1):
                if not isinstance(action, dict):
                    continue
                config = (
                    config_list[index - 1]
                    if index - 1 < len(config_list)
                    and isinstance(config_list[index - 1], dict)
                    else {}
                )
                tool_name = action.get("name") or config.get("action_name") or "tool"
                args = action.get("args")
                action_lines.append(f"{index}. {tool_name} args={args}")
        actions_text = "\n".join(action_lines) if action_lines else "(no actions)"
        return (
            "任务等待人工审批。\n"
            f"review_id={review_id}\n"
            f"task_id={task_id}\n"
            f"{actions_text}\n\n"
            "快速批准：y\n"
            "快速拒绝：n\n"
            f"指定批准：/approve {review_id}\n"
            f"指定拒绝：/reject {review_id} 原因"
        )

    def _task_has_pending_review(self, task: dict[str, Any]) -> bool:
        pending_review = task.get("pending_review")
        return isinstance(pending_review, dict) and bool(pending_review)

    def _task_run_count(self, task: dict[str, Any]) -> int:
        value = task.get("run_count")
        if isinstance(value, int):
            return value
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _has_active_watcher(self, *, task_id: str, run_count: int) -> bool:
        watcher = self._watchers.get((task_id, run_count))
        return watcher is not None and not watcher.done()

    async def _send_terminal_if_needed(
        self,
        *,
        chat_id: str,
        task: dict[str, Any],
    ) -> None:
        task_id = str(task["task_id"])
        run_count = self._task_run_count(task)
        delivered_run_count = self._delivered_terminal_runs.get(task_id, 0)
        if run_count <= delivered_run_count:
            await self._clear_task_reactions((task_id, run_count))
            return
        await self._send_message(
            chat_id=chat_id,
            text=self._format_terminal_message(task),
        )
        self._delivered_terminal_runs[task_id] = run_count
        await self._complete_task_reactions(
            key=(task_id, run_count),
            status=str(task["status"]),
        )

    def _format_terminal_message(self, task: dict[str, Any]) -> str:
        status = task["status"]
        task_id = task["task_id"]
        if status == "completed":
            result = task.get("last_result") or "(empty result)"
            return f"{result}\n\ntask_id={task_id}"
        if status == "failed":
            error = task.get("error") or "unknown error"
            return f"任务失败：{error}\n\ntask_id={task_id}"
        if status == "cancelled":
            return f"任务已取消。\n\ntask_id={task_id}"
        return f"任务结束，状态={status}\n\ntask_id={task_id}"


def _is_feishu_group_chat(chat_type: str) -> bool:
    return chat_type not in {"p2p", "private", "dm"}


def _strip_mention_token(text: str, token: str) -> str:
    if not token:
        return text
    pattern = re.compile(rf"(?<!\S){re.escape(token)}(?=$|[\s,.:;!?，。：；！？])")
    return pattern.sub("", text)


def build_feishu_session_key(
    message: FeishuMessage,
    *,
    agent_name: str,
) -> str:
    if not _is_feishu_group_chat(message.chat_type):
        return f"agent:{agent_name}:feishu:dm:{message.chat_id}"
    thread_part = f":thread:{message.thread_id}" if message.thread_id is not None else ""
    return (
        f"agent:{agent_name}:feishu:group:"
        f"{message.chat_id}{thread_part}:user:{message.user_id}"
    )


def build_feishu_identity_key(message: FeishuMessage) -> str:
    if not _is_feishu_group_chat(message.chat_type):
        return f"feishu:dm:{message.chat_id}"
    thread_part = f":thread:{message.thread_id}" if message.thread_id is not None else ""
    return f"feishu:group:{message.chat_id}{thread_part}:user:{message.user_id}"


async def run_feishu_adapter() -> None:
    app_id = os.getenv("FEISHU_APP_ID")
    app_secret = os.getenv("FEISHU_APP_SECRET")
    if not app_id:
        raise SystemExit("Missing FEISHU_APP_ID")
    if not app_secret:
        raise SystemExit("Missing FEISHU_APP_SECRET")
    connection_mode = os.getenv("FEISHU_CONNECTION_MODE", "websocket").strip().lower()
    if connection_mode != "websocket":
        raise SystemExit("Only FEISHU_CONNECTION_MODE=websocket is supported for now")
    gateway_base_url = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
    gateway_bearer_token = os.getenv("GATEWAY_BEARER_TOKEN")
    if not gateway_bearer_token:
        raise SystemExit("Missing GATEWAY_BEARER_TOKEN")
    default_agent_name = os.getenv("FEISHU_DEFAULT_AGENT", "main")
    session_db_path = os.getenv(
        "FEISHU_SESSION_DB",
        os.getenv("CHANNEL_SESSION_DB", ".ruyi_agent/channel_sessions.sqlite3"),
    )
    event_db_path = os.getenv(
        "FEISHU_EVENT_DB",
        str(Path(session_db_path).expanduser().with_name("feishu_events.sqlite3")),
    )
    require_mention = _env_bool("FEISHU_REQUIRE_MENTION", default=True)
    group_policy = os.getenv("FEISHU_GROUP_POLICY", "open").strip().lower()
    bot_open_id = os.getenv("FEISHU_BOT_OPEN_ID") or None
    bot_user_id = os.getenv("FEISHU_BOT_USER_ID") or None
    bot_union_id = os.getenv("FEISHU_BOT_UNION_ID") or None
    bot_name = os.getenv("FEISHU_BOT_NAME") or None
    if (
        require_mention
        and group_policy != "disabled"
        and not any([bot_open_id, bot_user_id, bot_union_id, bot_name])
    ):
        raise SystemExit(
            "Missing Feishu bot identity for group mention checks. Set one of "
            "FEISHU_BOT_OPEN_ID, FEISHU_BOT_USER_ID, FEISHU_BOT_UNION_ID, "
            "FEISHU_BOT_NAME, or set FEISHU_GROUP_POLICY=disabled for DM-only use."
        )
    session_store = ChannelSessionStore(session_db_path)
    event_store = FeishuEventStore(event_db_path)
    try:
        adapter = FeishuAdapter(
            gateway_client=GatewayHTTPClient(
                base_url=gateway_base_url,
                bearer_token=gateway_bearer_token,
            ),
            feishu_client=FeishuSDKClient(
                app_id=app_id,
                app_secret=app_secret,
                domain=os.getenv("FEISHU_DOMAIN", "feishu"),
                timeout=float(os.getenv("FEISHU_API_TIMEOUT", "10")),
            ),
            default_agent_name=default_agent_name,
            session_store=session_store,
            event_store=event_store,
            require_mention=require_mention,
            group_policy=group_policy,
            allowed_users=set(_env_list("FEISHU_ALLOWED_USERS")),
            allowed_groups=set(_env_list("FEISHU_ALLOWED_GROUPS")),
            bot_open_id=bot_open_id,
            bot_user_id=bot_user_id,
            bot_union_id=bot_union_id,
            bot_name=bot_name,
            task_poll_interval=float(os.getenv("FEISHU_TASK_POLL_INTERVAL", "2")),
            terminal_review_grace_checks=int(
                os.getenv("FEISHU_TERMINAL_REVIEW_GRACE_CHECKS", "3")
            ),
            media_root=os.getenv("FEISHU_MEDIA_ROOT"),
            media_max_bytes=int(
                os.getenv("FEISHU_MEDIA_MAX_BYTES", str(DEFAULT_FEISHU_MEDIA_MAX_BYTES))
            ),
            ack_mode=os.getenv("FEISHU_ACK_MODE", "reaction"),
            reactions_enabled=_env_bool("FEISHU_REACTIONS", default=True),
            processing_reaction=os.getenv("FEISHU_PROCESSING_REACTION", "Typing"),
            approval_reaction=os.getenv("FEISHU_APPROVAL_REACTION", "CheckMark"),
            failure_reaction=os.getenv("FEISHU_FAILURE_REACTION", "CrossMark"),
        )
        await adapter.run_forever()
    finally:
        event_store.close()
        session_store.close()
