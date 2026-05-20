from __future__ import annotations

import asyncio
import base64
import os
import re
import socket
import sqlite3
import threading
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import httpx

from ruyi_agent.channels.gateway_client import (
    GatewayClientError,
    GatewayHTTPClient,
    GatewayTaskClient,
)
from ruyi_agent.storage.channel_session_store import ChannelSessionStore


RUNNING_STATES = {"pending", "running"}
WAITING_STATES = {"waiting_for_human"}
TERMINAL_STATES = {"completed", "failed", "cancelled", "interrupted"}
TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_MDV2_SPECIAL_CHARS = r"_*[]()~`>#+-=|{}.!"
FENCED_CODE_PATTERN = re.compile(r"```(?P<lang>[^\n`]*)\n?(?P<body>.*?)```", re.DOTALL)
INLINE_CODE_PATTERN = re.compile(r"`([^`\n]+)`")
LINK_PATTERN = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
HEADER_PATTERN = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)
BOLD_PATTERN = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
ITALIC_PATTERN = re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", re.DOTALL)
STRIKE_PATTERN = re.compile(r"~~(.+?)~~", re.DOTALL)
SPOILER_PATTERN = re.compile(r"\|\|(.+?)\|\|", re.DOTALL)
BLOCKQUOTE_PATTERN = re.compile(r"^(> ?.*)$", re.MULTILINE)
TABLE_SEPARATOR_PATTERN = re.compile(r"^\s*\|?[\s:-]+(?:\|[\s:-]+)+\|?\s*$")
MEDIA_TAG_PATTERN = re.compile(
    r"(?m)^(?P<indent>\s*)`?MEDIA:(?P<path>[^`\s]+)`?\s*$"
)
TELEGRAM_API_HOST = "api.telegram.org"
TELEGRAM_FALLBACK_SEED_IPS = ["149.154.167.220", "149.154.167.99", "149.154.167.50"]
DEFAULT_TELEGRAM_MEDIA_MAX_BYTES = 50 * 1024 * 1024
IMAGE_ATTACHMENT_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
}
NETWORK_ERROR_PATTERNS = (
    "temporary failure in name resolution",
    "name or service not known",
    "nodename nor servname provided",
    "getaddrinfo failed",
)


@dataclass(slots=True)
class TelegramAttachment:
    kind: str
    filename: str
    content: bytes
    caption: str | None = None
    content_type: str | None = None


@dataclass(slots=True)
class TelegramInboundAttachment:
    kind: str
    filename: str
    content_type: str | None
    content: bytes


@dataclass(slots=True)
class TelegramAttachmentDownloadWarning:
    kind: str
    filename: str
    error: str


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _escape_mdv2(text: str) -> str:
    escaped: list[str] = []
    for char in text:
        if char == "\\" or char in TELEGRAM_MDV2_SPECIAL_CHARS:
            escaped.append(f"\\{char}")
        else:
            escaped.append(char)
    return "".join(escaped)


def _strip_mdv2(text: str) -> str:
    text = re.sub(r"\\([\\_*[\]()~`>#+\-=|{}.!])", r"\1", text)
    text = text.replace("*", "").replace("_", "").replace("~", "")
    text = text.replace("||", "").replace("`", "")
    return text


def _protect_segments(
    text: str,
    pattern: re.Pattern[str],
    renderer,
    placeholders: list[str],
) -> str:
    def replace(match: re.Match[str]) -> str:
        placeholder = f"\u0000TG{len(placeholders)}\u0000"
        placeholders.append(renderer(match))
        return placeholder

    return pattern.sub(replace, text)


def _restore_placeholders(text: str, placeholders: list[str]) -> str:
    for index in range(len(placeholders) - 1, -1, -1):
        text = text.replace(f"\u0000TG{index}\u0000", placeholders[index])
    return text


def _parse_pipe_row(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in stripped.split("|")]


def _wrap_markdown_tables(text: str) -> str:
    lines = text.splitlines()
    result: list[str] = []
    index = 0
    in_code_fence = False
    while index < len(lines):
        line = lines[index]
        if line.strip().startswith("```"):
            in_code_fence = not in_code_fence
            result.append(line)
            index += 1
            continue
        if (
            not in_code_fence
            and index + 1 < len(lines)
            and "|" in line
            and "|" in lines[index + 1]
            and TABLE_SEPARATOR_PATTERN.match(lines[index + 1])
        ):
            headers = _parse_pipe_row(line)
            index += 2
            rows: list[list[str]] = []
            while index < len(lines) and "|" in lines[index] and lines[index].strip():
                rows.append(_parse_pipe_row(lines[index]))
                index += 1
            if headers and rows:
                for row in rows:
                    title = row[0] if row else "row"
                    result.append(f"**{title}**")
                    pairs = zip(headers[1:], row[1:], strict=False)
                    for header, value in pairs:
                        result.append(f"- {header}: {value}")
                    result.append("")
                if result and result[-1] == "":
                    result.pop()
                continue
        result.append(line)
        index += 1
    return "\n".join(result)


def _format_telegram_markdown_v2(text: str) -> str:
    text = _wrap_markdown_tables(text)
    placeholders: list[str] = []

    def stash(rendered: str) -> str:
        placeholder = f"\u0000TG{len(placeholders)}\u0000"
        placeholders.append(rendered)
        return placeholder

    def render_fence(match: re.Match[str]) -> str:
        lang = match.group("lang")
        body = match.group("body").replace("\\", "\\\\").replace("`", "\\`")
        return f"```{lang}\n{body}```"

    text = _protect_segments(text, FENCED_CODE_PATTERN, render_fence, placeholders)
    text = _protect_segments(
        text,
        INLINE_CODE_PATTERN,
        lambda match: f"`{match.group(1).replace('\\', '\\\\')}`",
        placeholders,
    )
    text = _protect_segments(
        text,
        LINK_PATTERN,
        lambda match: (
            f"[{_escape_mdv2(match.group(1))}]"
            f"({match.group(2).replace('\\', '\\\\').replace(')', '\\)')})"
        ),
        placeholders,
    )

    text = HEADER_PATTERN.sub(
        lambda match: stash(f"*{_escape_mdv2(match.group(2).strip('* '))}*"),
        text,
    )
    text = BOLD_PATTERN.sub(
        lambda match: stash(f"*{_escape_mdv2(match.group(1))}*"),
        text,
    )
    text = STRIKE_PATTERN.sub(
        lambda match: stash(f"~{_escape_mdv2(match.group(1))}~"),
        text,
    )
    text = SPOILER_PATTERN.sub(
        lambda match: stash(f"||{_escape_mdv2(match.group(1))}||"),
        text,
    )
    text = ITALIC_PATTERN.sub(
        lambda match: stash(f"_{_escape_mdv2(match.group(1))}_"),
        text,
    )
    text = BLOCKQUOTE_PATTERN.sub(
        lambda match: stash(f"> {_escape_mdv2(match.group(1)[1:].lstrip())}"),
        text,
    )

    text = _escape_mdv2(text)
    return _restore_placeholders(text, placeholders)


def _split_telegram_message(text: str, limit: int = TELEGRAM_MAX_MESSAGE_LENGTH) -> list[str]:
    if _utf16_len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if _utf16_len(current + line) <= limit:
            current += line
            continue
        if current:
            chunks.append(current)
            current = ""
        if _utf16_len(line) <= limit:
            current = line
            continue
        for char in line:
            if _utf16_len(current + char) > limit and current:
                chunks.append(current)
                current = char
            else:
                current += char
    if current:
        chunks.append(current)
    return chunks


class TelegramAPIError(Exception):
    pass


class TelegramNetworkError(TelegramAPIError):
    pass


class UnsupportedTelegramChatTypeError(ValueError):
    pass


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


def _single_line_preview(text: str, *, limit: int = 80) -> str:
    preview = re.sub(r"\s+", " ", text).strip()
    if len(preview) <= limit:
        return preview
    return f"{preview[: limit - 3]}..."


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


def _looks_like_network_error(exc: BaseException) -> bool:
    if isinstance(
        exc,
        (
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadError,
            httpx.ReadTimeout,
            httpx.RemoteProtocolError,
            httpx.PoolTimeout,
        ),
    ):
        return True
    text = str(exc).lower()
    return any(pattern in text for pattern in NETWORK_ERROR_PATTERNS)


class TelegramFallbackResolver:
    def __init__(
        self,
        *,
        fallback_ips: list[str] | None = None,
        timeout: float = 5.0,
    ) -> None:
        self._configured_ips = fallback_ips or []
        self._timeout = timeout
        self._discovered_ips: list[str] = []
        self._sticky_ip: str | None = None
        self._loaded = False

    def mark_success(self, ip: str) -> None:
        self._sticky_ip = ip

    def mark_failure(self, ip: str) -> None:
        if self._sticky_ip == ip:
            self._sticky_ip = None

    async def get_fallback_ips(self) -> list[str]:
        if not self._loaded:
            self._discovered_ips = await self._discover_fallback_ips()
            self._loaded = True
        ordered: list[str] = []
        if self._sticky_ip:
            ordered.append(self._sticky_ip)
        for ip in [*self._configured_ips, *self._discovered_ips, *TELEGRAM_FALLBACK_SEED_IPS]:
            if ip not in ordered:
                ordered.append(ip)
        return ordered

    async def _discover_fallback_ips(self) -> list[str]:
        discovered: list[str] = []
        doh_urls = [
            "https://cloudflare-dns.com/dns-query",
            "https://dns.google/resolve",
        ]
        async with httpx.AsyncClient(
            timeout=self._timeout,
            headers={"Accept": "application/dns-json"},
        ) as client:
            for url in doh_urls:
                try:
                    response = await client.get(
                        url,
                        params={"name": TELEGRAM_API_HOST, "type": "A"},
                    )
                    response.raise_for_status()
                    payload = response.json()
                except Exception:
                    continue
                answers = payload.get("Answer")
                if not isinstance(answers, list):
                    continue
                for answer in answers:
                    if not isinstance(answer, dict):
                        continue
                    value = answer.get("data")
                    if isinstance(value, str) and _is_ipv4(value) and value not in discovered:
                        discovered.append(value)
        return discovered


def _is_ipv4(value: str) -> bool:
    try:
        socket.inet_aton(value)
    except OSError:
        return False
    return value.count(".") == 3


class _AsyncBytesStream(httpx.AsyncByteStream):
    def __init__(self, content: bytes) -> None:
        self._content = content

    async def __aiter__(self):
        yield self._content


class TelegramFallbackTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        *,
        resolver: TelegramFallbackResolver,
        base_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._resolver = resolver
        self._base_transport = base_transport or httpx.AsyncHTTPTransport(retries=1)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        body = await request.aread()
        primary_request = self._build_request(request, request.url, body)
        try:
            return await self._base_transport.handle_async_request(primary_request)
        except Exception as exc:
            if request.url.host != TELEGRAM_API_HOST or not _looks_like_network_error(exc):
                raise

        fallback_ips = await self._resolver.get_fallback_ips()
        last_exc: BaseException | None = None
        for ip in fallback_ips:
            fallback_request = self._build_fallback_request(request, ip, body)
            try:
                response = await self._base_transport.handle_async_request(fallback_request)
            except Exception as exc:
                last_exc = exc
                self._resolver.mark_failure(ip)
                continue
            self._resolver.mark_success(ip)
            return response
        if last_exc is not None:
            raise last_exc
        raise

    def _build_request(
        self,
        request: httpx.Request,
        url: httpx.URL,
        body: bytes,
    ) -> httpx.Request:
        return httpx.Request(
            request.method,
            url,
            headers=request.headers,
            content=body,
            extensions=dict(request.extensions),
        )

    def _build_fallback_request(
        self,
        request: httpx.Request,
        ip: str,
        body: bytes,
    ) -> httpx.Request:
        url = request.url.copy_with(host=ip)
        fallback_request = self._build_request(request, url, body)
        fallback_request.headers["Host"] = TELEGRAM_API_HOST
        fallback_request.extensions["sni_hostname"] = TELEGRAM_API_HOST
        return fallback_request

    async def aclose(self) -> None:
        await self._base_transport.aclose()


class TelegramUpdateStore:
    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self._init_db()

    def claim_update(self, update: "TelegramMessage") -> bool:
        now = _utc_now_iso()
        with self._lock:
            try:
                self._conn.execute(
                    """
                    INSERT INTO telegram_processed_updates (
                        update_id,
                        chat_id,
                        message_id,
                        first_seen_at,
                        processed_at
                    ) VALUES (?, ?, ?, ?, NULL)
                    """,
                    (
                        update.update_id,
                        str(update.chat_id),
                        str(update.message_id),
                        now,
                    ),
                )
                self._conn.commit()
            except sqlite3.IntegrityError:
                return False
        return True

    def mark_processed(self, update_id: int) -> None:
        now = _utc_now_iso()
        with self._lock:
            self._conn.execute(
                """
                UPDATE telegram_processed_updates
                SET processed_at = ?
                WHERE update_id = ?
                """,
                (now, update_id),
            )
            self._conn.commit()

    async def aclaim_update(self, update: "TelegramMessage") -> bool:
        return await asyncio.to_thread(self.claim_update, update)

    async def amark_processed(self, update_id: int) -> None:
        await asyncio.to_thread(self.mark_processed, update_id)

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
                CREATE TABLE IF NOT EXISTS telegram_processed_updates (
                    update_id INTEGER PRIMARY KEY,
                    chat_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    first_seen_at TEXT NOT NULL,
                    processed_at TEXT
                )
                """
            )
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()


@dataclass(slots=True)
class TelegramMessage:
    update_id: int
    chat_id: int
    user_id: int
    text: str
    message_id: int
    chat_type: str = "private"
    message_thread_id: int | None = None
    reply_to_message_id: int | None = None
    attachments: list[TelegramInboundAttachment] | None = None
    attachment_warnings: list[TelegramAttachmentDownloadWarning] | None = None


class TelegramClient(Protocol):
    async def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int,
    ) -> list[TelegramMessage]: ...

    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None: ...

    async def send_photo(
        self,
        *,
        chat_id: int,
        filename: str,
        content: bytes,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None: ...

    async def send_document(
        self,
        *,
        chat_id: int,
        filename: str,
        content: bytes,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None: ...


def _string_or_none(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _gateway_attachment_kind(kind: str) -> str:
    if kind in {"image", "document", "audio", "video", "file"}:
        return kind
    return "file"


class TelegramBotAPIClient:
    def __init__(
        self,
        *,
        bot_token: str,
        timeout: float = 30.0,
        default_parse_mode: str | None = None,
        fallback_resolver: TelegramFallbackResolver | None = None,
    ) -> None:
        self._base_url = f"https://api.telegram.org/bot{bot_token}"
        self._timeout = timeout
        self._default_parse_mode = default_parse_mode
        self._fallback_resolver = fallback_resolver or TelegramFallbackResolver(
            fallback_ips=_env_list("TELEGRAM_FALLBACK_IPS"),
        )

    async def get_updates(
        self,
        *,
        offset: int | None,
        timeout: int,
    ) -> list[TelegramMessage]:
        payload = await self._request(
            "getUpdates",
            json={
                "timeout": timeout,
                "offset": offset,
                "allowed_updates": ["message"],
            },
        )
        updates = payload.get("result", [])
        messages: list[TelegramMessage] = []
        for update in updates:
            if not isinstance(update, dict):
                continue
            message = update.get("message")
            if not isinstance(message, dict):
                continue
            text = message.get("text")
            caption = message.get("caption")
            chat = message.get("chat")
            from_user = message.get("from")
            if not isinstance(chat, dict) or not isinstance(from_user, dict):
                continue
            if not isinstance(text, str):
                text = caption if isinstance(caption, str) else ""
            chat_id = chat.get("id")
            chat_type = chat.get("type")
            user_id = from_user.get("id")
            message_id = message.get("message_id")
            update_id = update.get("update_id")
            message_thread_id = message.get("message_thread_id")
            reply_to_message = message.get("reply_to_message")
            reply_to_message_id = (
                reply_to_message.get("message_id")
                if isinstance(reply_to_message, dict)
                else None
            )
            if not all(isinstance(value, int) for value in [chat_id, user_id, message_id, update_id]):
                continue
            attachments, attachment_warnings = await self._extract_inbound_attachments(message)
            if not text and not attachments:
                continue
            messages.append(
                TelegramMessage(
                    update_id=update_id,
                    chat_id=chat_id,
                    user_id=user_id,
                    text=text,
                    message_id=message_id,
                    chat_type=chat_type if isinstance(chat_type, str) else "private",
                    message_thread_id=(
                        message_thread_id if isinstance(message_thread_id, int) else None
                    ),
                    reply_to_message_id=(
                        reply_to_message_id if isinstance(reply_to_message_id, int) else None
                    ),
                    attachments=attachments,
                    attachment_warnings=attachment_warnings,
                )
            )
        return messages

    async def _extract_inbound_attachments(
        self,
        message: dict[str, Any],
    ) -> tuple[list[TelegramInboundAttachment], list[TelegramAttachmentDownloadWarning]]:
        specs: list[tuple[str, dict[str, Any], str | None, str | None]] = []
        document = message.get("document")
        if isinstance(document, dict):
            specs.append(
                (
                    "document",
                    document,
                    _string_or_none(document.get("file_name")),
                    _string_or_none(document.get("mime_type")),
                )
            )
        audio = message.get("audio")
        if isinstance(audio, dict):
            specs.append(
                (
                    "audio",
                    audio,
                    _string_or_none(audio.get("file_name")) or "audio",
                    _string_or_none(audio.get("mime_type")),
                )
            )
        video = message.get("video")
        if isinstance(video, dict):
            specs.append(
                (
                    "video",
                    video,
                    _string_or_none(video.get("file_name")) or "video.mp4",
                    _string_or_none(video.get("mime_type")),
                )
            )
        voice = message.get("voice")
        if isinstance(voice, dict):
            specs.append(
                (
                    "audio",
                    voice,
                    "voice.ogg",
                    _string_or_none(voice.get("mime_type")) or "audio/ogg",
                )
            )
        photos = message.get("photo")
        if isinstance(photos, list) and photos:
            photo = next(
                (item for item in reversed(photos) if isinstance(item, dict)),
                None,
            )
            if photo is not None:
                specs.append(("image", photo, "photo.jpg", "image/jpeg"))

        attachments: list[TelegramInboundAttachment] = []
        warnings: list[TelegramAttachmentDownloadWarning] = []
        for kind, item, filename, content_type in specs:
            file_id = item.get("file_id")
            if not isinstance(file_id, str):
                continue
            try:
                content = await self._download_file(file_id)
            except TelegramAPIError as exc:
                warnings.append(
                    TelegramAttachmentDownloadWarning(
                        kind=kind,
                        filename=filename or file_id,
                        error=str(exc),
                    )
                )
                continue
            attachments.append(
                TelegramInboundAttachment(
                    kind=kind,
                    filename=filename or file_id,
                    content_type=content_type,
                    content=content,
                )
            )
        return attachments, warnings

    async def _download_file(self, file_id: str) -> bytes:
        payload = await self._request("getFile", json={"file_id": file_id})
        result = payload.get("result")
        if not isinstance(result, dict) or not isinstance(result.get("file_path"), str):
            raise TelegramAPIError("Telegram getFile returned invalid payload")
        file_path = result["file_path"]
        url = f"{self._base_url.replace('/bot', '/file/bot')}/{file_path}"
        async with httpx.AsyncClient(
            timeout=self._timeout,
            transport=TelegramFallbackTransport(resolver=self._fallback_resolver),
        ) as client:
            try:
                response = await client.get(url)
            except httpx.HTTPError as exc:
                error_cls = TelegramNetworkError if _looks_like_network_error(exc) else TelegramAPIError
                raise error_cls(
                    f"Telegram file download failed: file_id={file_id} error={exc}"
                ) from exc
        if not response.is_success:
            raise TelegramAPIError(
                f"Telegram file download failed: status={response.status_code}"
            )
        return response.content

    async def send_message(
        self,
        *,
        chat_id: int,
        text: str,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_to_message_id is not None:
            payload["reply_to_message_id"] = reply_to_message_id
        effective_parse_mode = parse_mode
        if effective_parse_mode is not None:
            payload["parse_mode"] = effective_parse_mode
            await self._request("sendMessage", json=payload)
            return
        await self._request("sendMessage", json=payload)

    async def send_photo(
        self,
        *,
        chat_id: int,
        filename: str,
        content: bytes,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None:
        await self._request_multipart(
            "sendPhoto",
            file_field="photo",
            filename=filename,
            content=content,
            chat_id=chat_id,
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            parse_mode=parse_mode,
        )

    async def send_document(
        self,
        *,
        chat_id: int,
        filename: str,
        content: bytes,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> None:
        await self._request_multipart(
            "sendDocument",
            file_field="document",
            filename=filename,
            content=content,
            chat_id=chat_id,
            caption=caption,
            reply_to_message_id=reply_to_message_id,
            parse_mode=parse_mode,
        )

    async def _request(self, method: str, *, json: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            transport=TelegramFallbackTransport(resolver=self._fallback_resolver),
        ) as client:
            try:
                response = await client.post(f"/{method}", json=json)
            except httpx.HTTPError as exc:
                error_cls = TelegramNetworkError if _looks_like_network_error(exc) else TelegramAPIError
                raise error_cls(
                    f"Telegram API request failed: method={method} error={exc}"
                ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise TelegramAPIError("Telegram API returned invalid JSON") from exc
        if not response.is_success or not isinstance(payload, dict) or not payload.get("ok"):
            description = payload.get("description") if isinstance(payload, dict) else response.text
            raise TelegramAPIError(
                f"Telegram API call failed: method={method} error={description}"
            )
        return payload

    async def _request_multipart(
        self,
        method: str,
        *,
        file_field: str,
        filename: str,
        content: bytes,
        chat_id: int,
        caption: str | None = None,
        reply_to_message_id: int | None = None,
        parse_mode: str | None = None,
    ) -> dict[str, Any]:
        data: dict[str, Any] = {"chat_id": str(chat_id)}
        if caption:
            data["caption"] = caption
        if reply_to_message_id is not None:
            data["reply_to_message_id"] = str(reply_to_message_id)
        effective_parse_mode = parse_mode
        if effective_parse_mode is not None and caption:
            data["parse_mode"] = effective_parse_mode
        files = {
            file_field: (filename, content),
        }
        async with httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            transport=TelegramFallbackTransport(resolver=self._fallback_resolver),
        ) as client:
            try:
                response = await client.post(f"/{method}", data=data, files=files)
            except httpx.HTTPError as exc:
                error_cls = TelegramNetworkError if _looks_like_network_error(exc) else TelegramAPIError
                raise error_cls(
                    f"Telegram API request failed: method={method} error={exc}"
                ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise TelegramAPIError("Telegram API returned invalid JSON") from exc
        if not response.is_success or not isinstance(payload, dict) or not payload.get("ok"):
            description = payload.get("description") if isinstance(payload, dict) else response.text
            raise TelegramAPIError(
                f"Telegram API call failed: method={method} error={description}"
            )
        return payload


class MermaidRenderError(Exception):
    pass


class KrokiMermaidRenderer:
    def __init__(
        self,
        *,
        base_url: str = "https://kroki.io",
        timeout: float = 20.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    async def render_png(self, source: str) -> bytes:
        payload = {
            "diagram_source": source,
            "diagram_type": "mermaid",
            "output_format": "png",
        }
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            try:
                response = await client.post(f"{self._base_url}/", json=payload)
            except httpx.HTTPError as exc:
                raise MermaidRenderError(f"Kroki request failed: {exc}") from exc
        if not response.is_success:
            raise MermaidRenderError(
                f"Kroki render failed: status={response.status_code}"
            )
        return response.content


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
        media_root: str | Path | None = None,
    ) -> None:
        self._gateway_client = gateway_client
        self._telegram_client = telegram_client
        self._default_agent_name = default_agent_name
        self._session_store = session_store or ChannelSessionStore(":memory:")
        self._update_store = update_store or TelegramUpdateStore(":memory:")
        self._poll_timeout = poll_timeout
        self._task_poll_interval = task_poll_interval
        self._terminal_review_grace_checks = max(0, terminal_review_grace_checks)
        self._message_parse_mode = message_parse_mode
        self._mermaid_renderer = mermaid_renderer or KrokiMermaidRenderer(
            base_url=os.getenv("KROKI_BASE_URL", "https://kroki.io")
        )
        self._media_root = (
            Path(media_root)
            if media_root is not None
            else Path(os.getenv("LOCAL_BACKEND_ROOT", os.getcwd()))
        ).expanduser().resolve()
        self._media_max_bytes = int(
            os.getenv(
                "TELEGRAM_MEDIA_MAX_BYTES",
                str(DEFAULT_TELEGRAM_MEDIA_MAX_BYTES),
            )
        )
        self._watchers: dict[tuple[str, int], asyncio.Task[None]] = {}
        self._delivered_terminal_runs: dict[str, int] = {}

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
                        _strip_mdv2(chunk_text) if parse_mode == "MarkdownV2" else chunk_text
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
        attachments: list[TelegramAttachment] = []
        stripped = text

        mermaid_pattern = re.compile(
            r"```mermaid\n(?P<body>.*?)```",
            re.DOTALL | re.IGNORECASE,
        )
        mermaid_parts: list[str] = []
        last_end = 0
        mermaid_index = 0
        for match in mermaid_pattern.finditer(stripped):
            mermaid_parts.append(stripped[last_end : match.start()])
            source = match.group("body").strip()
            try:
                image_bytes = await self._mermaid_renderer.render_png(source)
            except MermaidRenderError:
                mermaid_parts.append(match.group(0))
                last_end = match.end()
                continue
            mermaid_index += 1
            attachments.append(
                TelegramAttachment(
                    kind="photo",
                    filename=f"mermaid_{mermaid_index}.png",
                    content=image_bytes,
                    caption=f"Mermaid diagram {mermaid_index}",
                )
            )
            mermaid_parts.append(f"[Mermaid diagram {mermaid_index}]")
            last_end = match.end()
        mermaid_parts.append(stripped[last_end:])
        stripped = "".join(mermaid_parts)

        stripped, media_attachments, media_warnings = await self._extract_media_attachments(
            stripped
        )
        attachments.extend(media_attachments)
        if media_warnings:
            warning_text = "\n".join(media_warnings)
            stripped = f"{stripped}\n\n{warning_text}" if stripped else warning_text

        stripped = re.sub(r"\n{3,}", "\n\n", stripped).strip()
        return stripped, attachments

    async def _extract_media_attachments(
        self,
        text: str,
    ) -> tuple[str, list[TelegramAttachment], list[str]]:
        attachments: list[TelegramAttachment] = []
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
                warnings.append(f"文件不存在或不可访问：{raw_path}")
                lines.append(f"[missing file: {raw_path}]{line_ending}")
            else:
                attachments.append(attachment)
                lines.append(f"[Attachment: {attachment.filename}]{line_ending}")
        return "".join(lines), attachments, warnings

    async def _build_media_attachment(self, raw_path: str) -> TelegramAttachment | None:
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
                return await self._download_gateway_artifact(raw_path)
            content = resolved.read_bytes()
        except OSError:
            return await self._download_gateway_artifact(raw_path)
        kind = "photo" if resolved.suffix.lower() in IMAGE_ATTACHMENT_EXTENSIONS else "document"
        return TelegramAttachment(
            kind=kind,
            filename=resolved.name,
            content=content,
            caption=resolved.name,
        )

    async def _download_gateway_artifact(self, raw_path: str) -> TelegramAttachment | None:
        try:
            artifact = await self._gateway_client.download_artifact(path=raw_path)
        except Exception:
            return None
        kind = (
            "photo"
            if Path(artifact.filename).suffix.lower() in IMAGE_ATTACHMENT_EXTENSIONS
            else "document"
        )
        return TelegramAttachment(
            kind=kind,
            filename=artifact.filename,
            content=artifact.content,
            caption=artifact.filename,
            content_type=artifact.content_type,
        )

    async def run_forever(self) -> None:
        offset: int | None = None
        network_failures = 0
        while True:
            try:
                offset = await self.poll_once(offset=offset)
                network_failures = 0
            except TelegramNetworkError as exc:
                network_failures += 1
                delay = min(60, 5 * (2 ** (network_failures - 1)))
                print(
                    f"[telegram warning] network error: {exc}. retrying in {delay}s",
                    flush=True,
                )
                await asyncio.sleep(delay)
            except TelegramAPIError as exc:
                print(f"[telegram warning] {exc}", flush=True)
                await asyncio.sleep(1)

    async def poll_once(self, *, offset: int | None) -> int | None:
        updates = await self._telegram_client.get_updates(
            offset=offset,
            timeout=self._poll_timeout,
        )
        next_offset = offset
        for update in updates:
            candidate = update.update_id + 1
            next_offset = candidate if next_offset is None else max(next_offset, candidate)
            claimed = await self._update_store.aclaim_update(update)
            if not claimed:
                continue
            try:
                await self.handle_message(update)
            except Exception as exc:
                print(
                    "[telegram warning] update handling failed: "
                    f"update_id={update.update_id} error={exc}",
                    flush=True,
                )
            finally:
                await self._update_store.amark_processed(update.update_id)
        return next_offset

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

        latest_task = None if force_new else await self._find_session_task(session_key)
        if latest_task is None and not force_new:
            latest_task = await self._find_latest_task(
                self._legacy_lookup_metadata(message),
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
            await self._send_message(
                chat_id=message.chat_id,
                text=f"当前任务仍在处理中，请稍后再试。task_id={latest_task['task_id']}",
                reply_to_message_id=message.message_id,
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
                attachments=inbound_attachments,
            )
        else:
            task = await self._gateway_client.send_input(
                task_id=latest_task["task_id"],
                content=text,
                attachments=inbound_attachments,
            )
        task_id = str(task["task_id"])
        await self._bind_session(
            session_key=session_key,
            task_id=task_id,
            message=message,
            agent_name=active_agent_name,
        )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=self._task_run_count(task),
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=f"已收到，task_id={task_id}",
            reply_to_message_id=message.message_id,
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
        limit: int = 1,
    ) -> dict[str, Any] | None:
        items = await self._gateway_client.list_tasks(
            agent_name=agent_name or self._default_agent_name,
            metadata=metadata,
            limit=limit,
        )
        return items[0] if items else None

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

    async def _set_active_agent_name(
        self,
        *,
        identity_key: str,
        agent_name: str,
        message: TelegramMessage,
    ) -> None:
        await self._session_store.abind_session(
            session_key=identity_key,
            platform="telegram",
            agent_name=agent_name,
            current_task_id="",
            chat_id=str(message.chat_id),
            user_id=str(message.user_id),
            thread_id=(
                str(message.message_thread_id)
                if message.message_thread_id is not None
                else None
            ),
        )

    async def _handle_agent_command(
        self,
        *,
        message: TelegramMessage,
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
        agent_session_key = build_telegram_session_key(
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
            metadata=self._build_message_metadata(
                message,
                session_key=agent_session_key,
            ),
        )
        task_id = str(task["task_id"])
        await self._bind_session(
            session_key=agent_session_key,
            task_id=task_id,
            message=message,
            agent_name=requested_agent_name,
        )
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=self._task_run_count(task),
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=f"已切换到 agent={requested_agent_name}，并创建新会话 task_id={task_id}",
            reply_to_message_id=message.message_id,
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
        message: TelegramMessage,
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
                text=f"不能恢复不属于当前 Telegram 会话的 task：{task_id}",
                reply_to_message_id=message.message_id,
            )
            return
        agent_name = str(task.get("agent_name") or self._default_agent_name)
        await self._set_active_agent_name(
            identity_key=identity_key,
            agent_name=agent_name,
            message=message,
        )
        session_key = build_telegram_session_key(message, agent_name=agent_name)
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
        task: dict[str, Any],
        message: TelegramMessage,
    ) -> bool:
        metadata = task.get("metadata")
        if not isinstance(metadata, dict):
            return False
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
        message: TelegramMessage,
        agent_name: str | None = None,
    ) -> None:
        await self._session_store.abind_session(
            session_key=session_key,
            platform="telegram",
            agent_name=agent_name or self._default_agent_name,
            current_task_id=task_id,
            chat_id=str(message.chat_id),
            user_id=str(message.user_id),
            thread_id=(
                str(message.message_thread_id)
                if message.message_thread_id is not None
                else None
            ),
        )

    def _ensure_watcher(self, *, task_id: str, chat_id: int, run_count: int) -> None:
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
        chat_id: int,
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
                    return
                if self._task_has_pending_review(task):
                    await self._send_message(
                        chat_id=chat_id,
                        text=self._format_review_message(task),
                    )
                    return
                status = task["status"]
                if status in TERMINAL_STATES:
                    if not terminal_sent:
                        await self._send_terminal_if_needed(
                            chat_id=chat_id,
                            task=task,
                        )
                        terminal_sent = True
                    if terminal_review_grace_checks_remaining <= 0:
                        return
                    terminal_review_grace_checks_remaining -= 1
                await asyncio.sleep(self._task_poll_interval)
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
        message: TelegramMessage,
        metadata: dict[str, str],
        command: dict[str, Any],
    ) -> None:
        if command.get("type") == "invalid":
            await self._send_message(
                chat_id=message.chat_id,
                text=str(command["message"]),
                reply_to_message_id=message.message_id,
            )
            return
        session_key = metadata["channel_session_key"]
        latest_task = await self._find_session_task(session_key)
        if latest_task is None:
            latest_task = await self._find_latest_task(
                self._legacy_lookup_metadata(message)
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
        self._ensure_watcher(
            task_id=task_id,
            chat_id=message.chat_id,
            run_count=self._task_run_count(task),
        )
        await self._send_message(
            chat_id=message.chat_id,
            text=f"审批已提交，task_id={task_id}",
            reply_to_message_id=message.message_id,
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
        chat_id: int,
        task: dict[str, Any],
    ) -> None:
        task_id = str(task["task_id"])
        run_count = self._task_run_count(task)
        delivered_run_count = self._delivered_terminal_runs.get(task_id, 0)
        if run_count <= delivered_run_count:
            return
        await self._send_message(
            chat_id=chat_id,
            text=self._format_terminal_message(task),
        )
        self._delivered_terminal_runs[task_id] = run_count

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


async def run_telegram_adapter() -> None:
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not bot_token:
        raise SystemExit("Missing TELEGRAM_BOT_TOKEN")
    gateway_base_url = os.getenv("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
    gateway_bearer_token = os.getenv("GATEWAY_BEARER_TOKEN")
    if not gateway_bearer_token:
        raise SystemExit("Missing GATEWAY_BEARER_TOKEN")
    default_agent_name = os.getenv("TELEGRAM_DEFAULT_AGENT", "main")
    session_db_path = os.getenv(
        "TELEGRAM_SESSION_DB",
        os.getenv("CHANNEL_SESSION_DB", ".ruyi_agent/channel_sessions.sqlite3"),
    )
    update_db_path = os.getenv(
        "TELEGRAM_UPDATE_DB",
        str(Path(session_db_path).expanduser().with_name("telegram_updates.sqlite3")),
    )
    poll_timeout = int(os.getenv("TELEGRAM_POLL_TIMEOUT", "30"))
    session_store = ChannelSessionStore(session_db_path)
    update_store = TelegramUpdateStore(update_db_path)
    try:
        adapter = TelegramAdapter(
            gateway_client=GatewayHTTPClient(
                base_url=gateway_base_url,
                bearer_token=gateway_bearer_token,
            ),
            telegram_client=TelegramBotAPIClient(
                bot_token=bot_token,
                timeout=float(os.getenv("TELEGRAM_API_TIMEOUT", str(poll_timeout + 10))),
                default_parse_mode=os.getenv(
                    "TELEGRAM_MESSAGE_PARSE_MODE",
                    "MarkdownV2",
                ),
            ),
            default_agent_name=default_agent_name,
            session_store=session_store,
            update_store=update_store,
            poll_timeout=poll_timeout,
            task_poll_interval=float(os.getenv("TELEGRAM_TASK_POLL_INTERVAL", "2")),
            terminal_review_grace_checks=int(
                os.getenv("TELEGRAM_TERMINAL_REVIEW_GRACE_CHECKS", "3")
            ),
            message_parse_mode=os.getenv("TELEGRAM_MESSAGE_PARSE_MODE", "MarkdownV2"),
        )
        await adapter.run_forever()
    finally:
        update_store.close()
        session_store.close()


def build_telegram_session_key(
    message: TelegramMessage,
    *,
    agent_name: str,
) -> str:
    if message.chat_type == "private":
        return f"agent:{agent_name}:telegram:dm:{message.chat_id}"
    if message.chat_type in {"group", "supergroup"}:
        thread_part = (
            f":thread:{message.message_thread_id}"
            if message.message_thread_id is not None
            else ""
        )
        return (
            f"agent:{agent_name}:telegram:{message.chat_type}:"
            f"{message.chat_id}{thread_part}:user:{message.user_id}"
        )
    raise UnsupportedTelegramChatTypeError(
        f"Unsupported Telegram chat_type: {message.chat_type!r}"
    )


def build_telegram_identity_key(message: TelegramMessage) -> str:
    if message.chat_type == "private":
        return f"telegram:dm:{message.chat_id}"
    if message.chat_type in {"group", "supergroup"}:
        thread_part = (
            f":thread:{message.message_thread_id}"
            if message.message_thread_id is not None
            else ""
        )
        return (
            f"telegram:{message.chat_type}:"
            f"{message.chat_id}{thread_part}:user:{message.user_id}"
        )
    raise UnsupportedTelegramChatTypeError(
        f"Unsupported Telegram chat_type: {message.chat_type!r}"
    )
