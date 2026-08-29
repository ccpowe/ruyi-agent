from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from ruyi_agent.channels.gateway_dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.telegram.network import (
    TelegramAPIError,
    TelegramFallbackResolver,
    TelegramFallbackTransport,
    TelegramNetworkError,
    _looks_like_network_error,
)


IMAGE_ATTACHMENT_EXTENSIONS = {
    ".png",
    ".jpg",
    ".jpeg",
    ".gif",
    ".webp",
}


def _env_list(name: str) -> list[str]:
    raw = os.getenv(name, "")
    return [item.strip() for item in raw.split(",") if item.strip()]


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


def _current_run_artifacts(
    task: GatewayTask,
    run_count: int,
) -> list[GatewayPublishedArtifact]:
    return [item for item in task.artifacts if item.run_count == run_count]


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
            if not all(
                isinstance(value, int)
                for value in [chat_id, user_id, message_id, update_id]
            ):
                continue
            attachments, attachment_warnings = await self._extract_inbound_attachments(
                message
            )
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
                        message_thread_id
                        if isinstance(message_thread_id, int)
                        else None
                    ),
                    reply_to_message_id=(
                        reply_to_message_id
                        if isinstance(reply_to_message_id, int)
                        else None
                    ),
                    attachments=attachments,
                    attachment_warnings=attachment_warnings,
                )
            )
        return messages

    async def _extract_inbound_attachments(
        self,
        message: dict[str, Any],
    ) -> tuple[
        list[TelegramInboundAttachment], list[TelegramAttachmentDownloadWarning]
    ]:
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
                error_cls = (
                    TelegramNetworkError
                    if _looks_like_network_error(exc)
                    else TelegramAPIError
                )
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
                error_cls = (
                    TelegramNetworkError
                    if _looks_like_network_error(exc)
                    else TelegramAPIError
                )
                raise error_cls(
                    f"Telegram API request failed: method={method} error={exc}"
                ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise TelegramAPIError("Telegram API returned invalid JSON") from exc
        if (
            not response.is_success
            or not isinstance(payload, dict)
            or not payload.get("ok")
        ):
            description = (
                payload.get("description")
                if isinstance(payload, dict)
                else response.text
            )
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
                error_cls = (
                    TelegramNetworkError
                    if _looks_like_network_error(exc)
                    else TelegramAPIError
                )
                raise error_cls(
                    f"Telegram API request failed: method={method} error={exc}"
                ) from exc
        try:
            payload = response.json()
        except ValueError as exc:
            raise TelegramAPIError("Telegram API returned invalid JSON") from exc
        if (
            not response.is_success
            or not isinstance(payload, dict)
            or not payload.get("ok")
        ):
            description = (
                payload.get("description")
                if isinstance(payload, dict)
                else response.text
            )
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
