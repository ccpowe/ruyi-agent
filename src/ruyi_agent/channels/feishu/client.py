from __future__ import annotations

import asyncio
import io
import json
import re
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, Protocol

from ruyi_agent.gateway_protocol.dto import GatewayPublishedArtifact, GatewayTask


FEISHU_TEXT_CHUNK_LIMIT = 4000
DEFAULT_FEISHU_MEDIA_MAX_BYTES = 30 * 1024 * 1024
FEISHU_ACK_MODES = {"reaction", "message", "off"}


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


def _consume_cleanup_result(task: asyncio.Task[Any]) -> None:
    with suppress(BaseException):
        task.result()


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


def _current_run_artifacts(
    task: GatewayTask,
    run_count: int,
) -> list[GatewayPublishedArtifact]:
    return [item for item in task.artifacts if item.run_count == run_count]


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
        create_request_cls, create_body_cls, _, emoji_cls = (
            _import_lark_reaction_types()
        )
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
        builder = (
            lark.Client.builder().app_id(self._app_id).app_secret(self._app_secret)
        )
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
        raise FeishuAPIError(
            "Installed lark-oapi does not expose im.v1 message API"
        ) from exc
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
        raise FeishuAPIError(
            "Installed lark-oapi does not expose im.v1 file API"
        ) from exc
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
        return getattr(domain_class, "Lark", None) or getattr(
            domain_class, "LARK", None
        )
    return getattr(domain_class, "Feishu", None) or getattr(
        domain_class, "FEISHU", None
    )


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
        event_id=_string_value(header.get("event_id"))
        or _string_value(payload.get("event_id")),
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
