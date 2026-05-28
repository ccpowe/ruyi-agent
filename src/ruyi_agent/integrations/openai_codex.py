from __future__ import annotations

import uuid
import base64
import json
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Iterator

import httpx
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_openai import ChatOpenAI


DEFAULT_CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"
DEFAULT_CODEX_AUTH_JSON = "~/.ruyi_agent/openai_codex_auth.json"
CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_OAUTH_TOKEN_URL = "https://auth.openai.com/oauth/token"
CODEX_ACCESS_TOKEN_REFRESH_SKEW_SECONDS = 120
DEFAULT_CODEX_INSTRUCTIONS = (
    "You are a Codex backend model used by ruyi-agent. Answer directly."
)
CODEX_USER_AGENT = "codex_cli_rs/0.0.0 (ruyi-agent)"


@dataclass(slots=True)
class CodexCredentials:
    api_key: str
    refresh_token: str | None
    default_headers: dict[str, str]


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, list):
        return str(content or "").strip()
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict):
            text = block.get("text")
            if isinstance(text, str):
                parts.append(text)
    return "\n".join(part for part in parts if part).strip()


def _strip_none_values(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _strip_none_values(child)
            for key, child in value.items()
            if child is not None
        }
    if isinstance(value, list):
        return [_strip_none_values(child) for child in value]
    return value


def _decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    try:
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        decoded = base64.urlsafe_b64decode(payload)
        claims = json.loads(decoded)
    except Exception:
        return {}
    return claims if isinstance(claims, dict) else {}


def _chatgpt_account_id_from_token(token: str) -> str | None:
    claims = _decode_jwt_claims(token)
    auth_claim = claims.get("https://api.openai.com/auth")
    if not isinstance(auth_claim, dict):
        return None
    account_id = auth_claim.get("chatgpt_account_id")
    return account_id.strip() if isinstance(account_id, str) and account_id.strip() else None


def _chatgpt_account_id_from_tokens(tokens: dict[str, Any]) -> str | None:
    account_id = tokens.get("account_id")
    if isinstance(account_id, str) and account_id.strip():
        return account_id.strip()
    access_token = tokens.get("access_token")
    if isinstance(access_token, str):
        from_access = _chatgpt_account_id_from_token(access_token)
        if from_access:
            return from_access
    id_token = tokens.get("id_token")
    if isinstance(id_token, str):
        return _chatgpt_account_id_from_token(id_token)
    return None


def _codex_pool_tokens(payload: dict[str, Any], *, now: float) -> dict[str, Any] | None:
    pool = payload.get("credential_pool")
    if not isinstance(pool, dict):
        return None
    entries = pool.get("openai-codex")
    if not isinstance(entries, list):
        return None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        access_token = entry.get("access_token")
        if not isinstance(access_token, str) or not access_token.strip():
            continue
        reset_at = entry.get("last_error_reset_at")
        if isinstance(reset_at, (int, float)) and reset_at > now:
            continue
        return entry
    return None


def _sync_codex_pool_entries(
    payload: dict[str, Any],
    *,
    access_token: str,
    refresh_token: str | None,
    id_token: str | None,
    account_id: str | None,
) -> None:
    pool = payload.get("credential_pool")
    if not isinstance(pool, dict):
        return
    entries = pool.get("openai-codex")
    if not isinstance(entries, list):
        return
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("source") != "device_code":
            continue
        entry["access_token"] = access_token
        if refresh_token:
            entry["refresh_token"] = refresh_token
        if id_token:
            entry["id_token"] = id_token
        if account_id:
            entry["account_id"] = account_id
        for field_name in (
            "last_status",
            "last_status_at",
            "last_error_code",
            "last_error_reason",
            "last_error_message",
            "last_error_reset_at",
        ):
            entry[field_name] = None


def codex_default_headers(
    access_token: str,
    *,
    chatgpt_account_id: str | None = None,
) -> dict[str, str]:
    headers = {
        "User-Agent": CODEX_USER_AGENT,
        "originator": "codex_cli_rs",
    }
    account_id = chatgpt_account_id or _chatgpt_account_id_from_token(access_token)
    if account_id:
        headers["ChatGPT-Account-ID"] = account_id
    return headers


def _access_token_is_expiring(token: str, *, now: float, skew_seconds: int) -> bool:
    claims = _decode_jwt_claims(token)
    exp = claims.get("exp")
    if not isinstance(exp, int):
        return False
    return exp <= now + skew_seconds


def _refresh_codex_oauth_token(
    refresh_token: str,
    *,
    timeout_seconds: float,
) -> dict[str, str]:
    with httpx.Client(
        timeout=httpx.Timeout(max(5.0, timeout_seconds)),
        headers={"Accept": "application/json"},
    ) as client:
        response = client.post(
            CODEX_OAUTH_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
                "client_id": CODEX_OAUTH_CLIENT_ID,
            },
        )
    if response.status_code != 200:
        raise ValueError(f"OpenAI Codex token refresh failed: HTTP {response.status_code}")
    payload = response.json()
    access_token = payload.get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        raise ValueError("OpenAI Codex token refresh response missing access_token")
    next_refresh = payload.get("refresh_token")
    return {
        "access_token": access_token.strip(),
        "refresh_token": next_refresh.strip()
        if isinstance(next_refresh, str) and next_refresh.strip()
        else refresh_token,
    }


def _save_codex_tokens(
    path: Path,
    payload: dict[str, Any],
    *,
    access_token: str,
    refresh_token: str | None,
) -> None:
    providers = payload.setdefault("providers", {})
    if not isinstance(providers, dict):
        providers = {}
        payload["providers"] = providers
    state = providers.setdefault("openai-codex", {})
    if not isinstance(state, dict):
        state = {}
        providers["openai-codex"] = state
    tokens = state.setdefault("tokens", {})
    if not isinstance(tokens, dict):
        tokens = {}
        state["tokens"] = tokens
    tokens["access_token"] = access_token
    if refresh_token:
        tokens["refresh_token"] = refresh_token
    _sync_codex_pool_entries(
        payload,
        access_token=access_token,
        refresh_token=refresh_token,
        id_token=tokens.get("id_token") if isinstance(tokens.get("id_token"), str) else None,
        account_id=_chatgpt_account_id_from_tokens(tokens),
    )

    path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    fd = os.open(
        str(path),
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        stat.S_IRUSR | stat.S_IWUSR,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(serialized)


def _resolve_codex_auth_path(auth_json: str) -> Path:
    return Path(auth_json).expanduser()


def _parse_sse_payload(
    event_name: str | None,
    data_lines: list[str],
) -> dict[str, Any] | None:
    if not data_lines:
        return None
    try:
        payload = json.loads("\n".join(data_lines))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if event_name and "type" not in payload:
        payload["type"] = event_name
    return payload


def _codex_event_text(payload: dict[str, Any], *, yielded_delta: bool) -> str:
    event_type = payload.get("type")
    delta = payload.get("delta")
    if event_type == "response.output_text.delta" and isinstance(delta, str):
        return delta
    if yielded_delta or event_type != "response.output_text.done":
        return ""
    text = payload.get("text")
    return text if isinstance(text, str) else ""


def _raise_for_codex_event_error(payload: dict[str, Any]) -> None:
    event_type = str(payload.get("type") or "")
    error = payload.get("error")
    response = payload.get("response")
    if error is None and isinstance(response, dict):
        error = response.get("error")
    if error is not None:
        raise ValueError(f"OpenAI Codex response stream failed: {error!r}")
    if event_type in {"response.failed", "response.incomplete"}:
        raise ValueError(f"OpenAI Codex response stream ended with {event_type}")


def resolve_codex_credentials(
    auth_json: str = DEFAULT_CODEX_AUTH_JSON,
    *,
    now: float | None = None,
    refresh_if_expiring: bool = True,
    refresh_skew_seconds: int = CODEX_ACCESS_TOKEN_REFRESH_SKEW_SECONDS,
    refresh_timeout_seconds: float = 20.0,
) -> CodexCredentials:
    path = _resolve_codex_auth_path(auth_json)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise ValueError(
            f"OpenAI Codex auth file not found: {path}. Run scripts/probe_openai_codex.py "
            "--device-login --save-auth-json first."
        ) from exc
    if not isinstance(payload, dict):
        raise ValueError(f"OpenAI Codex auth file has invalid shape: {path}")

    resolved_now = time.time() if now is None else now
    providers = payload.get("providers")
    state = providers.get("openai-codex") if isinstance(providers, dict) else None
    tokens = state.get("tokens") if isinstance(state, dict) else payload.get("tokens")
    if not isinstance(tokens, dict) or not (
        isinstance(tokens.get("access_token"), str) and tokens["access_token"].strip()
    ):
        tokens = _codex_pool_tokens(payload, now=resolved_now) or tokens
    if not isinstance(tokens, dict):
        raise ValueError(f"OpenAI Codex auth file is missing tokens: {path}")

    access_token = tokens.get("access_token")
    if not isinstance(access_token, str) or not access_token.strip():
        raise ValueError(f"OpenAI Codex auth file is missing access_token: {path}")
    refresh_token = tokens.get("refresh_token")
    normalized_refresh_token = (
        refresh_token.strip()
        if isinstance(refresh_token, str) and refresh_token.strip()
        else None
    )
    if (
        refresh_if_expiring
        and normalized_refresh_token
        and _access_token_is_expiring(
            access_token.strip(),
            now=resolved_now,
            skew_seconds=refresh_skew_seconds,
        )
    ):
        refreshed = _refresh_codex_oauth_token(
            normalized_refresh_token,
            timeout_seconds=refresh_timeout_seconds,
        )
        access_token = refreshed["access_token"]
        normalized_refresh_token = refreshed.get("refresh_token") or normalized_refresh_token
        _save_codex_tokens(
            path,
            payload,
            access_token=access_token,
            refresh_token=normalized_refresh_token,
        )
    return CodexCredentials(
        api_key=access_token.strip(),
        refresh_token=normalized_refresh_token,
        default_headers=codex_default_headers(
            access_token.strip(),
            chatgpt_account_id=_chatgpt_account_id_from_tokens(tokens),
        ),
    )


class CodexChatModel(ChatOpenAI):
    """ChatOpenAI adapter for ChatGPT Codex's Responses-only backend."""

    codex_session_id: str | None = None

    def __init__(self, **kwargs: Any) -> None:
        auth_json = kwargs.pop("auth_json", None)
        if not kwargs.get("api_key") and auth_json:
            credentials = resolve_codex_credentials(str(auth_json))
            kwargs["api_key"] = credentials.api_key
            default_headers = dict(credentials.default_headers)
            default_headers.update(dict(kwargs.get("default_headers") or {}))
            kwargs["default_headers"] = default_headers
        kwargs.setdefault("base_url", DEFAULT_CODEX_BASE_URL)
        kwargs.setdefault("use_responses_api", True)
        kwargs.setdefault("streaming", True)
        kwargs.setdefault("temperature", None)
        super().__init__(**kwargs)

    @property
    def _llm_type(self) -> str:
        return "openai-codex"

    def _codex_http_headers(
        self,
        extra_headers: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        api_key = self.openai_api_key.get_secret_value() if self.openai_api_key else ""
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Accept": "text/event-stream",
        }
        headers.update(
            {
                str(key): str(value)
                for key, value in (self.default_headers or {}).items()
                if value is not None
            }
        )
        headers.update(
            {
                str(key): str(value)
                for key, value in (extra_headers or {}).items()
                if value is not None
            }
        )
        return headers

    def _codex_responses_url(self) -> str:
        return f"{str(self.openai_api_base).rstrip('/')}/responses"

    def _codex_http_timeout(self) -> httpx.Timeout:
        timeout = self.request_timeout
        if isinstance(timeout, httpx.Timeout):
            return timeout
        if isinstance(timeout, (int, float)):
            return httpx.Timeout(float(timeout))
        return httpx.Timeout(60.0)

    def _codex_stream_payload(
        self,
        messages: list[BaseMessage],
        *,
        stop: list[str] | None,
        kwargs: dict[str, Any],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        payload = self._get_request_payload(messages, stop=stop, stream=True, **kwargs)
        extra_headers = payload.pop("extra_headers", None)
        extra_body = payload.pop("extra_body", None)
        if isinstance(extra_body, dict):
            payload.update(extra_body)
        payload["stream"] = True
        return _strip_none_values(payload), self._codex_http_headers(extra_headers)

    def _stream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> Iterator[ChatGenerationChunk]:
        payload, headers = self._codex_stream_payload(
            messages,
            stop=stop,
            kwargs=kwargs,
        )
        yielded_delta = False
        with httpx.Client(timeout=self._codex_http_timeout()) as client:
            with client.stream(
                "POST",
                self._codex_responses_url(),
                headers=headers,
                json=payload,
            ) as response:
                response.raise_for_status()
                event_name: str | None = None
                data_lines: list[str] = []

                def flush_event() -> Iterator[ChatGenerationChunk]:
                    nonlocal event_name, data_lines, yielded_delta
                    payload = _parse_sse_payload(event_name, data_lines)
                    event_name = None
                    data_lines = []
                    if payload is None:
                        return
                    _raise_for_codex_event_error(payload)
                    text = _codex_event_text(payload, yielded_delta=yielded_delta)
                    if not text:
                        return
                    yielded_delta = True
                    yield ChatGenerationChunk(message=AIMessageChunk(content=text))

                for line in response.iter_lines():
                    if line == "":
                        yield from flush_event()
                    elif line.startswith("event:"):
                        event_name = line.split(":", 1)[1].strip()
                    elif line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].lstrip())
                yield from flush_event()

    async def _astream(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        payload, headers = self._codex_stream_payload(
            messages,
            stop=stop,
            kwargs=kwargs,
        )
        yielded_delta = False
        async with httpx.AsyncClient(timeout=self._codex_http_timeout()) as client:
            async with client.stream(
                "POST",
                self._codex_responses_url(),
                headers=headers,
                json=payload,
            ) as response:
                response.raise_for_status()
                event_name: str | None = None
                data_lines: list[str] = []

                async def flush_event() -> list[ChatGenerationChunk]:
                    nonlocal event_name, data_lines, yielded_delta
                    payload = _parse_sse_payload(event_name, data_lines)
                    event_name = None
                    data_lines = []
                    if payload is None:
                        return []
                    _raise_for_codex_event_error(payload)
                    text = _codex_event_text(payload, yielded_delta=yielded_delta)
                    if not text:
                        return []
                    yielded_delta = True
                    return [ChatGenerationChunk(message=AIMessageChunk(content=text))]

                async for line in response.aiter_lines():
                    if line == "":
                        for chunk in await flush_event():
                            yield chunk
                    elif line.startswith("event:"):
                        event_name = line.split(":", 1)[1].strip()
                    elif line.startswith("data:"):
                        data_lines.append(line.split(":", 1)[1].lstrip())
                for chunk in await flush_event():
                    yield chunk

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        chunks = list(self._stream(messages, stop=stop, **kwargs))
        text = "".join(chunk.text for chunk in chunks)
        return ChatResult(generations=[ChatGeneration(message=AIMessage(content=text))])

    async def _agenerate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any | None = None,
        **kwargs: Any,
    ) -> ChatResult:
        text_parts: list[str] = []
        async for chunk in self._astream(messages, stop=stop, **kwargs):
            text_parts.append(chunk.text)
        return ChatResult(
            generations=[ChatGeneration(message=AIMessage(content="".join(text_parts)))]
        )

    def _get_request_payload(
        self,
        input_: list[BaseMessage],
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        instructions = _content_to_text(payload.get("instructions"))
        input_items = payload.get("input")
        if isinstance(input_items, list):
            kept: list[Any] = []
            for item in input_items:
                if isinstance(item, dict) and item.get("role") == "system":
                    system_text = _content_to_text(item.get("content"))
                    if system_text:
                        instructions = (
                            f"{instructions}\n\n{system_text}"
                            if instructions
                            else system_text
                        )
                    continue
                kept.append(item)
            payload["input"] = kept

        payload["instructions"] = instructions or DEFAULT_CODEX_INSTRUCTIONS
        payload["store"] = False

        request_id = self.codex_session_id or uuid.uuid4().hex
        extra_headers = dict(payload.get("extra_headers") or {})
        extra_headers.setdefault("session_id", self.codex_session_id or request_id)
        extra_headers.setdefault("x-client-request-id", request_id)
        payload["extra_headers"] = extra_headers
        return payload
