from __future__ import annotations

import uuid
import base64
import json
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
from langchain_core.messages import BaseMessage
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


def codex_default_headers(access_token: str) -> dict[str, str]:
    headers = {
        "User-Agent": CODEX_USER_AGENT,
        "originator": "codex_cli_rs",
    }
    claims = _decode_jwt_claims(access_token)
    auth_claim = claims.get("https://api.openai.com/auth")
    if isinstance(auth_claim, dict):
        account_id = auth_claim.get("chatgpt_account_id")
        if isinstance(account_id, str) and account_id:
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

    providers = payload.get("providers")
    state = providers.get("openai-codex") if isinstance(providers, dict) else None
    tokens = state.get("tokens") if isinstance(state, dict) else payload.get("tokens")
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
            now=time.time() if now is None else now,
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
        default_headers=codex_default_headers(access_token.strip()),
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
