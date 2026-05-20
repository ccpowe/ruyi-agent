from __future__ import annotations

import base64
import json
from langchain_core.messages import HumanMessage, SystemMessage

import ruyi_agent.integrations.openai_codex as openai_codex
from ruyi_agent.integrations.openai_codex import (
    CodexChatModel,
    resolve_codex_credentials,
)


def _jwt_with_chatgpt_account_id(account_id: str, *, exp: int | None = None) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    claims = {
        "https://api.openai.com/auth": {
            "chatgpt_account_id": account_id,
        }
    }
    if exp is not None:
        claims["exp"] = exp
    payload = base64.urlsafe_b64encode(
        json.dumps(claims).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}."


def test_codex_chat_model_builds_codex_responses_payload() -> None:
    model = CodexChatModel(
        model="gpt-5.3-codex",
        api_key="codex-token",
        base_url="https://chatgpt.com/backend-api/codex",
        default_headers={},
        codex_session_id="session-1",
    )

    payload = model._get_request_payload(
        [
            SystemMessage(content="You are a probe."),
            HumanMessage(content="Say hi."),
        ],
        stream=True,
    )

    assert payload["instructions"] == "You are a probe."
    assert payload["input"] == [
        {"content": "Say hi.", "role": "user", "type": "message"}
    ]
    assert payload["store"] is False
    assert payload["stream"] is True
    assert payload["extra_headers"] == {
        "session_id": "session-1",
        "x-client-request-id": "session-1",
    }


def test_resolve_codex_credentials_reads_ruyi_auth_json(tmp_path) -> None:
    token = _jwt_with_chatgpt_account_id("acct-123")
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {
                    "openai-codex": {
                        "tokens": {
                            "access_token": token,
                            "refresh_token": "refresh-token",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    credentials = resolve_codex_credentials(str(auth_json))

    assert credentials.api_key == token
    assert credentials.default_headers == {
        "User-Agent": "codex_cli_rs/0.0.0 (ruyi-agent)",
        "originator": "codex_cli_rs",
        "ChatGPT-Account-ID": "acct-123",
    }


def test_resolve_codex_credentials_refreshes_expiring_access_token(
    tmp_path,
    monkeypatch,
) -> None:
    old_token = _jwt_with_chatgpt_account_id("acct-old", exp=900)
    new_token = _jwt_with_chatgpt_account_id("acct-new", exp=20_000)
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {
                    "openai-codex": {
                        "tokens": {
                            "access_token": old_token,
                            "refresh_token": "refresh-token",
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    def fake_refresh(refresh_token: str, *, timeout_seconds: float) -> dict[str, str]:
        assert refresh_token == "refresh-token"
        assert timeout_seconds == 20.0
        return {
            "access_token": new_token,
            "refresh_token": "new-refresh-token",
        }

    monkeypatch.setattr(openai_codex, "_refresh_codex_oauth_token", fake_refresh)

    credentials = resolve_codex_credentials(
        str(auth_json),
        now=1_000,
        refresh_if_expiring=True,
    )

    saved = json.loads(auth_json.read_text(encoding="utf-8"))
    saved_tokens = saved["providers"]["openai-codex"]["tokens"]
    assert credentials.api_key == new_token
    assert credentials.refresh_token == "new-refresh-token"
    assert credentials.default_headers["ChatGPT-Account-ID"] == "acct-new"
    assert saved_tokens["access_token"] == new_token
    assert saved_tokens["refresh_token"] == "new-refresh-token"
