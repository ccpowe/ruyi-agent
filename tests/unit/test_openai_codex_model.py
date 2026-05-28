from __future__ import annotations

import base64
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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


def _jwt_with_claims(claims: dict[str, object]) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
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


class _CodexSSEHandler(BaseHTTPRequestHandler):
    request_payload: dict[str, object] | None = None

    def do_POST(self) -> None:
        content_length = int(self.headers.get("content-length") or "0")
        body = self.rfile.read(content_length)
        type(self).request_payload = json.loads(body)
        events = [
            {
                "type": "response.created",
                "response": {
                    "id": "resp_test",
                    "object": "response",
                    "created_at": 1,
                    "status": "in_progress",
                    "model": "gpt-5.4",
                    "output": [],
                    "error": None,
                    "usage": None,
                },
            },
            {
                "type": "response.output_text.delta",
                "delta": "ruyi ",
                "output_index": 0,
                "content_index": 0,
            },
            {
                "type": "response.output_text.delta",
                "delta": "codex ok",
                "output_index": 0,
                "content_index": 0,
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_test",
                    "object": "response",
                    "created_at": 1,
                    "status": "completed",
                    "model": "gpt-5.4",
                    "output": None,
                    "error": None,
                    "usage": None,
                },
            },
        ]
        self.send_response(200)
        self.send_header("content-type", "text/event-stream")
        self.end_headers()
        for event in events:
            self.wfile.write(f"event: {event['type']}\n".encode())
            self.wfile.write(
                f"data: {json.dumps(event, separators=(',', ':'))}\n\n".encode()
            )

    def log_message(self, format: str, *args: object) -> None:
        return


def test_codex_chat_model_streams_codex_sse_with_null_completed_output() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CodexSSEHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        model = CodexChatModel(
            model="gpt-5.4",
            api_key="codex-token",
            base_url=f"http://127.0.0.1:{server.server_port}",
            default_headers={},
            codex_session_id="session-1",
        )

        response = model.invoke([HumanMessage(content="Say hi.")])

        assert response.content == "ruyi codex ok"
        assert _CodexSSEHandler.request_payload is not None
        assert _CodexSSEHandler.request_payload["stream"] is True
    finally:
        server.shutdown()
        thread.join(timeout=2)


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


def test_resolve_codex_credentials_reads_official_auth_json_account_id(tmp_path) -> None:
    token = _jwt_with_claims({"sub": "user-123"})
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "auth_mode": "chatgpt",
                "tokens": {
                    "id_token": _jwt_with_chatgpt_account_id("acct-from-id-token"),
                    "access_token": token,
                    "refresh_token": "refresh-token",
                    "account_id": "acct-from-token-data",
                },
            }
        ),
        encoding="utf-8",
    )

    credentials = resolve_codex_credentials(str(auth_json))

    assert credentials.api_key == token
    assert credentials.default_headers["ChatGPT-Account-ID"] == "acct-from-token-data"


def test_resolve_codex_credentials_falls_back_to_credential_pool(tmp_path) -> None:
    token = _jwt_with_claims({"sub": "user-123"})
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {},
                "credential_pool": {
                    "openai-codex": [
                        {
                            "source": "device_code",
                            "auth_type": "oauth",
                            "access_token": token,
                            "refresh_token": "pool-refresh-token",
                            "account_id": "acct-from-pool",
                            "last_status": "ok",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    credentials = resolve_codex_credentials(str(auth_json))

    assert credentials.api_key == token
    assert credentials.refresh_token == "pool-refresh-token"
    assert credentials.default_headers["ChatGPT-Account-ID"] == "acct-from-pool"


def test_resolve_codex_credentials_pool_fallback_skips_cooldown_entry(
    tmp_path,
) -> None:
    wedged_token = _jwt_with_claims({"sub": "wedged"})
    usable_token = _jwt_with_claims({"sub": "usable"})
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {},
                "credential_pool": {
                    "openai-codex": [
                        {
                            "access_token": wedged_token,
                            "refresh_token": "wedged-refresh-token",
                            "account_id": "wedged-acct",
                            "last_error_reset_at": 2_000,
                        },
                        {
                            "access_token": usable_token,
                            "refresh_token": "usable-refresh-token",
                            "account_id": "usable-acct",
                        },
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    credentials = resolve_codex_credentials(str(auth_json), now=1_000)

    assert credentials.api_key == usable_token
    assert credentials.refresh_token == "usable-refresh-token"
    assert credentials.default_headers["ChatGPT-Account-ID"] == "usable-acct"


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


def test_resolve_codex_credentials_refresh_syncs_credential_pool(
    tmp_path,
    monkeypatch,
) -> None:
    old_token = _jwt_with_chatgpt_account_id("acct-old", exp=900)
    new_token = _jwt_with_chatgpt_account_id("acct-new", exp=20_000)
    id_token = _jwt_with_chatgpt_account_id("acct-from-id-token")
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {
                    "openai-codex": {
                        "tokens": {
                            "id_token": id_token,
                            "access_token": old_token,
                            "refresh_token": "old-refresh-token",
                            "account_id": "acct-from-token-data",
                        }
                    }
                },
                "credential_pool": {
                    "openai-codex": [
                        {
                            "source": "device_code",
                            "access_token": old_token,
                            "refresh_token": "old-refresh-token",
                            "last_status": "exhausted",
                            "last_status_at": "2026-05-27T00:00:00Z",
                            "last_error_code": 401,
                            "last_error_reason": "token_invalidated",
                            "last_error_message": "stale token",
                            "last_error_reset_at": 9_999_999_999,
                        },
                        {
                            "source": "manual:codex",
                            "access_token": "manual-token",
                            "refresh_token": "manual-refresh-token",
                        },
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    def fake_refresh(refresh_token: str, *, timeout_seconds: float) -> dict[str, str]:
        assert refresh_token == "old-refresh-token"
        return {
            "access_token": new_token,
            "refresh_token": "new-refresh-token",
        }

    monkeypatch.setattr(openai_codex, "_refresh_codex_oauth_token", fake_refresh)

    resolve_codex_credentials(str(auth_json), now=1_000)

    saved = json.loads(auth_json.read_text(encoding="utf-8"))
    pool = saved["credential_pool"]["openai-codex"]
    device_code = next(entry for entry in pool if entry["source"] == "device_code")
    manual = next(entry for entry in pool if entry["source"] == "manual:codex")
    assert device_code["access_token"] == new_token
    assert device_code["refresh_token"] == "new-refresh-token"
    assert device_code["id_token"] == id_token
    assert device_code["account_id"] == "acct-from-token-data"
    assert device_code["last_status"] is None
    assert device_code["last_status_at"] is None
    assert device_code["last_error_code"] is None
    assert device_code["last_error_reason"] is None
    assert device_code["last_error_message"] is None
    assert device_code["last_error_reset_at"] is None
    assert manual["access_token"] == "manual-token"
    assert manual["refresh_token"] == "manual-refresh-token"
