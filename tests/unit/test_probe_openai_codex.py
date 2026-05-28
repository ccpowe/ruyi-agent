from __future__ import annotations

import base64
import json
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace

from scripts import probe_openai_codex


def _jwt_with_claims(claims: dict[str, object]) -> str:
    header = base64.urlsafe_b64encode(b'{"alg":"none"}').rstrip(b"=").decode()
    payload = base64.urlsafe_b64encode(
        json.dumps(claims).encode()
    ).rstrip(b"=").decode()
    return f"{header}.{payload}."


def test_probe_dry_run_reports_official_auth_json_account_header(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    access_token = _jwt_with_claims({"sub": "user-123"})
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "auth_mode": "chatgpt",
                "tokens": {
                    "access_token": access_token,
                    "refresh_token": "refresh-token",
                    "account_id": "acct-from-token-data",
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["probe_openai_codex.py", "--auth-json", str(auth_json)],
    )

    assert probe_openai_codex.main() == 0

    output = json.loads(capsys.readouterr().out)
    assert output["has_access_token"] is True
    assert output["codex_headers"]["ChatGPT-Account-ID"] == "present"


def test_probe_dry_run_falls_back_to_credential_pool(
    tmp_path,
    monkeypatch,
    capsys,
) -> None:
    access_token = _jwt_with_claims({"sub": "user-123"})
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {},
                "credential_pool": {
                    "openai-codex": [
                        {
                            "source": "device_code",
                            "access_token": access_token,
                            "refresh_token": "pool-refresh-token",
                            "account_id": "acct-from-pool",
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["probe_openai_codex.py", "--auth-json", str(auth_json)],
    )

    assert probe_openai_codex.main() == 0

    output = json.loads(capsys.readouterr().out)
    assert output["has_access_token"] is True
    assert output["has_refresh_token"] is True
    assert output["codex_headers"]["ChatGPT-Account-ID"] == "present"


class _CodexSSEHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        self.rfile.read(int(self.headers.get("content-length") or "0"))
        events = [
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


def test_probe_live_response_consumes_codex_sse_with_null_completed_output() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _CodexSSEHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        args = SimpleNamespace(
            base_url=f"http://127.0.0.1:{server.server_port}",
            timeout=20.0,
            instructions="Answer directly.",
            prompt="Say hi.",
            client_request_id="",
            session_id="session-1",
        )

        result = probe_openai_codex.create_response(
            args,
            "access-token",
            "gpt-5.4",
            "acct-123",
        )

        assert result["output_text"] == "ruyi codex ok"
    finally:
        server.shutdown()
        thread.join(timeout=2)


def test_save_ruyi_auth_json_preserves_metadata_and_syncs_pool(tmp_path) -> None:
    old_token = _jwt_with_claims({"sub": "old"})
    new_token = _jwt_with_claims({"sub": "new"})
    id_token = _jwt_with_claims(
        {
            "https://api.openai.com/auth": {
                "chatgpt_account_id": "acct-from-id-token",
            }
        }
    )
    auth_json = tmp_path / "auth.json"
    auth_json.write_text(
        json.dumps(
            {
                "providers": {},
                "credential_pool": {
                    "openai-codex": [
                        {
                            "source": "device_code",
                            "access_token": old_token,
                            "refresh_token": "old-refresh",
                            "last_status": "exhausted",
                            "last_error_reset_at": 9_999_999_999,
                        },
                        {
                            "source": "manual:codex",
                            "access_token": "manual-token",
                            "refresh_token": "manual-refresh",
                        },
                    ]
                },
            }
        ),
        encoding="utf-8",
    )

    probe_openai_codex.save_ruyi_auth_json(
        auth_json,
        {
            "access_token": new_token,
            "refresh_token": "new-refresh",
            "id_token": id_token,
            "account_id": "acct-from-token-data",
            "last_refresh": "2026-05-28T00:00:00Z",
        },
    )

    saved = json.loads(auth_json.read_text(encoding="utf-8"))
    tokens = saved["providers"]["openai-codex"]["tokens"]
    assert tokens["access_token"] == new_token
    assert tokens["refresh_token"] == "new-refresh"
    assert tokens["id_token"] == id_token
    assert tokens["account_id"] == "acct-from-token-data"
    device_code = next(
        entry
        for entry in saved["credential_pool"]["openai-codex"]
        if entry["source"] == "device_code"
    )
    manual = next(
        entry
        for entry in saved["credential_pool"]["openai-codex"]
        if entry["source"] == "manual:codex"
    )
    assert device_code["access_token"] == new_token
    assert device_code["refresh_token"] == "new-refresh"
    assert device_code["id_token"] == id_token
    assert device_code["account_id"] == "acct-from-token-data"
    assert device_code["last_status"] is None
    assert device_code["last_error_reset_at"] is None
    assert manual["access_token"] == "manual-token"
    assert manual["refresh_token"] == "manual-refresh"
