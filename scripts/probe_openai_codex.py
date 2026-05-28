from __future__ import annotations

import argparse
import base64
import json
import os
import stat
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import httpx


DEFAULT_CODEX_BASE_URL = "https://chatgpt.com/backend-api/codex"
DEFAULT_CODEX_AUTH_JSON = "~/.ruyi_agent/openai_codex_auth.json"
CODEX_OAUTH_CLIENT_ID = "app_EMoamEEZ73f0CkXaXp7hrann"
CODEX_OAUTH_TOKEN_URL = "https://auth.openai.com/oauth/token"
DEFAULT_MODEL = "gpt-5.3-codex"
DEFAULT_INSTRUCTIONS = (
    "You are a minimal Codex backend probe. Answer the user request directly."
)


def redact(value: str, secrets: list[str]) -> str:
    text = value
    for secret in secrets:
        if secret:
            text = text.replace(secret, "***")
    return text[:3000]


def b64url_json(segment: str) -> dict[str, Any]:
    padded = segment + "=" * (-len(segment) % 4)
    return json.loads(base64.urlsafe_b64decode(padded))


def jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    try:
        return b64url_json(parts[1])
    except Exception:
        return {}


def chatgpt_account_id_from_token(token: str) -> str | None:
    claims = jwt_claims(token)
    account = claims.get("https://api.openai.com/auth", {})
    if not isinstance(account, dict):
        return None
    account_id = account.get("chatgpt_account_id")
    return account_id.strip() if isinstance(account_id, str) and account_id.strip() else None


def chatgpt_account_id_from_tokens(tokens: dict[str, Any]) -> str | None:
    account_id = tokens.get("account_id")
    if isinstance(account_id, str) and account_id.strip():
        return account_id.strip()
    access_token = tokens.get("access_token")
    if isinstance(access_token, str):
        from_access = chatgpt_account_id_from_token(access_token)
        if from_access:
            return from_access
    id_token = tokens.get("id_token")
    if isinstance(id_token, str):
        return chatgpt_account_id_from_token(id_token)
    return None


def token_summary(token: str) -> dict[str, Any]:
    claims = jwt_claims(token)
    exp = claims.get("exp")
    now = int(time.time())
    account = claims.get("https://api.openai.com/auth", {})
    return {
        "jwt": bool(claims),
        "expires_at": (
            datetime.fromtimestamp(exp, tz=timezone.utc).isoformat()
            if isinstance(exp, int)
            else None
        ),
        "expires_in_seconds": exp - now if isinstance(exp, int) else None,
        "has_chatgpt_account_id": isinstance(account, dict)
        and bool(account.get("chatgpt_account_id")),
        "scopes": claims.get("scope") or claims.get("scp"),
    }


def codex_cloudflare_headers(
    access_token: str,
    *,
    chatgpt_account_id: str | None = None,
) -> dict[str, str]:
    headers = {
        "User-Agent": "codex_cli_rs/0.0.0 (ruyi-agent)",
        "originator": "codex_cli_rs",
    }
    account_id = chatgpt_account_id or chatgpt_account_id_from_token(access_token)
    if account_id:
        headers["ChatGPT-Account-ID"] = account_id
    return headers


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.expanduser().read_text(encoding="utf-8"))


def _has_access_token(tokens: dict[str, Any]) -> bool:
    access_token = tokens.get("access_token")
    return isinstance(access_token, str) and bool(access_token.strip())


def _tokens_payload(
    tokens: dict[str, Any],
    *,
    source: str,
    last_refresh: Any = None,
    auth_mode: Any = None,
) -> dict[str, Any]:
    return {
        "access_token": tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token"),
        "id_token": tokens.get("id_token"),
        "account_id": tokens.get("account_id") or chatgpt_account_id_from_tokens(tokens),
        "source": source,
        "last_refresh": last_refresh,
        "auth_mode": auth_mode,
    }


def _codex_pool_tokens(payload: dict[str, Any]) -> dict[str, Any] | None:
    pool = payload.get("credential_pool")
    if not isinstance(pool, dict):
        return None
    entries = pool.get("openai-codex")
    if not isinstance(entries, list):
        return None
    now = time.time()
    for entry in entries:
        if not isinstance(entry, dict) or not _has_access_token(entry):
            continue
        reset_at = entry.get("last_error_reset_at")
        if isinstance(reset_at, (int, float)) and reset_at > now:
            continue
        return entry
    return None


def extract_tokens_from_auth_json(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    providers = payload.get("providers")
    if isinstance(providers, dict):
        state = providers.get("openai-codex")
        if isinstance(state, dict):
            tokens = state.get("tokens")
            if isinstance(tokens, dict) and _has_access_token(tokens):
                return _tokens_payload(
                    tokens,
                    source=str(path),
                    last_refresh=state.get("last_refresh"),
                    auth_mode=state.get("auth_mode"),
                )
    tokens = payload.get("tokens")
    if isinstance(tokens, dict) and _has_access_token(tokens):
        return _tokens_payload(
            tokens,
            source=str(path),
            last_refresh=payload.get("last_refresh"),
            auth_mode=payload.get("auth_mode"),
        )
    pool_tokens = _codex_pool_tokens(payload)
    if pool_tokens:
        return _tokens_payload(
            pool_tokens,
            source=str(path),
            last_refresh=pool_tokens.get("last_refresh"),
            auth_mode=payload.get("auth_mode") or "chatgpt",
        )
    return {"source": str(path)}


def resolve_tokens(args: argparse.Namespace) -> dict[str, Any]:
    access_token = os.getenv(args.access_token_env, "").strip()
    refresh_token = os.getenv(args.refresh_token_env, "").strip()
    if access_token or refresh_token:
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "source": f"env:{args.access_token_env}/{args.refresh_token_env}",
        }

    auth_path = Path(args.auth_json).expanduser()
    if auth_path.is_file():
        return extract_tokens_from_auth_json(auth_path)

    codex_home = Path(os.getenv("CODEX_HOME", "~/.codex")).expanduser()
    codex_auth = codex_home / "auth.json"
    if args.allow_codex_cli_auth and codex_auth.is_file():
        data = extract_tokens_from_auth_json(codex_auth)
        data["source"] = str(codex_auth)
        return data

    return {"source": str(auth_path)}


def refresh_token(refresh_token: str, timeout: float) -> dict[str, Any]:
    with httpx.Client(
        timeout=httpx.Timeout(max(5.0, timeout)),
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
        try:
            body: Any = response.json()
        except Exception:
            body = response.text[:1000]
        raise RuntimeError(
            f"refresh failed status={response.status_code} body={body!r}"
        )
    payload = response.json()
    if not payload.get("access_token"):
        raise RuntimeError("refresh response missing access_token")
    return {
        "access_token": payload["access_token"],
        "refresh_token": payload.get("refresh_token") or refresh_token,
        "raw_keys": sorted(payload),
    }


def device_login(timeout: float, max_wait_seconds: int) -> dict[str, Any]:
    issuer = "https://auth.openai.com"
    with httpx.Client(timeout=httpx.Timeout(max(5.0, timeout))) as client:
        response = client.post(
            f"{issuer}/api/accounts/deviceauth/usercode",
            json={"client_id": CODEX_OAUTH_CLIENT_ID},
            headers={"Content-Type": "application/json"},
        )
    if response.status_code != 200:
        raise RuntimeError(f"device code request failed status={response.status_code}")

    device_data = response.json()
    user_code = str(device_data.get("user_code") or "").strip()
    device_auth_id = str(device_data.get("device_auth_id") or "").strip()
    poll_interval = max(3, int(device_data.get("interval") or 5))
    if not user_code or not device_auth_id:
        raise RuntimeError("device code response missing user_code or device_auth_id")

    print("OpenAI Codex device login:", flush=True)
    print(f"  URL:  {issuer}/codex/device", flush=True)
    print(f"  Code: {user_code}", flush=True)
    print("Waiting for browser authorization...", flush=True)

    deadline = time.monotonic() + max_wait_seconds
    code_payload: dict[str, Any] | None = None
    with httpx.Client(timeout=httpx.Timeout(max(5.0, timeout))) as client:
        while time.monotonic() < deadline:
            time.sleep(poll_interval)
            poll = client.post(
                f"{issuer}/api/accounts/deviceauth/token",
                json={"device_auth_id": device_auth_id, "user_code": user_code},
                headers={"Content-Type": "application/json"},
            )
            if poll.status_code == 200:
                payload = poll.json()
                if not isinstance(payload, dict):
                    raise RuntimeError("device polling returned non-object JSON")
                code_payload = payload
                break
            if poll.status_code in {403, 404}:
                continue
            raise RuntimeError(f"device polling failed status={poll.status_code}")

    if code_payload is None:
        raise RuntimeError("device login timed out")

    authorization_code = str(code_payload.get("authorization_code") or "").strip()
    code_verifier = str(code_payload.get("code_verifier") or "").strip()
    if not authorization_code or not code_verifier:
        raise RuntimeError("device auth response missing authorization_code or code_verifier")

    with httpx.Client(timeout=httpx.Timeout(max(5.0, timeout))) as client:
        token_response = client.post(
            CODEX_OAUTH_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "authorization_code",
                "code": authorization_code,
                "redirect_uri": f"{issuer}/deviceauth/callback",
                "client_id": CODEX_OAUTH_CLIENT_ID,
                "code_verifier": code_verifier,
            },
        )
    if token_response.status_code != 200:
        raise RuntimeError(f"token exchange failed status={token_response.status_code}")

    token_payload = token_response.json()
    access_token = str(token_payload.get("access_token") or "").strip()
    refresh_token = str(token_payload.get("refresh_token") or "").strip()
    id_token = str(token_payload.get("id_token") or "").strip()
    if not access_token:
        raise RuntimeError("token exchange response missing access_token")
    token_data: dict[str, Any] = {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "id_token": id_token,
    }
    return {
        "access_token": access_token,
        "refresh_token": refresh_token,
        "id_token": id_token,
        "account_id": chatgpt_account_id_from_tokens(token_data),
        "source": "device-code",
        "last_refresh": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "auth_mode": "chatgpt",
    }


def sync_codex_pool_entries(existing: dict[str, Any], tokens: dict[str, Any]) -> None:
    pool = existing.get("credential_pool")
    if not isinstance(pool, dict):
        return
    entries = pool.get("openai-codex")
    if not isinstance(entries, list):
        return
    access_token = tokens.get("access_token")
    if not access_token:
        return
    for entry in entries:
        if not isinstance(entry, dict) or entry.get("source") != "device_code":
            continue
        entry["access_token"] = access_token
        refresh_token = tokens.get("refresh_token")
        if refresh_token:
            entry["refresh_token"] = refresh_token
        id_token = tokens.get("id_token")
        if id_token:
            entry["id_token"] = id_token
        account_id = tokens.get("account_id") or chatgpt_account_id_from_tokens(tokens)
        if account_id:
            entry["account_id"] = account_id
        last_refresh = tokens.get("last_refresh")
        if last_refresh:
            entry["last_refresh"] = last_refresh
        for field_name in (
            "last_status",
            "last_status_at",
            "last_error_code",
            "last_error_reason",
            "last_error_message",
            "last_error_reset_at",
        ):
            entry[field_name] = None


def save_ruyi_auth_json(path: Path, tokens: dict[str, Any]) -> None:
    path = path.expanduser()
    try:
        existing = load_json(path) if path.is_file() else {}
    except Exception:
        existing = {}
    if not isinstance(existing, dict):
        existing = {}
    providers = existing.setdefault("providers", {})
    if not isinstance(providers, dict):
        providers = {}
        existing["providers"] = providers

    saved_tokens = {
        "access_token": tokens.get("access_token"),
        "refresh_token": tokens.get("refresh_token"),
    }
    id_token = tokens.get("id_token")
    if id_token:
        saved_tokens["id_token"] = id_token
    account_id = tokens.get("account_id") or chatgpt_account_id_from_tokens(tokens)
    if account_id:
        saved_tokens["account_id"] = account_id

    provider_tokens = dict(tokens)
    provider_tokens.update(saved_tokens)
    providers["openai-codex"] = {
        "tokens": saved_tokens,
        "last_refresh": tokens.get("last_refresh"),
        "auth_mode": "chatgpt",
    }
    sync_codex_pool_entries(existing, provider_tokens)
    existing["version"] = existing.get("version") or 1
    existing["active_provider"] = "openai-codex"
    existing["updated_at"] = datetime.now(timezone.utc).isoformat()

    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(existing, indent=2, sort_keys=True) + "\n"
    fd = os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, stat.S_IRUSR | stat.S_IWUSR)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(payload)


def list_models(
    args: argparse.Namespace,
    access_token: str,
    account_id: str | None,
) -> dict[str, Any]:
    url = urljoin(args.base_url.rstrip("/") + "/", "models?client_version=1.0.0")
    headers = {
        "Authorization": f"Bearer {access_token}",
        **codex_cloudflare_headers(access_token, chatgpt_account_id=account_id),
    }
    with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
        response = client.get(url, headers=headers)
    body_text = response.text[:2000]
    try:
        body: Any = response.json()
    except Exception:
        body = body_text
    model_ids: list[str] = []
    if isinstance(body, dict):
        data = body.get("data") or body.get("models")
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict) and isinstance(item.get("id"), str):
                    model_ids.append(item["id"])
                elif isinstance(item, dict) and isinstance(item.get("slug"), str):
                    model_ids.append(item["slug"])
                elif isinstance(item, str):
                    model_ids.append(item)
        if not model_ids:
            model_ids = extract_model_ids(body)
    return {
        "status_code": response.status_code,
        "ok": response.is_success,
        "model_ids": model_ids,
        "model_count": len(model_ids),
        "body_keys": sorted(body) if isinstance(body, dict) else [],
        "body": body if args.raw_models or not response.is_success else None,
    }


def extract_model_ids(value: Any) -> list[str]:
    model_ids: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, child in node.items():
                if key in {"id", "slug", "model"} and isinstance(child, str):
                    lowered = child.lower()
                    if "gpt" in lowered or "codex" in lowered:
                        model_ids.append(child)
                else:
                    walk(child)
        elif isinstance(node, list):
            for child in node:
                walk(child)

    walk(value)
    return list(dict.fromkeys(model_ids))


def choose_model(args: argparse.Namespace, model_ids: list[str]) -> str:
    if args.model:
        return args.model
    for needle in ("codex", "gpt-5.5", "gpt-5.4", "gpt-5.3", "gpt-5.2"):
        for model_id in model_ids:
            if needle in model_id:
                return model_id
    return DEFAULT_MODEL


def parse_sse_payload(
    event_name: str | None,
    data_lines: list[str],
) -> dict[str, Any] | None:
    if not data_lines:
        return None
    data = "\n".join(data_lines)
    if data == "[DONE]":
        return None
    try:
        payload = json.loads(data)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    if event_name and "type" not in payload:
        payload["type"] = event_name
    return payload


def codex_event_text(payload: dict[str, Any], *, yielded_delta: bool) -> str:
    event_type = payload.get("type")
    delta = payload.get("delta")
    if event_type == "response.output_text.delta" and isinstance(delta, str):
        return delta
    if yielded_delta or event_type != "response.output_text.done":
        return ""
    text = payload.get("text")
    return text if isinstance(text, str) else ""


def raise_for_codex_event_error(payload: dict[str, Any]) -> None:
    event_type = str(payload.get("type") or "")
    error = payload.get("error")
    response = payload.get("response")
    if error is None and isinstance(response, dict):
        error = response.get("error")
    if error is not None:
        raise RuntimeError(f"Codex response stream failed: {error!r}")
    if event_type in {"response.failed", "response.incomplete"}:
        raise RuntimeError(f"Codex response stream ended with {event_type}")


def create_response(
    args: argparse.Namespace,
    access_token: str,
    model: str,
    account_id: str | None,
) -> dict[str, Any]:
    request_id = args.client_request_id or args.session_id or uuid.uuid4().hex
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "text/event-stream",
        "session_id": args.session_id or request_id,
        "x-client-request-id": request_id,
        **codex_cloudflare_headers(
            access_token,
            chatgpt_account_id=account_id,
        ),
    }
    request_body: dict[str, Any] = {
        "model": model,
        "instructions": args.instructions,
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": args.prompt}],
            }
        ],
        "store": False,
        "stream": True,
    }
    url = urljoin(args.base_url.rstrip("/") + "/", "responses")
    event_counts: dict[str, int] = {}
    text_parts: list[str] = []
    final_response: dict[str, Any] | None = None
    yielded_delta = False
    event_name: str | None = None
    data_lines: list[str] = []

    def flush_event() -> None:
        nonlocal event_name, data_lines, final_response, yielded_delta
        payload = parse_sse_payload(event_name, data_lines)
        event_name = None
        data_lines = []
        if payload is None:
            return
        event_type = str(payload.get("type") or "")
        if event_type:
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        raise_for_codex_event_error(payload)
        text = codex_event_text(payload, yielded_delta=yielded_delta)
        if text:
            text_parts.append(text)
            yielded_delta = True
        response = payload.get("response")
        if isinstance(response, dict) and event_type in {
            "response.completed",
            "response.failed",
            "response.incomplete",
        }:
            final_response = response

    with httpx.Client(timeout=httpx.Timeout(args.timeout)) as client:
        with client.stream("POST", url, headers=headers, json=request_body) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line == "":
                    flush_event()
                elif line.startswith("event:"):
                    event_name = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data_lines.append(line.split(":", 1)[1].lstrip())
            flush_event()

    output = final_response.get("output") if isinstance(final_response, dict) else None
    return {
        "id": final_response.get("id") if isinstance(final_response, dict) else None,
        "status": final_response.get("status") if isinstance(final_response, dict) else None,
        "output_text": "".join(text_parts),
        "output_count": len(output) if isinstance(output, list) else None,
        "usage": final_response.get("usage") if isinstance(final_response, dict) else None,
        "stream_event_counts": event_counts,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ruyi OpenAI Codex probe: read Codex OAuth tokens and call "
            "chatgpt.com/backend-api/codex Responses with streaming enabled."
        )
    )
    parser.add_argument(
        "--base-url",
        default=os.getenv("RUYI_CODEX_BASE_URL", DEFAULT_CODEX_BASE_URL),
    )
    parser.add_argument(
        "--auth-json",
        default=os.getenv("RUYI_CODEX_AUTH_JSON", DEFAULT_CODEX_AUTH_JSON),
    )
    parser.add_argument("--allow-codex-cli-auth", action="store_true")
    parser.add_argument("--access-token-env", default="OPENAI_CODEX_ACCESS_TOKEN")
    parser.add_argument("--refresh-token-env", default="OPENAI_CODEX_REFRESH_TOKEN")
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--model", default="")
    parser.add_argument("--instructions", default=DEFAULT_INSTRUCTIONS)
    parser.add_argument("--prompt", default="Reply with exactly: codex backend ok")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--device-login", action="store_true")
    parser.add_argument("--device-timeout", type=int, default=15 * 60)
    parser.add_argument("--save-auth-json", action="store_true")
    parser.add_argument("--list-models", action="store_true")
    parser.add_argument("--raw-models", action="store_true")
    parser.add_argument("--live-response", action="store_true")
    parser.add_argument("--client-request-id", default="")
    parser.add_argument("--session-id", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.device_login:
        try:
            tokens = device_login(args.timeout, args.device_timeout)
        except Exception as exc:
            print(
                json.dumps(
                    {
                        "mode": "device-login",
                        "ok": False,
                        "error": f"{exc.__class__.__name__}: {exc}",
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
            )
            return 2
        if args.save_auth_json:
            save_ruyi_auth_json(Path(args.auth_json), tokens)
    else:
        tokens = resolve_tokens(args)
    access_token = str(tokens.get("access_token") or "").strip()
    refresh = str(tokens.get("refresh_token") or "").strip()
    account_id = chatgpt_account_id_from_tokens(tokens)
    secrets = [access_token, refresh]

    output: dict[str, Any] = {
        "mode": "live" if args.refresh or args.list_models or args.live_response else "dry-run",
        "base_url": args.base_url,
        "auth_source": tokens.get("source"),
        "has_access_token": bool(access_token),
        "has_refresh_token": bool(refresh),
        "last_refresh": tokens.get("last_refresh"),
        "auth_mode": tokens.get("auth_mode"),
        "access_token_summary": token_summary(access_token) if access_token else None,
        "codex_headers": {
            key: ("present" if key == "ChatGPT-Account-ID" else value)
            for key, value in codex_cloudflare_headers(
                access_token,
                chatgpt_account_id=account_id,
            ).items()
        },
        "codex_flow": [
            "device auth gets authorization_code from auth.openai.com/codex/device",
            "token exchange/refresh uses auth.openai.com/oauth/token",
            "runtime calls the Codex Responses endpoint with streaming enabled",
            "requests include originator=codex_cli_rs and ChatGPT-Account-ID when present",
        ],
    }

    if args.refresh:
        if not refresh:
            output["refresh_error"] = "No refresh token available."
            print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
            return 2
        try:
            refreshed = refresh_token(refresh, args.timeout)
            access_token = refreshed["access_token"]
            refresh = refreshed["refresh_token"]
            secrets = [access_token, refresh]
            output["refresh_result"] = {
                "ok": True,
                "raw_keys": refreshed["raw_keys"],
                "access_token_summary": token_summary(access_token),
            }
        except Exception as exc:
            output["refresh_result"] = {
                "ok": False,
                "error": redact(f"{exc.__class__.__name__}: {exc}", secrets),
            }

    model_ids: list[str] = []
    if args.list_models:
        if not access_token:
            output["models_result"] = {"ok": False, "error": "No access token available."}
        else:
            try:
                models = list_models(args, access_token, account_id)
                model_ids = models.get("model_ids") or []
                output["models_result"] = models
            except Exception as exc:
                output["models_result"] = {
                    "ok": False,
                    "error": redact(f"{exc.__class__.__name__}: {exc}", secrets),
                }

    if args.live_response:
        if not access_token:
            output["response_result"] = {"ok": False, "error": "No access token available."}
        else:
            model = choose_model(args, model_ids)
            try:
                output["response_result"] = {
                    "ok": True,
                    "model": model,
                    "detail": create_response(args, access_token, model, account_id),
                }
            except Exception as exc:
                output["response_result"] = {
                    "ok": False,
                    "model": model,
                    "error": redact(f"{exc.__class__.__name__}: {exc}", secrets),
                }

    print(json.dumps(output, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
