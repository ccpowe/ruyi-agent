"""Authenticated Team Console browser routes."""

from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import parse_qsl

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse, Response

from ruyi_agent.gateway.errors import GatewayTaskError

from .context import GatewayHttpContext
from .team_console_auth import (
    TEAM_CONSOLE_LOGIN_BODY_LIMIT,
    TEAM_CONSOLE_SESSION_COOKIE,
    TEAM_CONSOLE_SESSION_TTL_SECONDS,
    browser_request_is_same_origin,
)

_INVALID_FORM_PERCENT_ESCAPE = re.compile(r"%(?![0-9A-Fa-f]{2})")
_TEAM_CONSOLE_CSP = (
    "default-src 'none'; script-src 'self'; style-src 'self'; "
    "connect-src 'self'; img-src 'self'; font-src 'self'; "
    "base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
)
_TEAM_CONSOLE_LOGIN_CSP = (
    "default-src 'none'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'"
)
_COMMON_HEADERS = {
    "Cache-Control": "no-store",
    "Cross-Origin-Resource-Policy": "same-origin",
    "Referrer-Policy": "same-origin",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
}
_HTML_HEADERS = {**_COMMON_HEADERS, "Content-Security-Policy": _TEAM_CONSOLE_CSP}
_LOGIN_HEADERS = {
    **_COMMON_HEADERS,
    "Content-Security-Policy": _TEAM_CONSOLE_LOGIN_CSP,
}


def attach_console_routes(app: FastAPI, context: GatewayHttpContext) -> None:
    console_root = Path(__file__).resolve().parents[2] / "web" / "team_console"
    auth = context.console_auth

    @app.get("/debug/team/login", response_class=HTMLResponse, include_in_schema=False)
    async def team_console_login(request: Request) -> Response:
        if request.url.query:
            return _login_response(
                status_code=400,
                error="登录请求不能包含查询参数。",
            )
        if not auth.console_transport_allowed(request.scope):
            return _login_response(
                status_code=400,
                error="调试台登录要求 HTTPS；localhost 可使用 HTTP。",
            )
        if context.console_session_is_valid(request):
            return RedirectResponse(
                "/debug/team",
                status_code=303,
                headers=dict(_COMMON_HEADERS),
            )
        return _login_response()

    @app.post("/debug/team/login", include_in_schema=False)
    async def create_team_console_session(request: Request) -> Response:
        if request.url.query:
            return _login_response(
                status_code=400,
                error="登录请求不能包含查询参数。",
            )
        if not auth.console_transport_allowed(request.scope):
            return _login_response(
                status_code=400,
                error="调试台登录要求 HTTPS；localhost 可使用 HTTP。",
            )
        if not browser_request_is_same_origin(request.scope):
            return _login_response(
                status_code=403,
                error="登录请求未通过同源校验。",
            )
        token, error_status = await _read_login_token(request)
        if error_status is not None:
            messages = {
                400: "登录请求格式无效。",
                413: "登录请求过大。",
                415: "登录请求的内容类型不受支持。",
            }
            return _login_response(
                status_code=error_status,
                error=messages[error_status],
            )
        if token is None or not auth.verify_login_token(token):
            return _login_response(status_code=401, error="Gateway Token 无效。")

        response = RedirectResponse(
            "/debug/team",
            status_code=303,
            headers=dict(_COMMON_HEADERS),
        )
        response.set_cookie(
            TEAM_CONSOLE_SESSION_COOKIE,
            auth.issue_session(),
            max_age=TEAM_CONSOLE_SESSION_TTL_SECONDS,
            path="/",
            secure=auth.session_cookie_is_secure(request.scope),
            httponly=True,
            samesite="strict",
        )
        return response

    @app.post("/debug/team/logout", include_in_schema=False)
    async def destroy_team_console_session(request: Request) -> Response:
        if auth.authenticate_api_request(request.scope) != "console_session":
            raise GatewayTaskError(
                code="unauthorized",
                message="Missing or invalid team console session",
            )
        response = RedirectResponse(
            "/debug/team/login",
            status_code=303,
            headers=dict(_COMMON_HEADERS),
        )
        response.delete_cookie(
            TEAM_CONSOLE_SESSION_COOKIE,
            path="/",
            secure=auth.session_cookie_is_secure(request.scope),
            httponly=True,
            samesite="strict",
        )
        return response

    @app.get("/debug/team", response_class=HTMLResponse, include_in_schema=False)
    async def team_console(request: Request) -> Response:
        if not auth.console_transport_allowed(request.scope):
            return _login_response(
                status_code=400,
                error="调试台要求 HTTPS；localhost 可使用 HTTP。",
            )
        if not context.console_session_is_valid(request):
            return RedirectResponse(
                "/debug/team/login",
                status_code=303,
                headers=dict(_COMMON_HEADERS),
            )
        return HTMLResponse(
            (console_root / "index.html").read_text(encoding="utf-8"),
            headers=dict(_HTML_HEADERS),
        )

    @app.get("/debug/team/app.css", include_in_schema=False)
    async def team_console_css(request: Request) -> FileResponse:
        _require_console_session(context, request)
        return FileResponse(
            console_root / "app.css",
            media_type="text/css",
            headers=dict(_COMMON_HEADERS),
        )

    @app.get("/debug/team/app.js", include_in_schema=False)
    async def team_console_js(request: Request) -> FileResponse:
        _require_console_session(context, request)
        return FileResponse(
            console_root / "app.js",
            media_type="text/javascript",
            headers=dict(_COMMON_HEADERS),
        )


def _require_console_session(context: GatewayHttpContext, request: Request) -> None:
    if not context.console_session_is_valid(request):
        raise GatewayTaskError(
            code="unauthorized",
            message="Missing or invalid team console session",
        )


def _login_page(*, error: str | None = None) -> str:
    error_html = f'<p role="alert">{error}</p>' if error is not None else ""
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta name="color-scheme" content="light dark">
  <title>Ruyi Team Console · 登录</title>
</head>
<body>
  <main>
    <h1>Ruyi Team Console</h1>
    <p>输入当前 Gateway Bearer Token 以创建短期浏览器会话。</p>
    {error_html}
    <form method="post" action="/debug/team/login">
      <label for="token">Gateway Token</label>
      <input id="token" name="token" type="password" autocomplete="current-password"
             maxlength="4096" required autofocus>
      <button type="submit">登录</button>
    </form>
  </main>
</body>
</html>
"""


def _login_response(
    *,
    status_code: int = 200,
    error: str | None = None,
) -> HTMLResponse:
    headers = dict(_LOGIN_HEADERS)
    if status_code == 401:
        headers["WWW-Authenticate"] = 'Bearer realm="ruyi-team-console"'
    return HTMLResponse(
        _login_page(error=error),
        status_code=status_code,
        headers=headers,
    )


def _raw_header_values(request: Request, name: bytes) -> list[bytes]:
    return [
        value
        for raw_name, value in request.scope.get("headers", [])
        if raw_name.lower() == name
    ]


def _form_content_type_is_supported(request: Request) -> bool:
    values = _raw_header_values(request, b"content-type")
    if len(values) != 1:
        return False
    try:
        parts = [part.strip() for part in values[0].decode("ascii").split(";")]
    except UnicodeDecodeError:
        return False
    if not parts or parts[0].lower() != "application/x-www-form-urlencoded":
        return False
    parameters: dict[str, str] = {}
    for raw_parameter in parts[1:]:
        name, separator, value = raw_parameter.partition("=")
        name = name.strip().lower()
        value = value.strip().lower()
        if separator != "=" or not name or name in parameters:
            return False
        parameters[name] = value
    return not parameters or parameters == {"charset": "utf-8"}


async def _read_login_token(request: Request) -> tuple[str | None, int | None]:
    if not _form_content_type_is_supported(request):
        return None, 415
    lengths = _raw_header_values(request, b"content-length")
    if len(lengths) > 1:
        return None, 400
    if lengths:
        try:
            raw_length = lengths[0].decode("ascii")
        except UnicodeDecodeError:
            return None, 400
        if (
            not raw_length.isdecimal()
            or len(raw_length) > 10
            or str(int(raw_length)) != raw_length
        ):
            return None, 400
        if int(raw_length) > TEAM_CONSOLE_LOGIN_BODY_LIMIT:
            return None, 413
    chunks: list[bytes] = []
    size = 0
    try:
        async for chunk in request.stream():
            size += len(chunk)
            if size > TEAM_CONSOLE_LOGIN_BODY_LIMIT:
                return None, 413
            chunks.append(chunk)
    except Exception:
        return None, 400
    try:
        body = b"".join(chunks).decode("utf-8", errors="strict")
        if _INVALID_FORM_PERCENT_ESCAPE.search(body) is not None:
            return None, 400
        fields = parse_qsl(
            body,
            keep_blank_values=True,
            strict_parsing=True,
            encoding="utf-8",
            errors="strict",
            max_num_fields=2,
        )
    except (UnicodeDecodeError, ValueError):
        return None, 400
    if len(fields) != 1 or fields[0][0] != "token" or not fields[0][1]:
        return None, 400
    return fields[0][1], None
