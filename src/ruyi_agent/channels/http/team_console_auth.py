"""Browser-session authentication for the team debug console."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
from ipaddress import IPv4Address, IPv6Address, ip_address
import re
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal
from urllib.parse import SplitResult, urlsplit

from starlette.datastructures import MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send


TEAM_CONSOLE_SESSION_COOKIE = "ruyi_team_console_session"
TEAM_CONSOLE_REQUEST_HEADER = "x-ruyi-team-console"
TEAM_CONSOLE_REQUEST_HEADER_VALUE = "1"
TEAM_CONSOLE_SESSION_TTL_SECONDS = 8 * 60 * 60
TEAM_CONSOLE_LOGIN_BODY_LIMIT = 8 * 1024

_CONSOLE_AUTHENTICATED_STATE_KEY = "ruyi_team_console_cookie_authenticated"
_SESSION_VERSION = "v1"
_SESSION_NONCE_BYTES = 32
_SESSION_MAC_BYTES = hashlib.sha256().digest_size
_SESSION_MAX_LENGTH = 256
_SESSION_FUTURE_SKEW_SECONDS = 60
_COOKIE_HEADERS_MAX_BYTES = 16 * 1024
_SOURCE_HEADER_MAX_BYTES = 8 * 1024
_HOST_HEADER_MAX_BYTES = 320
_KEY_DERIVATION_CONTEXT = b"ruyi-agent/team-console/session-key/v1"
_DNS_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+")


@dataclass(frozen=True, slots=True)
class _Origin:
    scheme: Literal["http", "https"]
    hostname: str
    port: int


class TeamConsoleNoStoreMiddleware:
    """Add ``no-store`` without buffering authenticated streaming responses."""

    def __init__(self, app: ASGIApp) -> None:
        self._app = app

    async def __call__(
        self,
        scope: Scope,
        receive: Receive,
        send: Send,
    ) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        async def send_with_private_cache_policy(message: Message) -> None:
            state = scope.get("state")
            path = scope.get("path")
            is_console_surface = isinstance(path, str) and (
                path == "/debug/team" or path.startswith("/debug/team/")
            )
            if (
                message["type"] == "http.response.start"
                and (
                    is_console_surface
                    or (
                        isinstance(state, dict)
                        and state.get(_CONSOLE_AUTHENTICATED_STATE_KEY) is True
                    )
                )
            ):
                headers = MutableHeaders(scope=message)
                headers["Cache-Control"] = "no-store"
            await send(message)

        await self._app(scope, receive, send_with_private_cache_policy)


class TeamConsoleAuthenticator:
    """Validate Gateway Bearer credentials and short-lived console sessions."""

    def __init__(
        self,
        bearer_token: str,
        *,
        clock: Callable[[], float] = time.time,
        nonce_factory: Callable[[int], bytes] = secrets.token_bytes,
    ) -> None:
        self._bearer_token = bearer_token
        self._bearer_authorization = f"Bearer {bearer_token}"
        self._clock = clock
        self._nonce_factory = nonce_factory
        self._session_key = hmac.new(
            bearer_token.encode("utf-8"),
            _KEY_DERIVATION_CONTEXT,
            hashlib.sha256,
        ).digest()

    def verify_login_token(self, candidate: str) -> bool:
        """Compare a form credential without normalizing or reflecting it."""

        return secrets.compare_digest(
            candidate.encode("utf-8"),
            self._bearer_token.encode("utf-8"),
        )

    def issue_session(self) -> str:
        """Issue one stateless, versioned session ticket."""

        issued_at = int(self._clock())
        expires_at = issued_at + TEAM_CONSOLE_SESSION_TTL_SECONDS
        nonce = self._nonce_factory(_SESSION_NONCE_BYTES)
        if not isinstance(nonce, bytes) or len(nonce) != _SESSION_NONCE_BYTES:
            raise ValueError("nonce_factory must return exactly 32 bytes")
        payload = ".".join(
            (
                _SESSION_VERSION,
                str(issued_at),
                str(expires_at),
                _encode_base64url(nonce),
            )
        )
        mac = hmac.new(
            self._session_key,
            payload.encode("ascii"),
            hashlib.sha256,
        ).digest()
        return f"{payload}.{_encode_base64url(mac)}"

    def validate_session(self, ticket: str) -> bool:
        """Validate canonical encoding, lifetime, and MAC of a session ticket."""

        if not ticket or len(ticket) > _SESSION_MAX_LENGTH or not ticket.isascii():
            return False
        parts = ticket.split(".")
        if len(parts) != 5 or parts[0] != _SESSION_VERSION:
            return False
        issued_at = _parse_canonical_timestamp(parts[1])
        expires_at = _parse_canonical_timestamp(parts[2])
        nonce = _decode_base64url(parts[3], expected_length=_SESSION_NONCE_BYTES)
        supplied_mac = _decode_base64url(parts[4], expected_length=_SESSION_MAC_BYTES)
        if (
            issued_at is None
            or expires_at is None
            or nonce is None
            or supplied_mac is None
            or expires_at - issued_at != TEAM_CONSOLE_SESSION_TTL_SECONDS
        ):
            return False
        now = self._clock()
        if issued_at > now + _SESSION_FUTURE_SKEW_SECONDS or now >= expires_at:
            return False
        payload = ".".join(parts[:4])
        expected_mac = hmac.new(
            self._session_key,
            payload.encode("ascii"),
            hashlib.sha256,
        ).digest()
        return secrets.compare_digest(supplied_mac, expected_mac)

    def authenticate_console_session(self, scope: Scope) -> bool:
        """Authenticate a page or asset request using only its session cookie."""

        ticket = _unique_session_cookie(scope)
        return ticket is not None and self.validate_session(ticket)

    def authenticate_api_request(
        self,
        scope: Scope,
    ) -> Literal["bearer", "console_session"] | None:
        """Authenticate an API without downgrading an invalid Bearer header."""

        authorization = _header_values(scope, b"authorization")
        if authorization:
            if len(authorization) != 1 or not secrets.compare_digest(
                authorization[0].encode("utf-8"),
                self._bearer_authorization.encode("utf-8"),
            ):
                return None
            return "bearer"

        marker = _header_values(scope, TEAM_CONSOLE_REQUEST_HEADER.encode("ascii"))
        if marker != [TEAM_CONSOLE_REQUEST_HEADER_VALUE]:
            return None
        if not self.console_transport_allowed(scope):
            return None
        ticket = _unique_session_cookie(scope)
        if ticket is None or not self.validate_session(ticket):
            return None
        if not browser_request_is_same_origin(scope):
            return None
        return "console_session"

    def mark_console_api_authenticated(self, scope: Scope) -> None:
        state = scope.setdefault("state", {})
        state[_CONSOLE_AUTHENTICATED_STATE_KEY] = True

    def console_api_was_authenticated(self, scope: Scope) -> bool:
        state = scope.get("state")
        return (
            isinstance(state, dict)
            and state.get(_CONSOLE_AUTHENTICATED_STATE_KEY) is True
        )

    def console_transport_allowed(self, scope: Scope) -> bool:
        """Allow HTTPS everywhere and plaintext HTTP only on loopback hosts."""

        origin = _target_origin(scope)
        if origin is None:
            return False
        if origin.scheme == "https":
            return True
        return _is_loopback_hostname(origin.hostname)

    def session_cookie_is_secure(self, scope: Scope) -> bool:
        origin = _target_origin(scope)
        return origin is not None and origin.scheme == "https"


def browser_request_is_same_origin(scope: Scope) -> bool:
    """Fail closed on absent, duplicate, malformed, or contradictory signals."""

    target = _target_origin(scope)
    if target is None:
        return False

    fetch_site = _header_values(scope, b"sec-fetch-site")
    if len(fetch_site) > 1 or (fetch_site and fetch_site[0] != "same-origin"):
        return False

    origins = _header_values(scope, b"origin")
    referers = _header_values(scope, b"referer")
    if len(origins) > 1 or len(referers) > 1:
        return False
    if not fetch_site and not origins and not referers:
        return False
    if origins:
        source = _source_origin(origins[0], allow_path=False)
        if source is None or source != target:
            return False
    if referers:
        source = _source_origin(referers[0], allow_path=True)
        if source is None or source != target:
            return False
    return True


def _header_values(scope: Scope, name: bytes) -> list[str]:
    values: list[str] = []
    total_bytes = 0
    for raw_name, raw_value in scope.get("headers", []):
        if raw_name.lower() != name:
            continue
        total_bytes += len(raw_value)
        if total_bytes > _SOURCE_HEADER_MAX_BYTES:
            return ["", ""]
        try:
            values.append(raw_value.decode("ascii"))
        except UnicodeDecodeError:
            return ["", ""]
    return values


def _unique_session_cookie(scope: Scope) -> str | None:
    matches: list[str] = []
    total_bytes = 0
    for raw_name, raw_value in scope.get("headers", []):
        if raw_name.lower() != b"cookie":
            continue
        total_bytes += len(raw_value)
        if total_bytes > _COOKIE_HEADERS_MAX_BYTES:
            return None
        try:
            cookie_header = raw_value.decode("ascii")
        except UnicodeDecodeError:
            return None
        for field in cookie_header.split(";"):
            name, separator, value = field.partition("=")
            if name.strip() != TEAM_CONSOLE_SESSION_COOKIE:
                continue
            if separator != "=":
                return None
            matches.append(value)
    if len(matches) != 1:
        return None
    return matches[0]


def _target_origin(scope: Scope) -> _Origin | None:
    scheme_raw = scope.get("scheme")
    if not isinstance(scheme_raw, str):
        return None
    scheme = scheme_raw.lower()
    if scheme not in {"http", "https"}:
        return None
    hosts = [
        value for name, value in scope.get("headers", []) if name.lower() == b"host"
    ]
    if len(hosts) != 1 or len(hosts[0]) > _HOST_HEADER_MAX_BYTES:
        return None
    try:
        raw_host = hosts[0].decode("ascii")
    except UnicodeDecodeError:
        return None
    parsed_host = _parse_host(raw_host)
    if parsed_host is None:
        return None
    hostname, explicit_port = parsed_host
    port = explicit_port or (443 if scheme == "https" else 80)
    return _Origin(scheme=scheme, hostname=hostname, port=port)


def _source_origin(raw: str, *, allow_path: bool) -> _Origin | None:
    if (
        not raw
        or len(raw.encode("ascii", errors="ignore")) > _SOURCE_HEADER_MAX_BYTES
        or not raw.isascii()
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in raw)
    ):
        return None
    try:
        parsed: SplitResult = urlsplit(raw)
    except ValueError:
        return None
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"} or not parsed.netloc:
        return None
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        return None
    if not allow_path and (parsed.path or parsed.query):
        return None
    parsed_host = _parse_host(parsed.netloc)
    if parsed_host is None:
        return None
    hostname, explicit_port = parsed_host
    port = explicit_port or (443 if scheme == "https" else 80)
    return _Origin(scheme=scheme, hostname=hostname, port=port)


def _parse_host(raw: str) -> tuple[str, int | None] | None:
    if (
        not raw
        or len(raw) > _HOST_HEADER_MAX_BYTES
        or not raw.isascii()
        or any(ord(character) <= 0x20 or ord(character) == 0x7F for character in raw)
    ):
        return None

    port: int | None = None
    if raw.startswith("["):
        closing = raw.find("]")
        if closing < 0:
            return None
        address_text = raw[1:closing]
        remainder = raw[closing + 1 :]
        if remainder:
            if not remainder.startswith(":"):
                return None
            port = _parse_port(remainder[1:])
            if port is None:
                return None
        try:
            address = ip_address(address_text)
        except ValueError:
            return None
        if not isinstance(address, IPv6Address):
            return None
        return address.compressed, port

    if "[" in raw or "]" in raw or raw.count(":") > 1:
        return None
    hostname_raw = raw
    if ":" in raw:
        hostname_raw, port_raw = raw.rsplit(":", 1)
        port = _parse_port(port_raw)
        if port is None:
            return None
    if not hostname_raw:
        return None
    try:
        address = ip_address(hostname_raw)
    except ValueError:
        address = None
    if isinstance(address, IPv4Address):
        return str(address), port
    if address is not None:
        return None
    if all(character in "0123456789." for character in hostname_raw):
        return None

    trailing_dot = hostname_raw.endswith(".")
    label_text = hostname_raw[:-1] if trailing_dot else hostname_raw
    if not label_text or len(hostname_raw) > 253:
        return None
    labels = label_text.split(".")
    if any(_DNS_LABEL.fullmatch(label) is None for label in labels):
        return None
    hostname = label_text.lower() + ("." if trailing_dot else "")
    return hostname, port


def _parse_port(raw: str) -> int | None:
    if not raw or len(raw) > 5 or not raw.isascii() or not raw.isdecimal():
        return None
    port = int(raw)
    return port if 1 <= port <= 65535 else None


def _is_loopback_hostname(hostname: str) -> bool:
    if hostname == "localhost":
        return True
    try:
        return ip_address(hostname).is_loopback
    except ValueError:
        return False


def _parse_canonical_timestamp(raw: str) -> int | None:
    if not raw or len(raw) > 12 or not raw.isascii() or not raw.isdecimal():
        return None
    value = int(raw)
    if value < 0 or str(value) != raw:
        return None
    return value


def _encode_base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _decode_base64url(value: str, *, expected_length: int) -> bytes | None:
    if not value or _BASE64URL.fullmatch(value) is None:
        return None
    padding = "=" * (-len(value) % 4)
    try:
        decoded = base64.b64decode(
            value + padding,
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, binascii.Error):
        return None
    if len(decoded) != expected_length or _encode_base64url(decoded) != value:
        return None
    return decoded
