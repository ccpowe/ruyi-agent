from __future__ import annotations

from collections.abc import Iterable

import pytest

from ruyi_agent.channels.http.team_console_auth import (
    TEAM_CONSOLE_REQUEST_HEADER,
    TEAM_CONSOLE_SESSION_COOKIE,
    TEAM_CONSOLE_SESSION_TTL_SECONDS,
    TeamConsoleAuthenticator,
    browser_request_is_same_origin,
)


def _scope(
    *headers: tuple[bytes, bytes],
    scheme: str = "http",
    host: bytes | None = b"127.0.0.1",
) -> dict[str, object]:
    raw_headers = ([] if host is None else [(b"host", host)]) + list(headers)
    return {
        "type": "http",
        "method": "GET",
        "scheme": scheme,
        "path": "/",
        "raw_path": b"/",
        "query_string": b"",
        "headers": raw_headers,
        "state": {},
    }


def _cookie(ticket: str) -> tuple[bytes, bytes]:
    return b"cookie", f"{TEAM_CONSOLE_SESSION_COOKIE}={ticket}".encode("ascii")


def _console_headers(ticket: str) -> Iterable[tuple[bytes, bytes]]:
    return (
        _cookie(ticket),
        (TEAM_CONSOLE_REQUEST_HEADER.encode("ascii"), b"1"),
        (b"sec-fetch-site", b"same-origin"),
    )


def test_session_ticket_is_shared_across_workers_and_rotates_with_bearer() -> None:
    issuer = TeamConsoleAuthenticator(
        "strong-secret",
        clock=lambda: 1_000,
        nonce_factory=lambda size: b"n" * size,
    )
    same_principal_worker = TeamConsoleAuthenticator(
        "strong-secret",
        clock=lambda: 1_001,
    )
    rotated_principal = TeamConsoleAuthenticator(
        "rotated-secret",
        clock=lambda: 1_001,
    )

    ticket = issuer.issue_session()

    assert same_principal_worker.validate_session(ticket) is True
    assert rotated_principal.validate_session(ticket) is False


def test_session_ticket_rejects_tampering_expiry_and_future_issue() -> None:
    now = [1_000.0]
    auth = TeamConsoleAuthenticator(
        "strong-secret",
        clock=lambda: now[0],
        nonce_factory=lambda size: b"n" * size,
    )
    ticket = auth.issue_session()

    tampered = f"{ticket[:-1]}{'A' if ticket[-1] != 'A' else 'B'}"
    assert auth.validate_session(tampered) is False

    now[0] = 1_000 + TEAM_CONSOLE_SESSION_TTL_SECONDS
    assert auth.validate_session(ticket) is False

    now[0] = 900
    assert auth.validate_session(ticket) is False


@pytest.mark.parametrize(
    "ticket",
    [
        "",
        "v2.1.2.a.b",
        "v1.01.2.a.b",
        "v1.1.2.a.b",
        "v1.1.2.===.===",
        "x" * 257,
        "非-ascii",
    ],
)
def test_session_ticket_rejects_noncanonical_shapes(ticket: str) -> None:
    auth = TeamConsoleAuthenticator("strong-secret", clock=lambda: 1_000)

    assert auth.validate_session(ticket) is False


def test_nonce_factory_must_return_exactly_32_bytes() -> None:
    auth = TeamConsoleAuthenticator(
        "strong-secret",
        clock=lambda: 1_000,
        nonce_factory=lambda size: b"short",
    )

    with pytest.raises(ValueError, match="exactly 32 bytes"):
        auth.issue_session()


def test_invalid_authorization_never_falls_back_to_console_cookie() -> None:
    auth = TeamConsoleAuthenticator("strong-secret", clock=lambda: 1_000)
    ticket = auth.issue_session()
    scope = _scope(
        *_console_headers(ticket),
        (b"authorization", b"Bearer wrong"),
    )

    assert auth.authenticate_api_request(scope) is None


def test_duplicate_authorization_and_session_cookie_are_rejected() -> None:
    auth = TeamConsoleAuthenticator("strong-secret", clock=lambda: 1_000)
    ticket = auth.issue_session()
    duplicate_authorization = _scope(
        (b"authorization", b"Bearer strong-secret"),
        (b"authorization", b"Bearer strong-secret"),
    )
    duplicate_cookie = _scope(
        (b"cookie", f"{TEAM_CONSOLE_SESSION_COOKIE}={ticket}".encode("ascii")),
        (
            b"cookie",
            f"other=1; {TEAM_CONSOLE_SESSION_COOKIE}={ticket}".encode("ascii"),
        ),
        (TEAM_CONSOLE_REQUEST_HEADER.encode("ascii"), b"1"),
        (b"sec-fetch-site", b"same-origin"),
    )
    whitespace_cookie = _scope(
        (
            b"cookie",
            f"{TEAM_CONSOLE_SESSION_COOKIE}= {ticket}".encode("ascii"),
        ),
        (TEAM_CONSOLE_REQUEST_HEADER.encode("ascii"), b"1"),
        (b"sec-fetch-site", b"same-origin"),
    )

    assert auth.authenticate_api_request(duplicate_authorization) is None
    assert auth.authenticate_console_session(duplicate_cookie) is False
    assert auth.authenticate_api_request(duplicate_cookie) is None
    assert auth.authenticate_api_request(whitespace_cookie) is None


def test_cookie_api_auth_requires_marker_session_and_same_origin_signal() -> None:
    auth = TeamConsoleAuthenticator("strong-secret", clock=lambda: 1_000)
    ticket = auth.issue_session()
    valid = _scope(*_console_headers(ticket))
    no_marker = _scope(_cookie(ticket), (b"sec-fetch-site", b"same-origin"))
    no_source_signal = _scope(
        _cookie(ticket),
        (TEAM_CONSOLE_REQUEST_HEADER.encode("ascii"), b"1"),
    )
    origin_fallback = _scope(
        _cookie(ticket),
        (TEAM_CONSOLE_REQUEST_HEADER.encode("ascii"), b"1"),
        (b"origin", b"http://127.0.0.1"),
    )

    assert auth.authenticate_api_request(valid) == "console_session"
    assert auth.authenticate_api_request(no_marker) is None
    assert auth.authenticate_api_request(no_source_signal) is None
    assert auth.authenticate_api_request(origin_fallback) == "console_session"


def test_same_origin_rejects_every_contradictory_or_duplicate_signal() -> None:
    assert browser_request_is_same_origin(
        _scope(
            (b"sec-fetch-site", b"same-origin"),
            (b"origin", b"http://127.0.0.1"),
            (b"referer", b"http://127.0.0.1/debug/team"),
        )
    )
    invalid_scopes = [
        _scope((b"sec-fetch-site", b"cross-site")),
        _scope(
            (b"sec-fetch-site", b"same-origin"),
            (b"origin", b"https://attacker.example"),
        ),
        _scope(
            (b"origin", b"http://127.0.0.1"),
            (b"referer", b"http://attacker.example/path"),
        ),
        _scope(
            (b"origin", b"http://127.0.0.1"),
            (b"origin", b"http://127.0.0.1"),
        ),
        _scope((b"origin", b"null")),
        _scope((b"origin", b"http://user@127.0.0.1")),
    ]

    assert all(not browser_request_is_same_origin(scope) for scope in invalid_scopes)


@pytest.mark.parametrize(
    ("scheme", "host", "allowed", "secure"),
    [
        ("http", b"127.0.0.1", True, False),
        ("http", b"localhost:8000", True, False),
        ("http", b"[::1]:8000", True, False),
        ("http", b"gateway.example", False, False),
        ("https", b"gateway.example", True, True),
    ],
)
def test_console_transport_policy(
    scheme: str,
    host: bytes,
    allowed: bool,
    secure: bool,
) -> None:
    auth = TeamConsoleAuthenticator("strong-secret")
    scope = _scope(scheme=scheme, host=host)

    assert auth.console_transport_allowed(scope) is allowed
    assert auth.session_cookie_is_secure(scope) is secure


def test_forged_forwarded_proto_and_invalid_host_cannot_enable_console() -> None:
    auth = TeamConsoleAuthenticator("strong-secret")
    forged_proxy = _scope(
        (b"x-forwarded-proto", b"https"),
        scheme="http",
        host=b"gateway.example",
    )
    invalid_hosts = [
        _scope(host=None),
        _scope((b"host", b"127.0.0.1"), host=b"127.0.0.1"),
        _scope(host=b"user@127.0.0.1"),
        _scope(host=b"127.0.0.1:bad"),
        _scope(host=b"[::1"),
        _scope(host=b"127.0.0.1\r\nattack: yes"),
    ]

    assert auth.console_transport_allowed(forged_proxy) is False
    assert all(not auth.console_transport_allowed(scope) for scope in invalid_hosts)


def test_console_auth_marker_sets_no_store_state_flag() -> None:
    auth = TeamConsoleAuthenticator("strong-secret", clock=lambda: 1_000)
    ticket = auth.issue_session()
    scope = _scope(*_console_headers(ticket))

    assert auth.authenticate_api_request(scope) == "console_session"
    auth.mark_console_api_authenticated(scope)

    assert scope["state"] == {"ruyi_team_console_cookie_authenticated": True}
