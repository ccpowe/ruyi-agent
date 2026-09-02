from __future__ import annotations

import pytest

from ruyi_agent.safe_errors import (
    REDACTED_VALUE,
    safe_error_text,
    safe_exception_summary,
)


@pytest.mark.parametrize(
    ("source", "secret"),
    [
        ("Authorization: Bearer header-secret-value", "header-secret-value"),
        ("api_key=key-value-secret", "key-value-secret"),
        (
            "https://example.invalid/tool?access_token=url-param-secret&limit=1",
            "url-param-secret",
        ),
        (
            "received eyJhbGciOiJub25lIn0.eyJzdWIiOiJkZW1vIn0.signature-value",
            "eyJhbGciOiJub25lIn0.eyJzdWIiOiJkZW1vIn0.signature-value",
        ),
        (
            "provider returned sk-proj-synthetic-key-material",
            "sk-proj-synthetic-key-material",
        ),
        (
            "-----BEGIN PRIVATE KEY-----\nprivate-key-material\n-----END PRIVATE KEY-----",
            "private-key-material",
        ),
    ],
)
def test_safe_error_text_redacts_high_confidence_secret_corpus(
    source: str,
    secret: str,
) -> None:
    summary = safe_error_text(source)

    assert secret not in summary
    assert REDACTED_VALUE in summary
    assert "\n" not in summary


def test_safe_error_text_replaces_known_secrets_longest_first() -> None:
    short_secret = "overlapping-secret"
    long_secret = f"prefix-{short_secret}"

    summary = safe_error_text(
        f"provider failed with {long_secret}",
        known_secrets=(short_secret, long_secret),
    )

    assert summary == f"provider failed with {REDACTED_VALUE}"


def test_safe_error_text_bounds_short_known_token_replacement() -> None:
    secret = "abc123"
    source = "The abc token is prose; abc123x is a longer identifier."

    summary = safe_error_text(source, known_secrets=("abc", secret))

    assert summary == source
    assert safe_error_text(
        f"provider rejected {secret}", known_secrets=(secret,)
    ) == f"provider rejected {REDACTED_VALUE}"


def test_safe_error_text_keeps_benign_token_and_password_words() -> None:
    source = "The token password wording is documentation, not an assignment."

    assert safe_error_text(source) == source


def test_safe_error_text_keeps_unstructured_colon_reasons() -> None:
    source = "expected token: identifier after password: field"

    assert safe_error_text(source) == source


def test_safe_error_text_redacts_one_header_token_without_consuming_reason() -> None:
    source = "Authorization: Bearer abc rejected by upstream"
    secret = "header-secret-value"
    header_with_reason = f"Authorization: Bearer {secret} rejected by upstream"

    assert safe_error_text(source) == source
    summary = safe_error_text(header_with_reason)
    assert secret not in summary
    assert summary.endswith("rejected by upstream")


def test_safe_error_text_respects_crlf_and_quoted_value_boundaries() -> None:
    source = (
        "Authorization: Bearer header-secret-value\r\n"
        "password: \"quoted-secret\"\r\n"
        "password=unquoted-secret\r\n"
        "password: unquoted-secret\r\n"
        "next=kept"
    )

    summary = safe_error_text(source)

    for secret in (
        "header-secret-value",
        "quoted-secret",
        "unquoted-secret",
    ):
        assert secret not in summary
    assert summary.count(REDACTED_VALUE) == 4
    assert "next=kept" in summary
    assert "\n" not in summary


def test_safe_error_text_redacts_cookie_header_without_consuming_next_line() -> None:
    secret = "cookie-secret"
    summary = safe_error_text(
        f"Cookie: session={secret}; theme=dark\r\nreason=kept"
    )

    assert secret not in summary
    assert REDACTED_VALUE in summary
    assert "reason=kept" in summary


def test_safe_error_text_is_bounded_after_redaction() -> None:
    secret = "bounded-secret-value"
    summary = safe_error_text(
        f"api_key={secret}" + ("x" * 400),
        max_input_chars=128,
        max_length=80,
    )

    assert secret not in summary
    assert len(summary) <= 80
    assert "\n" not in summary


def test_safe_exception_summary_deduplicates_group_leaves_and_keeps_reason() -> None:
    secret = "group-secret-value"
    exc = ExceptionGroup(
        "outer",
        [
            ValueError("invalid arguments"),
            RuntimeError(f"Authorization: Bearer {secret}"),
            RuntimeError(f"Authorization: Bearer {secret}"),
        ],
    )

    summary = safe_exception_summary(exc)

    assert "ValueError: invalid arguments" in summary
    assert summary.count("RuntimeError:") == 1
    assert secret not in summary
    assert REDACTED_VALUE in summary
    assert "\n" not in summary


def test_safe_exception_summary_is_single_line_and_bounded() -> None:
    summary = safe_exception_summary(
        RuntimeError("line one\n" + ("x" * 400)),
        max_length=64,
    )

    assert summary.startswith("RuntimeError: line one")
    assert len(summary) <= 64
    assert "\n" not in summary
