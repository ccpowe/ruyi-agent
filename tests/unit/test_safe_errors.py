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


def test_safe_error_text_avoids_partial_short_known_secret_replacement() -> None:
    secret = "abc123"
    source = "The abcx token is prose; abc123x is a longer identifier."

    summary = safe_error_text(source, known_secrets=("abc", secret))

    assert summary == source
    assert safe_error_text(
        f"provider rejected {secret}", known_secrets=(secret,)
    ) == "Sensitive error details redacted."


@pytest.mark.parametrize(
    ("secret", "source"),
    [
        ("abc", "abc.def/abc/def"),
        ("1", "1.0"),
        ("a+b", "a+b.ext"),
    ],
)
def test_safe_error_text_keeps_short_known_secret_inside_credential_token(
    secret: str,
    source: str,
) -> None:
    assert safe_error_text(source, known_secrets=(secret,)) == source


def test_safe_error_text_redacts_structured_short_known_secret() -> None:
    assert safe_error_text("api_key=1", known_secrets=("1",)) == (
        f"api_key={REDACTED_VALUE}"
    )


@pytest.mark.parametrize("secret", ["a", "abcdefg", "1", "1234567"])
def test_safe_exception_summary_falls_back_for_standalone_short_known_secret(
    secret: str,
) -> None:
    summary = safe_exception_summary(
        RuntimeError(f"provider rejected {secret}"),
        known_secrets=(secret,),
    )

    assert summary == "RuntimeError: Sensitive error details redacted."


def test_safe_exception_summary_keeps_nonstandalone_short_known_secret_text() -> None:
    summary = safe_exception_summary(
        ValueError("validation failed"),
        known_secrets=("a",),
    )

    assert summary == "ValueError: validation failed"


def test_safe_exception_summary_falls_back_for_a_standalone_short_number() -> None:
    summary = safe_exception_summary(
        ValueError("Expected 1 result"),
        known_secrets=("1",),
    )

    assert summary == "ValueError: Sensitive error details redacted."
    assert "Expected 1 result" not in summary


def test_safe_exception_summary_keeps_diagnostics_without_short_secret() -> None:
    summary = safe_exception_summary(
        RuntimeError("provider rejected an invalid format"),
        known_secrets=("1234567",),
    )

    assert summary == "RuntimeError: provider rejected an invalid format"


@pytest.mark.parametrize("secret", ["@@", "密", "a b"])
def test_safe_error_entries_fall_back_for_matching_non_token_short_secret(
    secret: str,
) -> None:
    reason = f"provider rejected {secret}"

    assert safe_error_text(
        reason,
        known_secrets=(secret,),
    ) == "Sensitive error details redacted."
    assert safe_exception_summary(
        RuntimeError(reason),
        known_secrets=(secret,),
    ) == "RuntimeError: Sensitive error details redacted."


@pytest.mark.parametrize("secret", ["@@", "密", "a b"])
def test_safe_error_entries_keep_diagnostics_without_non_token_short_secret(
    secret: str,
) -> None:
    reason = "provider rejected an invalid format"

    assert safe_error_text(reason, known_secrets=(secret,)) == reason
    assert safe_exception_summary(
        RuntimeError(reason),
        known_secrets=(secret,),
    ) == f"RuntimeError: {reason}"


@pytest.mark.parametrize("max_length", [0, -1])
def test_safe_error_entries_return_empty_before_short_secret_fallback(
    max_length: int,
) -> None:
    reason = "Expected 1 result"

    assert safe_error_text(
        reason,
        known_secrets=("1",),
        max_length=max_length,
    ) == ""
    assert safe_exception_summary(
        ValueError(reason),
        known_secrets=("1",),
        max_length=max_length,
    ) == ""


def test_safe_exception_summary_downgrades_group_reasons_for_short_secret() -> None:
    summary = safe_exception_summary(
        ExceptionGroup(
            "outer",
            [ValueError("normal diagnostic"), RuntimeError("provider rejected a")],
        ),
        known_secrets=("a",),
    )

    assert "ValueError: Sensitive error details redacted." in summary
    assert "RuntimeError: Sensitive error details redacted." in summary
    assert "normal diagnostic" not in summary


def test_safe_error_text_keeps_benign_token_and_password_words() -> None:
    source = "The token password wording is documentation, not an assignment."

    assert safe_error_text(source) == source


def test_safe_error_text_keeps_unstructured_colon_reasons() -> None:
    source = "expected token: identifier after password: field"

    assert safe_error_text(source) == source


@pytest.mark.parametrize(
    ("scheme", "credential"),
    [
        ("Bearer", "abc"),
        ("Basic", "@@"),
        ("Token", "\u5bc6"),
    ],
)
def test_safe_error_text_redacts_authorization_scheme_credential_and_keeps_reason(
    scheme: str,
    credential: str,
) -> None:
    source = f"Authorization: {scheme} {credential} rejected by upstream"

    summary = safe_error_text(source)

    assert credential not in summary
    assert summary == (
        f"Authorization: {scheme} {REDACTED_VALUE} rejected by upstream"
    )


@pytest.mark.parametrize(
    ("key", "credential"),
    [
        ("password", "huntertwo"),
        ("api_key", "abcdefgh"),
    ],
)
def test_safe_error_text_redacts_strong_colon_credential_and_keeps_reason(
    key: str,
    credential: str,
) -> None:
    source = f"{key}:{credential} rejected by upstream"

    summary = safe_error_text(source)

    assert credential not in summary
    assert summary == f"{key}:{REDACTED_VALUE} rejected by upstream"


@pytest.mark.parametrize(
    "source",
    [
        "password: field",
        "expected token: identifier after password: field",
    ],
)
def test_safe_error_text_keeps_short_colon_prose(source: str) -> None:
    assert safe_error_text(source) == source


def test_safe_error_text_redacts_scheme_credential_without_crossing_crlf() -> None:
    credential = "abc"
    summary = safe_error_text(
        f"Authorization: Bearer {credential}\r\nreason=kept"
    )

    assert credential not in summary
    assert summary == f"Authorization: Bearer {REDACTED_VALUE} reason=kept"


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


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("Authorization:\r\nreason=kept", "Authorization: reason=kept"),
        ("Cookie:\r\nreason=kept", "Cookie: reason=kept"),
        ("password:\r\nreason=kept", "password: reason=kept"),
        ("password=\r\nreason=kept", "password= reason=kept"),
    ],
)
def test_safe_error_text_does_not_cross_crlf_after_empty_structured_value(
    source: str,
    expected: str,
) -> None:
    summary = safe_error_text(source, known_secrets=("1",))

    assert summary == expected
    assert "\n" not in summary


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
