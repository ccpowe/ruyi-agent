"""Narrow, stdlib-only redaction for error text that leaves Ruyi boundaries.

This module only sanitizes text explicitly passed to it.  It does not inspect
the process environment, historical records, or successful tool output.
"""

from __future__ import annotations

from collections.abc import Iterable
import re

REDACTED_VALUE = "[REDACTED]"
DEFAULT_SAFE_ERROR_INPUT_CHARS = 4096
DEFAULT_SAFE_ERROR_SUMMARY_CHARS = 1024
MAX_EXCEPTION_LEAVES = 16
MAX_EXCEPTION_NODES = MAX_EXCEPTION_LEAVES * 4
_MAX_EXCEPTION_REASON_CHARS = 384
_GENERIC_SAFE_ERROR_REASON = "Sensitive error details redacted."

_SENSITIVE_KEY_PATTERN = (
    r"(?:api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"auth(?:orization)?|bearer|token|password|passwd|client[_-]?secret|"
    r"secret|private[_-]?key)"
)
_AUTHORIZATION_HEADER_KEY_PATTERN = r"(?:authorization|proxy-authorization)"
_CREDENTIAL_HEADER_KEY_PATTERN = (
    r"(?:x-api-key|api-key|x-auth-token|x-access-token)"
)
_COOKIE_HEADER_KEY_PATTERN = r"(?:cookie|set-cookie)"
_CREDENTIAL_TOKEN_PATTERN = r"[A-Za-z0-9._~+/=-]+"
_CREDENTIAL_LIKE_VALUE_PATTERN = (
    r"(?=[A-Za-z0-9._~+/=-]{8,}(?=$|[,; \t}\]\[\r\n]))"
    r"(?=[A-Za-z0-9._~+/=-]*[0-9.=+/~\-])"
    r"[A-Za-z0-9._~+/=-]+"
)
_TOKEN_BOUNDARY_CHAR_CLASS = r"A-Za-z0-9._~+/=\-"
_SHORT_SECRET_BOUNDARY_CHAR_CLASS = r"A-Za-z0-9_-"
_MIN_UNBOUNDED_KNOWN_SECRET_CHARS = 8
_URL_SENSITIVE_PARAM_RE = re.compile(
    rf"(?i)(?P<prefix>[?&;]{_SENSITIVE_KEY_PATTERN}=)(?P<value>[^&#\s]+)"
)
_AUTHORIZATION_HEADER_RE = re.compile(
    rf"(?im)(?P<prefix>(?<![\w-]){_AUTHORIZATION_HEADER_KEY_PATTERN}"
    rf"(?![\w-])[ \t]*[:=][ \t]*(?:(?:bearer|basic|token)[ \t]+)?)"
    rf"(?P<value>{_CREDENTIAL_TOKEN_PATTERN})(?=[ \t]*(?:$|[,;\r\n]))"
)
_AUTHORIZATION_HEADER_LEADING_CREDENTIAL_RE = re.compile(
    rf"(?im)(?P<prefix>(?<![\w-]){_AUTHORIZATION_HEADER_KEY_PATTERN}"
    rf"(?![\w-])[ \t]*[:=][ \t]*(?:(?:bearer|basic|token)[ \t]+)?)"
    rf"(?P<value>{_CREDENTIAL_LIKE_VALUE_PATTERN})"
)
_CREDENTIAL_HEADER_RE = re.compile(
    rf"(?im)(?P<prefix>(?<![\w-]){_CREDENTIAL_HEADER_KEY_PATTERN}"
    rf"(?![\w-])[ \t]*[:=][ \t]*)"
    rf"(?P<value>{_CREDENTIAL_TOKEN_PATTERN})(?=[ \t]*(?:$|[,;\r\n]))"
)
_CREDENTIAL_HEADER_LEADING_CREDENTIAL_RE = re.compile(
    rf"(?im)(?P<prefix>(?<![\w-]){_CREDENTIAL_HEADER_KEY_PATTERN}"
    rf"(?![\w-])[ \t]*[:=][ \t]*)"
    rf"(?P<value>{_CREDENTIAL_LIKE_VALUE_PATTERN})"
)
_COOKIE_HEADER_RE = re.compile(
    rf"(?im)(?P<prefix>(?<![\w-]){_COOKIE_HEADER_KEY_PATTERN}"
    rf"(?![\w-])[ \t]*[:=][ \t]*)(?P<value>[^\r\n]*=[^\r\n]*)"
)
_QUOTED_KEY_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![\w-])['\"]?{_SENSITIVE_KEY_PATTERN}['\"]?[ \t]*"
    r"[:=][ \t]*)(?P<quote>['\"])(?P<value>[^'\"\r\n]*)(?P=quote)"
)
_UNQUOTED_EQUALS_KEY_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![\w-])['\"]?{_SENSITIVE_KEY_PATTERN}['\"]?[ \t]*"
    r"=[ \t]*)(?P<value>[^,;\s}\]\[\r\n]+)"
)
_UNQUOTED_COLON_KEY_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![\w-])['\"]?{_SENSITIVE_KEY_PATTERN}['\"]?[ \t]*"
    rf":[ \t]*)(?P<value>{_CREDENTIAL_LIKE_VALUE_PATTERN})"
)
_JWT_RE = re.compile(
    r"(?<![A-Za-z0-9_-])eyJ[A-Za-z0-9_-]{5,}\."
    r"[A-Za-z0-9_-]{1,}\.[A-Za-z0-9_-]{6,}(?![A-Za-z0-9_-])"
)
_COMMON_KEY_PREFIX_RE = re.compile(
    r"(?<![A-Za-z0-9_-])(?:"
    r"sk-(?:proj-)?[A-Za-z0-9_-]{8,}|"
    r"sk-ant-[A-Za-z0-9_-]{8,}|"
    r"(?:ghp|gho|ghs|ghu|ghr)_[A-Za-z0-9]{8,}|"
    r"github_pat_[A-Za-z0-9_]{8,}|"
    r"xox(?:a|b|p|r|s)-[A-Za-z0-9-]{8,}|"
    r"xapp-[A-Za-z0-9-]{8,}|"
    r"AIza[A-Za-z0-9_-]{16,}|"
    r"(?:AKIA|ASIA)[0-9A-Z]{16}|"
    r"(?:sk|rk)_live_[A-Za-z0-9]{8,}"
    r")(?![A-Za-z0-9_-])"
)
_PEM_PRIVATE_KEY_RE = re.compile(
    r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----[\s\S]{0,4096}?"
    r"-----END (?:[A-Z0-9]+ )*PRIVATE KEY-----"
)
_PEM_PRIVATE_KEY_START_RE = re.compile(
    r"-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----"
)
_TOKEN_LITERAL_RE = re.compile(rf"^[{_TOKEN_BOUNDARY_CHAR_CLASS}]+$")


def safe_error_text(
    text: object,
    *,
    known_secrets: Iterable[str] = (),
    max_input_chars: int = DEFAULT_SAFE_ERROR_INPUT_CHARS,
    max_length: int = DEFAULT_SAFE_ERROR_SUMMARY_CHARS,
) -> str:
    """Return a bounded, one-line error text with narrow high-confidence redaction."""

    normalized_secrets = _normalize_known_secrets(known_secrets)
    value = _limit_input(_exception_text(text), max_input_chars=max_input_chars)
    if _contains_ambiguous_short_known_secret(value, normalized_secrets):
        return _one_line_and_bound(_GENERIC_SAFE_ERROR_REASON, max_length=max_length)
    return _safe_error_text(
        value,
        known_secrets=normalized_secrets,
        max_input_chars=max_input_chars,
        max_length=max_length,
    )


def safe_exception_summary(
    exc: BaseException,
    *,
    known_secrets: Iterable[str] = (),
    max_length: int = DEFAULT_SAFE_ERROR_SUMMARY_CHARS,
) -> str:
    """Render exception-group leaves as a de-duplicated, safe one-line summary."""

    if max_length < 1:
        return ""

    normalized_secrets = _normalize_known_secrets(known_secrets)
    leaves, omitted = _exception_leaves(exc)
    leaf_texts = [
        (
            leaf,
            _limit_input(
                _exception_text(leaf),
                max_input_chars=DEFAULT_SAFE_ERROR_INPUT_CHARS,
            ),
        )
        for leaf in leaves
    ]
    use_generic_reason = any(
        _contains_ambiguous_short_known_secret(text, normalized_secrets)
        for _leaf, text in leaf_texts
    )
    summaries: list[str] = []
    seen: set[str] = set()
    for leaf, text in leaf_texts:
        class_name = leaf.__class__.__name__
        if use_generic_reason:
            reason = _GENERIC_SAFE_ERROR_REASON
        else:
            reason = _safe_error_text(
                text,
                known_secrets=normalized_secrets,
                max_input_chars=DEFAULT_SAFE_ERROR_INPUT_CHARS,
                max_length=_MAX_EXCEPTION_REASON_CHARS,
            )
        rendered = f"{class_name}: {reason or class_name}"
        if rendered not in seen:
            seen.add(rendered)
            summaries.append(rendered)
    if omitted:
        summaries.append("Additional exception details omitted.")
    return _one_line_and_bound(" | ".join(summaries), max_length=max_length)


def _safe_error_text(
    text: object,
    *,
    known_secrets: tuple[str, ...],
    max_input_chars: int,
    max_length: int,
) -> str:
    if max_length < 1:
        return ""
    value = _limit_input(_exception_text(text), max_input_chars=max_input_chars)
    if _contains_ambiguous_short_known_secret(value, known_secrets):
        return _one_line_and_bound(_GENERIC_SAFE_ERROR_REASON, max_length=max_length)
    value = _replace_known_secrets(value, known_secrets)
    value = _redact_pem_private_keys(value)
    value = _URL_SENSITIVE_PARAM_RE.sub(_redact_match, value)
    value = _COOKIE_HEADER_RE.sub(_redact_match, value)
    value = _AUTHORIZATION_HEADER_LEADING_CREDENTIAL_RE.sub(_redact_match, value)
    value = _CREDENTIAL_HEADER_LEADING_CREDENTIAL_RE.sub(_redact_match, value)
    value = _AUTHORIZATION_HEADER_RE.sub(_redact_match, value)
    value = _CREDENTIAL_HEADER_RE.sub(_redact_match, value)
    value = _QUOTED_KEY_VALUE_RE.sub(_redact_match, value)
    value = _UNQUOTED_EQUALS_KEY_VALUE_RE.sub(_redact_match, value)
    value = _UNQUOTED_COLON_KEY_VALUE_RE.sub(_redact_match, value)
    value = _JWT_RE.sub(REDACTED_VALUE, value)
    value = _COMMON_KEY_PREFIX_RE.sub(REDACTED_VALUE, value)
    return _one_line_and_bound(value, max_length=max_length)


def _normalize_known_secrets(known_secrets: Iterable[str]) -> tuple[str, ...]:
    values = {secret for secret in known_secrets if isinstance(secret, str) and secret}
    return tuple(sorted(values, key=len, reverse=True))


def _replace_known_secrets(value: str, known_secrets: tuple[str, ...]) -> str:
    for secret in known_secrets:
        if len(secret) >= _MIN_UNBOUNDED_KNOWN_SECRET_CHARS:
            value = value.replace(secret, REDACTED_VALUE)
    return value


def _contains_ambiguous_short_known_secret(
    value: str,
    known_secrets: tuple[str, ...],
) -> bool:
    """Detect a standalone 1--7 character configured value without rewriting it."""

    for secret in known_secrets:
        if (
            len(secret) >= _MIN_UNBOUNDED_KNOWN_SECRET_CHARS
            or not _is_token_literal(secret)
        ):
            continue
        pattern = re.compile(
            rf"(?<![{_SHORT_SECRET_BOUNDARY_CHAR_CLASS}]){re.escape(secret)}"
            rf"(?![{_SHORT_SECRET_BOUNDARY_CHAR_CLASS}])"
        )
        if pattern.search(value) is not None:
            return True
    return False


def _is_token_literal(value: str) -> bool:
    return bool(_TOKEN_LITERAL_RE.fullmatch(value))


def _redact_pem_private_keys(value: str) -> str:
    value = _PEM_PRIVATE_KEY_RE.sub(f"{REDACTED_VALUE} PEM private key", value)
    first_unclosed = _PEM_PRIVATE_KEY_START_RE.search(value)
    if first_unclosed is not None:
        return value[: first_unclosed.start()] + f"{REDACTED_VALUE} PEM private key"
    return value


def _redact_match(match: re.Match[str]) -> str:
    return f"{match.group('prefix')}{REDACTED_VALUE}"


def _exception_leaves(exc: BaseException) -> tuple[list[BaseException], bool]:
    leaves: list[BaseException] = []
    pending: list[BaseException] = [exc]
    omitted = False
    visited = 0
    while (
        pending
        and len(leaves) < MAX_EXCEPTION_LEAVES
        and visited < MAX_EXCEPTION_NODES
    ):
        visited += 1
        candidate = pending.pop()
        if isinstance(candidate, BaseExceptionGroup):
            remaining = MAX_EXCEPTION_LEAVES - len(leaves) - len(pending)
            if remaining < 1:
                omitted = True
                continue
            if len(candidate.exceptions) > remaining:
                omitted = True
            pending.extend(reversed(candidate.exceptions[:remaining]))
            continue
        leaves.append(candidate)
    return leaves, omitted or bool(pending) or visited == MAX_EXCEPTION_NODES


def _exception_text(value: object) -> str:
    try:
        return str(value)
    except Exception:
        return value.__class__.__name__


def _limit_input(value: str, *, max_input_chars: int) -> str:
    if max_input_chars < 1:
        return ""
    if len(value) <= max_input_chars:
        return value
    return value[:max_input_chars]


def _one_line_and_bound(value: str, *, max_length: int) -> str:
    one_line = " ".join(value.replace("\x00", " ").split())
    if len(one_line) <= max_length:
        return one_line
    if max_length <= 3:
        return one_line[:max_length]
    return one_line[: max_length - 3].rstrip() + "..."
