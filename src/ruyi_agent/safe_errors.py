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

_SENSITIVE_KEY_PATTERN = (
    r"(?:api[_-]?key|access[_-]?token|refresh[_-]?token|id[_-]?token|"
    r"auth(?:orization)?|bearer|token|password|passwd|client[_-]?secret|"
    r"secret|private[_-]?key)"
)
_HEADER_KEY_PATTERN = (
    r"(?:authorization|proxy-authorization|x-api-key|api-key|"
    r"x-auth-token|x-access-token|cookie|set-cookie)"
)
_URL_SENSITIVE_PARAM_RE = re.compile(
    rf"(?i)(?P<prefix>[?&;]{_SENSITIVE_KEY_PATTERN}=)(?P<value>[^&#\s]+)"
)
_HEADER_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![\w-]){_HEADER_KEY_PATTERN}(?![\w-])\s*[:=]\s*)"
    r"(?P<value>(?:bearer\s+)?[^,;\r\n]+)"
)
_QUOTED_KEY_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![\w-])['\"]?{_SENSITIVE_KEY_PATTERN}['\"]?\s*"
    r"[:=]\s*)(?P<quote>['\"])(?P<value>[^'\"\r\n]*)(?P=quote)"
)
_UNQUOTED_KEY_VALUE_RE = re.compile(
    rf"(?i)(?P<prefix>(?<![\w-])['\"]?{_SENSITIVE_KEY_PATTERN}['\"]?\s*"
    r"[:=]\s*)(?P<value>[^,;\s}\]\[\r\n]+)"
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


def safe_error_text(
    text: object,
    *,
    known_secrets: Iterable[str] = (),
    max_input_chars: int = DEFAULT_SAFE_ERROR_INPUT_CHARS,
    max_length: int = DEFAULT_SAFE_ERROR_SUMMARY_CHARS,
) -> str:
    """Return a bounded, one-line error text with narrow high-confidence redaction."""

    normalized_secrets = _normalize_known_secrets(known_secrets)
    return _safe_error_text(
        text,
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
    summaries: list[str] = []
    seen: set[str] = set()
    for leaf in leaves:
        class_name = leaf.__class__.__name__
        reason = _safe_error_text(
            _exception_text(leaf),
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
    value = _replace_known_secrets(value, known_secrets)
    value = _redact_pem_private_keys(value)
    value = _URL_SENSITIVE_PARAM_RE.sub(_redact_match, value)
    value = _HEADER_RE.sub(_redact_match, value)
    value = _QUOTED_KEY_VALUE_RE.sub(_redact_match, value)
    value = _UNQUOTED_KEY_VALUE_RE.sub(_redact_match, value)
    value = _JWT_RE.sub(REDACTED_VALUE, value)
    value = _COMMON_KEY_PREFIX_RE.sub(REDACTED_VALUE, value)
    return _one_line_and_bound(value, max_length=max_length)


def _normalize_known_secrets(known_secrets: Iterable[str]) -> tuple[str, ...]:
    values = {secret for secret in known_secrets if isinstance(secret, str) and secret}
    return tuple(sorted(values, key=len, reverse=True))


def _replace_known_secrets(value: str, known_secrets: tuple[str, ...]) -> str:
    for secret in known_secrets:
        value = value.replace(secret, REDACTED_VALUE)
    return value


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
