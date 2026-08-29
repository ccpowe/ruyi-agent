"""Shared validation for configured HTTP(S) endpoints."""

from __future__ import annotations

from ipaddress import IPv6Address
from urllib.parse import SplitResult, urlsplit

from ruyi_agent.config.errors import ConfigError


def validate_http_url(
    value: str,
    *,
    path: str,
    forbid_credentials: bool = True,
) -> str:
    """Return an absolute HTTP(S) URL safe to pass to HTTP clients."""

    parsed = _split_url(value, path=path)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise _absolute_url_error(path)
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise _absolute_url_error(path) from exc
    if hostname is None or not _valid_hostname(hostname):
        raise _absolute_url_error(path)
    if port is not None and not 0 <= port <= 65535:
        raise _absolute_url_error(path)
    if _has_empty_port(parsed.netloc):
        raise _absolute_url_error(path)
    if _authority_has_whitespace(value, parsed):
        raise _absolute_url_error(path)
    if forbid_credentials and (
        parsed.username is not None or parsed.password is not None
    ):
        raise ConfigError(f"{path} must not contain credentials.")
    return value


def _split_url(value: str, *, path: str) -> SplitResult:
    try:
        return urlsplit(value)
    except ValueError as exc:
        raise _absolute_url_error(path) from exc


def _valid_hostname(hostname: str) -> bool:
    if not hostname or any(character.isspace() for character in hostname):
        return False
    if ":" in hostname:
        try:
            IPv6Address(hostname.split("%", maxsplit=1)[0])
        except ValueError:
            return False
        return True
    try:
        ascii_hostname = hostname.encode("idna").decode("ascii")
    except UnicodeError:
        return False
    if ascii_hostname.endswith("."):
        ascii_hostname = ascii_hostname[:-1]
    if not ascii_hostname or len(ascii_hostname) > 253:
        return False
    labels = ascii_hostname.split(".")
    return all(_valid_hostname_label(label) for label in labels)


def _valid_hostname_label(label: str) -> bool:
    return (
        bool(label)
        and len(label) <= 63
        and label[0].isalnum()
        and label[-1].isalnum()
        and all(character.isalnum() or character == "-" for character in label)
    )


def _authority_has_whitespace(value: str, parsed: SplitResult) -> bool:
    return any(character.isspace() for character in parsed.netloc) or any(
        character in "\r\n\t" for character in value
    )


def _has_empty_port(netloc: str) -> bool:
    authority = netloc.rsplit("@", maxsplit=1)[-1]
    return authority.endswith(":")


def _absolute_url_error(path: str) -> ConfigError:
    return ConfigError(f"{path} must be an absolute HTTP(S) URL.")
