"""Validation for model-provider TOML tables."""

from __future__ import annotations

from collections.abc import Mapping
from urllib.parse import urlsplit

from ruyi_agent.config.provider_models import (
    LLMProviderSpec,
    SUPPORTED_MODEL_PROVIDERS,
    validate_provider_init_kwargs,
)


def parse_llm_provider_configs(raw_providers: object) -> dict[str, LLMProviderSpec]:
    if not isinstance(raw_providers, Mapping):
        raise ValueError("providers must be a table")
    providers: dict[str, LLMProviderSpec] = {}
    for provider_name, raw_provider in raw_providers.items():
        if not isinstance(provider_name, str) or not provider_name.strip():
            raise ValueError("providers keys must be non-empty strings")
        providers[provider_name] = _parse_provider(provider_name, raw_provider)
    return providers


def _parse_provider(provider_name: str, raw_provider: object) -> LLMProviderSpec:
    path = f"providers.{provider_name}"
    if not isinstance(raw_provider, Mapping):
        raise ValueError(f"{path} must be a table")
    allowed_keys = {"kind", "base_url", "api_key_env", "init_kwargs"}
    unexpected = sorted(set(raw_provider) - allowed_keys)
    if unexpected:
        raise ValueError(f"{path} has unexpected fields: {', '.join(unexpected)}")

    kind = _non_empty_string(raw_provider.get("kind"), path=f"{path}.kind")
    if kind not in SUPPORTED_MODEL_PROVIDERS:
        allowed = ", ".join(sorted(SUPPORTED_MODEL_PROVIDERS))
        raise ValueError(f"{path}.kind must be one of: {allowed}")
    base_url = _optional_string(raw_provider.get("base_url"), path=f"{path}.base_url")
    if base_url is not None:
        parsed_url = urlsplit(base_url)
        if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
            raise ValueError(f"{path}.base_url must be an absolute HTTP(S) URL")
    api_key_env = _optional_string(
        raw_provider.get("api_key_env"), path=f"{path}.api_key_env"
    )
    if kind != "openai_codex" and not api_key_env:
        raise ValueError(f"{path}.api_key_env must be a non-empty string")
    init_kwargs = raw_provider.get("init_kwargs", {})
    if not isinstance(init_kwargs, dict):
        raise ValueError(f"{path}.init_kwargs must be a table")
    validate_provider_init_kwargs(provider_name, init_kwargs)
    return LLMProviderSpec(
        name=provider_name,
        kind=kind,
        base_url=base_url,
        api_key_env=api_key_env,
        init_kwargs=dict(init_kwargs),
    )


def _optional_string(value: object, *, path: str) -> str | None:
    if value is None:
        return None
    return _non_empty_string(value, path=path)


def _non_empty_string(value: object, *, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{path} must be a non-empty string")
    return value
