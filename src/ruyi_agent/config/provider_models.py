"""Typed model-provider configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


SUPPORTED_MODEL_PROVIDERS = frozenset(
    {
        "anthropic",
        "deepseek",
        "litellm",
        "moonshot",
        "openai",
        "openai_codex",
        "openrouter",
    }
)
RESERVED_PROVIDER_INIT_KWARGS = frozenset(
    {"api_key", "base_url", "model", "model_provider"}
)


@dataclass(frozen=True, slots=True)
class LLMProviderSpec:
    name: str
    kind: str
    base_url: str | None = None
    api_key_env: str | None = None
    init_kwargs: dict[str, Any] = field(default_factory=dict)


def validate_provider_init_kwargs(
    provider_name: str,
    init_kwargs: dict[str, Any],
) -> None:
    reserved = sorted(set(init_kwargs) & RESERVED_PROVIDER_INIT_KWARGS)
    if reserved:
        raise ValueError(
            f"providers.{provider_name}.init_kwargs cannot include reserved keys: "
            + ", ".join(reserved)
        )
