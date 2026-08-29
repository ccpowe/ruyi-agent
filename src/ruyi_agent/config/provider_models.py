"""Typed model-provider configuration."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
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


class FrozenList(Sequence[Any]):
    """Hashable immutable representation of a configured list."""

    __slots__ = ("_items",)

    def __init__(self, values: Sequence[Any]) -> None:
        self._items = tuple(_freeze_config_value(value) for value in values)

    def __getitem__(self, index: int | slice) -> Any:
        return self._items[index]

    def __len__(self) -> int:
        return len(self._items)

    def __hash__(self) -> int:
        return hash(self._items)

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Sequence) and not isinstance(other, (str, bytes)):
            return tuple(self) == tuple(other)
        return NotImplemented

    def __repr__(self) -> str:
        return repr(list(self._items))


class FrozenDict(Mapping[str, Any]):
    """Hashable immutable mapping that recursively owns its values."""

    __slots__ = ("_data", "_hash")

    def __init__(self, values: Mapping[str, Any] | None = None) -> None:
        data = values or {}
        self._data = {
            key: _freeze_config_value(value) for key, value in data.items()
        }
        self._hash = hash(frozenset(self._data.items()))

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __hash__(self) -> int:
        return self._hash

    def __repr__(self) -> str:
        return repr(self._data)


def _freeze_config_value(value: Any) -> Any:
    if isinstance(value, FrozenDict | FrozenList):
        return value
    if isinstance(value, Mapping):
        return FrozenDict(value)
    if isinstance(value, list):
        return FrozenList(value)
    if isinstance(value, tuple):
        return tuple(_freeze_config_value(item) for item in value)
    if isinstance(value, set | frozenset):
        return frozenset(_freeze_config_value(item) for item in value)
    try:
        hash(value)
    except TypeError as exc:
        raise TypeError(
            f"Unsupported mutable provider init value: {type(value).__name__}"
        ) from exc
    return value


def _thaw_config_value(value: Any) -> Any:
    if isinstance(value, FrozenDict):
        return {key: _thaw_config_value(item) for key, item in value.items()}
    if isinstance(value, FrozenList):
        return [_thaw_config_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_thaw_config_value(item) for item in value)
    if isinstance(value, frozenset):
        return {_thaw_config_value(item) for item in value}
    return value


@dataclass(frozen=True, slots=True)
class LLMProviderSpec:
    name: str
    kind: str
    base_url: str | None = None
    api_key_env: str | None = None
    init_kwargs: FrozenDict = field(default_factory=FrozenDict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "init_kwargs", FrozenDict(self.init_kwargs))


def mutable_provider_init_kwargs(provider: LLMProviderSpec) -> dict[str, Any]:
    """Return a recursive defensive copy suitable for a provider SDK."""

    return {
        key: _thaw_config_value(value) for key, value in provider.init_kwargs.items()
    }


def validate_provider_init_kwargs(
    provider_name: str,
    init_kwargs: Mapping[str, Any],
) -> None:
    reserved = sorted(set(init_kwargs) & RESERVED_PROVIDER_INIT_KWARGS)
    if reserved:
        raise ValueError(
            f"providers.{provider_name}.init_kwargs cannot include reserved keys: "
            + ", ".join(reserved)
        )
