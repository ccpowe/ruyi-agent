from __future__ import annotations

from typing import Any

import pytest

from ruyi_agent.config.agent_parser import parse_skill_selection
from ruyi_agent.config.provider_models import FrozenDict, FrozenList, LLMProviderSpec
from ruyi_agent.integrations import model_providers


def test_provider_spec_owns_a_deeply_immutable_config_tree() -> None:
    source: dict[str, Any] = {
        "transport": {
            "headers": ["first", {"retry": [1, 2]}],
            "features": {"reasoning", "tools"},
        }
    }

    provider = LLMProviderSpec(name="custom", kind="openai", init_kwargs=source)
    source["transport"]["headers"].append("late")
    source["transport"]["features"].add("late")

    transport = provider.init_kwargs["transport"]
    assert isinstance(provider.init_kwargs, FrozenDict)
    assert isinstance(transport, FrozenDict)
    assert isinstance(transport["headers"], FrozenList)
    assert transport == {
        "headers": ["first", {"retry": [1, 2]}],
        "features": {"reasoning", "tools"},
    }
    with pytest.raises(TypeError):
        provider.init_kwargs["late"] = True
    with pytest.raises(TypeError):
        transport["late"] = True
    with pytest.raises(TypeError):
        transport["headers"][0] = "changed"


def test_provider_spec_hash_matches_deep_equality() -> None:
    left = LLMProviderSpec(
        name="custom",
        kind="openai",
        init_kwargs={"nested": {"items": [1, 2], "tags": {"a", "b"}}},
    )
    right = LLMProviderSpec(
        name="custom",
        kind="openai",
        init_kwargs={"nested": {"items": [1, 2], "tags": {"b", "a"}}},
    )

    assert left == right
    assert hash(left) == hash(right)
    assert len({left, right}) == 1


def test_provider_sdk_receives_a_recursive_mutable_copy(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    def fake_init_chat_model(
        model: str,
        *,
        model_provider: str,
        **kwargs: Any,
    ) -> object:
        captured.update(kwargs)
        assert model == "test-model"
        assert model_provider == "openai"
        return object()

    monkeypatch.setattr(model_providers, "init_chat_model", fake_init_chat_model)
    provider = LLMProviderSpec(
        name="custom",
        kind="openai",
        init_kwargs={
            "transport": {"headers": ["first"], "features": {"tools"}}
        },
    )

    model_providers.build_chat_model(
        model_name="test-model",
        provider_name="custom",
        providers={"custom": provider},
        getenv=lambda _name: None,
    )

    transport = captured["transport"]
    assert type(transport) is dict
    assert type(transport["headers"]) is list
    assert type(transport["features"]) is set
    transport["headers"].append("sdk-mutation")
    transport["features"].add("sdk-mutation")
    assert provider.init_kwargs["transport"] == {
        "headers": ["first"],
        "features": {"tools"},
    }


def test_skill_list_items_are_trimmed() -> None:
    assert parse_skill_selection([" frontend ", "backend"]) == (
        "frontend",
        "backend",
    )


def test_skill_list_rejects_whitespace_only_items() -> None:
    with pytest.raises(ValueError, match="must be a non-empty string"):
        parse_skill_selection(["frontend", "   "])
