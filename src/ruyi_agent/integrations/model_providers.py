"""Provider-specific LangChain model construction and compatibility adapters."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from langchain.chat_models import init_chat_model

from ruyi_agent.config.provider_models import (
    LLMProviderSpec,
    validate_provider_init_kwargs,
)


def build_chat_model(
    *,
    model_name: str,
    provider_name: str,
    providers: dict[str, LLMProviderSpec],
    getenv: Callable[[str], str | None],
) -> Any:
    """Build a chat model from already validated Agent/provider fields."""

    if not model_name:
        raise ValueError("Agent config field 'model' must be set.")
    if not provider_name:
        raise ValueError("Agent config field 'provider' must be set.")
    if provider_name not in providers:
        raise ValueError(f"Unknown provider: {provider_name!r}")
    provider = providers[provider_name]
    kwargs = _build_provider_kwargs(provider, getenv=getenv)
    if provider.kind == "moonshot":
        return _get_chat_moonshot_class()(model=model_name, **kwargs)
    if provider.kind == "deepseek":
        return _build_deepseek_model(model_name, **kwargs)
    if provider.kind == "litellm":
        return _build_litellm_model(model_name, **kwargs)
    if provider.kind == "openai_codex":
        return _build_openai_codex_model(model_name, **kwargs)
    return init_chat_model(model_name, model_provider=provider.kind, **kwargs)


def _resolve_api_key_from_provider(
    provider: LLMProviderSpec,
    *,
    getenv: Callable[[str], str | None],
) -> str | None:
    if not provider.api_key_env:
        return None
    api_key = getenv(provider.api_key_env)
    if api_key:
        return api_key
    raise ValueError(
        f"Environment variable {provider.api_key_env!r} configured by "
        f"providers.{provider.name}.api_key_env is not set."
    )


def _build_provider_kwargs(
    provider: LLMProviderSpec,
    *,
    getenv: Callable[[str], str | None],
) -> dict[str, Any]:
    validate_provider_init_kwargs(provider.name, provider.init_kwargs)
    kwargs: dict[str, Any] = dict(provider.init_kwargs)
    api_key = _resolve_api_key_from_provider(provider, getenv=getenv)
    if api_key:
        kwargs["api_key"] = api_key
    if provider.base_url:
        kwargs["base_url"] = provider.base_url
    return kwargs


def _get_chat_moonshot_class() -> Any:
    try:
        from langchain_moonshot import ChatMoonshot
    except ImportError as exc:
        raise ValueError(
            "Provider kind='moonshot' requires the 'langchain-moonshot' package. "
            "Install it with `uv add langchain-moonshot`."
        ) from exc
    return ChatMoonshot


def _get_chat_deepseek_class() -> Any:
    try:
        from langchain_deepseek import ChatDeepSeek
    except ImportError as exc:
        raise ValueError(
            "Provider kind='deepseek' requires the 'langchain-deepseek' package. "
            "Install it with `uv add langchain-deepseek`."
        ) from exc
    return ChatDeepSeek


def _get_chat_litellm_class() -> Any:
    try:
        from langchain_litellm import ChatLiteLLM
    except ImportError as exc:
        raise ValueError(
            "Provider kind='litellm' requires the 'langchain-litellm' package. "
            "Install it with `uv add langchain-litellm`."
        ) from exc
    return ChatLiteLLM


def _build_openai_codex_model(model: str, **kwargs: Any) -> Any:
    from ruyi_agent.integrations.openai_codex import CodexChatModel

    return CodexChatModel(model=model, **kwargs)


def _build_litellm_model(model: str, **kwargs: Any) -> Any:
    chat_litellm = _get_chat_litellm_class()
    if "base_url" in kwargs:
        kwargs["api_base"] = kwargs.pop("base_url")
    return chat_litellm(model=model, **kwargs)


def _message_has_tool_calls(message: Any, encoded: dict[str, Any]) -> bool:
    if isinstance(message, dict):
        return bool(
            message.get("tool_calls")
            or message.get("invalid_tool_calls")
            or message.get("function_call")
            or encoded.get("tool_calls")
            or encoded.get("function_call")
        )
    return bool(
        getattr(message, "tool_calls", None)
        or getattr(message, "invalid_tool_calls", None)
        or getattr(message, "additional_kwargs", {}).get("tool_calls")
        or encoded.get("tool_calls")
        or encoded.get("function_call")
    )


def _reasoning_content_for_request(message: Any, encoded: dict[str, Any]) -> str | None:
    if isinstance(message, dict):
        additional_kwargs = message.get("additional_kwargs", {})
        for field_name in ("reasoning_content", "reasoning"):
            value = message.get(field_name)
            if isinstance(value, str) and value:
                return value
    else:
        additional_kwargs = getattr(message, "additional_kwargs", {})
    for field_name in ("reasoning_content", "reasoning"):
        value = additional_kwargs.get(field_name)
        if isinstance(value, str) and value:
            return value
    return " " if _message_has_tool_calls(message, encoded) else None


def _messages_from_model_input(model: Any, input_: Any) -> list[Any]:
    if isinstance(input_, list):
        return input_
    convert_input = getattr(model, "_convert_input", None)
    if callable(convert_input):
        converted = convert_input(input_)
        to_messages = getattr(converted, "to_messages", None)
        if callable(to_messages):
            return list(to_messages())
    return []


def _patch_tool_call_reasoning_history(
    original_messages: list[Any],
    payload: dict[str, Any],
) -> None:
    encoded_messages = payload.get("messages")
    if not isinstance(encoded_messages, list):
        return
    for original, encoded in zip(original_messages, encoded_messages, strict=False):
        if not isinstance(encoded, dict) or not _message_has_tool_calls(original, encoded):
            continue
        reasoning_content = _reasoning_content_for_request(original, encoded)
        if reasoning_content is not None:
            encoded["reasoning_content"] = reasoning_content


def _build_deepseek_model(model: str, **kwargs: Any) -> Any:
    chat_deepseek = _get_chat_deepseek_class()

    class ReasoningChatDeepSeek(chat_deepseek):  # type: ignore[misc, valid-type]
        def _get_request_payload(
            self,
            input_: Any,
            *args: Any,
            **request_kwargs: Any,
        ) -> dict[str, Any]:
            payload = super()._get_request_payload(input_, *args, **request_kwargs)
            _patch_tool_call_reasoning_history(
                _messages_from_model_input(self, input_),
                payload,
            )
            return payload

    return ReasoningChatDeepSeek(model=model, **kwargs)
