from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from langchain_core.messages import AIMessage

from reasoning_chat_openai import (
    ReasoningChatOpenAI,
    _message_has_tool_calls,
    extract_reasoning_text,
)


ChatFactory = Callable[..., Any]
OPENAI_COMPATIBLE_REASONING_KINDS = {
    "generic",
    "openai_compatible",
    "openai-compatible",
}


@dataclass(frozen=True)
class ProviderConfig:
    name: str
    kind: str
    api_key: str | None = None
    base_url: str | None = None
    init_kwargs: Mapping[str, Any] = field(default_factory=dict)


class ReasoningChatModel:
    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self.inner, name)

    def invoke(self, input_: Any, **kwargs: Any) -> Any:
        message = self.inner.invoke(input_, **kwargs)
        return normalize_reasoning_message(message)


def normalize_reasoning_message(message: Any) -> Any:
    if isinstance(message, AIMessage):
        reasoning_text = extract_reasoning_text(message)
        if reasoning_text:
            message.additional_kwargs["reasoning_text"] = reasoning_text
    return message


def _messages_from_input(input_: Any) -> list[Any]:
    if isinstance(input_, list):
        return input_
    to_messages = getattr(input_, "to_messages", None)
    if callable(to_messages):
        return list(to_messages())
    return []


def _reasoning_content_for_request(
    message: AIMessage,
    encoded: Mapping[str, Any],
) -> str | None:
    reasoning_content = message.additional_kwargs.get("reasoning_content")
    if isinstance(reasoning_content, str) and reasoning_content:
        return reasoning_content

    reasoning = message.additional_kwargs.get("reasoning")
    if isinstance(reasoning, str) and reasoning:
        return reasoning

    if _message_has_tool_calls(message, encoded):
        return " "
    return None


def _patch_tool_call_reasoning_history(input_: Any, payload: dict[str, Any]) -> None:
    encoded_messages = payload.get("messages")
    if not isinstance(encoded_messages, list):
        return

    for original, encoded in zip(
        _messages_from_input(input_),
        encoded_messages,
        strict=False,
    ):
        if not isinstance(original, AIMessage) or not isinstance(encoded, dict):
            continue
        if not _message_has_tool_calls(original, encoded):
            continue
        reasoning_content = _reasoning_content_for_request(original, encoded)
        if reasoning_content is not None:
            encoded["reasoning_content"] = reasoning_content


def _provider_kwargs(provider: ProviderConfig) -> dict[str, Any]:
    kwargs = dict(provider.init_kwargs)
    if provider.api_key:
        kwargs["api_key"] = provider.api_key
    if provider.base_url:
        kwargs["base_url"] = provider.base_url
    return kwargs


def build_reasoning_chat_model(
    provider: ProviderConfig,
    *,
    model: str,
    moonshot_factory: ChatFactory | None = None,
    deepseek_factory: type[Any] | None = None,
    openrouter_factory: ChatFactory | None = None,
    **_: Any,
) -> Any:
    kwargs = _provider_kwargs(provider)
    if provider.kind == "moonshot":
        if moonshot_factory is None:
            from langchain_moonshot import ChatMoonshot

            moonshot_factory = ChatMoonshot
        kwargs.setdefault("thinking", True)
        return ReasoningChatModel(moonshot_factory(model=model, **kwargs))

    if provider.kind == "deepseek":
        if deepseek_factory is None:
            from langchain_deepseek import ChatDeepSeek

            deepseek_factory = ChatDeepSeek

        class ReasoningDeepSeekChat(deepseek_factory):  # type: ignore[misc, valid-type]
            def _get_request_payload(
                self,
                input_: Any,
                *args: Any,
                **kwargs: Any,
            ) -> dict[str, Any]:
                payload = super()._get_request_payload(input_, *args, **kwargs)
                _patch_tool_call_reasoning_history(input_, payload)
                return payload

        return ReasoningChatModel(ReasoningDeepSeekChat(model=model, **kwargs))

    if provider.kind == "glm":
        if not provider.api_key:
            raise ValueError("Provider kind='glm' requires api_key.")
        if not provider.base_url:
            raise ValueError("Provider kind='glm' requires base_url.")
        return ReasoningChatOpenAI.for_provider(
            "glm",
            model=model,
            api_key=provider.api_key,
            base_url=provider.base_url,
            **dict(provider.init_kwargs),
        )

    if provider.kind == "openrouter":
        if openrouter_factory is None:
            from langchain_openrouter import ChatOpenRouter

            openrouter_factory = ChatOpenRouter
        return ReasoningChatModel(openrouter_factory(model=model, **kwargs))

    if provider.kind in OPENAI_COMPATIBLE_REASONING_KINDS:
        if not provider.api_key:
            raise ValueError(f"Provider kind={provider.kind!r} requires api_key.")
        if not provider.base_url:
            raise ValueError(f"Provider kind={provider.kind!r} requires base_url.")
        return ReasoningChatOpenAI.for_provider(
            "generic",
            model=model,
            api_key=provider.api_key,
            base_url=provider.base_url,
            **dict(provider.init_kwargs),
        )

    raise ValueError(f"Unsupported provider kind: {provider.kind!r}")
