from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any, Literal

import openai
from langchain_core.messages import AIMessage, AIMessageChunk, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_openai import ChatOpenAI
from langchain_openai.chat_models.base import (
    _convert_dict_to_message,
    _create_usage_metadata,
)

ReasoningEchoMode = Literal["never", "always", "tool_calls"]

REASONING_FIELD_NAMES = (
    "reasoning_content",
    "reasoning",
    "reasoning_details",
)
THINK_BLOCK_PATTERN = re.compile(
    r"<(?P<tag>think|thinking|thought|reasoning|REASONING_SCRATCHPAD)>"
    r"(?P<body>.*?)"
    r"</(?P=tag)>",
    re.DOTALL | re.IGNORECASE,
)


def _model_extra(value: Any) -> dict[str, Any]:
    extra = getattr(value, "model_extra", None)
    return extra if isinstance(extra, dict) else {}


def _read_attr_or_extra(value: Any, name: str) -> Any:
    if isinstance(value, Mapping):
        return value.get(name)
    attr = getattr(value, name, None)
    if attr is not None:
        return attr
    return _model_extra(value).get(name)


def _dump_reasoning_details(details: Any) -> list[Any]:
    if not isinstance(details, list):
        return details
    dumped: list[Any] = []
    for item in details:
        if hasattr(item, "model_dump"):
            dumped.append(item.model_dump())
        else:
            dumped.append(item)
    return dumped


def extract_inline_think_blocks(content: Any) -> str:
    if not isinstance(content, str):
        return ""
    parts = [match.group("body").strip() for match in THINK_BLOCK_PATTERN.finditer(content)]
    return "\n\n".join(part for part in parts if part)


def strip_inline_think_blocks(content: Any) -> Any:
    if not isinstance(content, str):
        return content
    return THINK_BLOCK_PATTERN.sub("", content).strip()


def extract_reasoning_text(message: AIMessage | Mapping[str, Any]) -> str:
    if isinstance(message, AIMessage):
        values = message.additional_kwargs
        content = message.content
    else:
        values = message
        content = message.get("content")

    parts: list[str] = []
    for field_name in ("reasoning_content", "reasoning"):
        value = values.get(field_name)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())

    details = values.get("reasoning_details")
    if isinstance(details, list):
        for item in details:
            if not isinstance(item, Mapping):
                continue
            for key in ("summary", "thinking", "content", "text"):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    parts.append(value.strip())
                    break

    if not parts:
        inline = extract_inline_think_blocks(content)
        if inline:
            parts.append(inline)

    return "\n\n".join(parts)


def _merge_reasoning_fields(
    message_dict: dict[str, Any],
    raw_message: Any | None,
) -> dict[str, Any]:
    merged = dict(message_dict)
    for field_name in REASONING_FIELD_NAMES:
        value = merged.get(field_name)
        if value is None and raw_message is not None:
            value = _read_attr_or_extra(raw_message, field_name)
        if value is None:
            continue
        if field_name == "reasoning_details":
            value = _dump_reasoning_details(value)
        merged[field_name] = value
    return merged


def _convert_dict_to_message_with_reasoning(
    message_dict: Mapping[str, Any],
) -> BaseMessage:
    message = _convert_dict_to_message(message_dict)
    if not isinstance(message, AIMessage):
        return message

    for field_name in REASONING_FIELD_NAMES:
        value = message_dict.get(field_name)
        if value is not None:
            message.additional_kwargs[field_name] = value

    reasoning_text = extract_reasoning_text(message)
    if reasoning_text:
        message.additional_kwargs["reasoning_text"] = reasoning_text

    return message


def _message_has_tool_calls(message: AIMessage, encoded: Mapping[str, Any]) -> bool:
    return bool(
        message.tool_calls
        or message.invalid_tool_calls
        or message.additional_kwargs.get("tool_calls")
        or encoded.get("tool_calls")
        or encoded.get("function_call")
    )


class ReasoningChatOpenAI(ChatOpenAI):
    """ChatOpenAI prototype that preserves provider reasoning fields.

    This is intentionally kept under scripts/ while we validate provider behavior.
    It follows the same broad shape as Hermes Agent: provider-specific request
    knobs are centralized here, and response reasoning fields are normalized into
    AIMessage.additional_kwargs instead of being dropped by LangChain's default
    OpenAI adapter.
    """

    thinking: dict[str, Any] | bool | None = None
    reasoning_effort: str | None = None
    reasoning_echo: ReasoningEchoMode = "never"
    promote_reasoning_to_reasoning_content: bool = True
    synthesize_missing_tool_call_reasoning_content: bool = True

    @classmethod
    def for_provider(
        cls,
        provider: Literal["deepseek", "kimi", "glm", "generic"],
        *,
        model: str,
        api_key: str,
        base_url: str,
        **kwargs: Any,
    ) -> "ReasoningChatOpenAI":
        provider_key = provider.strip().lower()
        defaults: dict[str, Any] = {}
        if provider_key == "kimi":
            defaults.update(
                thinking={"type": "enabled"},
                reasoning_echo="tool_calls",
            )
        elif provider_key == "glm":
            defaults.update(thinking={"type": "enabled"})
        elif provider_key == "deepseek":
            defaults.update(reasoning_echo="never")
            if model.startswith("deepseek-v") and not model.startswith("deepseek-v3"):
                defaults.update(
                    thinking={"type": "enabled"},
                    reasoning_effort="medium",
                    reasoning_echo="tool_calls",
                )
        elif provider_key != "generic":
            raise ValueError(f"Unsupported provider preset: {provider!r}")

        defaults.update(kwargs)
        return cls(
            model=model,
            api_key=api_key,
            base_url=base_url,
            **defaults,
        )

    def _reasoning_content_for_request(
        self,
        message: AIMessage,
        encoded: Mapping[str, Any],
    ) -> str | None:
        reasoning_content = message.additional_kwargs.get("reasoning_content")
        if isinstance(reasoning_content, str) and reasoning_content:
            return reasoning_content

        if self.promote_reasoning_to_reasoning_content:
            reasoning = message.additional_kwargs.get("reasoning")
            if isinstance(reasoning, str) and reasoning:
                return reasoning

        if (
            self.synthesize_missing_tool_call_reasoning_content
            and _message_has_tool_calls(message, encoded)
        ):
            return " "
        return None

    def _should_echo_reasoning(self, message: AIMessage, encoded: Mapping[str, Any]) -> bool:
        if self.reasoning_echo == "always":
            return True
        if self.reasoning_echo == "tool_calls":
            return _message_has_tool_calls(message, encoded)
        return False

    def _patch_reasoning_request_messages(
        self,
        input_: Any,
        payload: dict[str, Any],
    ) -> None:
        encoded_messages = payload.get("messages")
        if not isinstance(encoded_messages, list):
            return
        original_messages = self._convert_input(input_).to_messages()
        for original, encoded in zip(original_messages, encoded_messages, strict=False):
            if not isinstance(original, AIMessage) or not isinstance(encoded, dict):
                continue
            if not self._should_echo_reasoning(original, encoded):
                continue
            reasoning_content = self._reasoning_content_for_request(original, encoded)
            if reasoning_content is not None:
                encoded["reasoning_content"] = reasoning_content

    def _patch_reasoning_request_params(self, payload: dict[str, Any]) -> None:
        if self.reasoning_effort:
            payload["reasoning_effort"] = self.reasoning_effort
        if self.thinking is None:
            return
        thinking = (
            {"type": "enabled" if self.thinking else "disabled"}
            if isinstance(self.thinking, bool)
            else dict(self.thinking)
        )
        extra_body = dict(payload.get("extra_body") or {})
        extra_body["thinking"] = thinking
        payload["extra_body"] = extra_body

    def _get_request_payload(
        self,
        input_: Any,
        *,
        stop: list[str] | None = None,
        **kwargs: Any,
    ) -> dict:
        payload = super()._get_request_payload(input_, stop=stop, **kwargs)
        self._patch_reasoning_request_params(payload)
        self._patch_reasoning_request_messages(input_, payload)
        return payload

    def _create_chat_result(
        self,
        response: dict | openai.BaseModel,
        generation_info: dict | None = None,
    ) -> ChatResult:
        generations: list[ChatGeneration] = []

        response_dict = (
            response
            if isinstance(response, dict)
            else response.model_dump(
                exclude={"choices": {"__all__": {"message": {"parsed"}}}}
            )
        )
        if response_dict.get("error"):
            raise ValueError(response_dict.get("error"))

        try:
            choices = response_dict["choices"]
        except KeyError as exc:
            msg = f"Response missing 'choices' key: {response_dict.keys()}"
            raise KeyError(msg) from exc
        if choices is None:
            msg = (
                "Received response with null value for 'choices'. "
                f"Full response keys: {list(response_dict.keys())}"
            )
            raise TypeError(msg)

        raw_choices = getattr(response, "choices", None) if not isinstance(response, dict) else None
        token_usage = response_dict.get("usage")
        service_tier = response_dict.get("service_tier")

        for index, choice in enumerate(choices):
            raw_message = None
            if raw_choices is not None and index < len(raw_choices):
                raw_message = getattr(raw_choices[index], "message", None)

            message_dict = _merge_reasoning_fields(choice["message"], raw_message)
            message = _convert_dict_to_message_with_reasoning(message_dict)
            if token_usage and isinstance(message, AIMessage):
                message.usage_metadata = _create_usage_metadata(token_usage, service_tier)

            current_generation_info = dict(generation_info or {})
            current_generation_info["finish_reason"] = (
                choice.get("finish_reason")
                if choice.get("finish_reason") is not None
                else current_generation_info.get("finish_reason")
            )
            if "logprobs" in choice:
                current_generation_info["logprobs"] = choice["logprobs"]
            generations.append(
                ChatGeneration(
                    message=message,
                    generation_info=current_generation_info,
                )
            )

        llm_output = {
            "token_usage": token_usage,
            "model_provider": "openai",
            "model_name": response_dict.get("model", self.model_name),
            "system_fingerprint": response_dict.get("system_fingerprint", ""),
        }
        if "id" in response_dict:
            llm_output["id"] = response_dict["id"]
        if service_tier:
            llm_output["service_tier"] = service_tier

        if isinstance(response, openai.BaseModel) and getattr(response, "choices", None):
            message = response.choices[0].message  # type: ignore[attr-defined]
            if hasattr(message, "parsed"):
                generations[0].message.additional_kwargs["parsed"] = message.parsed
            if hasattr(message, "refusal"):
                generations[0].message.additional_kwargs["refusal"] = message.refusal

        return ChatResult(generations=generations, llm_output=llm_output)

    def _convert_chunk_to_generation_chunk(
        self,
        chunk: dict,
        default_chunk_class: type,
        base_generation_info: dict | None,
    ) -> ChatGenerationChunk | None:
        generation_chunk = super()._convert_chunk_to_generation_chunk(
            chunk,
            default_chunk_class,
            base_generation_info,
        )
        if generation_chunk is None:
            return None
        message = generation_chunk.message
        if not isinstance(message, AIMessageChunk):
            return generation_chunk

        choices = (
            chunk.get("choices", [])
            or chunk.get("chunk", {}).get("choices", [])
        )
        if not choices:
            return generation_chunk
        delta = choices[0].get("delta") or {}
        if not isinstance(delta, Mapping):
            return generation_chunk

        for field_name in REASONING_FIELD_NAMES:
            value = delta.get(field_name)
            if value is not None:
                message.additional_kwargs[field_name] = value

        reasoning_text = extract_reasoning_text(message.additional_kwargs)
        if reasoning_text:
            message.additional_kwargs["reasoning_text"] = reasoning_text
        return generation_chunk
