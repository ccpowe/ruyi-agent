from __future__ import annotations

from langchain_core.messages import AIMessage, AIMessageChunk, HumanMessage

from reasoning_chat_openai import (
    ReasoningChatOpenAI,
    extract_reasoning_text,
    strip_inline_think_blocks,
)


def _model(**kwargs) -> ReasoningChatOpenAI:
    return ReasoningChatOpenAI(
        model="test-model",
        api_key="test-key",
        base_url="https://example.invalid/v1",
        **kwargs,
    )


def test_response_preserves_reasoning_content() -> None:
    chat = _model()

    result = chat._create_chat_result(
        {
            "id": "cmpl-test",
            "model": "test-model",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "final",
                        "reasoning_content": "private reasoning",
                    },
                    "finish_reason": "stop",
                }
            ],
            "usage": {
                "prompt_tokens": 1,
                "completion_tokens": 2,
                "total_tokens": 3,
            },
        }
    )

    message = result.generations[0].message
    assert isinstance(message, AIMessage)
    assert message.content == "final"
    assert message.additional_kwargs["reasoning_content"] == "private reasoning"
    assert message.additional_kwargs["reasoning_text"] == "private reasoning"


def test_response_preserves_reasoning_details() -> None:
    chat = _model()

    result = chat._create_chat_result(
        {
            "model": "test-model",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "final",
                        "reasoning_details": [
                            {"type": "reasoning.summary", "summary": "step summary"},
                            {"type": "reasoning.text", "text": "step text"},
                        ],
                    },
                    "finish_reason": "stop",
                }
            ],
        }
    )

    message = result.generations[0].message
    assert isinstance(message, AIMessage)
    assert message.additional_kwargs["reasoning_details"][0]["summary"] == "step summary"
    assert message.additional_kwargs["reasoning_text"] == "step summary\n\nstep text"


def test_inline_think_blocks_are_extractable_without_changing_content() -> None:
    chat = _model()

    result = chat._create_chat_result(
        {
            "model": "test-model",
            "choices": [
                {
                    "message": {
                        "role": "assistant",
                        "content": "<think>a hidden step</think>\nvisible answer",
                    },
                    "finish_reason": "stop",
                }
            ],
        }
    )

    message = result.generations[0].message
    assert isinstance(message, AIMessage)
    assert message.content == "<think>a hidden step</think>\nvisible answer"
    assert message.additional_kwargs["reasoning_text"] == "a hidden step"
    assert strip_inline_think_blocks(message.content) == "visible answer"


def test_request_can_echo_reasoning_content_for_tool_call_messages() -> None:
    chat = _model(reasoning_echo="tool_calls")
    assistant = AIMessage(
        content="",
        additional_kwargs={"reasoning_content": "must echo"},
        tool_calls=[{"name": "lookup", "args": {"q": "x"}, "id": "call_1"}],
    )

    payload = chat._get_request_payload(
        [
            HumanMessage(content="use tool"),
            assistant,
        ]
    )

    assert payload["messages"][1]["role"] == "assistant"
    assert payload["messages"][1]["reasoning_content"] == "must echo"


def test_request_synthesizes_missing_reasoning_content_for_strict_tool_call() -> None:
    chat = _model(reasoning_echo="tool_calls")
    assistant = AIMessage(
        content="",
        tool_calls=[{"name": "lookup", "args": {"q": "x"}, "id": "call_1"}],
    )

    payload = chat._get_request_payload(
        [
            HumanMessage(content="use tool"),
            assistant,
        ]
    )

    assert payload["messages"][1]["reasoning_content"] == " "


def test_request_preserves_multimodal_user_content() -> None:
    chat = _model(thinking={"type": "enabled"})
    payload = chat._get_request_payload(
        [
            HumanMessage(
                content=[
                    {"type": "text", "text": "describe this"},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": "data:image/png;base64,iVBORw0KGgo=",
                        },
                    },
                ]
            )
        ]
    )

    content = payload["messages"][0]["content"]
    assert content[0]["type"] == "text"
    assert content[1]["type"] == "image_url"
    assert payload["extra_body"]["thinking"] == {"type": "enabled"}


def test_extract_reasoning_text_promotes_reasoning_field() -> None:
    message = AIMessage(
        content="final",
        additional_kwargs={"reasoning": "reasoning field"},
    )

    assert extract_reasoning_text(message) == "reasoning field"


def test_stream_chunk_preserves_reasoning_content_delta() -> None:
    chat = _model()

    generation_chunk = chat._convert_chunk_to_generation_chunk(
        {
            "choices": [
                {
                    "delta": {
                        "role": "assistant",
                        "content": "",
                        "reasoning_content": "streamed reasoning",
                    },
                    "finish_reason": None,
                }
            ]
        },
        AIMessageChunk,
        None,
    )

    assert generation_chunk is not None
    assert generation_chunk.message.additional_kwargs["reasoning_content"] == (
        "streamed reasoning"
    )
    assert generation_chunk.message.additional_kwargs["reasoning_text"] == (
        "streamed reasoning"
    )
