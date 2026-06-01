from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from langchain_core.messages import AIMessage, HumanMessage

from reasoning_chat_openai import ReasoningChatOpenAI
from reasoning_provider_factory import ProviderConfig, build_reasoning_chat_model


@dataclass
class FakeNativeChat:
    model: str
    kwargs: dict[str, Any]

    def invoke(self, input_: object, **kwargs: Any) -> AIMessage:
        return AIMessage(
            content="final",
            additional_kwargs={"reasoning_content": "native reasoning"},
        )


def test_kimi_uses_native_moonshot_and_normalizes_reasoning_text() -> None:
    created: list[FakeNativeChat] = []

    def moonshot_factory(*, model: str, **kwargs: Any) -> FakeNativeChat:
        chat = FakeNativeChat(model=model, kwargs=kwargs)
        created.append(chat)
        return chat

    chat = build_reasoning_chat_model(
        ProviderConfig(
            name="kimi",
            kind="moonshot",
            base_url="https://api.moonshot.cn/v1",
            api_key="test-key",
        ),
        model="kimi-k2.6",
        moonshot_factory=moonshot_factory,
    )

    message = chat.invoke("hello")

    assert message.additional_kwargs["reasoning_text"] == "native reasoning"
    assert created[0].model == "kimi-k2.6"
    assert created[0].kwargs["api_key"] == "test-key"
    assert created[0].kwargs["base_url"] == "https://api.moonshot.cn/v1"
    assert created[0].kwargs["thinking"] is True


def test_deepseek_echoes_reasoning_content_for_tool_call_history() -> None:
    class FakeDeepSeekChat:
        def __init__(self, *, model: str, **kwargs: Any) -> None:
            self.model = model
            self.kwargs = kwargs

        def _get_request_payload(self, input_: object, **kwargs: Any) -> dict[str, Any]:
            return {
                "messages": [
                    {"role": "user", "content": "use tool"},
                    {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {"name": "lookup", "arguments": "{}"},
                            }
                        ],
                    },
                ]
            }

    assistant = AIMessage(
        content="",
        additional_kwargs={"reasoning_content": "must echo"},
        tool_calls=[{"name": "lookup", "args": {}, "id": "call_1"}],
    )
    chat = build_reasoning_chat_model(
        ProviderConfig(name="deepseek", kind="deepseek", api_key="test-key"),
        model="deepseek-v4-pro",
        deepseek_factory=FakeDeepSeekChat,
    )

    payload = chat._get_request_payload(
        [
            HumanMessage(content="use tool"),
            assistant,
        ]
    )

    assert payload["messages"][1]["reasoning_content"] == "must echo"


def test_glm_uses_openai_compatible_reasoning_adapter_with_thinking_and_images() -> None:
    chat = build_reasoning_chat_model(
        ProviderConfig(
            name="glm",
            kind="glm",
            base_url="https://open.bigmodel.cn/api/paas/v4/",
            api_key="test-key",
        ),
        model="glm-4.5v",
    )

    payload = chat._get_request_payload(
        [
            HumanMessage(
                content=[
                    {"type": "text", "text": "describe this"},
                    {
                        "type": "image_url",
                        "image_url": {"url": "data:image/png;base64,iVBORw0KGgo="},
                    },
                ]
            )
        ]
    )

    assert isinstance(chat, ReasoningChatOpenAI)
    assert payload["extra_body"]["thinking"] == {"type": "enabled"}
    assert payload["messages"][0]["content"][1]["type"] == "image_url"


def test_openrouter_uses_native_chatopenrouter_and_normalizes_reasoning_text() -> None:
    created: list[FakeNativeChat] = []

    def openrouter_factory(*, model: str, **kwargs: Any) -> FakeNativeChat:
        chat = FakeNativeChat(model=model, kwargs=kwargs)
        created.append(chat)
        return chat

    chat = build_reasoning_chat_model(
        ProviderConfig(
            name="openrouter",
            kind="openrouter",
            base_url="https://openrouter.ai/api/v1",
            api_key="test-key",
            init_kwargs={"reasoning": {"effort": "high", "summary": "auto"}},
        ),
        model="deepseek/deepseek-r1",
        openrouter_factory=openrouter_factory,
    )

    message = chat.invoke("hello")

    assert message.additional_kwargs["reasoning_text"] == "native reasoning"
    assert created[0].model == "deepseek/deepseek-r1"
    assert created[0].kwargs["api_key"] == "test-key"
    assert created[0].kwargs["base_url"] == "https://openrouter.ai/api/v1"
    assert created[0].kwargs["reasoning"] == {"effort": "high", "summary": "auto"}
