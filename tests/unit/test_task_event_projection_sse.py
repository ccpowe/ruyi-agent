from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest
from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, AIMessageChunk, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatGenerationChunk, ChatResult
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import tool

from ruyi_agent.gateway_protocol.sse import (
    GatewayTaskEvent,
    MAX_SSE_LINE_BYTES,
    SSEProtocolError,
    encode_gateway_task_event,
    encode_task_stream_event,
    is_valid_task_event_cursor,
    iter_gateway_task_events,
    iter_utf8_sse_lines,
    task_stream_event_from_gateway,
)
from ruyi_agent.gateway_protocol.projection import (
    AssistantDelta,
    assistant_delta_from_stream_part as project_assistant_delta,
)
from ruyi_agent.runtime.task_events import (
    MAX_ASSISTANT_DELTA_TEXT_LENGTH,
    MAX_DURABLE_TASK_EVENT_DATA_BYTES,
    MAX_EVENT_TEXT_LENGTH,
    MAX_TASK_EVENT_CURSOR_LENGTH,
    TaskStreamEvent,
    assistant_delta_from_stream_part,
    lifecycle_event_data,
)
from ruyi_agent.task_models import PublishedArtifact, TaskRecord


def _record(*, task_id: str = "task-1") -> TaskRecord:
    now = datetime(2026, 8, 28, 12, 0, tzinfo=UTC)
    return TaskRecord(
        task_id=task_id,
        agent_name="main",
        state="pending",
        thread_id=task_id,
        parent_task_id=None,
        root_task_id=task_id,
        depth=0,
        created_at=now,
        updated_at=now,
    )


class SequenceModel(BaseChatModel):
    responses: list[AIMessage]
    index: int = 0

    @property
    def _llm_type(self) -> str:
        return "outer-probe"

    def bind_tools(
        self,
        tools: Sequence[Any],
        **kwargs: Any,
    ) -> "SequenceModel":
        del tools, kwargs
        return self

    def _generate(self, messages: Any, **kwargs: Any) -> ChatResult:
        del messages, kwargs
        response = self._next_response()
        return ChatResult(generations=[ChatGeneration(message=response)])

    async def _agenerate(self, messages: Any, **kwargs: Any) -> ChatResult:
        return self._generate(messages, **kwargs)

    async def _astream(
        self,
        messages: Any,
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        del messages, stop, run_manager, kwargs
        response = self._next_response()
        tool_call_chunks = [
            {
                "name": call["name"],
                "args": json.dumps(call["args"]),
                "id": call["id"],
                "index": index,
                "type": "tool_call_chunk",
            }
            for index, call in enumerate(response.tool_calls)
        ]
        yield ChatGenerationChunk(
            message=AIMessageChunk(
                content=response.content,
                tool_call_chunks=tool_call_chunks,
            )
        )

    def _next_response(self) -> AIMessage:
        response = self.responses[self.index]
        if self.index < len(self.responses) - 1:
            self.index += 1
        return response


class SecretStreamingModel(BaseChatModel):
    secret: str

    @property
    def _llm_type(self) -> str:
        return "secret-probe"

    def _generate(self, messages: Any, **kwargs: Any) -> ChatResult:
        del messages, kwargs
        return ChatResult(
            generations=[ChatGeneration(message=AIMessage(content=self.secret))]
        )

    async def _agenerate(self, messages: Any, **kwargs: Any) -> ChatResult:
        return self._generate(messages, **kwargs)

    async def _astream(
        self,
        messages: Any,
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> AsyncIterator[ChatGenerationChunk]:
        del messages, stop, run_manager, kwargs
        yield ChatGenerationChunk(message=AIMessageChunk(content=self.secret))


def _message_stream_parts_containing(
    parts: list[dict[str, Any]], text: str
) -> list[dict[str, Any]]:
    return [
        part
        for part in parts
        if part.get("type") == "messages"
        and isinstance(data := part.get("data"), tuple)
        and isinstance(data[0], AIMessageChunk)
        and text in str(data[0].content)
    ]


def test_assistant_delta_projection_excludes_tools_reasoning_and_metadata() -> None:
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": (),
                "data": (
                    AIMessageChunk(content="hello"),
                    {
                        "provider": "secret",
                        "langgraph_node": "model",
                        "langgraph_path": ("__pregel_pull", "model"),
                    },
                ),
            }
        )
        == "hello"
    )
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": (),
                "data": (
                    AIMessageChunk(
                        content=[
                            {"type": "reasoning", "reasoning": "hidden"},
                            {"type": "text", "text": "safe"},
                            {"type": "output_text", "text": " output"},
                        ]
                    ),
                    {
                        "langgraph_node": "model",
                        "langgraph_path": ("__pregel_pull", "model"),
                    },
                ),
            }
        )
        == "safe output"
    )
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": (),
                "data": (
                    ToolMessage(content="secret", tool_call_id="call-1"),
                    {
                        "langgraph_node": "model",
                        "langgraph_path": ("__pregel_pull", "model"),
                    },
                ),
            }
        )
        is None
    )
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": (),
                "data": (
                    AIMessageChunk(content="tool-internal secret"),
                    {"langgraph_node": "tools"},
                ),
            }
        )
        is None
    )
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": ("tool-subgraph:call-1",),
                "data": (
                    AIMessageChunk(content="nested model secret"),
                    {
                        "langgraph_node": "model",
                        "langgraph_path": ("__pregel_pull", "model"),
                    },
                ),
            }
        )
        is None
    )


def test_assistant_delta_boundary_accepts_nominal_subclass_and_neutral_dto() -> None:
    class DerivedAIMessageChunk(AIMessageChunk):
        pass

    provenance = {
        "langgraph_node": "model",
        "langgraph_path": ("__pregel_pull", "model"),
    }
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": (),
                "data": (DerivedAIMessageChunk(content="hello"), provenance),
            }
        )
        == "hello"
    )
    delta = AssistantDelta(
        content="hello",
        provenance={"ns": (), **provenance},
    )
    projected = project_assistant_delta(
        {
            "type": "messages",
            "ns": (),
            "data": (delta, {"private": "ignored"}),
        }
    )
    assert projected == delta
    lookalike = type("Lookalike", (), {"content": "hello"})()
    assert (
        assistant_delta_from_stream_part(
            {"type": "messages", "ns": (), "data": (lookalike, provenance)}
        )
        is None
    )


def test_real_agent_stream_excludes_model_tokens_invoked_inside_tool() -> None:
    async def scenario() -> list[dict[str, Any]]:
        secret_model = SecretStreamingModel(secret="TOOL MODEL SECRET")

        @tool
        async def hidden_model_tool() -> str:
            """Use a private model and return only a redacted result."""

            await secret_model.ainvoke("hidden prompt")
            return "redacted"

        outer = create_agent(
            model=SequenceModel(
                responses=[
                    AIMessage(
                        content="",
                        tool_calls=[
                            {
                                "name": "hidden_model_tool",
                                "args": {},
                                "id": "call-1",
                                "type": "tool_call",
                            }
                        ],
                    ),
                    AIMessage(content="SAFE FINAL"),
                ]
            ),
            tools=[hidden_model_tool],
        )
        return [
            part
            async for part in outer.astream(
                {"messages": [{"role": "user", "content": "run tool"}]},
                stream_mode=["messages", "values"],
                version="v2",
            )
        ]

    parts = asyncio.run(scenario())
    secret_parts = _message_stream_parts_containing(parts, "TOOL MODEL SECRET")
    assert secret_parts
    assert all(part["ns"] == () for part in secret_parts)
    assert all(part["data"][1]["langgraph_node"] == "tools" for part in secret_parts)
    assert all(assistant_delta_from_stream_part(part) is None for part in secret_parts)
    safe_parts = _message_stream_parts_containing(parts, "SAFE FINAL")
    assert safe_parts
    assert all(part["ns"] == () for part in safe_parts)
    assert all(
        part["data"][1]["langgraph_path"] == ("__pregel_pull", "model")
        for part in safe_parts
    )
    assert any(
        "SAFE FINAL" in (assistant_delta_from_stream_part(part) or "")
        for part in safe_parts
    )


def test_real_tool_model_cannot_forge_public_model_node_metadata() -> None:
    async def scenario() -> list[dict[str, Any]]:
        secret_model = SecretStreamingModel(secret="OVERRIDE SECRET")

        @tool
        async def metadata_override_tool(config: RunnableConfig) -> str:
            """Call a private model while attempting to forge public metadata."""

            hidden_config = dict(config)
            hidden_config["metadata"] = {
                **dict(config.get("metadata") or {}),
                "langgraph_node": "model",
            }
            await secret_model.ainvoke("hidden prompt", config=hidden_config)
            return "redacted"

        outer = create_agent(
            model=SequenceModel(
                responses=[
                    AIMessage(
                        content="",
                        tool_calls=[
                            {
                                "name": "metadata_override_tool",
                                "args": {},
                                "id": "call-1",
                                "type": "tool_call",
                            }
                        ],
                    ),
                    AIMessage(content="SAFE OVERRIDE FINAL"),
                ]
            ),
            tools=[metadata_override_tool],
        )
        return [
            part
            async for part in outer.astream(
                {"messages": [{"role": "user", "content": "run tool"}]},
                stream_mode=["messages", "values"],
                version="v2",
            )
        ]

    parts = asyncio.run(scenario())
    secret_parts = _message_stream_parts_containing(parts, "OVERRIDE SECRET")
    assert secret_parts
    assert all(part["ns"] == () for part in secret_parts)
    assert all(part["data"][1]["langgraph_node"] == "model" for part in secret_parts)
    assert all(
        part["data"][1]["langgraph_path"] != ("__pregel_pull", "model")
        for part in secret_parts
    )
    assert all(assistant_delta_from_stream_part(part) is None for part in secret_parts)
    safe_parts = _message_stream_parts_containing(parts, "SAFE OVERRIDE FINAL")
    assert safe_parts
    assert all(part["ns"] == () for part in safe_parts)
    assert all(
        part["data"][1]["langgraph_path"] == ("__pregel_pull", "model")
        for part in safe_parts
    )
    assert any(
        "SAFE OVERRIDE FINAL" in (assistant_delta_from_stream_part(part) or "")
        for part in safe_parts
    )


def test_real_nested_agent_stream_excludes_subgraph_model_tokens() -> None:
    async def scenario() -> list[dict[str, Any]]:
        inner = create_agent(
            model=SecretStreamingModel(secret="NESTED AGENT SECRET"),
            tools=[],
        )

        @tool
        async def nested_agent_tool(config: RunnableConfig) -> str:
            """Delegate privately and return only a redacted result."""

            await inner.ainvoke(
                {"messages": [{"role": "user", "content": "hidden prompt"}]},
                config=config,
                version="v2",
            )
            return "redacted"

        outer = create_agent(
            model=SequenceModel(
                responses=[
                    AIMessage(
                        content="",
                        tool_calls=[
                            {
                                "name": "nested_agent_tool",
                                "args": {},
                                "id": "call-1",
                                "type": "tool_call",
                            }
                        ],
                    ),
                    AIMessage(content="SAFE FINAL"),
                ]
            ),
            tools=[nested_agent_tool],
        )
        return [
            part
            async for part in outer.astream(
                {"messages": [{"role": "user", "content": "delegate"}]},
                stream_mode=["messages", "values"],
                version="v2",
                subgraphs=True,
            )
        ]

    parts = asyncio.run(scenario())
    secret_parts = _message_stream_parts_containing(parts, "NESTED AGENT SECRET")
    assert secret_parts
    assert all(part["ns"] for part in secret_parts)
    assert all(part["data"][1]["langgraph_node"] == "model" for part in secret_parts)
    assert all(assistant_delta_from_stream_part(part) is None for part in secret_parts)


def test_lifecycle_projection_excludes_review_arguments_and_artifact_paths() -> None:
    record = _record()
    record.pending_review = {
        "review_id": "review-1",
        "action_requests": [
            {
                "name": "execute",
                "args": {"command": "cat /secret/token"},
                "provider_metadata": {"trace": "hidden"},
            }
        ],
        "review_configs": [
            {
                "action_name": "execute",
                "allowed_decisions": ["approve", "reject"],
                "extra": "hidden",
            }
        ],
    }
    record.artifacts = [
        PublishedArtifact(
            artifact_id="art-1",
            path="/secret/report.txt",
            name="report.txt",
            caption=None,
            content_type="text/plain",
            size=10,
            run_count=1,
        )
    ]
    data = lifecycle_event_data(record)
    assert data["pending_review"] == {
        "review_id": "review-1",
        "action_requests": [{"name": "execute"}],
        "review_configs": [
            {
                "action_name": "execute",
                "allowed_decisions": ["approve", "reject"],
            }
        ],
    }
    assert data["artifacts"] == [
        {
            "artifact_id": "art-1",
            "name": "report.txt",
            "caption": None,
            "content_type": "text/plain",
            "size": 10,
            "run_count": 1,
        }
    ]
    assert (
        assistant_delta_from_stream_part(
            {
                "type": "messages",
                "ns": (),
                "data": (
                    AIMessageChunk(
                        content="",
                        tool_call_chunks=[
                            {
                                "name": "execute",
                                "args": "{}",
                                "id": "call-1",
                                "index": 0,
                            }
                        ],
                    ),
                    {
                        "langgraph_node": "model",
                        "langgraph_path": ("__pregel_pull", "model"),
                    },
                ),
            }
        )
        is None
    )


def test_lifecycle_projection_has_an_aggregate_wire_budget() -> None:
    record = _record()
    large_text = "\U0001f680" * 5000
    record.pending_review = {
        "review_id": "review-1",
        "action_requests": [{"name": large_text} for _ in range(100)],
    }
    record.artifacts = [
        PublishedArtifact(
            artifact_id=f"art-{index}",
            path=f"/private/{index}",
            name=f"artifact-{index}",
            caption=large_text,
            content_type="text/plain",
            size=10,
            run_count=0,
        )
        for index in range(10)
    ]

    data = lifecycle_event_data(record)
    assert data["pending_review_truncated"] is True
    assert data["artifacts_truncated"] is True
    assert len(data["artifacts"]) < len(record.artifacts)
    assert (
        len(
            json.dumps(
                data,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
        )
        <= MAX_DURABLE_TASK_EVENT_DATA_BYTES
    )
    assert encode_task_stream_event(
        TaskStreamEvent(
            event_type="task.snapshot",
            task_id=record.task_id,
            run_count=record.run_count,
            created_at=record.updated_at,
            data=data,
            event_id="cursor",
        )
    )


def test_sse_codec_round_trip_and_remote_task_id_rewrite() -> None:
    async def scenario() -> None:
        stream_event = task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="assistant.delta",
                event_id=None,
                data={
                    "task_id": "downstream-task",
                    "run_count": 2,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "content": "token",
                },
            ),
            expected_task_id="downstream-task",
            public_task_id="proxy-task",
            run_count=2,
        )
        wire = encode_task_stream_event(stream_event).decode("utf-8").splitlines()

        async def lines() -> AsyncIterator[str]:
            for line in wire:
                yield line

        decoded = [event async for event in iter_gateway_task_events(lines())]
        assert len(decoded) == 1
        assert decoded[0].data["task_id"] == "proxy-task"
        assert decoded[0].data["content"] == "token"

    asyncio.run(scenario())


@pytest.mark.parametrize("event_type", ["task.failed", "stream.error"])
def test_remote_sse_error_content_is_replaced_at_public_boundary(
    event_type: str,
) -> None:
    common = {
        "task_id": "private-upstream-task",
        "run_count": 1,
        "created_at": "2026-08-28T12:00:00+00:00",
    }
    data = (
        {
            **common,
            "status": "failed",
            "last_result": None,
            "error": "private-upstream-task at /tasks/private-upstream-task",
            "updated_at": "2026-08-28T12:00:00+00:00",
            "pending_review": None,
            "artifacts": [],
        }
        if event_type == "task.failed"
        else {
            **common,
            "code": "private-upstream-task",
            "message": "see /tasks/private-upstream-task",
        }
    )

    projected = task_stream_event_from_gateway(
        GatewayTaskEvent(
            event_type=event_type,
            event_id="cursor-1" if event_type == "task.failed" else None,
            data=data,
        ),
        expected_task_id="private-upstream-task",
        public_task_id="public-task",
        run_count=1,
    )

    assert "private-upstream-task" not in json.dumps(projected.data)
    if event_type == "task.failed":
        assert projected.data["error"] == "Remote Gateway Task failed"
    else:
        assert projected.data == {
            "code": "upstream_task_stream_error",
            "message": "Remote Gateway Task stream failed",
        }


def test_sse_byte_decoder_handles_utf8_bom_and_split_crlf() -> None:
    async def chunks() -> AsyncIterator[bytes]:
        yield b"\xef"
        yield b"\xbb\xbfevent: assistant.delta\r"
        yield b'\ndata: {"content":"\xe4\xbd\xa0\xe5\xa5\xbd"}\r'
        yield b"\n\r\n"

    async def scenario() -> None:
        lines = iter_utf8_sse_lines(chunks())
        events = [event async for event in iter_gateway_task_events(lines)]
        assert events == [
            GatewayTaskEvent(
                event_type="assistant.delta",
                data={"content": "\u4f60\u597d"},
            )
        ]

    asyncio.run(scenario())


@pytest.mark.parametrize("line_ending", ["\n", "\r\n", "\r"])
def test_sse_byte_decoder_combines_multidata_and_preserves_cursor(
    line_ending: str,
) -> None:
    wire = line_ending.join(
        [
            "id: cursor-1",
            "retry: 1000",
            "event: assistant.delta",
            'data: {"content":"hello",',
            'data: "suffix":" world"}',
            "",
            "",
        ]
    ).encode("utf-8")

    async def chunks() -> AsyncIterator[bytes]:
        midpoint = len(wire) // 2
        yield wire[:midpoint]
        yield wire[midpoint:]

    async def scenario() -> None:
        events = [
            event
            async for event in iter_gateway_task_events(iter_utf8_sse_lines(chunks()))
        ]
        assert events == [
            GatewayTaskEvent(
                event_type="assistant.delta",
                event_id="cursor-1",
                data={"content": "hello", "suffix": " world"},
            )
        ]

    asyncio.run(scenario())


def test_sse_byte_decoder_rejects_invalid_utf8_and_oversized_line() -> None:
    async def invalid_utf8() -> AsyncIterator[bytes]:
        yield b"data: \xff\n"

    async def oversized_line() -> AsyncIterator[bytes]:
        yield b"x" * (MAX_SSE_LINE_BYTES + 1)

    async def scenario() -> None:
        with pytest.raises(SSEProtocolError, match="valid UTF-8"):
            await anext(iter_utf8_sse_lines(invalid_utf8()))
        with pytest.raises(SSEProtocolError, match="size limit"):
            await anext(iter_utf8_sse_lines(oversized_line()))

    asyncio.run(scenario())


def test_sse_decoder_discards_unterminated_record_at_eof() -> None:
    async def lines() -> AsyncIterator[str]:
        yield "event: assistant.delta"
        yield 'data: {"content":"not delivered"}'

    async def scenario() -> None:
        assert [event async for event in iter_gateway_task_events(lines())] == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "payload",
    [
        '{"value":' + ("1" * 5000) + "}",
        '{"value":' + ("[" * 2000) + "0" + ("]" * 2000) + "}",
        '{"value":NaN}',
    ],
)
def test_sse_decoder_maps_json_resource_failures_to_protocol_error(
    payload: str,
) -> None:
    async def lines() -> AsyncIterator[str]:
        yield "event: assistant.delta"
        yield f"data: {payload}"
        yield ""

    async def scenario() -> None:
        with pytest.raises(SSEProtocolError, match="invalid JSON"):
            await anext(iter_gateway_task_events(lines()))

    asyncio.run(scenario())


def test_sse_encoder_applies_the_limit_to_the_final_data_line() -> None:
    empty_payload_size = len(
        json.dumps(
            {"content": ""},
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    content_length = MAX_SSE_LINE_BYTES - len(b"data: ") - empty_payload_size + 1
    event = GatewayTaskEvent(
        event_type="assistant.delta",
        data={"content": "x" * content_length},
    )

    with pytest.raises(SSEProtocolError, match="line exceeds"):
        encode_gateway_task_event(event)


def test_remote_sanitizer_returns_only_wire_encodable_unicode() -> None:
    delta = task_stream_event_from_gateway(
        GatewayTaskEvent(
            event_type="assistant.delta",
            data={
                "task_id": "remote",
                "run_count": 1,
                "created_at": "2026-08-28T12:00:00+00:00",
                "content": "\U0001f680" * MAX_EVENT_TEXT_LENGTH,
            },
        ),
        expected_task_id="remote",
        public_task_id="local",
        run_count=1,
    )
    assert len(delta.data["content"]) == MAX_ASSISTANT_DELTA_TEXT_LENGTH
    assert encode_task_stream_event(delta)

    with pytest.raises(SSEProtocolError, match="size limit"):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="task.snapshot",
                event_id="cursor",
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "status": "running",
                    "last_result": "\U0001f680" * MAX_EVENT_TEXT_LENGTH,
                    "error": None,
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )


def test_remote_event_schema_rejects_paths_and_unknown_fields() -> None:
    with pytest.raises(SSEProtocolError):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="task.artifact_published",
                event_id="cursor",
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "status": "running",
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "artifact": {
                        "artifact_id": "art-1",
                        "name": "report.txt",
                        "caption": None,
                        "content_type": "text/plain",
                        "size": 10,
                        "run_count": 1,
                        "path": "/secret/report.txt",
                    },
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )


@pytest.mark.parametrize(
    ("event_type", "status"),
    [
        ("task.created", "running"),
        ("task.running", "completed"),
        ("task.completed", "running"),
        ("task.failed", "completed"),
        ("task.cancelled", "interrupted"),
        ("task.interrupted", "cancelled"),
    ],
)
def test_remote_lifecycle_event_must_match_its_status(
    event_type: str,
    status: str,
) -> None:
    with pytest.raises(SSEProtocolError, match="inconsistent"):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type=event_type,
                event_id="cursor",
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "status": status,
                    "last_result": None,
                    "error": None,
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )


def test_remote_review_event_requires_pending_review() -> None:
    with pytest.raises(SSEProtocolError, match="no pending review"):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="task.review_requested",
                event_id="cursor",
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "status": "waiting_for_human",
                    "last_result": None,
                    "error": None,
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )


def test_remote_event_schema_rejects_unhashable_status_and_reason() -> None:
    lifecycle_data = {
        "task_id": "remote",
        "run_count": 1,
        "created_at": "2026-08-28T12:00:00+00:00",
        "status": [],
        "last_result": None,
        "error": None,
        "updated_at": "2026-08-28T12:00:00+00:00",
        "pending_review": None,
        "artifacts": [],
    }
    with pytest.raises(SSEProtocolError, match="status is invalid"):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="task.snapshot",
                event_id="cursor",
                data=lifecycle_data,
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )

    with pytest.raises(SSEProtocolError, match="status is invalid"):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="task.artifact_published",
                event_id="cursor",
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "status": {},
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "artifact": {
                        "artifact_id": "artifact-1",
                        "name": "report.txt",
                        "caption": None,
                        "content_type": "text/plain",
                        "size": 1,
                        "run_count": 1,
                    },
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )

    with pytest.raises(SSEProtocolError, match="reason is invalid"):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="stream.end",
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "reason": {},
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )


@pytest.mark.parametrize(
    "cursor",
    [
        "x" * (MAX_TASK_EVENT_CURSOR_LENGTH + 1),
        "opaque-\u6e38\u6807",
        " leading",
        "trailing ",
        " ",
        "",
    ],
)
def test_remote_durable_cursor_must_be_reconnectable(cursor: str) -> None:
    assert not is_valid_task_event_cursor(cursor)
    with pytest.raises(SSEProtocolError):
        task_stream_event_from_gateway(
            GatewayTaskEvent(
                event_type="task.snapshot",
                event_id=cursor,
                data={
                    "task_id": "remote",
                    "run_count": 1,
                    "created_at": "2026-08-28T12:00:00+00:00",
                    "status": "running",
                    "last_result": None,
                    "error": None,
                    "updated_at": "2026-08-28T12:00:00+00:00",
                    "pending_review": None,
                    "artifacts": [],
                },
            ),
            expected_task_id="remote",
            public_task_id="local",
            run_count=1,
        )


def test_remote_durable_cursor_accepts_proxy_header_limit() -> None:
    cursor = "x" * MAX_TASK_EVENT_CURSOR_LENGTH
    assert is_valid_task_event_cursor(cursor)
    event = task_stream_event_from_gateway(
        GatewayTaskEvent(
            event_type="task.snapshot",
            event_id=cursor,
            data={
                "task_id": "remote",
                "run_count": 1,
                "created_at": "2026-08-28T12:00:00+00:00",
                "status": "running",
                "last_result": None,
                "error": None,
                "updated_at": "2026-08-28T12:00:00+00:00",
                "pending_review": None,
                "artifacts": [],
            },
        ),
        expected_task_id="remote",
        public_task_id="local",
        run_count=1,
    )
    assert event.event_id == cursor
    assert is_valid_task_event_cursor("opaque cursor with internal spaces")
