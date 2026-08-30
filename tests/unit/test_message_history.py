from __future__ import annotations

import asyncio
import base64
import json
from types import SimpleNamespace

import pytest
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.task_models import TaskRouteRecord
from ruyi_agent.gateway.routing import (
    TaskRouter,
    _decode_task_message_cursor,
    _encode_task_message_cursor,
)
from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.runtime.message_history import (
    TaskMessageHistoryUnavailableError,
    TaskMessageSnapshotNotFoundError,
    TaskMessageStateReader,
    project_task_messages,
    task_message_page_from_payload,
)


def test_project_task_messages_builds_strict_public_textual_transcript() -> None:
    messages = [
        SystemMessage(content="private system prompt", id="system-1"),
        HumanMessage(
            id="human-1",
            name="alice",
            content=[
                "first",
                {"type": "text", "text": "visible"},
                {"type": "output_text", "text": "also visible"},
                {"type": "thinking", "text": "hidden thought"},
                {"type": "reasoning", "text": "hidden reasoning"},
                {"type": "image_url", "image_url": "data:image/png;base64,..."},
                {"text": "untyped hidden text"},
            ],
            additional_kwargs={"reasoning_content": "hidden metadata"},
        ),
        AIMessage(
            id="assistant-1",
            content="calling tool",
            tool_calls=[
                {
                    "id": "call-1",
                    "name": "lookup",
                    "args": {"query": "public task data", "count": 2},
                }
            ],
            response_metadata={"provider_trace": "hidden provider data"},
        ),
        ToolMessage(
            id="tool-1",
            content="tool result",
            tool_call_id="call-1",
            name="lookup",
            status="success",
        ),
        HumanMessage(content="same message"),
        HumanMessage(content="same message"),
    ]

    first = project_task_messages("task-1", messages)
    second = project_task_messages("task-1", messages)

    assert [item.sequence for item in first] == [0, 1, 2, 3, 4]
    assert [item.role for item in first] == [
        "user",
        "assistant",
        "tool",
        "user",
        "user",
    ]
    assert first[0].content == "first\nvisible\nalso visible"
    assert first[0].name == "alice"
    assert first[1].tool_calls[0].tool_call_id == "call-1"
    assert first[1].tool_calls[0].arguments == {
        "query": "public task data",
        "count": 2,
    }
    assert first[2].tool_call_id == "call-1"
    assert first[2].status == "success"
    serialized = repr(first)
    assert "private system prompt" not in serialized
    assert "hidden thought" not in serialized
    assert "hidden reasoning" not in serialized
    assert "hidden metadata" not in serialized
    assert "hidden provider data" not in serialized
    assert "data:image" not in serialized
    assert first[3].message_id != first[4].message_id
    assert [item.message_id for item in first] == [item.message_id for item in second]


def test_project_task_messages_keeps_non_text_tool_call_message() -> None:
    message = AIMessage(
        content=[{"type": "thinking", "text": "do not expose"}],
        tool_calls=[{"id": "call-1", "name": "search", "args": {}}],
    )

    projected = project_task_messages("task-1", [message])

    assert len(projected) == 1
    assert projected[0].content == ""
    assert projected[0].tool_calls[0].name == "search"


def test_project_task_messages_ignores_unhashable_block_discriminator() -> None:
    message = HumanMessage(
        content=[
            {"type": [], "text": "must not be exposed"},
            {"type": "text", "text": "visible"},
        ]
    )

    projected = project_task_messages("task-1", [message])

    assert projected[0].content == "visible"


def test_remote_task_message_page_parser_rejects_non_json_arguments() -> None:
    with pytest.raises(ValueError, match="tool arguments"):
        task_message_page_from_payload(
            {
                "task_id": "upstream-1",
                "items": [
                    {
                        "sequence": 0,
                        "message_id": "message-1",
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "tool_call_id": "call-1",
                                "name": "lookup",
                                "arguments": {"bad": object()},
                            }
                        ],
                    }
                ],
                "next_cursor": None,
            }
        )


def test_task_message_state_reader_pins_latest_checkpoint_before_returning() -> None:
    class RecordingGraph:
        def __init__(self) -> None:
            self.configs: list[dict[str, object]] = []

        async def aget_state(self, config):
            self.configs.append(config)
            if len(self.configs) == 1:
                return SimpleNamespace(
                    config={
                        "configurable": {
                            "thread_id": "thread-1",
                            "checkpoint_id": "checkpoint-1",
                        }
                    },
                    metadata={"step": 1},
                    values={"messages": [HumanMessage(content="pending view")]},
                )
            return SimpleNamespace(
                config=config,
                metadata={"step": 1},
                values={"messages": [HumanMessage(content="committed view")]},
            )

    graph = RecordingGraph()
    reader = TaskMessageStateReader(object())
    reader._graph = graph

    snapshot = asyncio.run(reader.read(thread_id="thread-1"))

    assert snapshot.checkpoint_id == "checkpoint-1"
    assert snapshot.messages[0].content == "committed view"
    assert len(graph.configs) == 2
    assert graph.configs[1] == {
        "configurable": {
            "thread_id": "thread-1",
            "checkpoint_id": "checkpoint-1",
        }
    }


def test_task_message_state_reader_distinguishes_empty_and_missing_snapshot() -> None:
    class EmptyGraph:
        async def aget_state(self, config):
            return SimpleNamespace(config=config, metadata=None, values={})

    async def scenario() -> None:
        reader = TaskMessageStateReader(object())
        reader._graph = EmptyGraph()
        empty = await reader.read(thread_id="thread-1")
        assert empty.checkpoint_id is None
        assert empty.messages == ()
        with pytest.raises(TaskMessageSnapshotNotFoundError):
            await reader.read(
                thread_id="thread-1",
                checkpoint_id="missing-checkpoint",
            )

    asyncio.run(scenario())


def test_task_message_state_reader_wraps_invalid_checkpointer() -> None:
    async def scenario() -> None:
        reader = TaskMessageStateReader(object())
        with pytest.raises(TaskMessageHistoryUnavailableError):
            await reader.read(thread_id="thread-1")

    asyncio.run(scenario())


def test_task_message_cursor_is_snapshot_bound_and_task_bound() -> None:
    cursor = _encode_task_message_cursor(
        task_id="task-1",
        checkpoint_id="checkpoint-1",
        offset=20,
    )

    assert _decode_task_message_cursor(cursor, task_id="task-1") == (
        "checkpoint-1",
        20,
    )
    with pytest.raises(GatewayTaskError) as cross_task:
        _decode_task_message_cursor(cursor, task_id="task-2")
    assert cross_task.value.code == "invalid_request"

    for malformed in ("not-base64", "中文游标", "a" * 4097):
        with pytest.raises(GatewayTaskError) as caught:
            _decode_task_message_cursor(malformed, task_id="task-1")
        assert caught.value.code == "invalid_request"


@pytest.mark.parametrize(
    "payload",
    [
        {"version": 2, "task_id": "task-1", "checkpoint_id": "cp", "offset": 1},
        {"version": 1, "task_id": "task-1", "checkpoint_id": "cp", "offset": -1},
        {"version": 1, "task_id": "task-1", "checkpoint_id": "", "offset": 1},
        {
            "version": 1,
            "task_id": "task-1",
            "checkpoint_id": "cp",
            "offset": 1,
            "extra": True,
        },
    ],
)
def test_task_message_cursor_rejects_untrusted_fields(
    payload: dict[str, object],
) -> None:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    cursor = base64.urlsafe_b64encode(raw).decode().rstrip("=")

    with pytest.raises(GatewayTaskError) as caught:
        _decode_task_message_cursor(cursor, task_id="task-1")

    assert caught.value.code == "invalid_request"


class RemoteHistoryControl:
    def __init__(
        self,
        *,
        payload: dict[str, object] | None = None,
        error: A2AClientError | None = None,
    ) -> None:
        self.payload = payload
        self.error = error
        self.requests: list[tuple[str, str | None, int]] = []

    def ensure_remote_task_record(self, **kwargs):
        return SimpleNamespace(**kwargs)

    async def list_remote_task_messages(
        self,
        task_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> dict[str, object]:
        self.requests.append((task_id, cursor, limit))
        if self.error is not None:
            raise self.error
        assert self.payload is not None
        return self.payload


def _remote_route() -> TaskRouteRecord:
    return TaskRouteRecord(
        task_id="proxy-1",
        agent_name="remote-worker",
        metadata={},
        route_kind="remote_ref",
        upstream_task_id="upstream-1",
    )


def test_task_router_proxies_remote_cursor_and_rewrites_task_id() -> None:
    control = RemoteHistoryControl(
        payload={
            "task_id": "upstream-1",
            "items": [
                {
                    "sequence": 2,
                    "message_id": "message-3",
                    "role": "tool",
                    "content": "result",
                    "name": "lookup",
                    "tool_call_id": "call-1",
                    "tool_calls": [],
                    "status": "success",
                }
            ],
            "next_cursor": "downstream-opaque-cursor",
        }
    )
    router = TaskRouter(control=control, route_store=object())  # type: ignore[arg-type]

    page = asyncio.run(
        router.list_task_messages(
            _remote_route(),
            cursor="incoming-opaque-cursor",
            limit=7,
        )
    )

    assert control.requests == [("proxy-1", "incoming-opaque-cursor", 7)]
    assert page.task_id == "proxy-1"
    assert page.next_cursor == "downstream-opaque-cursor"
    assert page.items[0].tool_call_id == "call-1"


@pytest.mark.parametrize(
    ("status_code", "code", "expected_kind"),
    [
        (400, "invalid_request", None),
        (404, "task_not_found", "upstream_failure"),
        (503, "task_history_unavailable", "upstream_failure"),
    ],
)
def test_task_router_maps_remote_history_errors(
    status_code: int,
    code: str,
    expected_kind: str | None,
) -> None:
    control = RemoteHistoryControl(
        error=A2AClientError(
            status_code=status_code,
            code=code,
            message="downstream failure",
        )
    )
    router = TaskRouter(control=control, route_store=object())  # type: ignore[arg-type]

    with pytest.raises(GatewayTaskError) as caught:
        asyncio.run(
            router.list_task_messages(
                _remote_route(),
                cursor="opaque",
                limit=20,
            )
        )

    assert caught.value.code == code
    assert caught.value.kind == expected_kind


def test_task_router_rejects_remote_message_page_for_wrong_task() -> None:
    control = RemoteHistoryControl(
        payload={
            "task_id": "wrong-upstream-task",
            "items": [],
            "next_cursor": None,
        }
    )
    router = TaskRouter(control=control, route_store=object())  # type: ignore[arg-type]

    with pytest.raises(GatewayTaskError) as caught:
        asyncio.run(
            router.list_task_messages(
                _remote_route(),
                cursor=None,
                limit=20,
            )
        )

    assert caught.value.code == "upstream_gateway_error"


@pytest.mark.parametrize(
    "item",
    [
        {
            "sequence": 0,
            "message_id": "message-1",
            "role": [],
            "content": "bad role",
        },
        {
            "sequence": 0,
            "message_id": "message-1",
            "role": "tool",
            "content": "bad status",
            "tool_call_id": "call-1",
            "status": {},
        },
    ],
)
def test_task_router_maps_unhashable_remote_discriminators_to_upstream_error(
    item: dict[str, object],
) -> None:
    control = RemoteHistoryControl(
        payload={
            "task_id": "upstream-1",
            "items": [item],
            "next_cursor": None,
        }
    )
    router = TaskRouter(control=control, route_store=object())  # type: ignore[arg-type]

    with pytest.raises(GatewayTaskError) as caught:
        asyncio.run(
            router.list_task_messages(
                _remote_route(),
                cursor=None,
                limit=20,
            )
        )

    assert caught.value.code == "upstream_gateway_error"
