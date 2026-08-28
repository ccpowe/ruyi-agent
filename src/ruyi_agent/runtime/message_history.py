from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from langchain_core.messages import AIMessage, HumanMessage, ToolMessage
from langgraph.graph import END, START, StateGraph

from ruyi_agent.runtime.agent_factory import RuyiAgentState

TaskMessageRole = Literal["user", "assistant", "tool"]
TaskToolStatus = Literal["success", "error"]


class TaskMessageHistoryUnavailableError(RuntimeError):
    """The checkpoint-backed task transcript could not be reconstructed."""


class TaskMessageSnapshotNotFoundError(ValueError):
    """A caller-provided checkpoint no longer exists for the task thread."""


@dataclass(frozen=True, slots=True)
class TaskMessageToolCall:
    tool_call_id: str
    name: str
    arguments: dict[str, Any]


@dataclass(frozen=True, slots=True)
class TaskConversationMessage:
    sequence: int
    message_id: str
    role: TaskMessageRole
    content: str
    name: str | None = None
    tool_call_id: str | None = None
    tool_calls: tuple[TaskMessageToolCall, ...] = ()
    status: TaskToolStatus | None = None


@dataclass(frozen=True, slots=True)
class TaskMessageSnapshot:
    checkpoint_id: str | None
    messages: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class TaskMessagePage:
    task_id: str
    items: tuple[TaskConversationMessage, ...]
    next_cursor: str | None


class TaskMessageStateReader:
    """Reconstruct message state without compiling the configured runtime agent."""

    def __init__(self, checkpointer: Any) -> None:
        self._checkpointer = checkpointer
        self._graph: Any | None = None

    def _get_graph(self) -> Any:
        if self._graph is None:
            builder = StateGraph(RuyiAgentState)
            builder.add_edge(START, END)
            self._graph = builder.compile(checkpointer=self._checkpointer)
        return self._graph

    async def read(
        self,
        *,
        thread_id: str,
        checkpoint_id: str | None = None,
    ) -> TaskMessageSnapshot:
        """Read one exact checkpoint, locating and pinning latest when omitted."""

        try:
            graph = self._get_graph()
            if checkpoint_id is not None:
                snapshot = await graph.aget_state(
                    {
                        "configurable": {
                            "thread_id": thread_id,
                            "checkpoint_id": checkpoint_id,
                        }
                    }
                )
                if getattr(snapshot, "metadata", None) is None:
                    raise TaskMessageSnapshotNotFoundError(checkpoint_id)
                return _task_message_snapshot(snapshot, checkpoint_id=checkpoint_id)

            latest = await graph.aget_state({"configurable": {"thread_id": thread_id}})
            if getattr(latest, "metadata", None) is None:
                return TaskMessageSnapshot(checkpoint_id=None, messages=())

            latest_checkpoint_id = _snapshot_checkpoint_id(latest)
            # Without checkpoint_id LangGraph applies pending writes. Re-read the
            # exact checkpoint so a page never includes a partially committed step.
            exact = await graph.aget_state(getattr(latest, "config"))
            if getattr(exact, "metadata", None) is None:
                raise TaskMessageHistoryUnavailableError(
                    "Latest task message checkpoint disappeared while being read"
                )
            return _task_message_snapshot(
                exact,
                checkpoint_id=latest_checkpoint_id,
            )
        except TaskMessageSnapshotNotFoundError:
            raise
        except TaskMessageHistoryUnavailableError:
            raise
        except Exception as exc:
            raise TaskMessageHistoryUnavailableError(
                "Task message checkpoint could not be reconstructed"
            ) from exc


def project_task_messages(
    task_id: str,
    messages: Sequence[Any],
) -> tuple[TaskConversationMessage, ...]:
    """Build the stable public textual transcript from LangChain messages."""

    projected: list[TaskConversationMessage] = []
    missing_id_occurrences: defaultdict[str, int] = defaultdict(int)
    for message in messages:
        role = _public_role(message)
        if role is None:
            continue
        content = _public_text(getattr(message, "content", None))
        name = _optional_nonempty_string(getattr(message, "name", None))
        tool_call_id = None
        tool_calls: tuple[TaskMessageToolCall, ...] = ()
        status: TaskToolStatus | None = None
        if role == "assistant":
            tool_calls = _public_tool_calls(getattr(message, "tool_calls", None))
        elif role == "tool":
            tool_call_id = _optional_nonempty_string(
                getattr(message, "tool_call_id", None)
            )
            raw_status = getattr(message, "status", None)
            if isinstance(raw_status, str) and raw_status in {"success", "error"}:
                status = raw_status

        fingerprint = _message_fingerprint(
            role=role,
            content=content,
            name=name,
            tool_call_id=tool_call_id,
            tool_calls=tool_calls,
            status=status,
        )
        message_id = _optional_nonempty_string(getattr(message, "id", None))
        if message_id is None:
            occurrence = missing_id_occurrences[fingerprint]
            missing_id_occurrences[fingerprint] += 1
            message_id = _fallback_message_id(task_id, fingerprint, occurrence)

        projected.append(
            TaskConversationMessage(
                sequence=len(projected),
                message_id=message_id,
                role=role,
                content=content,
                name=name,
                tool_call_id=tool_call_id,
                tool_calls=tool_calls,
                status=status,
            )
        )
    return tuple(projected)


def task_message_page_from_payload(payload: Mapping[str, Any]) -> TaskMessagePage:
    """Validate an untrusted message page returned by a remote Gateway."""

    task_id = _required_nonempty_string(payload.get("task_id"), field="task_id")
    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        raise ValueError("Remote task message payload has invalid items")
    raw_next_cursor = payload.get("next_cursor")
    if raw_next_cursor is not None and (
        not isinstance(raw_next_cursor, str) or not raw_next_cursor
    ):
        raise ValueError("Remote task message payload has invalid next_cursor")

    items: list[TaskConversationMessage] = []
    previous_sequence = -1
    for raw_item in raw_items:
        if not isinstance(raw_item, Mapping):
            raise ValueError("Remote task message payload has an invalid item")
        sequence = raw_item.get("sequence")
        if (
            not isinstance(sequence, int)
            or isinstance(sequence, bool)
            or sequence < 0
            or sequence <= previous_sequence
        ):
            raise ValueError("Remote task message payload has invalid sequence")
        previous_sequence = sequence
        role = raw_item.get("role")
        if not isinstance(role, str) or role not in {"user", "assistant", "tool"}:
            raise ValueError("Remote task message payload has invalid role")
        content = raw_item.get("content")
        if not isinstance(content, str):
            raise ValueError("Remote task message payload has invalid content")
        message_id = _required_nonempty_string(
            raw_item.get("message_id"),
            field="message_id",
        )
        name = _validated_optional_string(raw_item.get("name"), field="name")
        tool_call_id = _validated_optional_string(
            raw_item.get("tool_call_id"),
            field="tool_call_id",
        )
        raw_status = raw_item.get("status")
        if raw_status is not None and (
            not isinstance(raw_status, str)
            or raw_status not in {"success", "error"}
        ):
            raise ValueError("Remote task message payload has invalid status")
        raw_tool_calls = raw_item.get("tool_calls", [])
        if not isinstance(raw_tool_calls, list):
            raise ValueError("Remote task message payload has invalid tool_calls")
        tool_calls = tuple(_tool_call_from_payload(call) for call in raw_tool_calls)
        if role != "assistant" and tool_calls:
            raise ValueError("Only assistant messages may contain tool_calls")
        if role != "tool" and (tool_call_id is not None or raw_status is not None):
            raise ValueError("Only tool messages may contain tool result fields")

        items.append(
            TaskConversationMessage(
                sequence=sequence,
                message_id=message_id,
                role=role,
                content=content,
                name=name,
                tool_call_id=tool_call_id,
                tool_calls=tool_calls,
                status=raw_status,
            )
        )
    return TaskMessagePage(
        task_id=task_id,
        items=tuple(items),
        next_cursor=raw_next_cursor,
    )


def _snapshot_checkpoint_id(snapshot: Any) -> str:
    config = getattr(snapshot, "config", None)
    configurable = config.get("configurable") if isinstance(config, Mapping) else None
    checkpoint_id = (
        configurable.get("checkpoint_id") if isinstance(configurable, Mapping) else None
    )
    if not isinstance(checkpoint_id, str) or not checkpoint_id:
        raise TaskMessageHistoryUnavailableError(
            "Latest task message state has no checkpoint id"
        )
    return checkpoint_id


def _task_message_snapshot(
    snapshot: Any,
    *,
    checkpoint_id: str,
) -> TaskMessageSnapshot:
    values = getattr(snapshot, "values", None)
    if not isinstance(values, Mapping):
        raise TaskMessageHistoryUnavailableError(
            "Task message checkpoint has invalid state"
        )
    messages = values.get("messages", [])
    if not isinstance(messages, Sequence) or isinstance(messages, (str, bytes)):
        raise TaskMessageHistoryUnavailableError(
            "Task message checkpoint has invalid messages"
        )
    return TaskMessageSnapshot(
        checkpoint_id=checkpoint_id,
        messages=tuple(messages),
    )


def _public_role(message: Any) -> TaskMessageRole | None:
    if isinstance(message, HumanMessage):
        return "user"
    if isinstance(message, AIMessage):
        return "assistant"
    if isinstance(message, ToolMessage):
        return "tool"
    return None


def _public_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
            continue
        if not isinstance(block, Mapping):
            continue
        block_type = block.get("type")
        if not isinstance(block_type, str) or block_type not in {
            "text",
            "output_text",
        }:
            continue
        text = block.get("text")
        if isinstance(text, str):
            parts.append(text)
    return "\n".join(parts)


def _public_tool_calls(value: Any) -> tuple[TaskMessageToolCall, ...]:
    if not isinstance(value, list):
        return ()
    calls: list[TaskMessageToolCall] = []
    for raw_call in value:
        if not isinstance(raw_call, Mapping):
            continue
        tool_call_id = _optional_nonempty_string(raw_call.get("id"))
        name = _optional_nonempty_string(raw_call.get("name"))
        if tool_call_id is None or name is None:
            continue
        raw_arguments = raw_call.get("args")
        arguments = (
            _json_object(raw_arguments) if isinstance(raw_arguments, Mapping) else {}
        )
        calls.append(
            TaskMessageToolCall(
                tool_call_id=tool_call_id,
                name=name,
                arguments=arguments,
            )
        )
    return tuple(calls)


def _message_fingerprint(
    *,
    role: TaskMessageRole,
    content: str,
    name: str | None,
    tool_call_id: str | None,
    tool_calls: tuple[TaskMessageToolCall, ...],
    status: TaskToolStatus | None,
) -> str:
    encoded = json.dumps(
        {
            "role": role,
            "content": content,
            "name": name,
            "tool_call_id": tool_call_id,
            "tool_calls": [
                {
                    "tool_call_id": call.tool_call_id,
                    "name": call.name,
                    "arguments": call.arguments,
                }
                for call in tool_calls
            ],
            "status": status,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _fallback_message_id(task_id: str, fingerprint: str, occurrence: int) -> str:
    seed = f"{task_id}:{fingerprint}:{occurrence}".encode()
    return f"msg_{hashlib.sha256(seed).hexdigest()[:24]}"


def _json_object(value: Mapping[Any, Any]) -> dict[str, Any]:
    return {str(key): _json_value(item) for key, item in value.items()}


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else str(value)
    if isinstance(value, Mapping):
        return _json_object(value)
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def _tool_call_from_payload(value: Any) -> TaskMessageToolCall:
    if not isinstance(value, Mapping):
        raise ValueError("Remote task message payload has an invalid tool call")
    tool_call_id = _required_nonempty_string(
        value.get("tool_call_id"),
        field="tool_call_id",
    )
    name = _required_nonempty_string(value.get("name"), field="name")
    arguments = value.get("arguments")
    if not isinstance(arguments, Mapping) or not _is_json_value(arguments):
        raise ValueError("Remote task message payload has invalid tool arguments")
    return TaskMessageToolCall(
        tool_call_id=tool_call_id,
        name=name,
        arguments=dict(arguments),
    )


def _is_json_value(value: Any) -> bool:
    if value is None or isinstance(value, (str, bool, int)):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    if isinstance(value, list):
        return all(_is_json_value(item) for item in value)
    if isinstance(value, Mapping):
        return all(
            isinstance(key, str) and _is_json_value(item) for key, item in value.items()
        )
    return False


def _optional_nonempty_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def _required_nonempty_string(value: Any, *, field: str) -> str:
    result = _optional_nonempty_string(value)
    if result is None:
        raise ValueError(f"Remote task message payload has invalid {field}")
    return result


def _validated_optional_string(value: Any, *, field: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(f"Remote task message payload has invalid {field}")
    return value
