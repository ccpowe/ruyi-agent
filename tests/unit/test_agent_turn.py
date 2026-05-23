from __future__ import annotations

import asyncio

from ruyi_agent.runtime.agent_turn import (
    normalize_agent_turn,
    normalize_agent_turn_result,
)


class GraphOutput:
    def __init__(self, value):
        self.value = value


class Interrupt:
    def __init__(self, value):
        self.value = value


class Snapshot:
    def __init__(self, interrupts):
        self.interrupts = interrupts


class SnapshotAgent:
    def __init__(self, snapshot):
        self.snapshot = snapshot
        self.state_configs = []

    async def aget_state(self, config):
        self.state_configs.append(config)
        return self.snapshot


def test_normalize_agent_turn_result_extracts_content_from_graph_output_value() -> None:
    outcome = normalize_agent_turn_result(
        GraphOutput(
            {
                "messages": [
                    {"role": "user", "content": "do it"},
                    {"role": "assistant", "content": "done"},
                ]
            }
        )
    )

    assert outcome.content == "done"
    assert outcome.review_payloads == []
    assert outcome.has_unresolved_tool_calls is False
    assert not hasattr(outcome, "raw_result")


def test_normalize_agent_turn_result_uses_last_assistant_message() -> None:
    outcome = normalize_agent_turn_result(
        {
            "messages": [
                {"role": "user", "content": "start"},
                {"role": "assistant", "content": "first"},
                {"role": "tool", "content": "intermediate"},
                {"role": "assistant", "content": "final reply"},
                {"role": "tool", "content": "trailing tool output"},
            ]
        }
    )

    assert outcome.content == "final reply"


def test_normalize_agent_turn_result_does_not_expose_raw_state_without_messages() -> None:
    outcome = normalize_agent_turn_result(GraphOutput({"state": "ok"}))

    assert outcome.content == ""


def test_normalize_agent_turn_result_stringifies_list_content_blocks() -> None:
    outcome = normalize_agent_turn_result(
        {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "first"},
                        "second",
                        {"type": "image_url", "image_url": "ignored"},
                    ],
                }
            ]
        }
    )

    assert outcome.content == "first\nsecond"


def test_normalize_agent_turn_result_hides_litellm_thinking_blocks() -> None:
    outcome = normalize_agent_turn_result(
        {
            "messages": [
                {
                    "role": "assistant",
                    "content": [
                        {"type": "thinking", "thinking": "hidden reasoning"},
                        {"type": "text", "text": "visible answer"},
                    ],
                }
            ]
        }
    )

    assert outcome.content == "visible answer"


def test_normalize_agent_turn_result_extracts_nested_review_payloads() -> None:
    review_payload = {
        "action_requests": [{"name": "execute", "args": {"command": "python -V"}}],
        "review_configs": [
            {"action_name": "execute", "allowed_decisions": ["approve", "reject"]}
        ],
    }

    outcome = normalize_agent_turn_result(
        {"node": {"__interrupt__": [Interrupt(review_payload)]}}
    )

    assert outcome.review_payloads == [review_payload]


def test_normalize_agent_turn_uses_snapshot_fallback_for_reviews() -> None:
    review_payload = {
        "action_requests": [{"name": "execute", "args": {"command": "python -V"}}],
        "review_configs": [{"action_name": "execute", "allowed_decisions": ["approve"]}],
    }
    config = {"configurable": {"thread_id": "thread-1"}}
    agent = SnapshotAgent(Snapshot([Interrupt(review_payload)]))

    outcome = asyncio.run(
        normalize_agent_turn(
            agent,
            config,
            {"messages": [{"role": "assistant", "content": ""}]},
        )
    )

    assert outcome.review_payloads == [review_payload]
    assert agent.state_configs == [config]


def test_normalize_agent_turn_ignores_snapshot_fallback_failure() -> None:
    class FailingSnapshotAgent:
        async def aget_state(self, config):
            raise RuntimeError("snapshot unavailable")

    outcome = asyncio.run(
        normalize_agent_turn(
            FailingSnapshotAgent(),
            {"configurable": {"thread_id": "thread-1"}},
            {"messages": [{"role": "assistant", "content": "done"}]},
        )
    )

    assert outcome.content == "done"
    assert outcome.review_payloads == []


def test_normalize_agent_turn_result_detects_unresolved_tool_calls() -> None:
    outcome = normalize_agent_turn_result(
        {
            "messages": [
                {
                    "role": "assistant",
                    "content": "",
                    "tool_calls": [
                        {
                            "id": "call-1",
                            "name": "execute",
                            "args": {"command": "python -V"},
                        }
                    ],
                }
            ]
        }
    )

    assert outcome.has_unresolved_tool_calls is True
