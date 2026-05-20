from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from ruyi_agent.channels.cli.stream_output import format_stream_chunk, summarize_update_data


def test_summarize_model_content_only_returns_final_text() -> None:
    data = {
        "model": {
            "messages": [
                SimpleNamespace(content="hello", tool_calls=[]),
            ]
        }
    }

    assert summarize_update_data(data) == "hello"


def test_summarize_model_content_is_not_truncated() -> None:
    long_text = "这是一个很长的 agent 回复。" * 30
    data = {
        "model": {
            "messages": [
                SimpleNamespace(content=long_text, tool_calls=[]),
            ]
        }
    }

    assert summarize_update_data(data) == long_text


def test_summarize_model_tool_calls_truncates_long_args() -> None:
    data = {
        "model": {
            "messages": [
                SimpleNamespace(
                    content="",
                    tool_calls=[
                        {
                            "name": "web_search",
                            "args": {
                                "query": "a" * 200,
                                "top_k": 5,
                            },
                        }
                    ],
                ),
            ]
        }
    }

    summary = summarize_update_data(data)

    assert summary is not None
    assert summary.startswith("调用工具: web_search(")
    assert "top_k" in summary
    assert "..." in summary


def test_summarize_tools_event_returns_tool_result() -> None:
    data = {
        "tools": {
            "messages": [
                {
                    "name": "sample_tool",
                    "content": "ai 已经统治地球了",
                }
            ]
        }
    }

    assert summarize_update_data(data) == "工具结果 sample_tool: ai 已经统治地球了"


def test_format_stream_chunk_ignores_middleware_payloads() -> None:
    chunk = {
        "type": "updates",
        "ns": (),
        "data": {"MemoryMiddleware.before_agent": None},
    }

    assert format_stream_chunk(chunk) is None


def test_format_stream_chunk_adds_subagent_prefix() -> None:
    chunk = {
        "type": "updates",
        "ns": ("planner", "background_research"),
        "data": {
            "model": {
                "messages": [
                    {
                        "content": "done",
                        "tool_calls": [],
                    }
                ]
            }
        },
    }

    assert (
        format_stream_chunk(chunk)
        == "[subagent: planner / background_research] done"
    )
