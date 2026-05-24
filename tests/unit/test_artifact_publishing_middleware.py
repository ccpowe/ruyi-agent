from __future__ import annotations

import json
import asyncio
from collections.abc import Sequence
from typing import Any

from deepagents.backends.protocol import FileDownloadResponse
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, ToolMessage
from langchain_core.outputs import ChatGeneration, ChatResult

from ruyi_agent.runtime.agent_factory import create_runtime_agent
from ruyi_agent.runtime.middleware.artifact_publishing import (
    ArtifactPublishingMiddleware,
)


class MemoryBackend:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def download_files(self, paths: list[str]):
        responses = []
        for path in paths:
            content = self.files.get(path)
            responses.append(
                FileDownloadResponse(
                    path=path,
                    content=content,
                    error=None if content is not None else "file_not_found",
                )
            )
        return responses


class FakeToolCallingModel(BaseChatModel):
    responses: list[Any]
    i: int = 0

    @property
    def _llm_type(self) -> str:
        return "fake-artifact-publishing-model"

    def bind_tools(
        self,
        tools: Sequence[Any],
        *,
        tool_choice: str | None = None,
        **kwargs: Any,
    ) -> "FakeToolCallingModel":
        return self

    def _generate(
        self,
        messages: Any,
        stop: Any = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        response = self.responses[self.i]
        if self.i < len(self.responses) - 1:
            self.i += 1
        return ChatResult(generations=[ChatGeneration(message=response)])

    async def _agenerate(
        self,
        messages: Any,
        stop: Any = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        return self._generate(messages, stop=stop, run_manager=run_manager, **kwargs)


def test_publish_artifact_registers_workspace_file() -> None:
    backend = MemoryBackend()
    backend.files["/workspace/out/report.html"] = b"<html>ok</html>"
    registered: list[dict[str, Any]] = []

    def register_artifact(*, task_id: str, artifact: dict[str, Any]) -> dict[str, Any]:
        registered.append({"task_id": task_id, **artifact})
        return {"artifact_id": "art_1", "run_count": 2, **artifact}

    middleware = ArtifactPublishingMiddleware(
        backend=backend,
        workspace_root="/workspace",
        register_artifact=register_artifact,
    )
    result = middleware.publish_artifact(
        path="/workspace/out/report.html",
        config={"configurable": {"task_id": "task-1"}},
    )
    payload = json.loads(result)

    assert payload == {
        "ok": True,
        "artifact_id": "art_1",
        "path": "/workspace/out/report.html",
        "name": "report.html",
        "caption": None,
        "content_type": "text/html",
        "size": 15,
        "run_count": 2,
    }
    assert registered == [
        {
            "task_id": "task-1",
            "path": "/workspace/out/report.html",
            "name": "report.html",
            "caption": None,
            "content_type": "text/html",
            "size": 15,
        }
    ]


def test_publish_artifact_reports_missing_file_with_hint() -> None:
    middleware = ArtifactPublishingMiddleware(
        backend=MemoryBackend(),
        workspace_root="/",
        register_artifact=lambda *, task_id, artifact: artifact,
    )
    result = middleware.publish_artifact(
        path="/wokespace/tetris.html",
        config={"configurable": {"task_id": "task-1"}},
    )
    payload = json.loads(result)

    assert payload["ok"] is False
    assert payload["error"] == "file_not_found"
    assert "backend workspace path" in payload["hint"]
    assert "/tetris.html" in payload["hint"]


def test_publish_artifact_rejects_windows_host_path() -> None:
    middleware = ArtifactPublishingMiddleware(
        backend=MemoryBackend(),
        workspace_root="/",
        register_artifact=lambda *, task_id, artifact: artifact,
    )
    result = middleware.publish_artifact(
        path=r"D:\Code\wokespace\tetris.html",
        config={"configurable": {"task_id": "task-1"}},
    )
    payload = json.loads(result)

    assert payload["ok"] is False
    assert payload["error"] == "host_path_forbidden"
    assert "backend workspace path" in payload["hint"]


def test_publish_artifact_tool_uses_agent_task_config() -> None:
    backend = MemoryBackend()
    backend.files["/tetris.html"] = b"<html>ok</html>"
    registered: list[dict[str, Any]] = []

    def register_artifact(*, task_id: str, artifact: dict[str, Any]) -> dict[str, Any]:
        registered.append({"task_id": task_id, **artifact})
        return {"artifact_id": "art_1", "run_count": 1, **artifact}

    model = FakeToolCallingModel(
        responses=[
            AIMessage(
                content="",
                tool_calls=[
                    {
                        "name": "publish_artifact",
                        "args": {"path": "/tetris.html"},
                        "id": "call-1",
                        "type": "tool_call",
                    }
                ],
            ),
            AIMessage(content="done"),
        ]
    )
    agent = create_runtime_agent(
        model=model,
        tools=[],
        system_prompt="Test artifact publishing",
        backend=backend,
        workspace_root="/",
        register_artifact=register_artifact,
    )

    async def scenario() -> list[ToolMessage]:
        result = await agent.ainvoke(
            {"messages": [{"role": "user", "content": "publish it"}]},
            config={"configurable": {"thread_id": "task-1", "task_id": "task-1"}},
            version="v2",
        )
        messages = result.value["messages"] if hasattr(result, "value") else result["messages"]
        return [message for message in messages if isinstance(message, ToolMessage)]

    tool_messages = asyncio.run(scenario())

    assert registered == [
        {
            "task_id": "task-1",
            "path": "/tetris.html",
            "name": "tetris.html",
            "caption": None,
            "content_type": "text/html",
            "size": 15,
        }
    ]
    assert json.loads(str(tool_messages[0].content))["ok"] is True
