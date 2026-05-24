from __future__ import annotations

import json
import mimetypes
import re
from collections.abc import Callable
from pathlib import PurePosixPath
from typing import Any

from deepagents.middleware._utils import append_to_system_message
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ContextT,
    ModelRequest,
    ModelResponse,
    ResponseT,
)
from langchain_core.tools import StructuredTool
from langgraph.config import get_config
from langgraph.prebuilt import ToolRuntime
from pydantic import BaseModel, Field


DEFAULT_ARTIFACT_MAX_BYTES = 50 * 1024 * 1024

ARTIFACT_PUBLISHING_SYSTEM_PROMPT = """## Artifact Publishing

Use `publish_artifact` when you create a file the user should receive through the
active channel. Pass a backend workspace absolute path such as `/report.xlsx`.
Do not use host absolute paths such as Windows drive paths, and do not write
`MEDIA:` tags in your final response.

If `publish_artifact` returns an error, inspect the workspace path and retry with
the corrected backend path or explain the issue to the user."""


class PublishArtifactSchema(BaseModel):
    path: str = Field(
        min_length=1,
        description="Backend workspace absolute path to the file to publish.",
    )
    name: str | None = Field(
        default=None,
        description="Optional delivered filename. Defaults to the path basename.",
    )
    caption: str | None = Field(
        default=None,
        description="Optional short caption shown by channels that support it.",
    )
    content_type: str | None = Field(
        default=None,
        description="Optional MIME type. Defaults to an extension-based guess.",
    )


class ArtifactPublishingMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Expose a structured small-file publishing tool to runtime agents."""

    def __init__(
        self,
        *,
        backend: Any,
        workspace_root: str,
        register_artifact: Callable[..., dict[str, Any]],
        max_bytes: int = DEFAULT_ARTIFACT_MAX_BYTES,
        system_prompt: str | None = ARTIFACT_PUBLISHING_SYSTEM_PROMPT,
    ) -> None:
        super().__init__()
        self._backend = backend
        self._workspace_root = _normalize_workspace_root(workspace_root)
        self._register_artifact = register_artifact
        self._max_bytes = max(1, max_bytes)
        self.system_prompt = system_prompt

        async def publish_artifact(
            path: str,
            name: str | None = None,
            caption: str | None = None,
            content_type: str | None = None,
            runtime: ToolRuntime = None,  # type: ignore[assignment]
        ) -> str:
            """Publish a workspace file as a task artifact."""
            return self.publish_artifact(
                path=path,
                name=name,
                caption=caption,
                content_type=content_type,
                config=_runtime_config(runtime),
            )

        # StructuredTool builds its hidden injected-argument set from raw function
        # annotations. This module uses postponed annotations, so make the runtime
        # injection marker concrete before constructing the tool.
        publish_artifact.__annotations__["runtime"] = ToolRuntime

        self.tools = [
            StructuredTool.from_function(
                coroutine=publish_artifact,
                name="publish_artifact",
                description=(
                    "Validate and publish a small file from the backend workspace "
                    "so the active channel can deliver it to the user."
                ),
                infer_schema=False,
                args_schema=PublishArtifactSchema,
            )
        ]

    def publish_artifact(
        self,
        *,
        path: str,
        name: str | None = None,
        caption: str | None = None,
        content_type: str | None = None,
        config: dict[str, Any] | None = None,
    ) -> str:
        task_id = _task_id_from_config(config)
        if task_id is None:
            return _json_error(
                "no_active_task",
                path=path,
                hint="publish_artifact can only be used while running a tracked task.",
            )

        path_error = self._validate_path(path)
        if path_error is not None:
            return _json_error(path_error, path=path, hint=_path_hint())

        result = self._read_file(path)
        if result["error"] is not None:
            return _json_error("file_not_found", path=path, hint=_path_hint())
        content = result["content"]
        if not isinstance(content, bytes):
            return _json_error("file_not_found", path=path, hint=_path_hint())
        size = len(content)
        if size > self._max_bytes:
            return _json_error(
                "file_too_large",
                path=path,
                hint=(
                    f"Artifact exceeds the {self._max_bytes} byte limit. "
                    "Publish a smaller file or use external storage."
                ),
                details={"size": size, "max_bytes": self._max_bytes},
            )

        filename = _safe_filename(name) or PurePosixPath(path).name or "artifact"
        artifact = {
            "path": path,
            "name": filename,
            "caption": caption.strip() if isinstance(caption, str) and caption.strip() else None,
            "content_type": content_type or _guess_content_type(filename),
            "size": size,
        }
        try:
            registered = self._register_artifact(task_id=task_id, artifact=artifact)
        except Exception as exc:  # noqa: BLE001
            return _json_error(
                "artifact_registration_failed",
                path=path,
                hint=f"Runtime could not register this artifact: {exc}",
            )
        return json.dumps({"ok": True, **registered}, ensure_ascii=False, sort_keys=True)

    def _validate_path(self, path: str) -> str | None:
        if not isinstance(path, str) or not path.strip():
            return "invalid_path"
        if "\\" in path or re.match(r"^[A-Za-z]:", path):
            return "host_path_forbidden"
        candidate = PurePosixPath(path)
        if not candidate.is_absolute() or ".." in candidate.parts:
            return "workspace_path_forbidden"
        if self._workspace_root != "/" and not candidate.is_relative_to(
            PurePosixPath(self._workspace_root)
        ):
            return "workspace_path_forbidden"
        return None

    def _read_file(self, path: str) -> dict[str, Any]:
        responses = self._backend.download_files([path])
        if not responses:
            return {"content": None, "error": "file_not_found"}
        item = responses[0]
        return {
            "content": getattr(item, "content", None),
            "error": getattr(item, "error", None),
        }

    def wrap_model_call(
        self,
        request: ModelRequest[ContextT],
        handler: Callable[[ModelRequest[ContextT]], ModelResponse[ResponseT]],
    ) -> ModelResponse[ResponseT]:
        if self.system_prompt is None:
            return handler(request)
        return handler(request.override(system_message=self._system_message(request)))

    async def awrap_model_call(
        self,
        request: ModelRequest[ContextT],
        handler: Callable[..., Any],
    ) -> Any:
        if self.system_prompt is None:
            return await handler(request)
        return await handler(request.override(system_message=self._system_message(request)))

    def _system_message(self, request: ModelRequest[ContextT]) -> Any:
        return append_to_system_message(request.system_message, self.system_prompt)


def _normalize_workspace_root(value: str) -> str:
    raw = (value or "/").strip()
    if raw != "/":
        raw = raw.rstrip("/")
    root = PurePosixPath(raw)
    if not root.is_absolute() or ".." in root.parts:
        return "/"
    return str(root)


def _task_id_from_config(config: dict[str, Any] | None) -> str | None:
    if not isinstance(config, dict):
        return None
    for key in ("configurable", "metadata"):
        values = config.get(key)
        if not isinstance(values, dict):
            continue
        task_id = values.get("task_id")
        if isinstance(task_id, str) and task_id:
            return task_id
    return None


def _runtime_config(runtime: ToolRuntime | None) -> dict[str, Any] | None:
    config = getattr(runtime, "config", None) if runtime is not None else None
    if isinstance(config, dict):
        return config
    try:
        current = get_config()
    except RuntimeError:
        return None
    return current if isinstance(current, dict) else None


def _safe_filename(name: str | None) -> str | None:
    if not isinstance(name, str) or not name.strip():
        return None
    filename = PurePosixPath(name.replace("\\", "/")).name.strip()
    if filename in {"", ".", ".."}:
        return None
    return filename


def _guess_content_type(filename: str) -> str:
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or "application/octet-stream"


def _path_hint() -> str:
    return (
        "Use a backend workspace path such as /tetris.html. Do not use host "
        "paths like D:\\Code\\file.txt or include a workspace folder name unless "
        "that folder exists inside the backend workspace."
    )


def _json_error(
    error: str,
    *,
    path: str,
    hint: str,
    details: dict[str, Any] | None = None,
) -> str:
    payload: dict[str, Any] = {
        "ok": False,
        "error": error,
        "path": path,
        "hint": hint,
    }
    if details:
        payload["details"] = details
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)
