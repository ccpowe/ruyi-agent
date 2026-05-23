from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import PurePosixPath
from typing import Any, NotRequired, TypedDict

import yaml
from yaml import YAMLError
from langchain.agents.middleware.types import AgentMiddleware, AgentState, ModelRequest
from langchain_core.runnables import RunnableConfig
from langgraph.runtime import Runtime

from deepagents.backends.protocol import BackendProtocol, LsResult
from deepagents.middleware._utils import append_to_system_message


class RuyiSkillMetadata(TypedDict):
    name: str
    description: str
    path: str
    allowed_tools: list[str]


class RuyiSkillsState(AgentState):
    skills_metadata: NotRequired[list[RuyiSkillMetadata]]
    ruyi_skills_view_hash: NotRequired[str | None]


class RuyiSkillsStateUpdate(TypedDict):
    skills_metadata: list[RuyiSkillMetadata]
    ruyi_skills_view_hash: str | None


class RuyiSkillsMiddleware(AgentMiddleware[RuyiSkillsState, Any, Any]):
    """Expose the task's pre-materialized skill view to the model."""

    state_schema = RuyiSkillsState

    def __init__(self, *, backend: BackendProtocol) -> None:
        self._backend = backend

    def before_agent(
        self,
        state: RuyiSkillsState,
        runtime: Runtime,
        config: RunnableConfig,
    ) -> RuyiSkillsStateUpdate | None:
        view_path = _config_string(config, "skill_view_path")
        view_hash = _config_string(config, "skill_view_hash")
        if not view_path:
            if state.get("ruyi_skills_view_hash") is None and "skills_metadata" in state:
                return None
            return {"ruyi_skills_view_hash": None, "skills_metadata": []}
        if state.get("ruyi_skills_view_hash") == view_hash and "skills_metadata" in state:
            return None
        return {
            "ruyi_skills_view_hash": view_hash,
            "skills_metadata": _list_view_skills(self._backend, view_path),
        }

    async def abefore_agent(
        self,
        state: RuyiSkillsState,
        runtime: Runtime,
        config: RunnableConfig,
    ) -> RuyiSkillsStateUpdate | None:
        return self.before_agent(state, runtime, config)

    def modify_request(self, request: ModelRequest[Any]) -> ModelRequest[Any]:
        skills = request.state.get("skills_metadata", [])
        if not skills:
            return request
        lines = [
            "## Skills System",
            "",
            (
                "You have access to these task-scoped skills. "
                "Read a skill's `SKILL.md` before using it."
            ),
            "",
            "**Available Skills:**",
        ]
        for skill in skills:
            lines.append(f"- **{skill['name']}**: {skill['description']}")
            lines.append(f"  -> Read `{skill['path']}` for full instructions")
        new_system_message = append_to_system_message(
            request.system_message,
            "\n".join(lines),
        )
        return request.override(system_message=new_system_message)

    def wrap_model_call(self, request: ModelRequest[Any], handler: Callable[..., Any]) -> Any:
        return handler(self.modify_request(request))

    async def awrap_model_call(
        self,
        request: ModelRequest[Any],
        handler: Callable[[ModelRequest[Any]], Awaitable[Any]],
    ) -> Any:
        return await handler(self.modify_request(request))


def _config_string(config: RunnableConfig, key: str) -> str | None:
    configurable = config.get("configurable")
    if not isinstance(configurable, dict):
        return None
    value = configurable.get(key)
    return value if isinstance(value, str) and value else None


def _list_view_skills(
    backend: BackendProtocol,
    view_path: str,
) -> list[RuyiSkillMetadata]:
    ls_result = backend.ls(view_path)
    items = ls_result.entries if isinstance(ls_result, LsResult) else ls_result
    skill_dirs = [
        item["path"]
        for item in items or []
        if item.get("is_dir") and isinstance(item.get("path"), str)
    ]
    skill_md_paths = [str(PurePosixPath(path) / "SKILL.md") for path in skill_dirs]
    responses = backend.download_files(skill_md_paths)
    skills: list[RuyiSkillMetadata] = []
    for path, response in zip(skill_md_paths, responses, strict=True):
        if getattr(response, "error", None) or getattr(response, "content", None) is None:
            continue
        content = response.content.decode("utf-8")
        metadata = _parse_skill_metadata(content)
        name = metadata.get("name")
        description = metadata.get("description")
        if not isinstance(name, str) or not isinstance(description, str):
            continue
        skills.append(
            {
                "name": name.strip(),
                "description": description.strip(),
                "path": path,
                "allowed_tools": _parse_allowed_tools(metadata.get("allowed-tools")),
            }
        )
    return skills


def _parse_skill_metadata(content: str) -> dict[str, Any]:
    if not content.startswith("---\n"):
        return {}
    parts = content.split("---", 2)
    if len(parts) < 3:
        return {}
    _prefix, frontmatter, _body = parts
    try:
        raw = yaml.safe_load(frontmatter)
    except YAMLError:
        return {}
    return raw if isinstance(raw, dict) else {}


def _parse_allowed_tools(raw: Any) -> list[str]:
    if not isinstance(raw, str):
        return []
    return [tool.strip(",") for tool in raw.split() if tool.strip(",")]
