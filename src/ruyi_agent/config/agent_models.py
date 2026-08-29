"""Typed agent configuration produced at the TOML boundary."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias


SkillSelection: TypeAlias = Literal["inherit", "none"] | tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BearerAuthConfig:
    """Environment-backed bearer authentication for a remote Agent."""

    type: Literal["bearer"]
    token_env: str


@dataclass(frozen=True, slots=True)
class LocalAgentConfig:
    """A validated, locally executable Agent declaration."""

    name: str
    public: bool
    description: str
    system_prompt: str
    provider: str
    model: str
    memory: tuple[str, ...]
    skills: SkillSelection
    server_names: tuple[str, ...]
    tool_names: tuple[str, ...]
    workers: tuple[str, ...]
    permission_profile: str | None = None
    tool_search: bool = False
    system_tools: frozenset[str] = field(default_factory=frozenset)
    disabled_system_tools: frozenset[str] = field(default_factory=frozenset)
    kind: Literal["local"] = field(default="local", init=False)

    def __getitem__(self, key: str) -> object:
        """Keep the former read-only catalog access while callers migrate.

        Runtime and config code uses attributes. This deliberately enumerated shim
        lets older catalog consumers upgrade without exposing the original raw TOML
        mapping or accepting arbitrary field names.
        """

        fields: dict[str, object] = {
            "kind": self.kind,
            "public": self.public,
            "name": self.name,
            "description": self.description,
            "system_prompt": self.system_prompt,
            "provider": self.provider,
            "model": self.model,
            "memory": self.memory,
            "skills": self.skills,
            "server_names": self.server_names,
            "tool_names": self.tool_names,
            "workers": self.workers,
            "permission_profile": self.permission_profile,
            "tool_search": self.tool_search,
            "system_tools": self.system_tools,
            "disabled_system_tools": self.disabled_system_tools,
        }
        try:
            return fields[key]
        except KeyError as exc:
            raise KeyError(key) from exc


@dataclass(frozen=True, slots=True)
class RemoteAgentConfig:
    """A validated reference to an Agent exposed by another Gateway."""

    name: str
    public: bool
    description: str
    url: str
    remote_agent_name: str
    auth: BearerAuthConfig | None = None
    kind: Literal["remote_ref"] = field(default="remote_ref", init=False)

    def __getitem__(self, key: str) -> object:
        """Provide the finite compatibility view used by old catalog consumers."""

        fields: dict[str, object] = {
            "kind": self.kind,
            "public": self.public,
            "name": self.name,
            "description": self.description,
            "url": self.url,
            "remote_agent_name": self.remote_agent_name,
            "auth": self.auth,
        }
        try:
            return fields[key]
        except KeyError as exc:
            raise KeyError(key) from exc


AgentConfig: TypeAlias = LocalAgentConfig | RemoteAgentConfig
AgentConfigs: TypeAlias = dict[str, AgentConfig]
