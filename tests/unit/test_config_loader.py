from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from langchain_core.messages import AIMessage, HumanMessage

import ruyi_agent.config.loader as config_loader


class FakeRegistry:
    def __init__(self, tools: list[object]) -> None:
        # 为什么用假的 registry：这里只验证配置解释，不依赖真实 MCP 拉取过程。
        self._tools = tools
        self.calls: list[dict[str, list[str]]] = []

    async def resolve_tools(
        self,
        *,
        server_names: list[str] | None = None,
        tool_names: list[str] | None = None,
    ) -> list[object]:
        # 为什么记录调用参数：需要确认配置里的 server_names 和 tool_names 被原样转给 registry。
        self.calls.append(
            {
                "server_names": list(server_names or []),
                "tool_names": list(tool_names or []),
            }
        )
        return self._tools


def test_load_toml_config_reads_generic_toml(tmp_path: Path) -> None:
    # 为什么测通用读取函数：所有配置入口都建立在它之上，出错会影响整个装配链路。
    config_path = tmp_path / "sample.toml"
    config_path.write_text('name = "demo"\n', encoding="utf-8")

    data = config_loader.load_toml_config(config_path)

    assert data == {"name": "demo"}


def test_load_toml_config_reports_invalid_toml_path(tmp_path: Path) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        "\n".join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                "model = qwen/qwen3.6-plus",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError) as exc_info:
        config_loader.load_toml_config(config_path)

    message = str(exc_info.value)
    assert str(config_path) in message
    assert "Invalid TOML" in message
    assert "quote string values" in message


def test_load_mcp_server_configs_returns_mcp_section(tmp_path: Path) -> None:
    # 为什么测 MCP 配置读取：确保 registry 初始化拿到的是纯 server 配置而不是整个 TOML。
    config_path = tmp_path / "mcp_servers.toml"
    config_path.write_text(
        '[mcp_servers.exa]\ntransport = "http"\nurl = "https://exa.invalid/mcp"\n',
        encoding="utf-8",
    )

    configs = config_loader.load_mcp_server_configs(config_path)

    assert configs == {
        "exa": {"transport": "http", "url": "https://exa.invalid/mcp"}
    }


def test_default_config_loaders_read_from_ruyi_config_dir(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    config_dir = project / ".ruyi_agent" / "config"
    config_dir.mkdir(parents=True)
    (config_dir / "mcp_servers.toml").write_text(
        '[mcp_servers.local]\ntransport = "stdio"\ncommand = "local-mcp"\n',
        encoding="utf-8",
    )
    monkeypatch.chdir(project)

    configs = config_loader.load_mcp_server_configs()

    assert configs == {
        "local": {"transport": "stdio", "command": "local-mcp"}
    }


def test_load_agent_configs_returns_main_agent_and_agents_section(tmp_path: Path) -> None:
    # 为什么测 agent 配置读取：运行时需要同时知道入口 main agent 和完整 agent 映射。
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                "workers = []",
            ]
        ),
        encoding="utf-8",
    )

    main_agent, agents = config_loader.load_agent_configs(config_path)

    assert main_agent == "main"
    assert agents["main"]["kind"] == "local"


def test_load_permission_config_parses_profiles(tmp_path: Path) -> None:
    config_path = tmp_path / "permissions.toml"
    config_path.write_text(
        '\n'.join(
            [
                'default_profile = "standard"',
                "",
                "[profiles.standard.tools.read_file]",
                'policy = "allow"',
                "",
                "[profiles.standard.tools.execute]",
                'policy = "require_approval"',
                'allowed_decisions = ["approve", "edit", "reject"]',
                "",
                "[[profiles.standard.execute.rules]]",
                'match = ["git", "status"]',
                'policy = "allow"',
            ]
        ),
        encoding="utf-8",
    )

    permissions = config_loader.load_permission_config(config_path)

    assert permissions.default_profile == "standard"
    profile = permissions.profiles["standard"]
    assert profile.tools["read_file"].policy == "allow"
    assert profile.tools["execute"].allowed_decisions == [
        "approve",
        "edit",
        "reject",
    ]
    assert profile.execute_rules[0].match == ["git", "status"]


def test_to_backend_paths_prefixes_each_relative_path() -> None:
    # 为什么测路径映射：memory 和 skills 都依赖这个规则进入 backend 路径空间。
    paths = config_loader.to_backend_paths(["AGENTS.md", "frontend-skill"], "/home/dev")

    assert paths == ["/home/dev/AGENTS.md", "/home/dev/frontend-skill"]


def test_to_backend_paths_handles_backend_root() -> None:
    paths = config_loader.to_backend_paths(["data/AGENTS.md", "data/skills"], "/")

    assert paths == ["/data/AGENTS.md", "/data/skills"]


def test_build_chat_model_from_config_builds_openai_compatible_model(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    built_model = object()

    def fake_init_chat_model(model: str, *, model_provider: str, **kwargs: object):
        calls.append(
            {
                "model": model,
                "model_provider": model_provider,
                "kwargs": kwargs,
            }
        )
        return built_model

    monkeypatch.setattr(config_loader, "init_chat_model", fake_init_chat_model)

    providers = {
        "deepseek": config_loader.LLMProviderSpec(
            name="deepseek",
            kind="openai",
            base_url="https://api.deepseek.com",
            api_key_env="DEEPSEEK_API_KEY",
        )
    }

    resolved = config_loader.build_chat_model_from_config(
        {
            "provider": "deepseek",
            "model": "deepseek-chat",
        },
        providers=providers,
        getenv=lambda name: {"DEEPSEEK_API_KEY": "deepseek-key"}.get(name),
    )

    assert resolved is built_model
    assert calls == [
        {
            "model": "deepseek-chat",
            "model_provider": "openai",
            "kwargs": {
                "api_key": "deepseek-key",
                "base_url": "https://api.deepseek.com",
            },
        }
    ]


def test_build_chat_model_from_config_builds_moonshot_model(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    built_model = object()

    class FakeChatMoonshot:
        def __new__(cls, **kwargs: object):
            calls.append(kwargs)
            return built_model

    monkeypatch.setattr(
        config_loader,
        "_get_chat_moonshot_class",
        lambda: FakeChatMoonshot,
    )

    providers = {
        "kimi": config_loader.LLMProviderSpec(
            name="kimi",
            kind="moonshot",
            base_url="https://api.moonshot.cn/v1",
            api_key_env="KIMI_API_KEY",
            init_kwargs={"thinking": True},
        )
    }

    resolved = config_loader.build_chat_model_from_config(
        {
            "provider": "kimi",
            "model": "kimi-k2.6",
        },
        providers=providers,
        getenv=lambda name: {"KIMI_API_KEY": "kimi-key"}.get(name),
    )

    assert resolved is built_model
    assert calls == [
        {
            "model": "kimi-k2.6",
            "thinking": True,
            "api_key": "kimi-key",
            "base_url": "https://api.moonshot.cn/v1",
        }
    ]


def test_build_chat_model_from_config_builds_deepseek_model_with_reasoning_echo(
    monkeypatch,
) -> None:
    calls: list[dict[str, object]] = []

    class FakeChatDeepSeek:
        def __init__(self, **kwargs: object) -> None:
            calls.append(kwargs)

        def _get_request_payload(
            self,
            input_: object,
            **_kwargs: object,
        ) -> dict[str, object]:
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

    monkeypatch.setattr(
        config_loader,
        "_get_chat_deepseek_class",
        lambda: FakeChatDeepSeek,
    )

    providers = {
        "deepseek": config_loader.LLMProviderSpec(
            name="deepseek",
            kind="deepseek",
            base_url="https://api.deepseek.com",
            api_key_env="DEEPSEEK_API_KEY",
        )
    }
    model = config_loader.build_chat_model_from_config(
        {
            "provider": "deepseek",
            "model": "deepseek-v4-pro",
        },
        providers=providers,
        getenv=lambda name: {"DEEPSEEK_API_KEY": "deepseek-key"}.get(name),
    )

    payload = model._get_request_payload(
        [
            HumanMessage(content="use tool"),
            AIMessage(
                content="",
                additional_kwargs={"reasoning_content": "must echo"},
                tool_calls=[{"name": "lookup", "args": {}, "id": "call_1"}],
            ),
        ]
    )

    assert calls == [
        {
            "model": "deepseek-v4-pro",
            "api_key": "deepseek-key",
            "base_url": "https://api.deepseek.com",
        }
    ]
    assert payload["messages"][1]["reasoning_content"] == "must echo"


def test_build_chat_model_from_config_builds_litellm_model(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    built_model = object()

    class FakeChatLiteLLM:
        def __new__(cls, **kwargs: object):
            calls.append(kwargs)
            return built_model

    monkeypatch.setattr(
        config_loader,
        "_get_chat_litellm_class",
        lambda: FakeChatLiteLLM,
    )

    providers = {
        "zai": config_loader.LLMProviderSpec(
            name="zai",
            kind="litellm",
            base_url="https://open.bigmodel.cn/api/paas/v4/",
            api_key_env="ZAI_API_KEY",
            init_kwargs={"request_timeout": 60},
        )
    }

    resolved = config_loader.build_chat_model_from_config(
        {
            "provider": "zai",
            "model": "zai/glm-5.1",
        },
        providers=providers,
        getenv=lambda name: {"ZAI_API_KEY": "zai-key"}.get(name),
    )

    assert resolved is built_model
    assert calls == [
        {
            "model": "zai/glm-5.1",
            "request_timeout": 60,
            "api_key": "zai-key",
            "api_base": "https://open.bigmodel.cn/api/paas/v4/",
        }
    ]


def test_build_chat_model_from_config_builds_openai_codex_model(monkeypatch) -> None:
    calls: list[dict[str, object]] = []
    built_model = object()

    def fake_build_openai_codex_model(model: str, **kwargs: object):
        calls.append({"model": model, "kwargs": kwargs})
        return built_model

    monkeypatch.setattr(
        config_loader,
        "_build_openai_codex_model",
        fake_build_openai_codex_model,
    )

    providers = {
        "codex": config_loader.LLMProviderSpec(
            name="codex",
            kind="openai_codex",
            base_url="https://chatgpt.com/backend-api/codex",
            init_kwargs={"auth_json": "~/.ruyi_agent/openai_codex_auth.json"},
        )
    }

    resolved = config_loader.build_chat_model_from_config(
        {
            "provider": "codex",
            "model": "gpt-5.3-codex",
        },
        providers=providers,
        getenv=lambda _name: None,
    )

    assert resolved is built_model
    assert calls == [
        {
            "model": "gpt-5.3-codex",
            "kwargs": {
                "auth_json": "~/.ruyi_agent/openai_codex_auth.json",
                "base_url": "https://chatgpt.com/backend-api/codex",
            },
        }
    ]


@pytest.mark.parametrize(
    ("provider_kind", "init_kwargs", "expected_key"),
    [
        ("moonshot", {"model": "other-model"}, "model"),
        ("openrouter", {"model_provider": "openai"}, "model_provider"),
    ],
)
def test_build_chat_model_from_config_rejects_reserved_provider_init_kwargs(
    provider_kind: str,
    init_kwargs: dict[str, object],
    expected_key: str,
) -> None:
    providers = {
        "provider": config_loader.LLMProviderSpec(
            name="provider",
            kind=provider_kind,
            api_key_env=None,
            init_kwargs=init_kwargs,
        )
    }

    with pytest.raises(ValueError, match=f"reserved keys: {expected_key}"):
        config_loader.build_chat_model_from_config(
            {
                "provider": "provider",
                "model": "some-model",
            },
            providers=providers,
            getenv=lambda _name: None,
        )


def test_build_chat_model_from_config_rejects_missing_configured_api_key_env() -> None:
    providers = {
        "deepseek": config_loader.LLMProviderSpec(
            name="deepseek",
            kind="openai",
            api_key_env="DEEPSEEK_API_KEY",
        )
    }
    with pytest.raises(ValueError, match="DEEPSEEK_API_KEY"):
        config_loader.build_chat_model_from_config(
            {
                "provider": "deepseek",
                "model": "deepseek-chat",
            },
            providers=providers,
            getenv=lambda _name: None,
        )


def test_build_chat_model_from_config_rejects_unknown_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider"):
        config_loader.build_chat_model_from_config(
            {"provider": "unknown", "model": "some-model"},
            providers={},
            getenv=lambda _name: None,
        )


def test_load_llm_provider_configs_parses_providers(tmp_path: Path) -> None:
    config_path = tmp_path / "llm_providers.toml"
    config_path.write_text(
        '\n'.join(
            [
                "[providers.deepseek]",
                'kind = "deepseek"',
                'base_url = "https://api.deepseek.com"',
                'api_key_env = "DEEPSEEK_API_KEY"',
                "",
                "[providers.zai]",
                'kind = "litellm"',
                'base_url = "https://open.bigmodel.cn/api/paas/v4/"',
                'api_key_env = "ZAI_API_KEY"',
                "",
                "[providers.openrouter]",
                'kind = "openrouter"',
                'api_key_env = "OPENROUTER_API_KEY"',
                "",
                "[providers.kimi]",
                'kind = "moonshot"',
                'base_url = "https://api.moonshot.cn/v1"',
                'api_key_env = "KIMI_API_KEY"',
                "",
                "[providers.kimi.init_kwargs]",
                "thinking = true",
                "",
                "[providers.codex]",
                'kind = "openai_codex"',
                'base_url = "https://chatgpt.com/backend-api/codex"',
                "",
                "[providers.codex.init_kwargs]",
                'auth_json = "~/.ruyi_agent/openai_codex_auth.json"',
            ]
        ),
        encoding="utf-8",
    )

    providers = config_loader.load_llm_provider_configs(config_path)

    assert providers["deepseek"].kind == "deepseek"
    assert providers["deepseek"].base_url == "https://api.deepseek.com"
    assert providers["deepseek"].api_key_env == "DEEPSEEK_API_KEY"
    assert providers["zai"].kind == "litellm"
    assert providers["zai"].base_url == "https://open.bigmodel.cn/api/paas/v4/"
    assert providers["zai"].api_key_env == "ZAI_API_KEY"
    assert providers["openrouter"].kind == "openrouter"
    assert providers["kimi"].kind == "moonshot"
    assert providers["kimi"].init_kwargs == {"thinking": True}
    assert providers["codex"].kind == "openai_codex"
    assert providers["codex"].api_key_env is None
    assert providers["codex"].init_kwargs == {
        "auth_json": "~/.ruyi_agent/openai_codex_auth.json"
    }


def test_load_llm_provider_configs_rejects_unexpected_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "llm_providers.toml"
    config_path.write_text(
        '\n'.join(
            [
                "[providers.deepseek]",
                'kind = "openai"',
                'base_url = "https://api.deepseek.com"',
                'api_key_env = "DEEPSEEK_API_KEY"',
                'api_key_envv = "DEEPSEEK_API_KEY"',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unexpected fields: api_key_envv"):
        config_loader.load_llm_provider_configs(config_path)


def test_load_llm_provider_configs_rejects_invalid_init_kwargs_type(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "llm_providers.toml"
    config_path.write_text(
        '\n'.join(
            [
                "[providers.kimi]",
                'kind = "moonshot"',
                'api_key_env = "KIMI_API_KEY"',
                "init_kwargs = 1",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="providers.kimi.init_kwargs must be a table"):
        config_loader.load_llm_provider_configs(config_path)


def test_load_llm_provider_configs_rejects_reserved_init_kwargs(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "llm_providers.toml"
    config_path.write_text(
        '\n'.join(
            [
                "[providers.kimi]",
                'kind = "moonshot"',
                'api_key_env = "KIMI_API_KEY"',
                "",
                "[providers.kimi.init_kwargs]",
                'model = "other-model"',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="reserved keys: model"):
        config_loader.load_llm_provider_configs(config_path)


def test_load_llm_provider_configs_requires_api_key_env(tmp_path: Path) -> None:
    config_path = tmp_path / "llm_providers.toml"
    config_path.write_text(
        '\n'.join(
            [
                "[providers.openrouter]",
                'kind = "openrouter"',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="api_key_env must be a non-empty string"):
        config_loader.load_llm_provider_configs(config_path)


def test_load_agent_configs_rejects_legacy_model_fields(tmp_path: Path) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                'model_provider = "openrouter"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                "workers = []",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unexpected fields: model_provider"):
        config_loader.load_agent_configs(config_path)


def test_build_local_worker_spec_resolves_tools_and_keeps_skill_names(
    monkeypatch,
) -> None:
    # 为什么测本地 worker 构造：这是当前把配置转成可执行 worker 定义的关键路径。
    built_model = object()
    monkeypatch.setattr(config_loader, "init_chat_model", lambda *_a, **_k: built_model)
    fake_tools = [object(), object()]
    registry = FakeRegistry(fake_tools)
    agent_configs = {
        "repo_wiki": {
            "kind": "local",
            "public": False,
            "name": "repo_wiki",
            "description": "repo helper",
            "system_prompt": "prompt",
            "provider": "openrouter",
            "model": "deepseek/deepseek-v4-flash",
            "memory": [],
            "skills": ["frontend-skill"],
            "server_names": ["deepwiki"],
            "tool_names": ["exa.web_search_exa"],
            "workers": [],
            "permission_profile": "standard",
        }
    }

    worker = asyncio.run(
        config_loader.build_local_worker_spec(
            "repo_wiki",
            agent_configs,
            registry,
            home_dir="/sandbox/home",
            providers={
                "openrouter": config_loader.LLMProviderSpec(
                    name="openrouter",
                    kind="openrouter",
                    api_key_env=None,
                )
            },
            getenv=lambda _name: None,
            skills_root="/sandbox/skills",
        )
    )

    assert registry.calls == [
        {
            "server_names": ["deepwiki"],
            "tool_names": ["exa.web_search_exa"],
        }
    ]
    assert worker.name == "repo_wiki"
    assert worker.description == "repo helper"
    assert worker.system_prompt == "prompt"
    assert worker.model is built_model
    assert worker.tools == fake_tools
    assert worker.skills == ["frontend-skill"]
    assert worker.permission_profile == "standard"


def test_build_local_worker_spec_keeps_special_skill_modes(monkeypatch) -> None:
    built_model = object()
    monkeypatch.setattr(config_loader, "init_chat_model", lambda *_a, **_k: built_model)
    registry = FakeRegistry([])
    base_config = {
        "kind": "local",
        "public": False,
        "name": "repo_wiki",
        "description": "repo helper",
        "system_prompt": "prompt",
        "provider": "openrouter",
        "model": "deepseek/deepseek-v4-flash",
        "memory": [],
        "server_names": [],
        "tool_names": [],
        "workers": [],
    }

    specs = {}
    for mode in ("inherit", "none"):
        agent_configs = {"repo_wiki": {**base_config, "skills": mode}}
        specs[mode] = asyncio.run(
            config_loader.build_local_worker_spec(
                "repo_wiki",
                agent_configs,
                registry,
                home_dir="/sandbox/home",
                providers={
                    "openrouter": config_loader.LLMProviderSpec(
                        name="openrouter",
                        kind="openrouter",
                        api_key_env=None,
                    )
                },
                getenv=lambda _name: None,
                skills_root="/sandbox/skills",
            )
        )

    assert specs["inherit"].skills == "inherit"
    assert specs["none"].skills == "none"

def test_build_local_worker_spec_rejects_non_worker_kind() -> None:
    # 为什么测 kind 校验：避免错误配置在更深层才暴露成难排查的问题。
    registry = FakeRegistry([])

    with pytest.raises(ValueError, match="not a local agent"):
        asyncio.run(
            config_loader.build_local_worker_spec(
                "main",
                {
                    "main": {
                        "kind": "remote_ref",
                        "public": False,
                        "name": "main",
                        "description": "desc",
                        "url": "https://example.com/a2a",
                        "remote_agent_name": "main",
                    }
                },
                registry,
                providers={},
                getenv=lambda _name: None,
                home_dir="/sandbox/home",
                skills_root="/sandbox/skills",
            )
        )


def test_build_remote_ref_returns_remote_spec() -> None:
    # 为什么测远端引用构造：remote_ref 只有连接信息，必须确认配置能正确映射到引用结构。
    agent_configs = {
        "remote_code_wiki": {
            "kind": "remote_ref",
            "public": False,
            "name": "remote_code_wiki",
            "description": "remote helper",
            "url": "https://example.com/a2a",
            "remote_agent_name": "code_wiki",
            "auth": {"type": "bearer", "token_env": "REMOTE_CODE_WIKI_TOKEN"},
        }
    }

    remote_ref = config_loader.build_remote_ref("remote_code_wiki", agent_configs)

    assert remote_ref.name == "remote_code_wiki"
    assert remote_ref.description == "remote helper"
    assert remote_ref.url == "https://example.com/a2a"
    assert remote_ref.remote_agent_name == "code_wiki"
    assert remote_ref.auth == {
        "type": "bearer",
        "token_env": "REMOTE_CODE_WIKI_TOKEN",
    }


def test_build_local_worker_spec_resolves_tools_and_backend_paths(monkeypatch) -> None:
    # 为什么测本地 worker 规格：当前本地 worker 的实现依赖这一步把配置变成可执行定义。
    built_model = object()
    monkeypatch.setattr(config_loader, "init_chat_model", lambda *_a, **_k: built_model)
    fake_tools = [object()]
    registry = FakeRegistry(fake_tools)
    agent_configs = {
        "background_research": {
            "kind": "local",
            "public": False,
            "name": "background_research",
            "description": "background helper",
            "system_prompt": "prompt",
            "provider": "openrouter",
            "model": "deepseek/deepseek-v4-flash",
            "memory": ["AGENTS.md"],
            "skills": ["frontend-skill"],
            "server_names": ["exa"],
            "tool_names": ["deepwiki.ask_question"],
        }
    }

    spec = asyncio.run(
        config_loader.build_local_worker_spec(
            "background_research",
            agent_configs,
            registry,
            providers={
                "openrouter": config_loader.LLMProviderSpec(
                    name="openrouter",
                    kind="openrouter",
                    api_key_env=None,
                )
            },
            getenv=lambda _name: None,
            home_dir="/sandbox/home",
            skills_root="/sandbox/skills",
        )
    )

    assert registry.calls == [
        {
            "server_names": ["exa"],
            "tool_names": ["deepwiki.ask_question"],
        }
    ]
    assert spec.name == "background_research"
    assert spec.description == "background helper"
    assert spec.system_prompt == "prompt"
    assert spec.model is built_model
    assert spec.tools == fake_tools
    assert spec.memory == ["/sandbox/home/AGENTS.md"]
    assert spec.skills == ["frontend-skill"]


def test_build_all_local_worker_specs_builds_every_local_agent(monkeypatch) -> None:
    monkeypatch.setattr(config_loader, "init_chat_model", lambda *_a, **_k: object())
    registry = FakeRegistry([object()])
    agent_configs = {
        "main": {
            "kind": "local",
            "public": True,
            "name": "main",
            "description": "main agent",
            "system_prompt": "prompt",
            "provider": "openrouter",
            "model": "deepseek/deepseek-v4-flash",
            "memory": [],
            "skills": [],
            "server_names": [],
            "tool_names": [],
            "workers": ["worker"],
        },
        "worker": {
            "kind": "local",
            "public": False,
            "name": "worker",
            "description": "worker agent",
            "system_prompt": "worker prompt",
            "provider": "openrouter",
            "model": "deepseek/deepseek-v4-flash",
            "memory": [],
            "skills": [],
            "server_names": [],
            "tool_names": [],
            "workers": [],
        },
        "remote_code_wiki": {
            "kind": "remote_ref",
            "public": True,
            "name": "remote_code_wiki",
            "description": "remote helper",
            "url": "https://example.com/a2a",
            "remote_agent_name": "code_wiki",
        },
    }

    specs = asyncio.run(
        config_loader.build_all_local_worker_specs(
            agent_configs,
            registry,
            providers={
                "openrouter": config_loader.LLMProviderSpec(
                    name="openrouter",
                    kind="openrouter",
                    api_key_env=None,
                )
            },
            getenv=lambda _name: None,
            home_dir="/sandbox/home",
            skills_root="/sandbox/skills",
        )
    )

    assert sorted(specs) == ["main", "worker"]
    assert specs["main"].delegation_local_worker_specs is None
    assert specs["worker"].delegation_local_worker_specs is None


def test_build_all_and_public_remote_refs_use_distinct_scopes() -> None:
    agent_configs = {
        "main": {
            "kind": "local",
            "public": True,
            "name": "main",
            "description": "main agent",
            "system_prompt": "prompt",
            "provider": "openrouter",
            "model": "deepseek/deepseek-v4-flash",
            "memory": [],
            "skills": [],
            "server_names": [],
            "tool_names": [],
            "workers": ["private_remote"],
        },
        "private_remote": {
            "kind": "remote_ref",
            "public": False,
            "name": "private_remote",
            "description": "private remote",
            "url": "https://example.com/private",
            "remote_agent_name": "private",
        },
        "public_remote": {
            "kind": "remote_ref",
            "public": True,
            "name": "public_remote",
            "description": "public remote",
            "url": "https://example.com/public",
            "remote_agent_name": "public",
        },
    }

    all_refs = asyncio.run(config_loader.build_all_remote_refs(agent_configs))
    public_refs = asyncio.run(config_loader.build_public_remote_refs(agent_configs))
    main_refs = config_loader.select_remote_refs_for_agent(
        "main",
        agent_configs,
        all_refs,
    )

    assert sorted(all_refs) == ["private_remote", "public_remote"]
    assert list(public_refs) == ["public_remote"]
    assert list(main_refs) == ["private_remote"]


def test_select_agent_worker_scopes_are_per_agent() -> None:
    local_specs = {
        "main": config_loader.LocalWorkerSpec(
            name="main",
            description="main",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=[],
        ),
        "worker": config_loader.LocalWorkerSpec(
            name="worker",
            description="worker",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=[],
        ),
        "checker": config_loader.LocalWorkerSpec(
            name="checker",
            description="checker",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=[],
        ),
    }
    remote_refs = {
        "remote": config_loader.RemoteRef(
            name="remote",
            description="remote",
            url="https://example.com/a2a",
            remote_agent_name="remote",
        )
    }
    agent_configs = {
        "main": {"kind": "local", "workers": ["worker", "remote"]},
        "worker": {"kind": "local", "workers": ["checker"]},
        "checker": {"kind": "local", "workers": []},
        "remote": {"kind": "remote_ref"},
    }

    assert sorted(
        config_loader.select_local_worker_specs_for_agent(
            "main", agent_configs, local_specs
        )
    ) == ["worker"]
    assert sorted(
        config_loader.select_remote_refs_for_agent(
            "main", agent_configs, remote_refs
        )
    ) == ["remote"]
    assert sorted(
        config_loader.select_local_worker_specs_for_agent(
            "worker", agent_configs, local_specs
        )
    ) == ["checker"]


def test_load_agent_configs_rejects_remote_ref_with_local_only_fields(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["remote_code_wiki"]',
                "",
                "[agents.remote_code_wiki]",
                'kind = "remote_ref"',
                "public = false",
                'name = "remote_code_wiki"',
                'description = "remote helper"',
                'url = "https://example.com/a2a"',
                'remote_agent_name = "code_wiki"',
                "workers = []",
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unexpected fields: workers"):
        config_loader.load_agent_configs(config_path)


def test_load_agent_configs_requires_main_agent_to_point_to_local_agent(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "remote_code_wiki"',
                "",
                "[agents.assistant]",
                'kind = "local"',
                "public = true",
                'name = "assistant"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                "workers = []",
                "",
                "[agents.remote_code_wiki]",
                'kind = "remote_ref"',
                "public = false",
                'name = "remote_code_wiki"',
                'description = "remote helper"',
                'url = "https://example.com/a2a"',
                'remote_agent_name = "code_wiki"',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="must reference an agent with kind='local'"):
        config_loader.load_agent_configs(config_path)


def test_load_agent_configs_rejects_self_worker(tmp_path: Path) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["main"]',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="cannot list itself in workers"):
        config_loader.load_agent_configs(config_path)


def test_load_agent_configs_rejects_local_worker_cycle(tmp_path: Path) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["worker"]',
                "",
                "[agents.worker]",
                'kind = "local"',
                "public = false",
                'name = "worker"',
                'description = "worker"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["main"]',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="Local worker graph contains a cycle"):
        config_loader.load_agent_configs(config_path)


def test_load_agent_configs_allows_local_to_remote_ref_worker(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["remote_wiki"]',
                "",
                "[agents.remote_wiki]",
                'kind = "remote_ref"',
                "public = false",
                'name = "remote_wiki"',
                'description = "remote helper"',
                'url = "https://example.com/a2a"',
                'remote_agent_name = "wiki"',
            ]
        ),
        encoding="utf-8",
    )

    main_agent_name, agent_configs = config_loader.load_agent_configs(config_path)

    assert main_agent_name == "main"
    assert agent_configs["remote_wiki"]["kind"] == "remote_ref"


def test_load_agent_configs_rejects_longer_local_worker_cycle(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        '\n'.join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "desc"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["research"]',
                "",
                "[agents.research]",
                'kind = "local"',
                "public = false",
                'name = "research"',
                'description = "research"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["checker"]',
                "",
                "[agents.checker]",
                'kind = "local"',
                "public = false",
                'name = "checker"',
                'description = "checker"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "deepseek/deepseek-v4-flash"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["research"]',
            ]
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="research -> checker -> research"):
        config_loader.load_agent_configs(config_path)
