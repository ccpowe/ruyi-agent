from __future__ import annotations

import asyncio
from pathlib import Path

import httpx
import pytest

from ruyi_agent.config import ConfigError as PublicConfigError
from ruyi_agent.config import loader as config_loader
from ruyi_agent.config.errors import ConfigError
from ruyi_agent.config.loader import RemoteRef
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError


INVALID_HTTP_URLS = [
    "relative/path",
    "https://example.com:not-a-port/a2a",
    "https://example.com:/a2a",
    "https://example.com:65536/a2a",
    "https:///a2a",
    "https://bad host.example/a2a",
    "https://bad_host.example/a2a",
    "https://[::1/a2a",
    "https://999.999.999.999/a2a",
    "https://a\u200db.example/a2a",
    "https://☃.example/a2a",
    "https://\u0378.example/a2a",
]
VALID_HTTP_URLS = [
    "https://example.com/a2a",
    "https://例え.テスト/a2a",
    "http://127.0.0.1:8080/a2a",
    "http://[::1]:8080/a2a",
    "http://[fe80::1%25eth0]:8080/a2a",
]


def _write_agents_config(path: Path, *, remote_url: str) -> None:
    path.write_text(
        "\n".join(
            [
                'main_agent = "main"',
                "",
                "[agents.main]",
                'kind = "local"',
                "public = true",
                'name = "main"',
                'description = "main"',
                'system_prompt = "prompt"',
                'provider = "openrouter"',
                'model = "model"',
                "memory = []",
                "skills = []",
                "server_names = []",
                "tool_names = []",
                'workers = ["remote"]',
                "",
                "[agents.remote]",
                'kind = "remote_ref"',
                "public = true",
                'name = "remote"',
                'description = "remote"',
                f'url = "{remote_url}"',
                'remote_agent_name = "worker"',
            ]
        ),
        encoding="utf-8",
    )


def _write_provider_config(path: Path, *, base_url: str) -> None:
    path.write_text(
        "\n".join(
            [
                "[providers.openrouter]",
                'kind = "openai"',
                f'base_url = "{base_url}"',
                'api_key_env = "OPENROUTER_API_KEY"',
            ]
        ),
        encoding="utf-8",
    )


@pytest.mark.parametrize("remote_url", INVALID_HTTP_URLS)
def test_remote_agent_url_rejects_malformed_authorities(
    tmp_path: Path,
    remote_url: str,
) -> None:
    config_path = tmp_path / "agents.toml"
    _write_agents_config(config_path, remote_url=remote_url)

    with pytest.raises(ConfigError, match=r"absolute HTTP\(S\) URL"):
        config_loader.load_agent_configs(config_path)


@pytest.mark.parametrize("base_url", INVALID_HTTP_URLS)
def test_provider_base_url_rejects_malformed_authorities(
    tmp_path: Path,
    base_url: str,
) -> None:
    config_path = tmp_path / "llm_providers.toml"
    _write_provider_config(config_path, base_url=base_url)

    with pytest.raises(ConfigError, match=r"absolute HTTP\(S\) URL"):
        config_loader.load_llm_provider_configs(config_path)


@pytest.mark.parametrize("remote_url", VALID_HTTP_URLS)
def test_remote_agent_url_keeps_httpx_compatible_hosts(
    tmp_path: Path,
    remote_url: str,
) -> None:
    config_path = tmp_path / "agents.toml"
    _write_agents_config(config_path, remote_url=remote_url)

    _main_agent_name, agents = config_loader.load_agent_configs(config_path)

    assert agents["remote"].url == remote_url


@pytest.mark.parametrize("base_url", VALID_HTTP_URLS)
def test_provider_base_url_keeps_httpx_compatible_hosts(
    tmp_path: Path,
    base_url: str,
) -> None:
    config_path = tmp_path / "llm_providers.toml"
    _write_provider_config(config_path, base_url=base_url)

    providers = config_loader.load_llm_provider_configs(config_path)

    assert providers["openrouter"].base_url == base_url


@pytest.mark.parametrize(
    ("kind", "configured_url"),
    [
        ("agent", "https://user:secret@example.com/a2a"),
        ("provider", "https://user:secret@example.com/v1"),
    ],
)
def test_configured_http_urls_reject_credentials(
    tmp_path: Path,
    kind: str,
    configured_url: str,
) -> None:
    config_path = tmp_path / f"{kind}.toml"
    if kind == "agent":
        _write_agents_config(config_path, remote_url=configured_url)
        loader = config_loader.load_agent_configs
    else:
        _write_provider_config(config_path, base_url=configured_url)
        loader = config_loader.load_llm_provider_configs

    with pytest.raises(ConfigError, match="must not contain credentials"):
        loader(config_path)


def test_blank_codex_optional_url_remains_supported(tmp_path: Path) -> None:
    config_path = tmp_path / "llm_providers.toml"
    config_path.write_text(
        "\n".join(
            [
                "[providers.codex]",
                'kind = "openai_codex"',
                'base_url = ""',
                'api_key_env = ""',
            ]
        ),
        encoding="utf-8",
    )

    providers = config_loader.load_llm_provider_configs(config_path)

    assert providers["codex"].base_url is None
    assert providers["codex"].api_key_env is None


def test_bad_remote_url_stops_before_a2a_or_gateway_boundary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "agents.toml"
    _write_agents_config(
        config_path,
        remote_url="https://999.999.999.999/a2a",
    )
    reached_boundaries: list[str] = []

    def reached_a2a(*_args: object, **_kwargs: object) -> None:
        reached_boundaries.append("a2a")

    def reached_gateway(*_args: object, **_kwargs: object) -> None:
        reached_boundaries.append("gateway")

    monkeypatch.setattr(A2AClient, "_http_transport", reached_a2a)
    monkeypatch.setattr(GatewayTaskModule, "__init__", reached_gateway)

    with pytest.raises(ConfigError, match="Agent 'remote' field 'url'"):
        _main_agent_name, configs = config_loader.load_agent_configs(config_path)
        remote_ref = config_loader.build_remote_ref("remote", configs)
        A2AClient()._http_transport(remote_ref)  # pragma: no cover
        GatewayTaskModule(  # pragma: no cover
            main_agent_name="main",
            agent_configs=configs,
            control=object(),  # type: ignore[arg-type]
        )

    assert reached_boundaries == []
    assert config_loader.ConfigError is ConfigError
    assert PublicConfigError is ConfigError


def test_a2a_defensively_wraps_programmatic_invalid_url() -> None:
    remote_ref = RemoteRef(
        name="remote",
        description="remote",
        url="https://999.999.999.999/a2a",
        remote_agent_name="worker",
        auth=None,
    )

    with pytest.raises(A2AClientError, match="Remote gateway request failed") as exc:
        asyncio.run(A2AClient().get_task(remote_ref, task_id="task-1"))

    assert isinstance(exc.value.__cause__, httpx.InvalidURL)
