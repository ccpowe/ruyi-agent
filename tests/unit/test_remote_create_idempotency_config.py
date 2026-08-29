from __future__ import annotations

from pathlib import Path

import pytest

import ruyi_agent.config.agent_runtime as agent_runtime
import ruyi_agent.config.loader as config_loader


@pytest.mark.parametrize("toml_value", ["[]", "{}"])
def test_remote_create_idempotency_rejects_toml_non_string_values(
    tmp_path: Path,
    toml_value: str,
) -> None:
    config_path = tmp_path / "agents.toml"
    config_path.write_text(
        "\n".join(
            [
                "[agents.remote]",
                'kind = "remote_ref"',
                "public = false",
                'name = "remote"',
                'description = "remote helper"',
                'url = "https://example.com/a2a"',
                'remote_agent_name = "worker"',
                f"create_idempotency = {toml_value}",
            ]
        ),
        encoding="utf-8",
    )
    raw = config_loader.load_toml_config(config_path)

    with pytest.raises(ValueError, match="create_idempotency.*must be"):
        config_loader.coerce_agent_configs(raw["agents"])


@pytest.mark.parametrize("value", [[], {}])
def test_remote_ref_programmatic_create_idempotency_rejects_non_strings(
    value: object,
) -> None:
    with pytest.raises(ValueError, match="create_idempotency.*must be"):
        agent_runtime.RemoteRef(
            name="remote",
            description="remote helper",
            url="https://example.com/a2a",
            remote_agent_name="worker",
            create_idempotency=value,  # type: ignore[arg-type]
        )
