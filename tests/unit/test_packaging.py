from __future__ import annotations

import tomllib
from importlib.resources import files
from pathlib import Path


def test_pyproject_exposes_ruyi_console_script() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["project"]["scripts"]["ruyi"] == (
        "ruyi_agent.entrypoints.main:main"
    )


def test_pyproject_is_packaged_for_uv_tool_install() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    assert pyproject["build-system"]["build-backend"] == "setuptools.build_meta"


def test_pyproject_includes_ruyi_home_template_package_data() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    package_data = pyproject["tool"]["setuptools"]["package-data"]

    assert package_data["ruyi_agent.templates.ruyi_home"] == ["ruyi.toml"]
    assert package_data["ruyi_agent.templates.ruyi_home.config"] == [
        "*.toml",
        "*.toml.example",
    ]


def test_ruyi_home_templates_are_non_empty() -> None:
    template_root = files("ruyi_agent.templates.ruyi_home")
    expected_templates = [
        "ruyi.toml",
        "config/agents.toml",
        "config/agents.toml.example",
        "config/llm_providers.toml",
        "config/mcp_servers.toml",
        "config/permissions.toml",
    ]

    for template_name in expected_templates:
        template = template_root.joinpath(template_name)
        assert template.is_file()
        assert template.read_bytes().strip()
