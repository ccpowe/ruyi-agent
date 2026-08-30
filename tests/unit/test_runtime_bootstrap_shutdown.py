from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from functools import wraps
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

import ruyi_agent.runtime.bootstrap as bootstrap_module
from ruyi_agent.config.paths import RuyiPaths
from ruyi_agent.config.runtime_settings import load_runtime_settings


def async_test(function: Any) -> Any:
    """Run one async scenario without adding a pytest plugin dependency."""

    @wraps(function)
    def run(*args: Any, **kwargs: Any) -> Any:
        return asyncio.run(function(*args, **kwargs))

    return run


def _typed_runtime_settings(tmp_path: Path):
    ruyi_home = tmp_path / ".ruyi_agent"
    ruyi_home.mkdir()
    (ruyi_home / "ruyi.toml").write_text("", encoding="utf-8")
    return load_runtime_settings(
        RuyiPaths(
            ruyi_home=ruyi_home,
            config_dir=ruyi_home / "config",
            data_dir=ruyi_home / "data",
            skills_dir=ruyi_home / "skills",
            workspace=tmp_path,
        ),
        env={},
    )


@async_test
async def test_bootstrap_closes_control_before_stores_checkpointer_and_backend(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    settings = _typed_runtime_settings(tmp_path)
    order: list[str] = []
    open_resources = {
        "checkpointer",
        "backend",
        "route",
        "command",
        "task",
        "mailbox",
        "review",
    }

    class FakeBackendRuntime:
        home_dir = str(tmp_path)
        skills_root = "/skills"
        backend = object()
        kind = "fake"

        def close(self) -> None:
            assert "checkpointer" not in open_resources
            open_resources.remove("backend")
            order.append("backend")

    class FakeStore:
        label = ""

        def __init__(self, path: str) -> None:
            del path

        def close(self) -> None:
            open_resources.remove(self.label)
            order.append(self.label)

    def store_type(label: str) -> type[FakeStore]:
        return type(f"{label.title()}Store", (FakeStore,), {"label": label})

    @asynccontextmanager
    async def fake_checkpointer() -> Any:
        try:
            yield object()
        finally:
            assert not open_resources.intersection(
                {"route", "command", "task", "mailbox", "review"}
            )
            open_resources.remove("checkpointer")
            order.append("checkpointer")

    class FakeSaver:
        @staticmethod
        def from_conn_string(path: str) -> Any:
            del path
            return fake_checkpointer()

    class FakeControl:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            del args, kwargs
            self.recovery_started = False

        async def wake_pending_mailbox_tasks(self) -> None:
            return None

        def start_mailbox_recovery(self) -> None:
            self.recovery_started = True

        async def close(self) -> None:
            assert self.recovery_started is True
            assert open_resources == {
                "checkpointer",
                "backend",
                "route",
                "command",
                "task",
                "mailbox",
                "review",
            }
            order.append("control")

    class FakeRegistry:
        def __init__(self, configs: Any) -> None:
            del configs

        async def refresh(self) -> Any:
            return SimpleNamespace(server_statuses=[])

    class FakeSkillCatalog:
        def __init__(self, workspace_root: Path) -> None:
            del workspace_root

        def scan(self) -> Any:
            return SimpleNamespace(skills={})

    def fake_create_backend_runtime(active_settings: Any) -> FakeBackendRuntime:
        assert active_settings is settings
        return FakeBackendRuntime()

    monkeypatch.setattr(
        bootstrap_module,
        "create_backend_runtime",
        fake_create_backend_runtime,
    )
    monkeypatch.setattr(bootstrap_module, "AsyncSqliteSaver", FakeSaver)
    monkeypatch.setattr(bootstrap_module, "GatewayRouteStore", store_type("route"))
    monkeypatch.setattr(bootstrap_module, "GatewayCommandStore", store_type("command"))
    monkeypatch.setattr(bootstrap_module, "TaskStore", store_type("task"))
    monkeypatch.setattr(bootstrap_module, "MailboxStore", store_type("mailbox"))
    monkeypatch.setattr(bootstrap_module, "ReviewAuditStore", store_type("review"))
    monkeypatch.setattr(bootstrap_module, "AgentMailbox", lambda store: store)
    monkeypatch.setattr(bootstrap_module, "AgentControl", FakeControl)
    monkeypatch.setattr(bootstrap_module, "MCPRegistry", FakeRegistry)
    monkeypatch.setattr(bootstrap_module, "SkillCatalog", FakeSkillCatalog)
    monkeypatch.setattr(bootstrap_module, "SkillSyncer", lambda **kwargs: object())
    monkeypatch.setattr(
        bootstrap_module,
        "load_agent_configs",
        lambda: ("main", {}),
    )
    monkeypatch.setattr(bootstrap_module, "load_llm_provider_configs", lambda: {})
    monkeypatch.setattr(bootstrap_module, "load_permission_config", lambda: {})
    monkeypatch.setattr(
        bootstrap_module,
        "PermissionPolicy",
        lambda config: SimpleNamespace(default_profile="default"),
    )
    monkeypatch.setattr(bootstrap_module, "load_mcp_server_configs", lambda: {})
    monkeypatch.setattr(
        bootstrap_module,
        "build_all_local_worker_specs",
        lambda *args, **kwargs: _async_value({}),
    )
    monkeypatch.setattr(
        bootstrap_module,
        "build_all_remote_refs",
        lambda *args, **kwargs: _async_value({}),
    )
    monkeypatch.setattr(
        bootstrap_module, "GatewayTaskModule", lambda **kwargs: object()
    )
    monkeypatch.setenv("CHECKPOINT_DB", str(tmp_path / "checkpoints.sqlite"))
    monkeypatch.setenv("GATEWAY_ROUTE_DB", str(tmp_path / "routes.sqlite"))
    monkeypatch.setenv("TASK_DB", str(tmp_path / "tasks.sqlite"))
    monkeypatch.setenv("REVIEW_AUDIT_DB", str(tmp_path / "reviews.sqlite"))

    entered = asyncio.Event()

    async def run_lifespan() -> None:
        async with bootstrap_module.bootstrap_application(settings):
            assert order == []
            entered.set()
            await asyncio.Event().wait()

    lifespan = asyncio.create_task(run_lifespan())
    await entered.wait()
    lifespan.cancel()
    with pytest.raises(asyncio.CancelledError):
        await lifespan

    assert order == [
        "control",
        "review",
        "mailbox",
        "task",
        "command",
        "route",
        "checkpointer",
        "backend",
    ]
    assert open_resources == set()

    # Startup failure after recovery begins must take the identical close path.
    order.clear()
    open_resources.update(
        {
            "checkpointer",
            "backend",
            "route",
            "command",
            "task",
            "mailbox",
            "review",
        }
    )

    def fail_gateway(**kwargs: Any) -> object:
        del kwargs
        raise RuntimeError("gateway assembly failed")

    monkeypatch.setattr(bootstrap_module, "GatewayTaskModule", fail_gateway)
    with pytest.raises(RuntimeError, match="gateway assembly failed"):
        async with bootstrap_module.bootstrap_application(settings):
            raise AssertionError("failed startup must not yield")
    assert order == [
        "control",
        "review",
        "mailbox",
        "task",
        "command",
        "route",
        "checkpointer",
        "backend",
    ]
    assert open_resources == set()


@pytest.mark.parametrize("failure_stage", ["skills", "config", "mcp"])
@async_test
async def test_bootstrap_closes_backend_once_for_early_startup_failure(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    failure_stage: str,
) -> None:
    settings = _typed_runtime_settings(tmp_path)
    closes = 0

    class FakeBackendRuntime:
        home_dir = str(tmp_path)
        skills_root = "/skills"
        backend = object()
        kind = "fake"

        def close(self) -> None:
            nonlocal closes
            closes += 1

    class FakeSkillCatalog:
        def __init__(self, workspace_root: Path) -> None:
            del workspace_root

        def scan(self) -> Any:
            if failure_stage == "skills":
                raise RuntimeError("skills failed")
            return SimpleNamespace(skills={})

    class FakeRegistry:
        def __init__(self, configs: Any) -> None:
            del configs

        async def refresh(self) -> Any:
            if failure_stage == "mcp":
                raise RuntimeError("mcp failed")
            return SimpleNamespace(server_statuses=[])

    def load_configs() -> tuple[str, dict[str, Any]]:
        if failure_stage == "config":
            raise RuntimeError("config failed")
        return "main", {}

    def fake_create_backend_runtime(active_settings: Any) -> FakeBackendRuntime:
        assert active_settings is settings
        return FakeBackendRuntime()

    monkeypatch.setattr(
        bootstrap_module,
        "create_backend_runtime",
        fake_create_backend_runtime,
    )
    monkeypatch.setattr(bootstrap_module, "SkillCatalog", FakeSkillCatalog)
    monkeypatch.setattr(bootstrap_module, "SkillSyncer", lambda **kwargs: object())
    monkeypatch.setattr(bootstrap_module, "load_agent_configs", load_configs)
    monkeypatch.setattr(bootstrap_module, "load_llm_provider_configs", lambda: {})
    monkeypatch.setattr(bootstrap_module, "load_permission_config", lambda: {})
    monkeypatch.setattr(
        bootstrap_module,
        "PermissionPolicy",
        lambda config: SimpleNamespace(default_profile="default"),
    )
    monkeypatch.setattr(bootstrap_module, "load_mcp_server_configs", lambda: {})
    monkeypatch.setattr(bootstrap_module, "MCPRegistry", FakeRegistry)

    with pytest.raises(RuntimeError, match=f"{failure_stage} failed"):
        async with bootstrap_module.bootstrap_application(settings):
            raise AssertionError("failed startup must not yield")
    assert closes == 1


async def _async_value(value: Any) -> Any:
    return value
