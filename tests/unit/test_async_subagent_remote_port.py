from __future__ import annotations

import asyncio
from pathlib import Path
from fastapi import FastAPI
import httpx
import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.config.loader import LocalWorkerSpec
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore

from tests.support.async_subagent_runtime import (
    FakeAgentFactory,
    FlakyRemoteA2AClient,
    AlwaysFailingRemoteA2AClient,
    build_specs,
    build_test_remote_refs,
)
from tests.unit.gateway_http_support import build_local_agent_config


def test_format_exception_summary_expands_exception_group() -> None:
    # 为什么测异常组展开：当前最需要的是把 TaskGroup 包裹下的真实错误显示出来。
    exc = ExceptionGroup(
        "outer",
        [
            ValueError("bad input"),
            RuntimeError("boom"),
        ],
    )

    summary = async_subagent_runtime._format_exception_summary(exc)

    assert "ValueError: bad input" in summary
    assert "RuntimeError: boom" in summary
    assert "sub-exception" not in summary


def test_run_agent_turn_records_expanded_exception_summary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测失败落库：展开后的异常信息要真正进入 task 状态，而不是只停留在 helper 层。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    class FailingAgent:
        async def ainvoke(self, payload, *, config, version):
            raise ExceptionGroup(
                "outer",
                [
                    ValueError("bad input"),
                    RuntimeError("boom"),
                ],
            )

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )
    control._compiled_agents["background_research"] = FailingAgent()

    async def scenario() -> str:
        started = await control.spawn_agent("background_research", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        return await control.wait_agent(task_id)

    status = asyncio.run(scenario())

    assert "state=failed" in status
    assert "ValueError: bad input" in status
    assert "RuntimeError: boom" in status


def test_spawn_remote_ref_runs_via_a2a_gateway(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    remote_db = str(tmp_path / "remote-tasks.sqlite")
    remote_task_store = TaskStore(remote_db)
    remote_mailbox_store = MailboxStore(remote_db)
    remote_control = async_subagent_runtime.AgentControl(
        {
            "code_wiki": LocalWorkerSpec(
                name="code_wiki",
                description="remote code wiki",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=["/sandbox/home/AGENTS.md"],
                skills=["frontend-skill"],
            )
        },
        {},
        checkpointer=object(),
        backend=object(),
        task_store=remote_task_store,
        mailbox=AgentMailbox(remote_mailbox_store),
        remote_poll_interval=0.01,
    )
    remote_service = GatewayTaskModule(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": build_local_agent_config(
                "code_wiki",
                "remote code wiki",
            )
        },
        control=remote_control,
    )
    remote_app = create_gateway_app(
        service=remote_service, bearer_token="remote-secret"
    )
    remote_root_app = FastAPI()
    remote_root_app.mount("/a2a", remote_app)
    transport = httpx.ASGITransport(app=remote_root_app)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=A2AClient(transports={"https://example.com/a2a": transport}),
        remote_poll_interval=0.01,
    )

    async def scenario() -> tuple[str, str, str, str]:
        try:
            started = await control.spawn_agent("remote_code_wiki", "research this")
            task_id = started.split("task_id=")[1].split()[0]
            status_before = await control.check_agent(task_id)
            status_after = await control.wait_agent(task_id)
            sent = await control.send_input(task_id, "follow up")
            await control.wait_agent(task_id)
            return task_id, status_before, status_after, sent
        finally:
            await control.close()
            await remote_control.close()

    try:
        task_id, status_before, status_after, sent = asyncio.run(scenario())
    finally:
        remote_mailbox_store.close()
        remote_task_store.close()

    assert f"task_id={task_id}" in sent
    assert "route=remote_ref" in status_before
    assert "state=" in status_before
    assert "state=completed" in status_after
    assert "result=done" in status_after
    assert len(factory.created) == 1


def test_wait_agent_retries_transient_remote_status_failures() -> None:
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=FlakyRemoteA2AClient(),
        remote_poll_interval=0.01,
        remote_status_retry_attempts=2,
    )

    async def scenario() -> tuple[str, str]:
        started = await control.spawn_agent("remote_code_wiki", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        status_after = await control.wait_agent(task_id)
        return task_id, status_after

    task_id, status_after = asyncio.run(scenario())

    assert f"task_id={task_id}" in status_after
    assert "state=completed" in status_after
    assert "result=remote done" in status_after


def test_check_agent_keeps_last_known_state_when_remote_status_temporarily_unavailable() -> (
    None
):
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=AlwaysFailingRemoteA2AClient(),
        remote_poll_interval=0.01,
        remote_status_retry_attempts=2,
    )

    async def scenario() -> tuple[str, str]:
        started = await control.spawn_agent("remote_code_wiki", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        status = await control.check_agent(task_id)
        return task_id, status

    task_id, status = asyncio.run(scenario())

    assert f"task_id={task_id}" in status
    assert "state=running" in status
    assert "warning=remote_status_temporarily_unavailable" in status


def test_remote_task_webhook_event_publishes_to_mailbox() -> None:
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=AlwaysFailingRemoteA2AClient(),
        mailbox=mailbox,
    )

    async def scenario() -> tuple[bool, list]:
        record = await control.spawn_task(
            "remote_code_wiki",
            "research this",
            parent_thread_id="main-thread",
        )
        handled = await control.handle_remote_task_event(
            {
                "task_id": record.upstream_task_id,
                "agent_name": "remote_code_wiki",
                "status": "completed",
                "last_result": "remote done",
                "error": None,
                "run_count": 1,
                "created_at": "2026-04-23T00:00:00Z",
                "updated_at": "2026-04-23T00:00:01Z",
            }
        )
        return handled, mailbox.drain("main-thread")

    handled, messages = asyncio.run(scenario())

    assert handled is True
    assert len(messages) == 1
    assert messages[0].child_agent_name == "remote_code_wiki"
    assert messages[0].status == "completed"
    assert messages[0].content == "remote done"


def test_remote_task_webhook_event_relays_client_webhook(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    class CapturingAsyncClient:
        def __init__(self, *, timeout: float) -> None:
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url, *, headers, json):
            calls.append(
                {
                    "url": url,
                    "headers": headers,
                    "json": json,
                }
            )

    monkeypatch.setattr(
        async_subagent_runtime.httpx,
        "AsyncClient",
        CapturingAsyncClient,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=AlwaysFailingRemoteA2AClient(),
    )

    async def scenario() -> tuple[bool, async_subagent_runtime.TaskRecord]:
        record = await control.spawn_task(
            "remote_code_wiki",
            "research this",
            webhook={"url": "https://client.example/hooks", "token": "client-secret"},
        )
        handled = await control.handle_remote_task_event(
            {
                "task_id": record.upstream_task_id,
                "agent_name": "remote_code_wiki",
                "status": "completed",
                "last_result": "remote done",
                "error": None,
                "run_count": 1,
                "created_at": "2026-04-23T00:00:00Z",
                "updated_at": "2026-04-23T00:00:01Z",
            }
        )
        return handled, record

    handled, record = asyncio.run(scenario())

    assert handled is True
    assert len(calls) == 1
    assert calls[0]["url"] == "https://client.example/hooks"
    assert calls[0]["headers"] == {
        "Content-Type": "application/json",
        "Authorization": "Bearer client-secret",
    }
    payload = calls[0]["json"]
    assert isinstance(payload, dict)
    assert payload["task_id"] == record.task_id
    assert payload["agent_name"] == "remote_code_wiki"
    assert payload["status"] == "completed"
    assert payload["last_result"] == "remote done"
