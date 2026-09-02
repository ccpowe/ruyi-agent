from __future__ import annotations

import asyncio
from pathlib import Path
from fastapi import FastAPI
import httpx
import pytest

from ruyi_agent.task_models import TaskRecord
from ruyi_agent.runtime.delegation.contracts import _format_exception_summary
import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.runtime.delegation.notifications import SettledRunNotifier
from ruyi_agent.runtime.delegation.policy import DelegationPolicy
from ruyi_agent.runtime.delegation.registry import AgentRegistry
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.delegation.tools import DelegationTools
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError
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
    wait_for_task_state,
)
from tests.unit.gateway_http_support import build_local_agent_config


def test_format_exception_summary_expands_exception_group() -> None:
    # 为什么测异常组展开：当前最需要的是把 TaskGroup 包裹下的真实错误显示出来。
    secret = "task-summary-secret-value"
    exc = ExceptionGroup(
        "outer",
        [
            ValueError("bad input"),
            RuntimeError(f"password={secret}"),
        ],
    )

    summary = _format_exception_summary(exc)

    assert "ValueError: bad input" in summary
    assert "RuntimeError: password=[REDACTED]" in summary
    assert secret not in summary
    assert "sub-exception" not in summary


def test_run_agent_turn_records_expanded_exception_summary(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    # 为什么测失败落库：展开后的异常信息要真正进入 task 状态，而不是只停留在 helper 层。
    secret = "task-record-secret-value"
    class FailingFactory:
        def __call__(self, **kwargs):
            return FailingAgent()

    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        FailingFactory(),
    )

    class FailingAgent:
        async def ainvoke(self, payload, *, config, version):
            raise ExceptionGroup(
                "outer",
                [
                    ValueError("bad input"),
                    RuntimeError(f"api_key={secret}"),
                ],
            )

    store = TaskStore(str(tmp_path / "task-errors.sqlite"))
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=store,
    )

    async def scenario() -> TaskRecord:
        started = await control.spawn_task("background_research", "research this")
        return await wait_for_task_state(
            control,
            started.task_id,
            states={"completed", "failed", "cancelled", "interrupted"},
        )

    try:
        status = asyncio.run(scenario())
        events = store.list_task_events(
            task_id=status.task_id,
            run_count=status.run_count,
            after_event_id=0,
        )
    finally:
        asyncio.run(control.close())
        store.close()

    assert status.state == "failed"
    assert "ValueError: bad input" in (status.error or "")
    assert "RuntimeError: api_key=[REDACTED]" in (status.error or "")
    assert secret not in (status.error or "")
    failed_event = next(event for event in events if event.event_type == "task.failed")
    assert secret not in str(failed_event.data)
    assert failed_event.data["error"] == status.error


def test_spawn_remote_ref_runs_via_a2a_gateway(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    remote_db = str(tmp_path / "remote-tasks.sqlite")
    remote_task_store = TaskStore(remote_db)
    remote_mailbox_store = MailboxStore(remote_db)
    remote_mailbox = AgentMailbox(remote_mailbox_store)

    factory = FakeAgentFactory()
    claimed_batches: list[list] = []
    claim_inputs: list[tuple[str, str, str]] = []
    second_run_claimed = asyncio.Event()

    def create_follow_up_agent(**kwargs):
        agent = factory(**kwargs)
        original_ainvoke = agent.ainvoke

        async def ainvoke(payload, *, config, version):
            result = await original_ainvoke(payload, config=config, version=version)
            if len(agent.calls) == 2:
                configurable = config["configurable"]
                task_id = configurable["task_id"]
                thread_id = configurable["thread_id"]
                run_id = configurable["mailbox_run_id"]
                claim_inputs.append((task_id, thread_id, run_id))
                claimed_batches.append(
                    remote_mailbox.claim(
                        recipient_task_id=task_id,
                        recipient_thread_id=thread_id,
                        run_id=run_id,
                    )
                )
                second_run_claimed.set()
            return result

        agent.ainvoke = ainvoke
        return agent

    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", create_follow_up_agent)
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
        mailbox=remote_mailbox,
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

    async def scenario() -> tuple[str, TaskRecord, TaskRecord, TaskRecord, TaskRecord]:
        try:
            started = await control.spawn_task("remote_code_wiki", "research this")
            task_id = started.task_id
            status_before = control.get_task_record(task_id)
            status_after = await control.refresh_task(task_id)
            [downstream] = remote_control.list_persisted_task_records()
            initial_downstream = await wait_for_task_state(
                remote_control,
                downstream.task_id,
                states={"completed"},
            )
            assert initial_downstream.run_count == 1
            sent = await control.send_task_input(task_id, "follow up")

            [agent] = factory.created
            async with asyncio.timeout(5):
                await second_run_claimed.wait()
            completed_downstream = await wait_for_task_state(
                remote_control,
                downstream.task_id,
                states={"completed"},
            )
            assert completed_downstream.run_count == 2
            await remote_control.wake_pending_mailbox_tasks()
            after_recovery = remote_control.get_task_record(downstream.task_id)
            assert after_recovery.state == "completed"
            assert after_recovery.run_count == 2
            message_rows = remote_mailbox_store._conn.execute(
                """
                SELECT content, status
                FROM agent_mailbox_messages
                WHERE recipient_task_id = ?
                ORDER BY message_id
                """,
                (downstream.task_id,),
            ).fetchall()
            assert [(row["content"], row["status"]) for row in message_rows] == [
                ("follow up", "delivered")
            ]
            assert remote_mailbox.has_triggering_messages(downstream.task_id) is False
            assert len(agent.calls) == 2
            assert len(claimed_batches) == 1
            assert [message.content for message in claimed_batches[0]] == ["follow up"]
            assert claim_inputs == [
                (
                    downstream.task_id,
                    downstream.thread_id,
                    agent.calls[1]["config"]["configurable"]["mailbox_run_id"],
                )
            ]
            final = await control.refresh_task(task_id)
            return task_id, status_before, status_after, sent, final
        finally:
            await control.close()
            await remote_control.close()

    try:
        task_id, status_before, status_after, sent, final = asyncio.run(scenario())
    finally:
        remote_mailbox_store.close()
        remote_task_store.close()

    assert sent.task_id == task_id
    assert status_before.route_kind == "remote_ref"
    assert status_before.state in {"running", "completed"}
    assert status_after.state == "completed"
    assert status_after.result == "done"
    assert sent.state in {"pending", "running", "completed"}
    assert final.state == "completed"
    assert final.run_count == 2
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

    async def scenario() -> tuple[str, TaskRecord]:
        started = await control.spawn_task("remote_code_wiki", "research this")
        task_id = started.task_id
        status_after = await control.refresh_task(task_id)
        return task_id, status_after

    task_id, status_after = asyncio.run(scenario())

    assert status_after.task_id == task_id
    assert status_after.state == "completed"
    assert status_after.result == "remote done"


def test_check_agent_keeps_last_known_state_when_remote_status_temporarily_unavailable():
    manager = TaskManager()
    task_id = "remote-task-2"
    manager.create_task_record(
        task_id,
        "remote_code_wiki",
        parent_task_id=None,
        root_task_id=task_id,
        depth=1,
        route_kind="remote_ref",
        parent_thread_id="main-thread",
    )
    policy = DelegationPolicy(
        manager,
        node_id="node-test",
        max_delegation_depth=3,
        max_tasks_per_root=20,
        permission_default_profile="",
    )
    notifier = SettledRunNotifier(manager, None)

    class RefreshUnavailablePort:
        async def spawn_task(
            self, agent_name, task, *, parent_task_id=None, parent_thread_id=None
        ):
            raise AssertionError("spawn_task should not be called")

        async def refresh_task(self, requested_task_id):
            assert requested_task_id == task_id
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message="temporarily unavailable",
            )

        async def send_task_input(self, requested_task_id, message):
            raise AssertionError("send_task_input should not be called")

        async def cancel_task(self, requested_task_id):
            raise AssertionError("cancel_task should not be called")

    tools = DelegationTools(
        AgentRegistry(build_specs(), build_test_remote_refs()),
        manager,
        policy,
        notifier,
        remote_poll_interval=0.01,
    )
    check_tool = next(
        tool
        for tool in tools.build_tools(command_port=RefreshUnavailablePort())
        if tool.name == "check_agent"
    )
    status = asyncio.run(
        check_tool.ainvoke(
            {"task_id": task_id},
            config={"configurable": {"thread_id": "main-thread"}},
        )
    )

    assert f"task_id={task_id}" in status
    assert "state=pending" in status
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
        httpx,
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

    async def scenario() -> tuple[bool, TaskRecord]:
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
