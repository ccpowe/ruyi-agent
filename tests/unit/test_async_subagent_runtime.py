from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import logging
from fastapi import FastAPI
import httpx
import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.integrations.a2a.client import A2AClient, A2AClientError
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.runtime.delegation.context import (
    CONTEXT_VERSION,
    CONTEXT_VERSION_FIELD,
    DEPTH_FIELD,
    MAX_DEPTH_FIELD,
    MAX_TASKS_PER_ROOT_FIELD,
    ROOT_ID_FIELD,
    VISITED_NODES_FIELD,
)
from ruyi_agent.channels.http.api import AgentControlGatewayRuntime, GatewayService, create_gateway_app
from ruyi_agent.storage.task_store import TaskStore


class FakeAgent:
    def __init__(self) -> None:
        # 为什么用假 agent：这里只验证本地 async runtime 的调度语义，不依赖真实 LLM 调用。
        self.calls: list[dict] = []

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await asyncio.sleep(0)
        return {"messages": [{"role": "assistant", "content": "done"}]}


class FakeAgentFactory:
    def __init__(self) -> None:
        # 为什么保留工厂：需要验证 runtime 会缓存 agent，而不是每次重新构造。
        self.created: list[FakeAgent] = []

    def __call__(self, **kwargs):
        agent = FakeAgent()
        self.created.append(agent)
        return agent


class InterruptingAgent:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await asyncio.sleep(0)
        if len(self.calls) == 1:
            return {
                "__interrupt__": [
                    {
                        "value": {
                            "action_requests": [
                                {
                                    "name": "execute",
                                    "args": {"command": "python -V"},
                                }
                            ],
                            "review_configs": [
                                {
                                    "action_name": "execute",
                                    "allowed_decisions": [
                                        "approve",
                                        "edit",
                                        "reject",
                                    ],
                                }
                            ],
                        }
                    }
                ]
            }
        return {"messages": [{"role": "assistant", "content": "resumed done"}]}


class InterruptingAgentFactory:
    def __init__(self) -> None:
        self.created: list[InterruptingAgent] = []

    def __call__(self, **kwargs):
        agent = InterruptingAgent()
        self.created.append(agent)
        return agent


class ResumeBlockingInterruptingAgent:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.resume_started = asyncio.Event()

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await asyncio.sleep(0)
        if len(self.calls) == 1:
            return {
                "__interrupt__": [
                    {
                        "value": {
                            "action_requests": [
                                {
                                    "name": "execute",
                                    "args": {"command": "python -V"},
                                }
                            ],
                            "review_configs": [
                                {
                                    "action_name": "execute",
                                    "allowed_decisions": ["approve", "reject"],
                                }
                            ],
                        }
                    }
                ]
            }
        self.resume_started.set()
        await asyncio.Event().wait()
        return {"messages": [{"role": "assistant", "content": "unreachable"}]}


class ResumeBlockingInterruptingAgentFactory:
    def __init__(self) -> None:
        self.created: list[ResumeBlockingInterruptingAgent] = []

    def __call__(self, **kwargs):
        agent = ResumeBlockingInterruptingAgent()
        self.created.append(agent)
        return agent


class SnapshotInterrupt:
    def __init__(self, value: dict[str, object]) -> None:
        self.value = value


class SnapshotState:
    def __init__(self, interrupts: list[SnapshotInterrupt]) -> None:
        self.interrupts = interrupts


class SnapshotInterruptAgent:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.state_calls: list[dict] = []

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await asyncio.sleep(0)
        if len(self.calls) == 1:
            return {
                "messages": [
                    {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "name": "web_fetch_exa",
                                "args": {"urls": ["https://news.ycombinator.com/"]},
                                "id": "call-1",
                            }
                        ],
                    }
                ]
            }
        return {"messages": [{"role": "assistant", "content": "snapshot resumed"}]}

    async def aget_state(self, config):
        self.state_calls.append(config)
        if len(self.calls) == 1:
            return SnapshotState(
                [
                    SnapshotInterrupt(
                        {
                            "action_requests": [
                                {
                                    "name": "web_fetch_exa",
                                    "args": {
                                        "urls": ["https://news.ycombinator.com/"]
                                    },
                                }
                            ],
                            "review_configs": [
                                {
                                    "action_name": "web_fetch_exa",
                                    "allowed_decisions": [
                                        "approve",
                                        "edit",
                                        "reject",
                                    ],
                                }
                            ],
                        }
                    )
                ]
            )
        return SnapshotState([])


class SnapshotInterruptAgentFactory:
    def __init__(self) -> None:
        self.created: list[SnapshotInterruptAgent] = []

    def __call__(self, **kwargs):
        agent = SnapshotInterruptAgent()
        self.created.append(agent)
        return agent


class ContentAwareInterruptingAgent:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def ainvoke(self, payload, *, config, version):
        self.calls.append(
            {
                "payload": payload,
                "config": config,
                "version": version,
            }
        )
        await asyncio.sleep(0)
        if hasattr(payload, "resume"):
            return {"messages": [{"role": "assistant", "content": "done: needs review"}]}
        content = payload["messages"][0]["content"]
        if content == "needs review":
            return {
                "__interrupt__": [
                    {
                        "value": {
                            "action_requests": [
                                {
                                    "name": "execute",
                                    "args": {"command": "python -V"},
                                }
                            ],
                            "review_configs": [
                                {
                                    "action_name": "execute",
                                    "allowed_decisions": ["approve", "reject"],
                                }
                            ],
                        }
                    }
                ]
            }
        return {"messages": [{"role": "assistant", "content": f"done: {content}"}]}


class ContentAwareInterruptingAgentFactory:
    def __init__(self) -> None:
        self.created: list[ContentAwareInterruptingAgent] = []

    def __call__(self, **kwargs):
        agent = ContentAwareInterruptingAgent()
        self.created.append(agent)
        return agent


class BlockingAgent:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def ainvoke(self, payload, *, config, version):
        self.started.set()
        await asyncio.Event().wait()
        return {"messages": [{"role": "assistant", "content": "unreachable"}]}


class RemoteRefreshAfterRestartA2AClient:
    def __init__(self) -> None:
        self.get_calls: list[str] = []
        self.sent_inputs: list[str] = []

    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        return {
            "task_id": "remote-task-persisted",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:00Z",
        }

    async def get_task(self, remote_ref, *, task_id: str):
        self.get_calls.append(task_id)
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": "remote persisted done",
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:02Z",
        }

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        self.sent_inputs.append(input_content)
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": f"remote continued: {input_content}",
            "error": None,
            "run_count": 2,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:03Z",
        }

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("cancel_task should not be called in this test")


class FlakyRemoteA2AClient:
    def __init__(self) -> None:
        self.get_calls = 0

    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        return {
            "task_id": "remote-task-1",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:00Z",
        }

    async def get_task(self, remote_ref, *, task_id: str):
        self.get_calls += 1
        if self.get_calls == 1:
            raise A2AClientError(
                status_code=502,
                code="upstream_gateway_error",
                message=f"Remote gateway request failed for '{remote_ref.name}'",
            )
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": "remote done",
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
        }

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        raise AssertionError("send_input should not be called in this test")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("cancel_task should not be called in this test")


class AlwaysFailingRemoteA2AClient:
    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        return {
            "task_id": "remote-task-2",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:00Z",
        }

    async def get_task(self, remote_ref, *, task_id: str):
        raise A2AClientError(
            status_code=502,
            code="upstream_gateway_error",
            message=f"Remote gateway request failed for '{remote_ref.name}'",
        )

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        raise AssertionError("send_input should not be called in this test")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("cancel_task should not be called in this test")


class ShouldNotCallRemoteA2AClient:
    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        raise AssertionError("remote create_task should not be called")

    async def get_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote get_task should not be called")

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")


class SlowRemoteA2AClient:
    def __init__(self) -> None:
        self.create_calls = 0

    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        self.create_calls += 1
        call_id = self.create_calls
        await asyncio.sleep(0.01)
        return {
            "task_id": f"remote-task-{call_id}",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:00Z",
        }

    async def get_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote get_task should not be called")

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")


class RecordingRemoteA2AClient:
    def __init__(self) -> None:
        self.created_metadata: list[dict[str, object]] = []

    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        self.created_metadata.append(dict(metadata))
        return {
            "task_id": "remote-task-recorded",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:00Z",
        }

    async def get_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote get_task should not be called")

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")


class ReviewRemoteA2AClient:
    def __init__(self) -> None:
        self.submitted: list[dict[str, object]] = []

    async def create_task(self, remote_ref, *, input_content: str, metadata: dict, attachments=None):
        return {
            "task_id": "remote-review-task",
            "agent_name": remote_ref.name,
            "status": "waiting_for_human",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
            "pending_review": {
                "review_id": "remote-review-1",
                "action_requests": [
                    {
                        "name": "execute",
                        "args": {"command": "python -V"},
                    }
                ],
                "review_configs": [
                    {
                        "action_name": "execute",
                        "allowed_decisions": ["approve", "reject"],
                    }
                ],
            },
        }

    async def get_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote get_task should not be called")

    async def send_input(self, remote_ref, *, task_id: str, input_content: str, attachments=None):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")

    async def submit_review_decision(
        self,
        remote_ref,
        *,
        task_id: str,
        review_id: str,
        decisions: list[dict[str, object]],
    ):
        self.submitted.append(
            {
                "task_id": task_id,
                "review_id": review_id,
                "decisions": decisions,
            }
        )
        return {
            "task_id": task_id,
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": "remote resumed",
            "error": None,
            "run_count": 2,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:02Z",
            "pending_review": None,
        }


def build_specs() -> dict[str, LocalWorkerSpec]:
    return {
        "background_research": LocalWorkerSpec(
            name="background_research",
            description="background helper",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=["/sandbox/home/AGENTS.md"],
            skills=["/sandbox/skills/frontend-skill"],
        )
    }


def build_test_remote_refs() -> dict[str, RemoteRef]:
    return {
        "remote_code_wiki": RemoteRef(
            name="remote_code_wiki",
            description="remote helper",
            url="https://example.com/a2a",
            remote_agent_name="code_wiki",
            auth={"type": "bearer", "token_env": "REMOTE_CODE_WIKI_TOKEN"},
        )
    }


def test_spawn_wait_and_check_agent_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测完整生命周期：本地 async subagent 的最核心价值就是统一的 spawn/check/wait 语义。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=ReviewRemoteA2AClient(),  # type: ignore[arg-type]
    )

    async def scenario() -> tuple[str, str, str]:
        started = await control.spawn_agent("background_research", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        status_before = await control.check_agent(task_id)
        status_after = await control.wait_agent(task_id)
        return task_id, status_before, status_after

    task_id, status_before, status_after = asyncio.run(scenario())

    assert "agent=background_research" in status_before
    assert "state=" in status_before
    assert "state=completed" in status_after
    assert "result=done" in status_after
    assert len(factory.created) == 1
    configurable = factory.created[0].calls[0]["config"]["configurable"]
    assert configurable["thread_id"] == task_id
    assert configurable["task_id"] == task_id
    assert configurable["parent_task_id"] is None
    assert configurable["root_task_id"] == task_id
    assert configurable["delegation_depth"] == 1


def test_wait_agent_reports_worker_human_review_without_tool_interrupt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = InterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, async_subagent_runtime.TaskRecord]:
        record = await control.spawn_task("background_research", "needs review")
        if record.active_run is not None:
            await record.active_run
        waiting = control.get_task_record(record.task_id)
        assert waiting.state == "waiting_for_human"
        status = await control.wait_agent(record.task_id)
        assert "state=running" in status
        assert "waiting_for_human" not in status
        assert "review_id=" not in status
        pending = control.get_task_record(record.task_id)
        updated = await control.submit_review_decision(
            pending.pending_review["review_id"],
            [{"type": "approve"}],
            wait=True,
        )
        return status, updated

    status, record = asyncio.run(scenario())

    assert "state=running" in status
    assert "waiting_for_human" not in status
    assert "review_id=" not in status
    assert record.state == "completed"
    assert record.result == "resumed done"
    assert record.pending_review is None
    assert len(factory.created[0].calls) == 2
    assert factory.created[0].calls[1]["payload"].resume == {
        "decisions": [{"type": "approve"}]
    }


def test_worker_interrupts_are_read_from_state_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = SnapshotInterruptAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, async_subagent_runtime.TaskRecord]:
        record = await control.spawn_task("background_research", "fetch hn")
        if record.active_run is not None:
            await record.active_run
        waiting = control.get_task_record(record.task_id)
        assert waiting.state == "waiting_for_human"
        status = await control.wait_agent(record.task_id)
        assert "state=running" in status
        assert "waiting_for_human" not in status
        pending = control.get_task_record(record.task_id)
        updated = await control.submit_review_decision(
            pending.pending_review["review_id"],
            [{"type": "approve"}],
            wait=True,
        )
        return status, updated

    status, record = asyncio.run(scenario())

    assert "state=running" in status
    assert "waiting_for_human" not in status
    assert record.state == "completed"
    assert record.result == "snapshot resumed"
    assert record.pending_review is None
    assert len(factory.created[0].state_calls) >= 1


def test_wait_agent_resolves_human_review_from_config_callback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = InterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> str:
        record = await control.spawn_task("background_research", "needs review")

        async def resolve_pending_reviews() -> bool:
            pending = control.get_task_record(record.task_id)
            if (
                pending.state != "waiting_for_human"
                or pending.pending_review is None
            ):
                return False
            await control.submit_review_decision(
                pending.pending_review["review_id"],
                [{"type": "approve"}],
            )
            return True

        return await control.wait_agent(
            record.task_id,
            config={
                "configurable": {
                    "resolve_pending_reviews": resolve_pending_reviews,
                }
            },
        )

    status = asyncio.run(scenario())

    assert "state=completed" in status
    assert "result=resumed done" in status
    assert len(factory.created[0].calls) == 2


def test_child_review_is_mirrored_to_root_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        if root.active_run is not None:
            await root.active_run
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if child.active_run is not None:
            await child.active_run
        return control.get_task_record(root.task_id), control.get_task_record(child.task_id)

    root, child = asyncio.run(scenario())

    assert child.state == "waiting_for_human"
    assert child.pending_review is not None
    assert root.pending_review is not None
    assert root.pending_review["review_id"] == child.pending_review["review_id"]
    assert root.pending_review["source_task_id"] == child.task_id


def test_submit_review_prefers_waiting_child_over_root_mirror(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ContentAwareInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        if root.active_run is not None:
            await root.active_run
        child = await control.spawn_task(
            "background_research",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        if child.active_run is not None:
            await child.active_run
        waiting_child = control.get_task_record(child.task_id)
        updated_child = await control.submit_review_decision(
            waiting_child.pending_review["review_id"],
            [{"type": "approve"}],
            wait=True,
        )
        return control.get_task_record(root.task_id), updated_child

    root, child = asyncio.run(scenario())

    assert child.state == "completed"
    assert child.result == "done: needs review"
    assert root.pending_review is None


def test_submit_review_decision_default_does_not_wait_for_resumed_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = ResumeBlockingInterruptingAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> str:
        record = await control.spawn_task("background_research", "needs review")
        assert record.active_run is not None
        await record.active_run
        waiting = control.get_task_record(record.task_id)
        assert waiting.state == "waiting_for_human"
        updated = await control.submit_review_decision(
            waiting.pending_review["review_id"],
            [{"type": "approve"}],
        )
        assert updated.state == "running"
        assert updated.active_run is not None
        await factory.created[0].resume_started.wait()
        updated.active_run.cancel()
        try:
            await updated.active_run
        except asyncio.CancelledError:
            pass
        return "running"

    state_before_cancel = asyncio.run(scenario())

    assert state_before_cancel == "running"
    assert len(factory.created[0].calls) == 2


def test_remote_review_decision_is_forwarded_to_upstream_gateway() -> None:
    a2a_client = ReviewRemoteA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,  # type: ignore[arg-type]
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        record = await control.spawn_task("remote_code_wiki", "needs review")
        assert record.state == "waiting_for_human"
        assert record.pending_review is not None
        pending = control.list_pending_review_records()
        assert [item.task_id for item in pending] == [record.task_id]
        return await control.submit_review_decision(
            "remote-review-1",
            [{"type": "approve"}],
        )

    record = asyncio.run(scenario())

    assert record.state == "completed"
    assert record.result == "remote resumed"
    assert record.pending_review is None
    assert a2a_client.submitted == [
        {
            "task_id": "remote-review-task",
            "review_id": "remote-review-1",
            "decisions": [{"type": "approve"}],
        }
    ]


def test_sync_remote_waiting_review_is_mirrored_to_root_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=ReviewRemoteA2AClient(),  # type: ignore[arg-type]
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        if root.active_run is not None:
            await root.active_run
        child = await control.spawn_task(
            "remote_code_wiki",
            "needs review",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        return control.get_task_record(root.task_id), control.get_task_record(child.task_id)

    root, child = asyncio.run(scenario())

    assert child.pending_review is not None
    assert root.pending_review is not None
    assert root.pending_review["review_id"] == child.pending_review["review_id"]
    assert root.pending_review["source_task_id"] == child.task_id


def test_root_spawn_records_depth_1_and_self_as_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        record = await control.spawn_task("background_research", "root task")
        assert record.active_run is not None
        await record.active_run
        return record

    record = asyncio.run(scenario())

    assert record.parent_task_id is None
    assert record.root_task_id == record.task_id
    assert record.depth == 1


def test_nested_spawn_inherits_root_and_increments_depth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[
        async_subagent_runtime.TaskRecord,
        async_subagent_runtime.TaskRecord,
    ]:
        root = await control.spawn_task("background_research", "root task")
        child = await control.spawn_task(
            "background_research",
            "child task",
            parent_task_id=root.task_id,
            parent_thread_id=root.thread_id,
        )
        assert root.active_run is not None
        assert child.active_run is not None
        root_run = root.active_run
        child_run = child.active_run
        await root_run
        await child_run
        return root, child

    root, child = asyncio.run(scenario())

    assert child.parent_task_id == root.task_id
    assert child.root_task_id == root.task_id
    assert child.depth == 2


def test_spawn_agent_extracts_parent_context_from_configurable_task_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        root = await control.spawn_task("background_research", "root task")
        started = await control.spawn_agent(
            "background_research",
            "child task",
            config={
                "configurable": {
                    "thread_id": root.thread_id,
                    "task_id": root.task_id,
                    "root_task_id": root.root_task_id,
                    "delegation_depth": root.depth,
                }
            },
        )
        child_task_id = started.split("task_id=")[1].split()[0]
        await control.wait_agent(root.task_id)
        await control.wait_agent(child_task_id)
        return control.get_task_record(child_task_id)

    child = asyncio.run(scenario())

    assert child.depth == 2
    assert child.parent_task_id == child.root_task_id


def test_spawn_agent_falls_back_to_thread_id_when_task_id_missing(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        root = await control.spawn_task("background_research", "root task")
        with caplog.at_level(logging.WARNING, logger="ruyi_agent.runtime.delegation.async_runtime"):
            started = await control.spawn_agent(
                "background_research",
                "child task",
                config={"configurable": {"thread_id": root.thread_id}},
            )
        child_task_id = started.split("task_id=")[1].split()[0]
        await control.wait_agent(root.task_id)
        await control.wait_agent(child_task_id)
        return control.get_task_record(child_task_id)

    child = asyncio.run(scenario())

    assert child.depth == 2
    assert "falling back to thread_id" in caplog.text


def test_spawn_rejects_when_depth_exceeds_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        max_delegation_depth=1,
    )

    async def scenario() -> tuple[str, list[async_subagent_runtime.TaskRecord]]:
        root = await control.spawn_task("background_research", "root task")
        denied = await control.spawn_agent(
            "background_research",
            "child task",
            config={
                "configurable": {
                    "thread_id": root.thread_id,
                    "task_id": root.task_id,
                }
            },
        )
        await control.wait_agent(root.task_id)
        return denied, control.list_task_records()

    denied, records = asyncio.run(scenario())

    assert "Delegation depth limit exceeded" in denied
    assert "Complete the remaining work yourself" in denied
    assert len(records) == 1


def test_spawn_rejects_when_root_task_budget_exhausted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        max_tasks_per_root=2,
    )

    async def scenario() -> tuple[str, list[async_subagent_runtime.TaskRecord]]:
        root = await control.spawn_task("background_research", "root task")
        first_child = await control.spawn_task(
            "background_research",
            "first child",
            parent_task_id=root.task_id,
        )
        denied = await control.spawn_agent(
            "background_research",
            "second child",
            config={
                "configurable": {
                    "thread_id": root.thread_id,
                    "task_id": root.task_id,
                }
            },
        )
        await control.wait_agent(root.task_id)
        await control.wait_agent(first_child.task_id)
        return denied, control.list_task_records()

    denied, records = asyncio.run(scenario())

    assert "Task budget exhausted" in denied
    assert "max_tasks_per_root=2" in denied
    assert len(records) == 2


def test_remote_ref_spawn_also_enforces_depth_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=ShouldNotCallRemoteA2AClient(),
        max_delegation_depth=1,
    )

    async def scenario() -> str:
        root = await control.spawn_task("background_research", "root task")
        denied = await control.spawn_agent(
            "remote_code_wiki",
            "remote child",
            config={
                "configurable": {
                    "thread_id": root.thread_id,
                    "task_id": root.task_id,
                }
            },
        )
        await control.wait_agent(root.task_id)
        return denied

    denied = asyncio.run(scenario())

    assert "Delegation depth limit exceeded" in denied


def test_remote_child_spawn_injects_delegation_context_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    a2a_client = RecordingRemoteA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,
        node_id="node-a",
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        root = await control.spawn_task("background_research", "root task")
        child = await control.spawn_task(
            "remote_code_wiki",
            "remote child",
            parent_task_id=root.task_id,
            metadata={"channel": "tg"},
        )
        await control.wait_agent(root.task_id)
        return child

    child = asyncio.run(scenario())

    assert child.depth == 2
    assert len(a2a_client.created_metadata) == 1
    metadata = a2a_client.created_metadata[0]
    assert metadata["channel"] == "tg"
    assert metadata[CONTEXT_VERSION_FIELD] == CONTEXT_VERSION
    assert metadata[ROOT_ID_FIELD].startswith("node-a:")
    assert metadata[DEPTH_FIELD] == 2
    assert metadata[MAX_DEPTH_FIELD] == 3
    assert metadata[MAX_TASKS_PER_ROOT_FIELD] == 20
    assert metadata[VISITED_NODES_FIELD] == '["node-a"]'


def test_concurrent_remote_spawns_cannot_exceed_root_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    a2a_client = SlowRemoteA2AClient()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=a2a_client,
        max_tasks_per_root=2,
    )

    async def scenario() -> tuple[list[str], list[async_subagent_runtime.TaskRecord]]:
        root = await control.spawn_task("background_research", "root task")
        config = {
            "configurable": {
                "thread_id": root.thread_id,
                "task_id": root.task_id,
            }
        }
        results = await asyncio.gather(
            control.spawn_agent("remote_code_wiki", "remote child 1", config=config),
            control.spawn_agent("remote_code_wiki", "remote child 2", config=config),
        )
        await control.wait_agent(root.task_id)
        return results, control.list_task_records()

    results, records = asyncio.run(scenario())

    assert sum("Started worker task" in result for result in results) == 1
    assert sum("Task budget exhausted" in result for result in results) == 1
    assert a2a_client.create_calls == 1
    assert len(records) == 2


def test_send_input_reuses_same_agent_thread(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测继续输入：这决定本地 async worker 是一次性任务还是可持续推进的 agent 会话。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> str:
        started = await control.spawn_agent("background_research", "first task")
        task_id = started.split("task_id=")[1].split()[0]
        await control.wait_agent(task_id)
        sent = await control.send_input(task_id, "follow up")
        await control.wait_agent(task_id)
        return task_id, sent

    task_id, sent = asyncio.run(scenario())
    assert f"task_id={task_id}" in sent

    assert len(factory.created) == 1
    assert len(factory.created[0].calls) == 2
    first_thread = factory.created[0].calls[0]["config"]["configurable"]["thread_id"]
    second_thread = factory.created[0].calls[1]["config"]["configurable"]["thread_id"]
    assert first_thread == second_thread == task_id


def test_send_input_after_failed_task_clears_previous_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingThenSuccessfulAgent:
        def __init__(self) -> None:
            self.calls = 0

        async def ainvoke(self, payload, *, config, version):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("first failure")
            return {"messages": [{"role": "assistant", "content": "recovered"}]}

    agent = FailingThenSuccessfulAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, str, str, str | None]:
        started = await control.spawn_agent("background_research", "first task")
        task_id = started.split("task_id=")[1].split()[0]
        failed = await control.wait_agent(task_id)
        sent = await control.send_input(task_id, "retry")
        running_error = control.get_task_record(task_id).error
        recovered = await control.wait_agent(task_id)
        return failed, sent, recovered, running_error

    failed, sent, recovered, running_error = asyncio.run(scenario())

    assert "state=failed" in failed
    assert "RuntimeError: first failure" in failed
    assert "Sent input" in sent
    assert running_error is None
    assert "state=completed" in recovered
    assert "result=recovered" in recovered


def test_send_input_after_cancelled_task_restarts_same_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, str]:
        record = await control.spawn_task("background_research", "first task")
        await record.active_run
        await control.cancel_task(record.task_id)
        sent = await control.send_input(record.task_id, "resume after cancel")
        await control.wait_agent(record.task_id)
        return record.task_id, sent

    task_id, sent = asyncio.run(scenario())

    assert f"task_id={task_id}" in sent
    assert len(factory.created[0].calls) == 2
    assert (
        factory.created[0].calls[1]["config"]["configurable"]["thread_id"] == task_id
    )


def test_explicit_cancel_marks_task_cancelled(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        record = await control.spawn_task("background_research", "block")
        await agent.started.wait()
        return await control.cancel_task(record.task_id)

    record = asyncio.run(scenario())

    assert record.state == "cancelled"


def test_passive_run_cancellation_marks_task_interrupted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> async_subagent_runtime.TaskRecord:
        record = await control.spawn_task("background_research", "block")
        await agent.started.wait()
        assert record.active_run is not None
        record.active_run.cancel()
        try:
            await record.active_run
        except asyncio.CancelledError:
            pass
        return control.get_task_record(record.task_id)

    record = asyncio.run(scenario())

    assert record.state == "interrupted"
    assert record.error is not None
    assert record.error.startswith("Task interrupted:")


def test_task_store_restores_local_running_task_as_interrupted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    task_store.save_task(
        async_subagent_runtime.TaskRecord(
            task_id="local-running-task",
            agent_name="background_research",
            state="running",
            thread_id="local-running-task",
            parent_task_id=None,
            root_task_id="local-running-task",
            parent_thread_id="main-thread",
            depth=1,
            created_at=datetime(2026, 4, 23, tzinfo=UTC),
            updated_at=datetime(2026, 4, 23, tzinfo=UTC),
            run_count=1,
            route_kind="local",
        )
    )

    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
    )

    listing_before = asyncio.run(second_control.list_agents())
    assert "task_id=local-running-task" not in listing_before

    second_control.load_tasks_for_thread("main-thread")
    restored = second_control.get_task_record("local-running-task")
    assert restored.state == "interrupted"
    assert restored.active_run is None
    listing = asyncio.run(second_control.list_agents())
    assert "task_id=local-running-task" in listing
    assert "state=interrupted" in listing
    task_store.close()


def test_task_store_reload_preserves_live_local_run(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    agent = BlockingAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        task_store=task_store,
    )

    async def scenario() -> tuple[str, str | None]:
        record = await control.spawn_task(
            "background_research",
            "block",
            parent_thread_id="main-thread",
        )
        await agent.started.wait()
        control.load_tasks_for_thread("main-thread")
        reloaded = control.get_task_record(record.task_id)
        assert reloaded.active_run is not None
        assert not reloaded.active_run.done()
        state = reloaded.state
        error = reloaded.error
        await control.cancel_task(record.task_id)
        return state, error

    state, error = asyncio.run(scenario())

    assert state == "running"
    assert error is None
    task_store.close()


def test_remote_task_store_recovers_and_refreshes_after_restart(
    tmp_path: Path,
) -> None:
    task_store = TaskStore(str(tmp_path / "tasks.sqlite"))
    first_client = RemoteRefreshAfterRestartA2AClient()
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=first_client,
        task_store=task_store,
        remote_poll_interval=0.01,
    )

    async def spawn_remote() -> str:
        started = await first_control.spawn_agent("remote_code_wiki", "remote task")
        return started.split("task_id=")[1].split()[0]

    task_id = asyncio.run(spawn_remote())
    stored = task_store.get_task(task_id)
    assert stored is not None
    assert stored.state == "running"
    assert stored.upstream_task_id == "remote-task-persisted"

    second_client = RemoteRefreshAfterRestartA2AClient()
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        a2a_client=second_client,
        task_store=task_store,
        remote_poll_interval=0.01,
    )

    async def refresh_and_continue() -> tuple[str, str]:
        status = await second_control.check_agent(task_id)
        sent = await second_control.send_input(task_id, "follow up")
        return status, sent

    status, sent = asyncio.run(refresh_and_continue())

    assert "state=completed" in status
    assert "result=remote persisted done" in status
    assert second_client.get_calls == ["remote-task-persisted"]
    assert "Sent input" in sent
    assert second_client.sent_inputs == ["follow up"]
    persisted_after = task_store.get_task(task_id)
    assert persisted_after is not None
    assert persisted_after.run_count == 2
    assert persisted_after.result == "remote continued: follow up"
    task_store.close()


def test_list_agents_returns_current_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测列表能力：主 agent 需要知道当前 runtime 里有哪些异步子任务正在被管理。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, str]:
        started = await control.spawn_agent("background_research", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        await control.wait_agent(task_id)
        listing = await control.list_agents()
        return task_id, listing

    task_id, listing = asyncio.run(scenario())

    assert f"task_id={task_id}" in listing
    assert "agent=background_research" in listing
    assert "name=remote_code_wiki" in listing
    assert "kind=remote_ref" in listing


def test_background_local_task_publishes_terminal_message_to_mailbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "research this",
            parent_thread_id="main-thread",
        )
        assert record.active_run is not None
        await record.active_run
        return mailbox.drain("main-thread")

    messages = asyncio.run(scenario())

    assert len(messages) == 1
    assert messages[0].child_agent_name == "background_research"
    assert messages[0].child_task_id
    assert messages[0].status == "completed"
    assert messages[0].content == "done"


def test_wait_agent_suppresses_mailbox_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "research this",
            parent_thread_id="main-thread",
        )
        await control.wait_agent(record.task_id)
        return mailbox.drain("main-thread")

    messages = asyncio.run(scenario())

    assert messages == []


def test_spawn_unknown_agent_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测未知 agent：tool 参数错误不应直接打断整个主流程。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    message = asyncio.run(control.spawn_agent("missing_agent", "research this"))
    assert "Unknown agent target" in message
    assert "background_research" in message
    assert "remote_code_wiki" in message


def test_build_tools_exposes_available_agent_names(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测工具描述：主 agent 是否知道可用 worker，很大程度取决于工具描述是否带出可选类型。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    tools = control.build_tools()
    spawn_tool = next(tool for tool in tools if tool.name == "spawn_agent")
    assert "background_research" in spawn_tool.description
    assert "remote_code_wiki" in spawn_tool.description
    assert "spawnable via remote gateway" in spawn_tool.description


def test_build_tools_for_agent_limits_spawn_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    main_spec = LocalWorkerSpec(
        name="main",
        description="main agent",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
    )
    background_spec = build_specs()["background_research"]
    extra_spec = LocalWorkerSpec(
        name="extra_worker",
        description="extra helper",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
    )
    remote_refs = build_test_remote_refs()
    other_remote = RemoteRef(
        name="other_remote",
        description="other remote",
        url="https://example.com/other",
        remote_agent_name="other",
    )
    main_spec.delegation_local_worker_specs = {
        "background_research": background_spec,
    }
    main_spec.delegation_remote_refs = remote_refs
    control = async_subagent_runtime.AgentControl(
        {
            "main": main_spec,
            "background_research": background_spec,
            "extra_worker": extra_spec,
        },
        {
            **remote_refs,
            "other_remote": other_remote,
        },
        checkpointer=object(),
        backend=object(),
    )

    tools = control.build_tools_for("main")
    spawn_tool = next(tool for tool in tools if tool.name == "spawn_agent")
    wait_tool = next(tool for tool in tools if tool.name == "wait_agent")
    check_tool = next(tool for tool in tools if tool.name == "check_agent")
    send_tool = next(tool for tool in tools if tool.name == "send_input")
    cancel_tool = next(tool for tool in tools if tool.name == "cancel_agent")
    list_tool = next(tool for tool in tools if tool.name == "list_agents")

    assert "background_research" in spawn_tool.description
    assert "remote_code_wiki" in spawn_tool.description
    assert "extra_worker" not in spawn_tool.description
    assert "other_remote" not in spawn_tool.description

    denied = asyncio.run(
        spawn_tool.ainvoke({"agent_name": "extra_worker", "task": "do this"})
    )
    out_of_scope_record = asyncio.run(
        control.spawn_task("extra_worker", "hidden task")
    )
    other_thread_record = asyncio.run(
        control.spawn_task(
            "background_research",
            "other visible type hidden owner",
            parent_thread_id="other-thread",
        )
    )
    allowed_record = asyncio.run(
        control.spawn_task(
            "background_research",
            "visible task",
            parent_thread_id="main-thread",
        )
    )
    main_config = {"configurable": {"thread_id": "main-thread"}}
    listing = asyncio.run(list_tool.ainvoke({}, config=main_config))
    wait_denied = asyncio.run(
        wait_tool.ainvoke({"task_id": out_of_scope_record.task_id}, config=main_config)
    )
    check_denied = asyncio.run(
        check_tool.ainvoke({"task_id": out_of_scope_record.task_id}, config=main_config)
    )
    same_target_wait_denied = asyncio.run(
        wait_tool.ainvoke({"task_id": other_thread_record.task_id}, config=main_config)
    )
    send_denied = asyncio.run(
        send_tool.ainvoke(
            {
                "task_id": out_of_scope_record.task_id,
                "message": "follow up",
            },
            config=main_config,
        )
    )
    cancel_denied = asyncio.run(
        cancel_tool.ainvoke(
            {"task_id": out_of_scope_record.task_id},
            config=main_config,
        )
    )

    assert "not allowed for 'main'" in denied
    assert "background_research" in denied
    assert "extra_worker" not in listing
    assert "other_remote" not in listing
    assert out_of_scope_record.task_id not in listing
    assert other_thread_record.task_id not in listing
    assert allowed_record.task_id in listing
    assert "not visible to 'main'" in wait_denied
    assert "not visible to 'main'" in check_denied
    assert "not visible to 'main'" in same_target_wait_denied
    assert "not visible to 'main'" in send_denied
    assert "not visible to 'main'" in cancel_denied


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
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    monkeypatch.setenv("REMOTE_CODE_WIKI_TOKEN", "remote-secret")

    remote_control = async_subagent_runtime.AgentControl(
        {
            "code_wiki": LocalWorkerSpec(
                name="code_wiki",
                description="remote code wiki",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=["/sandbox/home/AGENTS.md"],
                skills=["/sandbox/skills/frontend-skill"],
            )
        },
        {},
        checkpointer=object(),
        backend=object(),
        remote_poll_interval=0.01,
    )
    remote_service = GatewayService(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote code wiki",
            }
        },
        runtime=AgentControlGatewayRuntime(remote_control),
    )
    remote_app = create_gateway_app(service=remote_service, bearer_token="remote-secret")
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
        started = await control.spawn_agent("remote_code_wiki", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        status_before = await control.check_agent(task_id)
        status_after = await control.wait_agent(task_id)
        sent = await control.send_input(task_id, "follow up")
        return task_id, status_before, status_after, sent

    task_id, status_before, status_after, sent = asyncio.run(scenario())

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


def test_check_agent_keeps_last_known_state_when_remote_status_temporarily_unavailable() -> None:
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
