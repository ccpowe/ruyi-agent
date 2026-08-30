from __future__ import annotations

import asyncio
from typing import Literal

from langchain_core.messages import AIMessage, AIMessageChunk

from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.runtime.delegation.async_runtime import AgentControl
from ruyi_agent.runtime.skills.types import SkillEntry


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
        self.compile_kwargs: list[dict[str, object]] = []

    def __call__(self, **kwargs):
        self.compile_kwargs.append(dict(kwargs))
        agent = FakeAgent()
        self.created.append(agent)
        return agent


async def wait_for_task_state(
    control: AgentControl,
    task_id: str,
    *,
    states: set[str] | frozenset[str],
    timeout: float = 5.0,
):
    """Wait through the application API without observing a live run handle."""

    async with asyncio.timeout(timeout):
        while True:
            record = control.get_task_record(task_id)
            if record.state in states:
                return record
            await asyncio.sleep(0.01)


class StreamingAgent:
    def __init__(self, *, omit_values: bool = False, fail: bool = False) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self.omit_values = omit_values
        self.fail = fail
        self.stream_calls: list[dict] = []
        self.invoke_calls = 0
        self.state_calls = 0

    async def astream(self, payload, *, config, stream_mode, version):
        self.stream_calls.append(
            {
                "payload": payload,
                "config": config,
                "stream_mode": stream_mode,
                "version": version,
            }
        )
        self.started.set()
        await self.release.wait()
        yield {
            "type": "messages",
            "ns": (),
            "data": (
                AIMessageChunk(content="live "),
                {
                    "provider": "hidden",
                    "langgraph_node": "model",
                    "langgraph_path": ("__pregel_pull", "model"),
                },
            ),
        }
        if self.fail:
            raise RuntimeError("stream exploded")
        if not self.omit_values:
            yield {
                "type": "values",
                "data": {"messages": [AIMessage(content="stream done")]},
                "interrupts": (),
            }

    async def ainvoke(self, payload, *, config, version):
        del payload, config, version
        self.invoke_calls += 1
        raise AssertionError("ainvoke must not run after astream is available")

    async def aget_state(self, config):
        del config
        self.state_calls += 1

        class Snapshot:
            values = {"messages": [AIMessage(content="snapshot done")]}
            interrupts = ()

        return Snapshot()


class StreamingAgentFactory:
    def __init__(self, *, omit_values: bool = False, fail: bool = False) -> None:
        self.omit_values = omit_values
        self.fail = fail
        self.created: list[StreamingAgent] = []

    def __call__(self, **kwargs):
        del kwargs
        agent = StreamingAgent(omit_values=self.omit_values, fail=self.fail)
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
                                    "args": {"urls": ["https://news.ycombinator.com/"]},
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
            return {
                "messages": [{"role": "assistant", "content": "done: needs review"}]
            }
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

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
        raise AssertionError("send_input should not be called in this test")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("cancel_task should not be called in this test")


class AlwaysFailingRemoteA2AClient:
    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
        raise AssertionError("send_input should not be called in this test")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("cancel_task should not be called in this test")


class ShouldNotCallRemoteA2AClient:
    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
        raise AssertionError("remote create_task should not be called")

    async def get_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote get_task should not be called")

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")


class SlowRemoteA2AClient:
    def __init__(self) -> None:
        self.create_calls = 0

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")


class RecordingRemoteA2AClient:
    def __init__(self) -> None:
        self.created_metadata: list[dict[str, object]] = []

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
        raise AssertionError("remote send_input should not be called")

    async def cancel_task(self, remote_ref, *, task_id: str):
        raise AssertionError("remote cancel_task should not be called")


class FailOnceRemoteCreateA2AClient:
    def __init__(self) -> None:
        self.idempotency_keys: list[str | None] = []

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
        del input_content, metadata, attachments
        self.idempotency_keys.append(idempotency_key)
        if len(self.idempotency_keys) == 1:
            raise A2AClientError(
                status_code=400,
                code="invalid_request",
                message="remote create was authoritatively rejected",
            )
        return {
            "task_id": "remote-task-replayed",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-08-29T00:00:00Z",
            "updated_at": "2026-08-29T00:00:01Z",
        }


class CancelledRemoteCreateA2AClient:
    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
        del remote_ref, input_content, metadata, attachments, idempotency_key
        raise asyncio.CancelledError


class SuccessfulRemoteCreateA2AClient:
    def __init__(self) -> None:
        self.idempotency_keys: list[str | None] = []

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
        del input_content, metadata, attachments
        self.idempotency_keys.append(idempotency_key)
        return {
            "task_id": "remote-task-after-restart",
            "agent_name": remote_ref.name,
            "status": "running",
            "last_result": None,
            "error": None,
            "run_count": 1,
            "created_at": "2026-08-29T00:00:00Z",
            "updated_at": "2026-08-29T00:00:01Z",
        }


class ReviewRemoteA2AClient:
    def __init__(self) -> None:
        self.submitted: list[dict[str, object]] = []

    async def create_task(
        self,
        remote_ref,
        *,
        input_content: str,
        metadata: dict,
        attachments=None,
        idempotency_key=None,
    ):
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

    async def send_input(
        self,
        remote_ref,
        *,
        task_id: str,
        input_content: str,
        attachments=None,
        idempotency_key=None,
    ):
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


class UploadResponse:
    def __init__(self, path: str, error: str | None = None) -> None:
        self.path = path
        self.error = error


class UploadBackend:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}

    def upload_files(self, files: list[tuple[str, bytes]]) -> list[UploadResponse]:
        for path, content in files:
            self.files[path] = content
        return [UploadResponse(path) for path, _content in files]


def build_specs() -> dict[str, LocalWorkerSpec]:
    return {
        "background_research": LocalWorkerSpec(
            name="background_research",
            description="background helper",
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=["/sandbox/home/AGENTS.md"],
            skills=["frontend-skill"],
        )
    }


def write_test_skill(tmp_path, name: str) -> SkillEntry:
    skill_dir = tmp_path / name
    skill_dir.mkdir(parents=True)
    skill_dir.joinpath("SKILL.md").write_text(
        f"---\nname: {name}\ndescription: {name} desc\n---\n",
        encoding="utf-8",
    )
    return SkillEntry(
        name=name,
        description=f"{name} desc",
        path=skill_dir,
        source_root=tmp_path,
    )


def build_test_remote_refs(
    *,
    create_idempotency: Literal["none", "ruyi_gateway_v1"] = "none",
) -> dict[str, RemoteRef]:
    return {
        "remote_code_wiki": RemoteRef(
            name="remote_code_wiki",
            description="remote helper",
            url="https://example.com/a2a",
            remote_agent_name="code_wiki",
            auth={"type": "bearer", "token_env": "REMOTE_CODE_WIKI_TOKEN"},
            create_idempotency=create_idempotency,
        )
    }
