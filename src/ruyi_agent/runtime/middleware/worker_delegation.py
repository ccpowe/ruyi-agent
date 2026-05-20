from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from deepagents.middleware._utils import append_to_system_message
from langchain.agents.middleware.types import (
    AgentMiddleware,
    ContextT,
    ModelRequest,
    ModelResponse,
    ResponseT,
)

from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef

# 这段 prompt 只负责教主 agent 如何理解和调度当前 runtime 已登记的下级目标，
# 不再引入 deepagents 默认的 task/general-purpose 语义。
WORKER_DELEGATION_SYSTEM_PROMPT = """## Registered agent targets

You have access to agent control tools that manage delegated work inside the current runtime.

### Tools:
- `spawn_agent`: Start a new local worker task or remote-ref task. Returns a task ID immediately.
- `wait_agent`: Wait for a task to finish when you need the result in the current turn.
- `check_agent`: Check the current status of a task without blocking.
- `send_input`: Continue an existing task on the same thread with additional instructions.
- `cancel_agent`: Cancel a worker task that is no longer needed.
- `list_agents`: List registered targets and tracked worker tasks.

### Workflow:
1. **Start** — Use `spawn_agent` with a valid registered `agent_name` from the lists below.
2. **Wait when needed** — If the current turn depends on the result, call `wait_agent` after spawning.
3. **Check on request** — If the task can run in the background, report the task ID and continue. Use `check_agent` only when status is needed.
4. **Continue** — Use `send_input` to add follow-up instructions after a task finishes a run.
5. **Cancel** — Use `cancel_agent` to stop a task that is no longer useful.

### Critical rules:
- `agent_name` must be chosen exactly from the registered target names below.
- Local workers run inside the current runtime, while remote refs run through their configured remote gateway.
- Remote refs may fail because of network, authentication, or upstream gateway errors.
- Do not invent agent target names.
- If a task is still running, do not call `send_input` again until it finishes or is cancelled.
- Task status in conversation history may be stale. When status matters, call a tool to refresh it.
- For work that must finish before you respond, use `spawn_agent` followed by `wait_agent`.
"""


class WorkerDelegationMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Inject worker and remote-ref delegation guidance into the model system prompt."""

    def __init__(
        self,
        *,
        specs: dict[str, LocalWorkerSpec],
        remote_refs: dict[str, RemoteRef] | None = None,
        build_tools: Callable[[], list[Any]] | None = None,
        system_prompt: str | None = WORKER_DELEGATION_SYSTEM_PROMPT,
    ) -> None:
        super().__init__()
        if not specs and not remote_refs:
            msg = "At least one local worker spec or remote ref must be provided"
            raise ValueError(msg)
        if build_tools is None:
            msg = "Worker delegation tool factory must be provided when targets are configured"
            raise ValueError(msg)

        self._specs = specs
        self._remote_refs = remote_refs or {}
        self.tools = build_tools()
        if not self.tools:
            msg = "Worker delegation tool factory returned no tools"
            raise ValueError(msg)
        if system_prompt:
            self.system_prompt = system_prompt + self._render_available_targets()
        else:
            self.system_prompt = system_prompt

    def _render_available_targets(self) -> str:
        sections: list[str] = []
        if self._specs:
            local_workers = "\n".join(
                f"- {spec.name}: {spec.description}"
                for spec in sorted(self._specs.values(), key=lambda item: item.name)
            )
            sections.append("\n\nAvailable local workers:\n" + local_workers)
        if self._remote_refs:
            remote_refs = "\n".join(
                (
                    f"- {ref.name}: {ref.description} "
                    "(remote_ref, spawnable via remote gateway)"
                )
                for ref in sorted(
                    self._remote_refs.values(), key=lambda item: item.name
                )
            )
            sections.append(
                "\n\nAvailable remote refs:\n"
                + remote_refs
                + "\n\nThese remote refs can be spawned through their configured remote gateway."
            )
        return "".join(sections)

    def wrap_model_call(  # 会导致system_message重复么？
        self,
        request: ModelRequest[ContextT],
        handler: Callable[[ModelRequest[ContextT]], ModelResponse[ResponseT]],
    ) -> ModelResponse[ResponseT]:
        if self.system_prompt is not None:
            # 每次模型调用前都追加一段 delegation 说明，
            # 保证上下文压缩后依然能重新看到可用 worker 类型。
            new_system_message = append_to_system_message(
                request.system_message,
                self.system_prompt,
            )
            return handler(request.override(system_message=new_system_message))
        return handler(request)

    async def awrap_model_call(
        self,
        request: ModelRequest[ContextT],
        handler: Callable[
            [ModelRequest[ContextT]], Awaitable[ModelResponse[ResponseT]]
        ],
    ) -> ModelResponse[ResponseT]:
        if self.system_prompt is not None:
            new_system_message = append_to_system_message(
                request.system_message,
                self.system_prompt,
            )
            return await handler(request.override(system_message=new_system_message))
        return await handler(request)


LocalAsyncSubAgentMiddleware = WorkerDelegationMiddleware
