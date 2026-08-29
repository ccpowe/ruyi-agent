"""Stable contracts and value helpers for the delegation runtime."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field

from ruyi_agent.task_models import (
    ACTIVE_TASK_STATES,
    SETTLED_TASK_STATES,
    PublishedArtifact,
)

# 显式把已登记的目标写进 tool description，
# 这样模型在调用 spawn_agent 时能拿到合法名称，也能区分本地 worker 和远端引用。
SPAWN_AGENT_TOOL_DESCRIPTION = """Start a delegated task and return a task ID immediately when the target is spawnable.

Available local workers:
{available_local_workers}

Available remote refs:
{available_remote_refs}

## Usage notes:
1. `agent_name` must be exactly one of the registered target names above.
2. Use this tool for tasks that can be delegated to a focused local worker or remote ref.
3. If the current turn depends on the result, call `wait_agent` after spawning.
4. If the task can continue in the background, report the task ID and stop.
5. Remote refs run through the remote gateway and may have network or auth failures.
6. Do not invent agent target names.
"""


class SpawnAgentSchema(BaseModel):
    """
    spawn_agent 工具参数 schema

    使用显式 schema 是为了把 agent_name 和 task 的业务语义写进工具参数
    描述，减少模型调用时编造目标名或传入模糊任务的概率。
    """

    # 使用显式 schema 而不是函数签名推断，
    # 是为了把 agent_name/task 的语义描述写得更准确。
    agent_name: str = Field(
        description=(
            "The local worker type to use. Must be one of the available "
            "types listed in the tool description."
        )
    )
    task: str = Field(
        description=("A detailed task description for the worker to execute.")
    )


class TaskIdSchema(BaseModel):
    """只接收 task_id 的工具参数 schema"""

    task_id: str = Field(
        description="The exact task_id string returned by spawn_agent."
    )


class SendInputSchema(BaseModel):
    """send_input 工具参数 schema"""

    task_id: str = Field(
        description="The exact task_id string returned by spawn_agent."
    )
    message: str = Field(
        description="Follow-up instructions or new context for the same task."
    )


class ListAgentsSchema(BaseModel):
    """list_agents 工具的空参数 schema"""

    pass


class UnknownAgentTargetError(ValueError):
    """请求的 agent 目标未在当前 runtime 中注册"""

    pass


class UnavailableAgentTargetError(UnknownAgentTargetError):
    """请求的本地 agent 已配置，但当前 runtime 无法构造其执行定义"""

    pass


class RemoteExecutorNotImplementedError(ValueError):
    """远端执行器能力缺失或暂不可用"""

    pass


class UnknownWorkerTaskError(ValueError):
    """请求的 worker task 不存在或对调用方不可见"""

    pass


class TaskAlreadyRunningError(ValueError):
    """同一个 task 上已有未结束的活跃 run"""

    pass


class DurableTaskMailboxRequiredError(RuntimeError):
    """Idempotent local input requires a persistent Task Mailbox."""

    pass


class MaxDelegationDepthError(ValueError):
    """
    委托深度超过限制

    Attributes:
        current_depth: 当前准备创建的任务深度
        max_depth: 允许的最大委托深度
    """

    def __init__(self, *, current_depth: int, max_depth: int) -> None:
        """保存深度限制错误的上下文字段"""
        self.current_depth = current_depth
        self.max_depth = max_depth
        super().__init__(f"current_depth={current_depth} max_depth={max_depth}")


def _now() -> datetime:
    """返回当前 UTC 时间"""
    # 为什么抽成单独时间入口：任务状态和测试都依赖统一时间语义，避免时间来源散落各处。
    return datetime.now(UTC)


def _validate_remote_task_state(
    task_id: str,
    payload: dict[str, Any],
) -> tuple[str, int]:
    status = payload.get("status")
    if (
        not isinstance(status, str)
        or status not in ACTIVE_TASK_STATES | SETTLED_TASK_STATES
    ):
        raise ValueError(f"Remote task '{task_id}' returned invalid status")
    run_count = payload.get("run_count")
    if (
        not isinstance(run_count, int)
        or isinstance(run_count, bool)
        or run_count < 0
    ):
        raise ValueError(f"Remote task '{task_id}' returned invalid run_count")
    return status, run_count


def _flatten_exception_messages(exc: BaseException) -> list[str]:
    """
    展开异常或异常组中的可读错误信息

    Args:
        exc: 捕获到的异常对象

    Returns:
        展平后的异常摘要列表
    """
    # 为什么单独展开异常组：TaskGroup / ExceptionGroup 的 str() 信息太弱，
    # 必须把真正的子异常拿出来才能定位并发任务失败原因。
    if isinstance(exc, BaseExceptionGroup):
        messages: list[str] = []
        for sub_exc in exc.exceptions:
            messages.extend(_flatten_exception_messages(sub_exc))
        return messages

    message = str(exc).strip()
    if not message:
        message = exc.__class__.__name__
    return [f"{exc.__class__.__name__}: {message}"]


def _format_exception_summary(exc: BaseException) -> str:
    """
    格式化任务失败摘要

    Args:
        exc: 捕获到的异常对象

    Returns:
        去重后的单行错误摘要
    """
    # 为什么统一格式化异常：任务状态里需要稳定、可读、可截断的错误摘要。
    messages = _flatten_exception_messages(exc)
    unique_messages: list[str] = []
    for message in messages:
        if message not in unique_messages:
            unique_messages.append(message)
    return " | ".join(unique_messages)


def _format_interrupted_error(exc: BaseException) -> str:
    """Format a runtime interruption as a stable, user-readable task error."""
    return "Task interrupted: " + _format_exception_summary(exc)


def _parse_task_timestamp(value: Any, *, fallback: datetime) -> datetime:
    """
    解析远端任务时间戳

    远端网关返回的时间可能是 datetime、ISO 字符串或缺失值。这里统一转换为
    timezone-aware datetime，解析失败时保留本地已有时间。

    Args:
        value: 远端返回的时间戳字段
        fallback: 解析失败时使用的本地时间

    Returns:
        解析后的 datetime
    """
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        return fallback
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return fallback
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _artifact_string(artifact: dict[str, Any], key: str) -> str:
    value = artifact.get(key)
    if isinstance(value, str) and value:
        return value
    raise ValueError(f"Artifact field '{key}' must be a non-empty string")


def _artifact_optional_string(artifact: dict[str, Any], key: str) -> str | None:
    value = artifact.get(key)
    if value is None:
        return None
    if isinstance(value, str):
        return value if value else None
    raise ValueError(f"Artifact field '{key}' must be a string or null")


def _artifact_int(artifact: dict[str, Any], key: str) -> int:
    value = artifact.get(key)
    if isinstance(value, int) and value >= 0:
        return value
    raise ValueError(f"Artifact field '{key}' must be a non-negative integer")


def _published_artifact_to_dict(artifact: PublishedArtifact) -> dict[str, Any]:
    return {
        "artifact_id": artifact.artifact_id,
        "path": artifact.path,
        "name": artifact.name,
        "caption": artifact.caption,
        "content_type": artifact.content_type,
        "size": artifact.size,
        "run_count": artifact.run_count,
    }
