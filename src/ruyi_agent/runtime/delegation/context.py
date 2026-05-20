"""
Delegation Context - 跨网关委托上下文

这个模块实现了委托任务在本地 worker 和远端网关之间传递的上下文协议。
它把委托树根、当前深度、任务预算和已访问节点编码到 metadata 中，用于
限制递归委托和防止跨网关调用形成环路。

核心功能：
1. 创建根任务和子任务的委托上下文
2. 从入站 metadata 中解析并校验委托上下文
3. 将委托上下文注入出站 metadata
4. 清理调用方 metadata 中的保留字段
5. 校验节点 ID、深度、预算和访问路径

使用场景：
- 本地任务继续委托子 worker 时继承 root/depth 信息
- 网关把任务转发给远端 agent 时传递委托预算
- 远端网关接收入站任务时校验是否超过深度限制
- 多网关级联时通过 visited_nodes 防止委托环路

数据流：
  build_root_context/build_child_context → inject_context_metadata
    → 远端网关 metadata → parse_inbound_metadata → DelegationContext

关键概念：
- root_id: 委托树的全局根标识
- depth: 当前任务在委托树中的深度
- max_depth: 当前链路允许的最大委托深度
- max_tasks_per_root: 当前委托树允许的最大任务数
- visited_nodes: 委托链路中已经经过的网关节点
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

MetadataScalar = str | int | float | bool | None

CONTEXT_VERSION = 1
CONTEXT_VERSION_FIELD = "_deepagents_context_version"
ROOT_ID_FIELD = "_deepagents_root_id"
DEPTH_FIELD = "_deepagents_depth"
MAX_DEPTH_FIELD = "_deepagents_max_depth"
MAX_TASKS_PER_ROOT_FIELD = "_deepagents_max_tasks_per_root"
VISITED_NODES_FIELD = "_deepagents_visited_nodes"

RESERVED_METADATA_FIELDS = {
    CONTEXT_VERSION_FIELD,
    ROOT_ID_FIELD,
    DEPTH_FIELD,
    MAX_DEPTH_FIELD,
    MAX_TASKS_PER_ROOT_FIELD,
    VISITED_NODES_FIELD,
}

MAX_ROOT_ID_LENGTH = 256
MAX_NODE_ID_LENGTH = 128
MAX_VISITED_NODES = 64


class DelegationContextError(ValueError):
    """委托上下文相关错误的基类"""
    pass


class InvalidDelegationContextError(DelegationContextError):
    """入站委托上下文字段缺失、类型错误或格式非法"""
    pass


class DelegationLoopError(DelegationContextError):
    """当前节点已出现在委托路径中，继续执行会形成环路"""
    pass


class DelegationContextDepthError(DelegationContextError):
    """
    委托深度超过当前允许上限

    Attributes:
        current_depth: 入站上下文中的当前深度
        max_depth: 本地和上游共同允许的最大深度
    """
    def __init__(self, *, current_depth: int, max_depth: int) -> None:
        """保存深度限制错误的上下文字段"""
        self.current_depth = current_depth
        self.max_depth = max_depth
        super().__init__(f"current_depth={current_depth} max_depth={max_depth}")


@dataclass(frozen=True, slots=True)
class DelegationContext:
    """
    委托任务上下文

    记录一棵委托树在跨 worker、跨网关传递时需要携带的控制信息。
    frozen=True 保证上下文创建后不可变，避免下游调用意外修改预算或路径。

    Attributes:
        root_id: 委托树根标识，通常由 node_id 和根 task_id 组成
        depth: 当前任务在委托树中的深度
        max_depth: 当前链路允许的最大委托深度
        max_tasks_per_root: 当前委托树允许的最大任务数
        visited_nodes: 已经经过的网关节点 ID 列表
    """
    root_id: str
    depth: int
    max_depth: int
    max_tasks_per_root: int
    visited_nodes: tuple[str, ...]


def build_root_context(
    *,
    node_id: str,
    task_id: str,
    max_depth: int,
    max_tasks_per_root: int,
) -> DelegationContext:
    """
    创建根任务委托上下文

    根任务是委托树的起点，depth 固定为 1，visited_nodes 从当前节点开始。

    Args:
        node_id: 当前网关节点 ID
        task_id: 根任务 ID
        max_depth: 当前 runtime 允许的最大委托深度
        max_tasks_per_root: 当前 runtime 允许的单树任务预算

    Returns:
        新建的根委托上下文

    Raises:
        InvalidDelegationContextError: node_id 为空或超过长度限制
    """
    node_id = validate_node_id(node_id)
    return DelegationContext(
        root_id=f"{node_id}:{task_id}",
        depth=1,
        max_depth=max_depth,
        max_tasks_per_root=max_tasks_per_root,
        visited_nodes=(node_id,),
    )


def build_child_context(
    parent: DelegationContext,
    *,
    depth: int,
) -> DelegationContext:
    """
    基于父上下文创建子任务上下文

    子任务继承 root、预算和已访问节点，只更新调用方计算出的 depth。

    Args:
        parent: 父任务委托上下文
        depth: 子任务在委托树中的深度

    Returns:
        子任务委托上下文
    """
    return DelegationContext(
        root_id=parent.root_id,
        depth=depth,
        max_depth=parent.max_depth,
        max_tasks_per_root=parent.max_tasks_per_root,
        visited_nodes=parent.visited_nodes,
    )


def parse_inbound_metadata(
    metadata: dict[str, MetadataScalar],
    *,
    node_id: str,
    local_max_depth: int,
    local_max_tasks_per_root: int,
) -> tuple[dict[str, MetadataScalar], DelegationContext | None]:
    """
    解析入站 metadata 中的委托上下文

    如果 metadata 不包含任何保留字段，说明请求不是跨网关委托，返回清理后
    metadata 和 None。如果包含保留字段，则要求上下文字段完整并严格校验。

    Args:
        metadata: 入站请求 metadata
        node_id: 当前网关节点 ID
        local_max_depth: 本地配置的最大委托深度
        local_max_tasks_per_root: 本地配置的单树任务预算

    Returns:
        清理后的用户 metadata，以及解析出的委托上下文或 None

    Raises:
        InvalidDelegationContextError: 上下文字段缺失、格式错误或超出限制
        DelegationLoopError: 当前节点已出现在 visited_nodes 中
        DelegationContextDepthError: 入站 depth 超过有效最大深度
    """
    clean_metadata = strip_reserved_metadata(metadata)
    if not any(field in metadata for field in RESERVED_METADATA_FIELDS):
        return clean_metadata, None

    # 只要出现任意保留字段，就必须携带完整上下文，避免半截 metadata 绕过限制。
    missing_fields = [
        field for field in RESERVED_METADATA_FIELDS if field not in metadata
    ]
    if missing_fields:
        raise InvalidDelegationContextError(
            "Delegation context is incomplete: "
            + ", ".join(sorted(missing_fields))
        )

    version = _require_positive_int(metadata[CONTEXT_VERSION_FIELD])
    if version != CONTEXT_VERSION:
        raise InvalidDelegationContextError(
            f"Unsupported delegation context version: {version}"
        )

    root_id = _require_non_empty_string(
        metadata[ROOT_ID_FIELD],
        field_name=ROOT_ID_FIELD,
        max_length=MAX_ROOT_ID_LENGTH,
    )
    depth = _require_positive_int(metadata[DEPTH_FIELD])
    inbound_max_depth = _require_positive_int(metadata[MAX_DEPTH_FIELD])
    inbound_max_tasks = _require_positive_int(metadata[MAX_TASKS_PER_ROOT_FIELD])
    visited_nodes = _parse_visited_nodes(metadata[VISITED_NODES_FIELD])

    node_id = validate_node_id(node_id)
    # 为什么检查 visited_nodes：跨网关级联时，重复节点意味着路由已经成环。
    if node_id in visited_nodes:
        raise DelegationLoopError(f"Node '{node_id}' already appears in route")

    # 有效预算取上游和本地配置的较小值，下游不能放宽上游已经施加的限制。
    effective_max_depth = min(inbound_max_depth, local_max_depth)
    effective_max_tasks = min(inbound_max_tasks, local_max_tasks_per_root)
    if depth > effective_max_depth:
        raise DelegationContextDepthError(
            current_depth=depth,
            max_depth=effective_max_depth,
        )

    accepted_visited_nodes = (*visited_nodes, node_id)
    if len(accepted_visited_nodes) > MAX_VISITED_NODES:
        raise InvalidDelegationContextError(
            f"Delegation visited nodes exceeds limit: {MAX_VISITED_NODES}"
        )

    return clean_metadata, DelegationContext(
        root_id=root_id,
        depth=depth,
        max_depth=effective_max_depth,
        max_tasks_per_root=effective_max_tasks,
        visited_nodes=accepted_visited_nodes,
    )


def inject_context_metadata(
    metadata: dict[str, Any],
    context: DelegationContext,
) -> dict[str, Any]:
    """
    将委托上下文注入出站 metadata

    Args:
        metadata: 调用方提供的原始 metadata
        context: 需要传递给下游的委托上下文

    Returns:
        包含保留上下文字段的新 metadata
    """
    payload = dict(metadata)
    # 用户 metadata 不允许覆盖协议保留字段，避免篡改 root、depth 或预算。
    for field in RESERVED_METADATA_FIELDS:
        payload.pop(field, None)
    payload.update(
        {
            CONTEXT_VERSION_FIELD: CONTEXT_VERSION,
            ROOT_ID_FIELD: context.root_id,
            DEPTH_FIELD: context.depth,
            MAX_DEPTH_FIELD: context.max_depth,
            MAX_TASKS_PER_ROOT_FIELD: context.max_tasks_per_root,
            VISITED_NODES_FIELD: json.dumps(
                list(context.visited_nodes),
                ensure_ascii=True,
                separators=(",", ":"),
            ),
        }
    )
    return payload


def strip_reserved_metadata(
    metadata: dict[str, MetadataScalar],
) -> dict[str, MetadataScalar]:
    """
    移除 metadata 中的委托协议保留字段

    Args:
        metadata: 原始 metadata

    Returns:
        不包含保留字段的 metadata 副本
    """
    return {
        key: value
        for key, value in metadata.items()
        if key not in RESERVED_METADATA_FIELDS
    }


def validate_node_id(node_id: str) -> str:
    """
    校验网关节点 ID

    Args:
        node_id: 待校验的节点 ID

    Returns:
        原始节点 ID

    Raises:
        InvalidDelegationContextError: node_id 为空、非字符串或超过长度限制
    """
    return _require_non_empty_string(
        node_id,
        field_name="node_id",
        max_length=MAX_NODE_ID_LENGTH,
    )


def _require_positive_int(value: object) -> int:
    """
    要求值为正整数

    Args:
        value: 待校验的值

    Returns:
        校验后的整数

    Raises:
        InvalidDelegationContextError: value 不是大于 0 的 int
    """
    if type(value) is not int or value < 1:
        raise InvalidDelegationContextError("Delegation context integer must be > 0")
    return value


def _require_non_empty_string(
    value: object,
    *,
    field_name: str,
    max_length: int,
) -> str:
    """
    要求值为非空字符串且不超过长度限制

    Args:
        value: 待校验的值
        field_name: 错误信息中使用的字段名
        max_length: 允许的最大字符串长度

    Returns:
        校验后的字符串

    Raises:
        InvalidDelegationContextError: value 类型、空值或长度不合法
    """
    if not isinstance(value, str):
        raise InvalidDelegationContextError(f"{field_name} must be a string")
    if not value.strip():
        raise InvalidDelegationContextError(f"{field_name} must be non-empty")
    if len(value) > max_length:
        raise InvalidDelegationContextError(
            f"{field_name} exceeds max length {max_length}"
        )
    return value


def _parse_visited_nodes(value: object) -> tuple[str, ...]:
    """
    解析 visited_nodes 字段

    visited_nodes 在 metadata 中以 JSON 字符串传递，解析后必须是非空字符串
    列表，并且每个节点 ID 都满足长度限制。

    Args:
        value: metadata 中的 visited_nodes 原始值

    Returns:
        已校验的节点 ID 元组

    Raises:
        InvalidDelegationContextError: JSON、列表结构或节点 ID 不合法
    """
    if not isinstance(value, str):
        raise InvalidDelegationContextError(
            f"{VISITED_NODES_FIELD} must be a JSON string"
        )
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError as exc:
        raise InvalidDelegationContextError(
            f"{VISITED_NODES_FIELD} must be valid JSON"
        ) from exc
    if not isinstance(parsed, list):
        raise InvalidDelegationContextError(
            f"{VISITED_NODES_FIELD} must decode to a list"
        )
    if not parsed:
        raise InvalidDelegationContextError(
            f"{VISITED_NODES_FIELD} must be non-empty"
        )
    if len(parsed) > MAX_VISITED_NODES:
        raise InvalidDelegationContextError(
            f"{VISITED_NODES_FIELD} exceeds limit {MAX_VISITED_NODES}"
        )
    nodes = tuple(
        _require_non_empty_string(
            item,
            field_name=VISITED_NODES_FIELD,
            max_length=MAX_NODE_ID_LENGTH,
        )
        for item in parsed
    )
    return nodes
