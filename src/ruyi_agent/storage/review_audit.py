"""
审计日志 - 权限审批与评审事件的持久化存储

这个模块提供基于 SQLite 的追加式审计日志能力，用于记录权限判定、
人工审批和相关评审事件，便于后续追踪、查询和问题排查。

核心功能：
1. 将审计事件以结构化记录写入 SQLite
2. 支持按 review_id / task_id 查询最近事件
3. 统一处理时间序列化和 JSON 负载编码

使用场景：
- 记录工具调用的权限审核链路
- 追踪某个任务或评审单的关键事件
- 为运维、排障和合规审计提供历史数据

数据流：
  事件字段 → ReviewAuditEvent → SQLite INSERT/SELECT → 审计记录对象

关键概念：
- append-only: 审计记录只追加，不在此模块中修改或删除
- payload: 与事件关联的扩展结构化数据
- review_id / task_id: 审计查询的两个主要维度
"""

from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def _now() -> datetime:
    """返回当前 UTC 时间。"""

    return datetime.now(UTC)


def _serialize_datetime(value: datetime) -> str:
    """
    将时间对象序列化为 ISO 8601 字符串

    若传入的是 naive datetime，则按 UTC 解释，避免写入后出现时区歧义。
    """

    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.isoformat()


def _parse_datetime(value: str) -> datetime:
    """
    解析 ISO 8601 时间字符串

    同时兼容 `Z` 结尾和无时区信息的字符串；无时区时默认补为 UTC。
    """

    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed


def _json_dumps(value: Any) -> str | None:
    """将任意 JSON 兼容对象编码为稳定字符串；`None` 保持为空。"""

    if value is None:
        return None
    return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)


def _json_loads(value: str | None) -> Any:
    """将数据库中的 JSON 字符串还原为 Python 对象。"""

    if value is None:
        return None
    return json.loads(value)


@dataclass(slots=True)
class ReviewAuditEvent:
    """
    单条审计事件记录

    该数据类同时承担内存中的领域对象和数据库行的结构化映射。

    Attributes:
        audit_id: 审计事件唯一标识
        event_type: 事件类型
        created_at: 事件创建时间
        source: 事件来源
        review_id: 关联的评审单 ID
        task_id: 关联的任务 ID
        thread_id: 关联的线程 ID
        agent_name: 触发事件的 agent 名称
        profile_name: 生效的权限 profile 名称
        backend_kind: 后端类型
        workspace_root: 工作区根目录
        tool_name: 关联工具名称
        tool_call_id: 工具调用 ID
        policy_decision: 权限决策结果
        risk: 风险分类标签
        reason: 事件原因说明
        payload: 扩展负载数据
    """

    audit_id: str
    event_type: str
    created_at: datetime
    source: str
    review_id: str | None = None
    task_id: str | None = None
    thread_id: str | None = None
    agent_name: str | None = None
    profile_name: str | None = None
    backend_kind: str | None = None
    workspace_root: str | None = None
    tool_name: str | None = None
    tool_call_id: str | None = None
    policy_decision: str | None = None
    risk: str | None = None
    reason: str | None = None
    payload: dict[str, Any] | None = None


class ReviewAuditStore:
    """
    基于 SQLite 的追加式审计日志存储

    该类负责初始化数据库、写入事件、按常见维度查询事件，并保证在多线程场景下
    复用同一连接时具备基本的串行化保护。

    主要功能：
    - append: 追加写入一条审计事件
    - list_events: 查询最近的审计事件
    - close: 关闭底层数据库连接

    设计要点：
    - 使用单连接 + RLock，适合轻量本地服务内的顺序审计写入
    - 文件库启用 WAL，提高读写并发时的可用性

    Attributes:
        _db_path: SQLite 数据库路径
        _lock: 串行化数据库访问的可重入锁
        _conn: 复用的 SQLite 连接
    """

    def __init__(self, db_path: str) -> None:
        """
        初始化审计存储

        Args:
            db_path: SQLite 数据库文件路径；可为 `:memory:`
        """

        self._db_path = db_path
        self._ensure_parent_dir()
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self._db_path,
            check_same_thread=False,
            timeout=30.0,
        )
        self._init_db()

    def append(
        self,
        event_type: str,
        *,
        source: str,
        review_id: str | None = None,
        task_id: str | None = None,
        thread_id: str | None = None,
        agent_name: str | None = None,
        profile_name: str | None = None,
        backend_kind: str | None = None,
        workspace_root: str | None = None,
        tool_name: str | None = None,
        tool_call_id: str | None = None,
        policy_decision: str | None = None,
        risk: str | None = None,
        reason: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> ReviewAuditEvent:
        """
        追加写入一条审计事件

        Args:
            event_type: 事件类型
            source: 事件来源
            review_id: 关联的评审单 ID
            task_id: 关联的任务 ID
            thread_id: 关联的线程 ID
            agent_name: 关联的 agent 名称
            profile_name: 生效的权限 profile
            backend_kind: 后端类型
            workspace_root: 工作区根目录
            tool_name: 关联工具名称
            tool_call_id: 工具调用 ID
            policy_decision: 权限决策结果
            risk: 风险标签
            reason: 原因说明
            payload: 额外结构化数据

        Returns:
            已写入的审计事件对象
        """

        event = ReviewAuditEvent(
            audit_id=str(uuid.uuid4()),
            event_type=event_type,
            created_at=_now(),
            source=source,
            review_id=review_id,
            task_id=task_id,
            thread_id=thread_id,
            agent_name=agent_name,
            profile_name=profile_name,
            backend_kind=backend_kind,
            workspace_root=workspace_root,
            tool_name=tool_name,
            tool_call_id=tool_call_id,
            policy_decision=policy_decision,
            risk=risk,
            reason=reason,
            payload=payload,
        )
        with self._lock:
            # 审计日志强调“先落盘再返回”，因此每次 append 后立即提交事务。
            self._conn.execute(
                """
                INSERT INTO review_audit_events (
                    audit_id,
                    event_type,
                    created_at,
                    source,
                    review_id,
                    task_id,
                    thread_id,
                    agent_name,
                    profile_name,
                    backend_kind,
                    workspace_root,
                    tool_name,
                    tool_call_id,
                    policy_decision,
                    risk,
                    reason,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.audit_id,
                    event.event_type,
                    _serialize_datetime(event.created_at),
                    event.source,
                    event.review_id,
                    event.task_id,
                    event.thread_id,
                    event.agent_name,
                    event.profile_name,
                    event.backend_kind,
                    event.workspace_root,
                    event.tool_name,
                    event.tool_call_id,
                    event.policy_decision,
                    event.risk,
                    event.reason,
                    _json_dumps(event.payload),
                ),
            )
            self._conn.commit()
        return event

    def list_events(
        self,
        *,
        review_id: str | None = None,
        task_id: str | None = None,
        limit: int = 100,
    ) -> list[ReviewAuditEvent]:
        """
        查询最近的审计事件

        支持按 `review_id`、`task_id` 过滤；若都不传，则返回最近的全量事件。

        Args:
            review_id: 可选的评审单过滤条件
            task_id: 可选的任务过滤条件
            limit: 返回记录数上限，最小按 1 处理

        Returns:
            审计事件列表，按创建时间倒序排列
        """

        clauses: list[str] = []
        params: list[Any] = []
        if review_id is not None:
            clauses.append("review_id = ?")
            params.append(review_id)
        if task_id is not None:
            clauses.append("task_id = ?")
            params.append(task_id)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        params.append(max(limit, 1))
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT
                    audit_id,
                    event_type,
                    created_at,
                    source,
                    review_id,
                    task_id,
                    thread_id,
                    agent_name,
                    profile_name,
                    backend_kind,
                    workspace_root,
                    tool_name,
                    tool_call_id,
                    policy_decision,
                    risk,
                    reason,
                    payload_json
                FROM review_audit_events
                {where}
                ORDER BY created_at DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def close(self) -> None:
        """关闭底层 SQLite 连接。"""

        with self._lock:
            self._conn.close()

    def _ensure_parent_dir(self) -> None:
        """确保数据库文件所在目录存在。"""

        parent = Path(self._db_path).expanduser().resolve().parent
        parent.mkdir(parents=True, exist_ok=True)

    def _init_db(self) -> None:
        """
        初始化数据库结构和索引

        仅在首次打开数据库时创建表与索引；对磁盘数据库启用 WAL，
        以减少追加写入与查询并发时的锁冲突。
        """

        with self._lock:
            self._conn.execute("PRAGMA busy_timeout = 30000")
            if self._db_path != ":memory:":
                # 审计日志是高频追加写入场景，WAL 比默认 journal 更适合读写并发。
                self._conn.execute("PRAGMA journal_mode = WAL")
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS review_audit_events (
                    audit_id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    review_id TEXT,
                    task_id TEXT,
                    thread_id TEXT,
                    agent_name TEXT,
                    profile_name TEXT,
                    backend_kind TEXT,
                    workspace_root TEXT,
                    tool_name TEXT,
                    tool_call_id TEXT,
                    policy_decision TEXT,
                    risk TEXT,
                    reason TEXT,
                    payload_json TEXT
                )
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_review_audit_review_id
                ON review_audit_events(review_id)
                """
            )
            self._conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_review_audit_task_id
                ON review_audit_events(task_id)
                """
            )
            self._conn.commit()

    def _row_to_event(self, row: tuple[Any, ...]) -> ReviewAuditEvent:
        """将 SQLite 查询结果行转换为审计事件对象。"""

        return ReviewAuditEvent(
            audit_id=row[0],
            event_type=row[1],
            created_at=_parse_datetime(row[2]),
            source=row[3],
            review_id=row[4],
            task_id=row[5],
            thread_id=row[6],
            agent_name=row[7],
            profile_name=row[8],
            backend_kind=row[9],
            workspace_root=row[10],
            tool_name=row[11],
            tool_call_id=row[12],
            policy_decision=row[13],
            risk=row[14],
            reason=row[15],
            payload=_json_loads(row[16]),
        )
