from __future__ import annotations

import sqlite3

from ruyi_agent.storage.task_codecs import (
    TASK_SELECT_COLUMNS,
    TASK_WRITE_COLUMNS,
    row_to_task_record,
    task_insert_sql,
    task_record_values,
)
from ruyi_agent.storage.task_database import TaskDatabase
from ruyi_agent.task_models import TaskRecord


class TaskRootBudgetExceededError(ValueError):
    """A persisted delegation tree has reached its cumulative Task limit."""

    def __init__(
        self,
        *,
        root_task_id: str,
        current_count: int,
        max_tasks_per_root: int,
    ) -> None:
        self.root_task_id = root_task_id
        self.current_count = current_count
        self.max_tasks_per_root = max_tasks_per_root
        super().__init__(
            "root_task_id="
            f"{root_task_id} current_count={current_count} "
            f"max_tasks_per_root={max_tasks_per_root}"
        )


class StoredTaskAlreadyExistsError(sqlite3.IntegrityError):
    """A stable Task identity already exists in persistent storage."""

    def __init__(self, record: TaskRecord) -> None:
        self.record = record
        super().__init__(f"Task already exists: {record.task_id}")


class TaskRepository:
    """Persist Task records; callers compose cross-resource transactions."""

    def __init__(self, database: TaskDatabase) -> None:
        self._database = database

    def save(self, record: TaskRecord) -> None:
        with self._database.transaction(immediate=True) as connection:
            if not self.exists_locked(connection, record.task_id):
                self.enforce_root_budget_locked(connection, record)
            connection.execute(task_insert_sql(upsert=True), task_record_values(record))

    def insert(self, record: TaskRecord) -> None:
        with self._database.transaction(immediate=True) as connection:
            self.ensure_new_identity_locked(connection, record.task_id)
            self.enforce_root_budget_locked(connection, record)
            self.insert_locked(connection, record)

    def update(self, record: TaskRecord) -> None:
        with self._database.transaction() as connection:
            self.update_locked(connection, record)

    def get(self, task_id: str) -> TaskRecord | None:
        with self._database.locked_connection() as connection:
            return self.get_locked(connection, task_id)

    def get_by_parent_thread_id(
        self,
        *,
        task_id: str,
        parent_thread_id: str,
    ) -> TaskRecord | None:
        with self._database.locked_connection() as connection:
            row = connection.execute(
                f"""
                SELECT {TASK_SELECT_COLUMNS}
                FROM agent_tasks
                WHERE task_id = ? AND parent_thread_id = ?
                """,
                (task_id, parent_thread_id),
            ).fetchone()
        return row_to_task_record(row) if row is not None else None

    def list_all(self) -> list[TaskRecord]:
        with self._database.locked_connection() as connection:
            rows = connection.execute(
                f"SELECT {TASK_SELECT_COLUMNS} FROM agent_tasks"
            ).fetchall()
        return [row_to_task_record(row) for row in rows]

    def list_by_parent_thread_id(self, parent_thread_id: str) -> list[TaskRecord]:
        with self._database.locked_connection() as connection:
            rows = connection.execute(
                f"""
                SELECT {TASK_SELECT_COLUMNS}
                FROM agent_tasks
                WHERE parent_thread_id = ?
                """,
                (parent_thread_id,),
            ).fetchall()
        return [row_to_task_record(row) for row in rows]

    def count_under_root(self, root_task_id: str) -> int:
        with self._database.locked_connection() as connection:
            row = connection.execute(
                "SELECT COUNT(*) FROM agent_tasks WHERE root_task_id = ?",
                (root_task_id,),
            ).fetchone()
        assert row is not None
        return int(row[0])

    @staticmethod
    def insert_locked(connection: sqlite3.Connection, record: TaskRecord) -> None:
        connection.execute(task_insert_sql(upsert=False), task_record_values(record))

    @staticmethod
    def update_locked(connection: sqlite3.Connection, record: TaskRecord) -> None:
        assignments = ", ".join(
            f"{column} = ?" for column in TASK_WRITE_COLUMNS[1:]
        )
        values = task_record_values(record)
        cursor = connection.execute(
            f"UPDATE agent_tasks SET {assignments} WHERE task_id = ?",
            (*values[1:], record.task_id),
        )
        if cursor.rowcount != 1:
            raise KeyError(f"Task does not exist: {record.task_id}")

    @staticmethod
    def get_locked(
        connection: sqlite3.Connection,
        task_id: str,
    ) -> TaskRecord | None:
        row = connection.execute(
            f"SELECT {TASK_SELECT_COLUMNS} FROM agent_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        return row_to_task_record(row) if row is not None else None

    @staticmethod
    def exists_locked(connection: sqlite3.Connection, task_id: str) -> bool:
        row = connection.execute(
            "SELECT 1 FROM agent_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        return row is not None

    @staticmethod
    def ensure_new_identity_locked(
        connection: sqlite3.Connection,
        task_id: str,
    ) -> None:
        record = TaskRepository.get_locked(connection, task_id)
        if record is not None:
            raise StoredTaskAlreadyExistsError(record)

    @staticmethod
    def enforce_root_budget_locked(
        connection: sqlite3.Connection,
        record: TaskRecord,
    ) -> None:
        candidate_limit = record.delegation_max_tasks_per_root
        if candidate_limit is None:
            return
        if candidate_limit < 1:
            raise ValueError("delegation_max_tasks_per_root must be at least 1")
        stored_limit_row = connection.execute(
            """
            SELECT MIN(delegation_max_tasks_per_root)
            FROM agent_tasks
            WHERE root_task_id = ?
              AND delegation_max_tasks_per_root IS NOT NULL
            """,
            (record.root_task_id,),
        ).fetchone()
        stored_limit = (
            int(stored_limit_row[0])
            if stored_limit_row is not None and stored_limit_row[0] is not None
            else candidate_limit
        )
        effective_limit = min(candidate_limit, stored_limit)
        count_row = connection.execute(
            "SELECT COUNT(*) FROM agent_tasks WHERE root_task_id = ?",
            (record.root_task_id,),
        ).fetchone()
        assert count_row is not None
        current_count = int(count_row[0])
        if current_count >= effective_limit:
            raise TaskRootBudgetExceededError(
                root_task_id=record.root_task_id,
                current_count=current_count,
                max_tasks_per_root=effective_limit,
            )
