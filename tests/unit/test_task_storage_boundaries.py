from __future__ import annotations

import ast
from pathlib import Path

from ruyi_agent.runtime.task_events import TaskEventLedger, TaskStreamEvent
from ruyi_agent.storage.task_event_repository import StoredTaskEvent
from ruyi_agent.storage.task_repository import (
    StoredTaskAlreadyExistsError,
    TaskRootBudgetExceededError,
)
from ruyi_agent.storage.task_store import (
    StoredTaskAlreadyExistsError as FacadeAlreadyExistsError,
)
from ruyi_agent.storage.task_store import StoredTaskEvent as FacadeStoredTaskEvent
from ruyi_agent.storage.task_store import (
    TaskRootBudgetExceededError as FacadeBudgetError,
)


ROOT = Path(__file__).parents[2]
PRODUCTION_MODULES = (
    "src/ruyi_agent/storage/task_codecs.py",
    "src/ruyi_agent/storage/task_database.py",
    "src/ruyi_agent/storage/task_event_repository.py",
    "src/ruyi_agent/storage/task_repository.py",
    "src/ruyi_agent/storage/task_review_uow.py",
    "src/ruyi_agent/storage/task_schema.py",
    "src/ruyi_agent/storage/task_store.py",
    "src/ruyi_agent/storage/task_unit_of_work.py",
    "src/ruyi_agent/runtime/task_event_contracts.py",
    "src/ruyi_agent/runtime/task_event_cursor.py",
    "src/ruyi_agent/runtime/task_event_ledger.py",
    "src/ruyi_agent/runtime/task_event_projection.py",
    "src/ruyi_agent/runtime/task_events.py",
)


def test_task_storage_facades_keep_public_type_identity() -> None:
    assert FacadeStoredTaskEvent is StoredTaskEvent
    assert FacadeAlreadyExistsError is StoredTaskAlreadyExistsError
    assert FacadeBudgetError is TaskRootBudgetExceededError
    assert TaskEventLedger.__module__.endswith("task_event_ledger")
    assert TaskStreamEvent.__module__.endswith("gateway_protocol.contracts")


def test_task_storage_modules_stay_below_line_budget() -> None:
    for relative_path in PRODUCTION_MODULES:
        line_count = len((ROOT / relative_path).read_text().splitlines())
        assert line_count < 1000, f"{relative_path} has {line_count} lines"


def test_storage_components_do_not_depend_on_runtime_or_transport() -> None:
    for relative_path in PRODUCTION_MODULES[:8]:
        tree = ast.parse((ROOT / relative_path).read_text())
        imported_modules = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        forbidden = {
            module
            for module in imported_modules
            if module.startswith(
                (
                    "ruyi_agent.channels",
                    "ruyi_agent.gateway",
                    "ruyi_agent.runtime",
                )
            )
        }
        assert not forbidden, f"{relative_path} imports {sorted(forbidden)}"
