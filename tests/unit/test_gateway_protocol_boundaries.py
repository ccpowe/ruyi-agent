from __future__ import annotations

import ast
from pathlib import Path

from ruyi_agent.gateway_protocol.contracts import TaskStreamEvent
from ruyi_agent.gateway_protocol.cursor import (
    decode_task_event_cursor,
    encode_task_event_cursor,
)
from ruyi_agent.gateway_protocol.projection import (
    lifecycle_event_data,
    lifecycle_event_type,
)
from ruyi_agent.runtime import task_event_contracts, task_event_cursor
from ruyi_agent.runtime import task_event_projection, task_events


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src" / "ruyi_agent"
PROTOCOL = SRC / "gateway_protocol"
REMOVED = (
    SRC / "gateway" / "_http_transport.py",
    SRC / "gateway" / "sse.py",
    SRC / "channels" / "gateway_dto.py",
)


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    modules: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            modules.add(node.module)
    return modules


def _defined_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef))
    }


def test_protocol_package_is_the_only_gateway_protocol_boundary() -> None:
    assert all(not path.exists() for path in REMOVED)

    protocol_files = sorted(PROTOCOL.glob("*.py"))
    assert protocol_files
    assert sum(len(path.read_text(encoding="utf-8").splitlines()) for path in protocol_files) <= 1500
    assert all(len(path.read_text(encoding="utf-8").splitlines()) < 700 for path in protocol_files)

    forbidden = (
        "ruyi_agent.gateway.",
        "ruyi_agent.channels.",
        "ruyi_agent.integrations.",
        "ruyi_agent.runtime.",
        "ruyi_agent.storage.",
    )
    for path in protocol_files:
        for module in _imported_modules(path):
            assert not module.startswith(forbidden), (path, module)

    old_imports = (
        "ruyi_agent.gateway._http_transport",
        "ruyi_agent.gateway.sse",
        "ruyi_agent.channels.gateway_dto",
    )
    for path in SRC.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        assert not any(module in text for module in old_imports), path


def test_protocol_objects_are_reexported_without_second_event_implementation() -> None:
    assert task_event_contracts.TaskStreamEvent is TaskStreamEvent
    assert task_events.TaskStreamEvent is TaskStreamEvent
    assert task_event_cursor.encode_task_event_cursor is encode_task_event_cursor
    assert task_event_cursor.decode_task_event_cursor is decode_task_event_cursor
    assert task_event_projection.lifecycle_event_data is lifecycle_event_data
    assert task_event_projection.lifecycle_event_type is lifecycle_event_type

    definitions: dict[str, list[Path]] = {}
    for path in SRC.rglob("*.py"):
        for name in _defined_names(path):
            definitions.setdefault(name, []).append(path)
    assert definitions["TaskStreamEvent"] == [PROTOCOL / "contracts.py"]
    assert definitions["GatewayTaskEvent"] == [PROTOCOL / "sse.py"]
    assert definitions["SSEProtocolError"] == [PROTOCOL / "sse.py"]
    assert definitions["GatewayHTTPTransport"] == [PROTOCOL / "transport.py"]
    assert definitions["GatewayProtocolClient"] == [PROTOCOL / "client.py"]
    assert definitions["TaskWebhookEvent"] == [PROTOCOL / "dto.py"]
