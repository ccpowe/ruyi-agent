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


def _adapter_ast_violations(tree: ast.AST) -> set[str]:
    violations: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "AsyncClient":
            violations.add("AsyncClient")
        if isinstance(node, ast.Attribute) and node.attr == "AsyncClient":
            violations.add("AsyncClient")
        if isinstance(node, ast.Attribute) and node.attr in {
            "aiter_raw",
            "request",
            "stream",
        }:
            violations.add(node.attr)
        if isinstance(node, ast.Constant) and node.value in {
            "Accept",
            "Accept-Encoding",
            "Last-Event-ID",
            "Idempotency-Key",
            "text/event-stream",
        }:
            violations.add(str(node.value))
        if isinstance(node, ast.Name) and node.id in {
            "decode_strict_json_bytes",
            "iter_gateway_task_events",
            "iter_utf8_sse_lines",
            "normalize_gateway_error_payload",
        }:
            violations.add(node.id)
    return violations


def test_protocol_package_is_the_only_gateway_protocol_boundary() -> None:
    assert all(not path.exists() for path in REMOVED)

    protocol_files = sorted(PROTOCOL.glob("*.py"))
    assert protocol_files
    assert {path.name for path in protocol_files} == {
        "__init__.py",
        "client.py",
        "contracts.py",
        "cursor.py",
        "dto.py",
        "projection.py",
        "sse.py",
        "transport.py",
    }
    assert (
        sum(
            len(path.read_text(encoding="utf-8").splitlines())
            for path in protocol_files
        )
        <= 2075
    )
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) < 700
        for path in protocol_files
    )
    per_file_caps = {
        "__init__.py": 12,
        "client.py": 180,
        "contracts.py": 235,
        "cursor.py": 65,
        "dto.py": 255,
        "projection.py": 425,
        "sse.py": 575,
        "transport.py": 340,
    }
    assert all(
        len(path.read_text(encoding="utf-8").splitlines()) <= per_file_caps[path.name]
        for path in protocol_files
    )

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

    contracts_modules = _imported_modules(PROTOCOL / "contracts.py")
    assert all(
        module.split(".", 1)[0]
        in "__future__ asyncio collections dataclasses datetime json re typing".split()
        for module in contracts_modules
    )
    for adapter in (
        SRC / "channels" / "gateway_client.py",
        SRC / "integrations" / "a2a" / "client.py",
    ):
        assert not _adapter_ast_violations(ast.parse(adapter.read_text()))

    read_json_tree = ast.parse((PROTOCOL / "transport.py").read_text())
    read_json = next(
        node
        for node in ast.walk(read_json_tree)
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "_read_json_response"
    )
    read_json_attrs = {
        node.attr for node in ast.walk(read_json) if isinstance(node, ast.Attribute)
    }
    assert "aiter_bytes" in read_json_attrs
    assert not {"aiter_raw", "aread"} & read_json_attrs

    sse_chunks = next(
        node
        for node in ast.walk(read_json_tree)
        if isinstance(node, ast.FunctionDef) and node.name == "_sse_chunks"
    )
    assert any(
        isinstance(node, ast.Attribute) and node.attr == "aiter_raw"
        for node in ast.walk(sse_chunks)
    )


def test_protocol_adapter_ast_guard_has_a_forbidden_and_approved_self_check() -> None:
    forbidden = ast.parse(
        "import httpx\n"
        "async def fetch(client):\n"
        "    return await client.request('GET', '/data')\n"
        "    return httpx.AsyncClient()\n"
    )
    violations = _adapter_ast_violations(forbidden)
    assert {"AsyncClient", "request"} <= violations
    approved = ast.parse(
        "def build(token, client):\n"
        "    headers = client.headers(token)\n"
        "    return client.stream_raw('/artifact', headers=headers)\n"
    )
    assert not _adapter_ast_violations(approved)


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
