from __future__ import annotations

import json
import pytest

from ruyi_agent.runtime.delegation.context import (
    CONTEXT_VERSION,
    CONTEXT_VERSION_FIELD,
    DEPTH_FIELD,
    MAX_DEPTH_FIELD,
    MAX_TASKS_PER_ROOT_FIELD,
    ROOT_ID_FIELD,
    VISITED_NODES_FIELD,
    DelegationContext,
    DelegationContextDepthError,
    DelegationLoopError,
    InvalidDelegationContextError,
    inject_context_metadata,
    parse_inbound_metadata,
)


def build_metadata(**overrides):
    metadata = {
        "channel": "tg",
        CONTEXT_VERSION_FIELD: CONTEXT_VERSION,
        ROOT_ID_FIELD: "node-a:root-1",
        DEPTH_FIELD: 2,
        MAX_DEPTH_FIELD: 3,
        MAX_TASKS_PER_ROOT_FIELD: 20,
        VISITED_NODES_FIELD: json.dumps(["node-a"]),
    }
    metadata.update(overrides)
    return metadata


def test_parse_inbound_metadata_strips_reserved_fields_and_appends_node() -> None:
    clean, context = parse_inbound_metadata(
        build_metadata(),
        node_id="node-b",
        local_max_depth=5,
        local_max_tasks_per_root=10,
    )

    assert clean == {"channel": "tg"}
    assert context == DelegationContext(
        root_id="node-a:root-1",
        depth=2,
        max_depth=3,
        max_tasks_per_root=10,
        visited_nodes=("node-a", "node-b"),
    )


def test_parse_inbound_metadata_rejects_loop() -> None:
    with pytest.raises(DelegationLoopError):
        parse_inbound_metadata(
            build_metadata(**{VISITED_NODES_FIELD: json.dumps(["node-a", "node-b"])}),
            node_id="node-b",
            local_max_depth=5,
            local_max_tasks_per_root=10,
        )


def test_parse_inbound_metadata_rejects_depth_exceeded() -> None:
    with pytest.raises(DelegationContextDepthError):
        parse_inbound_metadata(
            build_metadata(**{DEPTH_FIELD: 4}),
            node_id="node-b",
            local_max_depth=5,
            local_max_tasks_per_root=10,
        )


def test_parse_inbound_metadata_rejects_invalid_visited_nodes() -> None:
    with pytest.raises(InvalidDelegationContextError):
        parse_inbound_metadata(
            build_metadata(**{VISITED_NODES_FIELD: json.dumps([123])}),
            node_id="node-b",
            local_max_depth=5,
            local_max_tasks_per_root=10,
        )


def test_inject_context_metadata_removes_stale_reserved_fields() -> None:
    metadata = inject_context_metadata(
        build_metadata(**{DEPTH_FIELD: 99}),
        DelegationContext(
            root_id="node-a:root-2",
            depth=3,
            max_depth=4,
            max_tasks_per_root=8,
            visited_nodes=("node-a", "node-b"),
        ),
    )

    assert metadata["channel"] == "tg"
    assert metadata[CONTEXT_VERSION_FIELD] == CONTEXT_VERSION
    assert metadata[ROOT_ID_FIELD] == "node-a:root-2"
    assert metadata[DEPTH_FIELD] == 3
    assert metadata[MAX_DEPTH_FIELD] == 4
    assert metadata[MAX_TASKS_PER_ROOT_FIELD] == 8
    assert json.loads(metadata[VISITED_NODES_FIELD]) == ["node-a", "node-b"]
