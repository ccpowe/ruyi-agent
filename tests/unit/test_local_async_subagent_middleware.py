from __future__ import annotations

from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.runtime.middleware.worker_delegation import WorkerDelegationMiddleware


def test_worker_delegation_lists_local_workers_and_remote_refs() -> None:
    middleware = WorkerDelegationMiddleware(
        specs={
            "background_research": LocalWorkerSpec(
                name="background_research",
                description="background helper",
                system_prompt="prompt",
                model=object(),
                tools=[],
                memory=[],
                skills=[],
            )
        },
        remote_refs={
            "remote_code_wiki": RemoteRef(
                name="remote_code_wiki",
                description="remote helper",
                url="https://example.com/a2a",
                remote_agent_name="code_wiki",
            )
        },
        build_tools=lambda: ["worker-tool"],
    )

    assert middleware.tools == ["worker-tool"]
    assert middleware.system_prompt is not None
    assert "Available local workers" in middleware.system_prompt
    assert "background_research" in middleware.system_prompt
    assert "Available remote refs" in middleware.system_prompt
    assert "remote_code_wiki" in middleware.system_prompt
    assert "spawned through their configured remote gateway" in middleware.system_prompt


def test_worker_delegation_rejects_missing_targets() -> None:
    try:
        WorkerDelegationMiddleware(
            specs={},
            remote_refs={},
            build_tools=lambda: ["worker-tool"],
        )
    except ValueError as exc:
        assert "At least one local worker spec or remote ref" in str(exc)
    else:
        raise AssertionError("expected WorkerDelegationMiddleware to reject no targets")


def test_worker_delegation_rejects_missing_tools() -> None:
    try:
        WorkerDelegationMiddleware(
            specs={
                "background_research": LocalWorkerSpec(
                    name="background_research",
                    description="background helper",
                    system_prompt="prompt",
                    model=object(),
                    tools=[],
                    memory=[],
                    skills=[],
                )
            },
            build_tools=None,
        )
    except ValueError as exc:
        assert "Worker delegation tool factory must be provided" in str(exc)
    else:
        raise AssertionError("expected WorkerDelegationMiddleware to reject no tools")


def test_worker_delegation_rejects_empty_tool_factory_result() -> None:
    try:
        WorkerDelegationMiddleware(
            specs={
                "background_research": LocalWorkerSpec(
                    name="background_research",
                    description="background helper",
                    system_prompt="prompt",
                    model=object(),
                    tools=[],
                    memory=[],
                    skills=[],
                )
            },
            build_tools=lambda: [],
        )
    except ValueError as exc:
        assert "returned no tools" in str(exc)
    else:
        raise AssertionError("expected WorkerDelegationMiddleware to reject no tools")
