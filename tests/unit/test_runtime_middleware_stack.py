from __future__ import annotations

import ruyi_agent.runtime.middleware.stack as stack
from ruyi_agent.runtime.middleware.artifact_publishing import ArtifactPublishingMiddleware
from ruyi_agent.runtime.middleware.ruyi_skills import RuyiSkillsMiddleware
from ruyi_agent.runtime.middleware.stack import build_runtime_middleware


def test_runtime_stack_uses_ruyi_skills_middleware(monkeypatch) -> None:
    monkeypatch.setattr(
        stack,
        "create_summarization_middleware",
        lambda _model, _backend: object(),
    )

    middleware = build_runtime_middleware(
        resolved_model=object(),
        backend=object(),
        skills=None,
        memory=None,
        local_worker_specs=None,
        remote_refs=None,
    )

    assert any(isinstance(item, RuyiSkillsMiddleware) for item in middleware)


def test_runtime_stack_adds_artifact_publishing_middleware_when_enabled(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        stack,
        "create_summarization_middleware",
        lambda _model, _backend: object(),
    )

    middleware = build_runtime_middleware(
        resolved_model=object(),
        backend=object(),
        skills=None,
        memory=None,
        local_worker_specs=None,
        remote_refs=None,
        workspace_root="/workspace",
        register_artifact=lambda *, task_id, artifact: artifact,
    )

    assert any(isinstance(item, ArtifactPublishingMiddleware) for item in middleware)
