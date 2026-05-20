from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

from rich.console import Console

from ruyi_agent.channels.cli.approval_presenter import ApprovalPresenter
from ruyi_agent.channels.cli.event_adapter import (
    extract_interrupt_requests,
    runtime_event_from_stream_chunk,
)
from ruyi_agent.channels.cli.commands import COMMAND_NAMES, CliState, SlashCommandHandler
from ruyi_agent.channels.cli.interactive import _stream_turn_once
from ruyi_agent.channels.cli.interactive import _has_pending_task_review
from ruyi_agent.channels.cli.renderer import InteractiveRenderer
from ruyi_agent.control_plane.contracts import (
    ReviewActionSnapshot,
    ReviewDecision,
    ReviewDecisionKind,
    ReviewSnapshot,
)
from ruyi_agent.runtime.events import RuntimeEventKind


def test_runtime_event_from_model_content_chunk() -> None:
    event = runtime_event_from_stream_chunk(
        {
            "type": "updates",
            "ns": (),
            "data": {
                "model": {
                    "messages": [
                        {
                            "content": "hello",
                            "tool_calls": [],
                        }
                    ]
                }
            },
        }
    )

    assert event is not None
    assert event.kind == RuntimeEventKind.CONTENT_UPDATE
    assert event.text == "hello"


def test_runtime_event_from_tool_call_chunk() -> None:
    event = runtime_event_from_stream_chunk(
        {
            "type": "updates",
            "ns": ("planner",),
            "data": {
                "model": {
                    "messages": [
                        {
                            "content": "",
                            "tool_calls": [
                                {
                                    "name": "read_file",
                                    "args": {"path": "main.py"},
                                }
                            ],
                        }
                    ]
                }
            },
        }
    )

    assert event is not None
    assert event.kind == RuntimeEventKind.TOOL_CALL_STARTED
    assert event.namespace == "planner"
    assert "read_file" in event.text


def test_command_names_include_first_mvp_surface() -> None:
    assert {
        "/help",
        "/exit",
        "/clear",
        "/agent",
        "/thread",
        "/new",
        "/tasks",
        "/reviews",
        "/status",
    }.issubset(set(COMMAND_NAMES))


def test_extract_interrupt_requests_handles_nested_interrupts() -> None:
    review_payload = {
        "action_requests": [{"name": "shell", "args": {"cmd": "git status"}}],
        "review_configs": [{"allowed_decisions": ["approve", "reject"]}],
    }

    extracted = extract_interrupt_requests(
        {
            "node": [
                {
                    "__interrupt__": [
                        SimpleNamespace(value=review_payload),
                    ]
                }
            ]
        }
    )

    assert extracted == [review_payload]


def test_slash_command_thread_and_agent_switching() -> None:
    renderer = _FakeRenderer()
    state = CliState(agent_name="main", thread_id="thread-1")
    handler = SlashCommandHandler(
        runtime=_FakeRuntime(),
        state=state,
        renderer=renderer,
        review_control=_FakeReviewControl([]),
        resolve_reviews=_noop_async,
    )

    asyncio.run(handler.execute("/thread thread-2"))
    asyncio.run(handler.execute("/agent worker"))

    assert state.thread_id == "thread-2"
    assert state.agent_name == "worker"


def test_reviews_command_lets_resolver_render_pending_reviews() -> None:
    renderer = _FakeRenderer()
    state = CliState(agent_name="main", thread_id="thread-1")
    calls = {"resolve_reviews": 0}

    async def resolve_reviews() -> None:
        calls["resolve_reviews"] += 1

    handler = SlashCommandHandler(
        runtime=_FakeRuntime(),
        state=state,
        renderer=renderer,
        review_control=_FakeReviewControl([object()]),
        resolve_reviews=resolve_reviews,
    )

    asyncio.run(handler.execute("/reviews"))

    assert calls["resolve_reviews"] == 1
    assert renderer.render_reviews_calls == 0


def test_stream_turn_checks_pending_reviews_after_rendered_events() -> None:
    calls = {"resolve_reviews": 0}

    async def resolve_reviews() -> None:
        calls["resolve_reviews"] += 1

    asyncio.run(
        _stream_turn_once(
            agent=_FakeStreamAgent(),
            payload={"messages": []},
            config={},
            renderer=_FakeRenderer(),
            resolve_task_reviews=resolve_reviews,
        )
    )

    assert calls["resolve_reviews"] == 1


def test_has_pending_task_review_ignores_root_and_skipped_reviews() -> None:
    review_control = _FakeReviewControl(
        [
            SimpleNamespace(review_id="root-review", task_id=""),
            SimpleNamespace(review_id="skipped-review", task_id="task-1"),
            SimpleNamespace(review_id="task-review", task_id="task-2"),
        ]
    )

    assert _has_pending_task_review(review_control, {"skipped-review"})
    assert not _has_pending_task_review(
        review_control,
        {"skipped-review", "task-review"},
    )


def test_review_prompt_omits_args_until_details() -> None:
    console = Console(record=True, width=100)
    renderer = InteractiveRenderer(console)
    review = _make_review()

    renderer.render_review_prompt(review)
    prompt_text = console.export_text()

    assert "Approval Required" in prompt_text
    assert "web_search_exa" in prompt_text
    assert "latest US breaking news headlines today" not in prompt_text

    console.clear()
    renderer.render_review_details(review)
    details_text = console.export_text()

    assert "Approval Details" in details_text
    assert "latest US breaking news headlines today" in details_text


def test_approval_presenter_uses_transient_prompt_by_default() -> None:
    console = Console(record=True, width=100)
    renderer = InteractiveRenderer(console)
    prompt_session = _FakePromptSession(["a"])
    presenter = ApprovalPresenter(renderer, prompt_session=prompt_session)

    decisions = asyncio.run(presenter.request_decisions(_make_review()))
    text = console.export_text()

    assert decisions is not None
    assert decisions[0].decision == ReviewDecisionKind.APPROVE
    assert text == ""
    assert prompt_session.app.erase_when_done
    assert "Approval required: web_search_exa" in prompt_session.bottom_toolbars[0]
    assert "latest US breaking news headlines today" not in prompt_session.bottom_toolbars[0]


def test_transcript_renderer_labels_user_and_approval_summary() -> None:
    console = Console(record=True, width=100)
    renderer = InteractiveRenderer(console)
    review = _make_review()

    renderer.render_user_message("委派 subagent 去搜集当前美国头条新闻")
    renderer.render_approval_summary(
        review,
        [
            ReviewDecision(
                action_id="call-1",
                decision=ReviewDecisionKind.APPROVE,
            )
        ],
    )
    text = console.export_text()

    assert "You" in text
    assert "委派 subagent 去搜集当前美国头条新闻" in text
    assert "Approval" in text
    assert "approve web_search_exa" in text


def _make_review() -> ReviewSnapshot:
    now = datetime.now(UTC)
    return ReviewSnapshot(
        review_id="review-1",
        task_id="task-123456789",
        agent_name="news_research",
        thread_id="thread-1",
        status="pending",
        actions=[
            ReviewActionSnapshot(
                action_id="call-1",
                tool_name="web_search_exa",
                args={"query": "latest US breaking news headlines today"},
                reason="tool policy for web_search_exa: require_approval",
                allowed_decisions=[
                    ReviewDecisionKind.APPROVE,
                    ReviewDecisionKind.REJECT,
                    ReviewDecisionKind.EDIT,
                ],
            )
        ],
        allowed_decisions=[
            ReviewDecisionKind.APPROVE,
            ReviewDecisionKind.REJECT,
            ReviewDecisionKind.EDIT,
        ],
        reason="tool policy for web_search_exa: require_approval",
        created_at=now,
        updated_at=now,
    )


class _FakeRenderer:
    def __init__(self) -> None:
        self.render_reviews_calls = 0
        self.messages: list[str] = []

    def help(self) -> None:
        pass

    def clear(self) -> None:
        pass

    def info(self, text: str) -> None:
        self.messages.append(text)

    def error(self, text: str) -> None:
        self.messages.append(text)

    def render_error(self, exc: BaseException) -> None:
        self.messages.append(str(exc))

    def render_agents(self, **kwargs) -> None:
        pass

    def render_tasks(self, tasks) -> None:
        pass

    def render_reviews(self, reviews) -> None:
        self.render_reviews_calls += 1

    def render_event(self, event) -> None:
        pass

    def status(self, **kwargs) -> None:
        pass


class _FakePromptApp:
    erase_when_done = False


class _FakePromptSession:
    def __init__(self, responses: list[str]) -> None:
        self.app = _FakePromptApp()
        self._responses = responses
        self.bottom_toolbars: list[str] = []

    async def prompt_async(self, *args, **kwargs) -> str:
        bottom_toolbar = kwargs.get("bottom_toolbar")
        if isinstance(bottom_toolbar, str):
            self.bottom_toolbars.append(bottom_toolbar)
        if not self._responses:
            raise AssertionError("No fake prompt responses left")
        return self._responses.pop(0)


class _FakeWorkerControl:
    def list_task_records(self) -> list[object]:
        return []


class _FakeRuntime:
    checkpoint_db = "checkpoints.sqlite"
    worker_control = _FakeWorkerControl()
    agent_configs = {
        "main": {"description": "main agent"},
        "worker": {"description": "worker agent"},
    }

    def list_local_agent_names(self) -> list[str]:
        return ["main", "worker"]

    def get_local_agent(self, agent_name: str) -> object:
        if agent_name not in self.agent_configs:
            raise ValueError(f"Unknown agent: {agent_name}")
        return object()


class _FakeReviewControl:
    def __init__(self, reviews: list[object]) -> None:
        self._reviews = reviews

    def list_pending_reviews(self) -> list[object]:
        return self._reviews


async def _noop_async() -> None:
    pass


class _FakeStreamAgent:
    async def astream(self, *args, **kwargs):
        yield {
            "type": "updates",
            "ns": (),
            "data": {
                "model": {
                    "messages": [
                        {
                            "content": "hello",
                            "tool_calls": [],
                        }
                    ]
                }
            },
        }
