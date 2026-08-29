from __future__ import annotations

import asyncio
import pytest

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.storage.mailbox_store import MailboxStore

from tests.support.async_subagent_runtime import (
    FakeAgent,
    FakeAgentFactory,
    build_specs,
    build_test_remote_refs,
)


def test_list_agents_returns_current_tasks(monkeypatch: pytest.MonkeyPatch) -> None:
    # 为什么测列表能力：主 agent 需要知道当前 runtime 里有哪些异步子任务正在被管理。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, str]:
        started = await control.spawn_agent("background_research", "research this")
        task_id = started.split("task_id=")[1].split()[0]
        await control.wait_agent(task_id)
        listing = await control.list_agents()
        return task_id, listing

    task_id, listing = asyncio.run(scenario())

    assert f"task_id={task_id}" in listing
    assert "agent=background_research" in listing
    assert "name=remote_code_wiki" in listing
    assert "kind=remote_ref" in listing


def test_background_local_task_publishes_terminal_message_to_mailbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "research this",
            parent_thread_id="main-thread",
        )
        assert control.get_live_run(record.task_id) is not None
        await control.get_live_run(record.task_id)
        return mailbox.drain("main-thread")

    messages = asyncio.run(scenario())

    assert len(messages) == 1
    assert messages[0].child_agent_name == "background_research"
    assert messages[0].child_task_id
    assert messages[0].run_count == 1
    assert messages[0].status == "completed"
    assert messages[0].content == "done"


def test_background_local_task_publishes_each_settled_run_to_mailbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> tuple[list, list]:
        record = await control.spawn_task(
            "background_research",
            "first run",
            parent_thread_id="main-thread",
        )
        assert control.get_live_run(record.task_id) is not None
        await control.get_live_run(record.task_id)
        first_messages = mailbox.drain("main-thread")

        followup = await control.send_task_input(record.task_id, "second run")
        assert control.get_live_run(followup.task_id) is not None
        await control.get_live_run(followup.task_id)
        second_messages = mailbox.drain("main-thread")
        return first_messages, second_messages

    first_messages, second_messages = asyncio.run(scenario())

    assert [message.run_count for message in first_messages] == [1]
    assert [message.run_count for message in second_messages] == [2]
    assert first_messages[0].child_task_id == second_messages[0].child_task_id
    assert first_messages[0].status == second_messages[0].status == "completed"


def test_send_input_to_active_run_queues_mailbox_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class BlockingAgent(FakeAgent):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def ainvoke(self, payload, *, config, version):
            self.calls.append(
                {"payload": payload, "config": config, "version": version}
            )
            self.started.set()
            await self.release.wait()
            return {"messages": [{"role": "assistant", "content": "done"}]}

    agent = BlockingAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> tuple[str, list]:
        record = await control.spawn_task("background_research", "first")
        await agent.started.wait()
        continued = await control.send_task_input(record.task_id, "new constraint")
        continued_state = continued.state
        messages = mailbox.drain(record.thread_id)
        agent.release.set()
        assert control.get_live_run(record.task_id) is not None
        await control.get_live_run(record.task_id)
        return continued_state, messages

    state, messages = asyncio.run(scenario())

    assert state == "running"
    assert [message.content for message in messages] == ["new constraint"]
    assert messages[0].child_task_id is None


def test_send_input_to_settled_task_wakes_mailbox_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mailbox = AgentMailbox()

    class ConsumingAgent(FakeAgent):
        async def ainvoke(self, payload, *, config, version):
            self.calls.append(
                {"payload": payload, "config": config, "version": version}
            )
            if payload == {"messages": []}:
                task_id = config["configurable"]["task_id"]
                thread_id = config["configurable"]["thread_id"]
                messages = mailbox.claim(
                    recipient_task_id=task_id,
                    recipient_thread_id=thread_id,
                )
                mailbox.acknowledge([message.message_id for message in messages])
            await asyncio.sleep(0)
            return {"messages": [{"role": "assistant", "content": "done"}]}

    agent = ConsumingAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: agent,
    )
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> tuple[int, list[dict]]:
        record = await control.spawn_task("background_research", "first")
        assert control.get_live_run(record.task_id) is not None
        await control.get_live_run(record.task_id)
        continued = await control.send_task_input(record.task_id, "follow up")
        assert control.get_live_run(continued.task_id) is not None
        await control.get_live_run(continued.task_id)
        await asyncio.sleep(0)
        return control._task_manager.get_task(record.task_id).run_count, agent.calls

    run_count, calls = asyncio.run(scenario())

    assert run_count == 2
    assert calls[1]["payload"] == {"messages": []}
    assert (
        mailbox.has_triggering_messages(calls[1]["config"]["configurable"]["task_id"])
        is False
    )


def test_restart_loads_persisted_task_and_wakes_pending_mailbox_input(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    db_path = tmp_path / "tasks.sqlite"
    first_task_store = TaskStore(str(db_path))
    first_mailbox_store = MailboxStore(str(db_path))
    first_mailbox = AgentMailbox(first_mailbox_store)
    first_agent = FakeAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: first_agent,
    )
    first_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=first_mailbox,
        task_store=first_task_store,
    )

    async def create_persisted_task() -> tuple[str, str]:
        record = await first_control.spawn_task("background_research", "first")
        assert first_control.get_live_run(record.task_id) is not None
        await first_control.get_live_run(record.task_id)
        await asyncio.sleep(0)
        first_mailbox.publish_input(
            recipient_task_id=record.task_id,
            recipient_thread_id=record.thread_id,
            content="resume after restart",
        )
        return record.task_id, record.thread_id

    task_id, _ = asyncio.run(create_persisted_task())
    first_mailbox_store.close()
    first_task_store.close()

    second_task_store = TaskStore(str(db_path))
    second_mailbox_store = MailboxStore(str(db_path))
    second_mailbox = AgentMailbox(second_mailbox_store)

    class RestartConsumingAgent(FakeAgent):
        async def ainvoke(self, payload, *, config, version):
            self.calls.append(
                {"payload": payload, "config": config, "version": version}
            )
            messages = second_mailbox.claim(
                recipient_task_id=config["configurable"]["task_id"],
                recipient_thread_id=config["configurable"]["thread_id"],
            )
            assert [message.content for message in messages] == ["resume after restart"]
            await asyncio.sleep(0)
            return {"messages": [{"role": "assistant", "content": "resumed"}]}

    restarted_agent = RestartConsumingAgent()
    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        lambda **kwargs: restarted_agent,
    )
    second_control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=second_mailbox,
        task_store=second_task_store,
    )

    async def recover() -> int:
        await second_control.wake_pending_mailbox_tasks()
        record = second_control._task_manager.get_task(task_id)
        assert second_control.get_live_run(record.task_id) is not None
        await second_control.get_live_run(record.task_id)
        return record.run_count

    try:
        assert asyncio.run(recover()) == 2
    finally:
        second_mailbox_store.close()
        second_task_store.close()


def test_wait_agent_suppresses_mailbox_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
    )

    async def scenario() -> list:
        record = await control.spawn_task(
            "background_research",
            "research this",
            parent_thread_id="main-thread",
        )
        await control.wait_agent(record.task_id)
        return mailbox.drain("main-thread")

    messages = asyncio.run(scenario())

    assert messages == []


def test_mailbox_delivery_flags_are_persisted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    db_path = tmp_path / "tasks.sqlite"
    task_store = TaskStore(str(db_path))
    mailbox = AgentMailbox()
    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
        mailbox=mailbox,
        task_store=task_store,
    )

    async def scenario() -> tuple[str, str]:
        delivered = await control.spawn_task(
            "background_research",
            "background result",
            parent_thread_id="main-thread",
        )
        assert control.get_live_run(delivered.task_id) is not None
        await control.get_live_run(delivered.task_id)

        suppressed = await control.spawn_task(
            "background_research",
            "wait for result",
            parent_thread_id="main-thread",
        )
        await control.wait_agent(suppressed.task_id)
        return delivered.task_id, suppressed.task_id

    delivered_task_id, suppressed_task_id = asyncio.run(scenario())
    task_store.close()

    reopened_store = TaskStore(str(db_path))
    try:
        delivered = reopened_store.get_task(delivered_task_id)
        suppressed = reopened_store.get_task(suppressed_task_id)
    finally:
        reopened_store.close()

    assert delivered is not None
    assert delivered.mailbox_delivered is True
    assert suppressed is not None
    assert suppressed.mailbox_suppressed is True


def test_spawn_unknown_agent_raises_clear_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测未知 agent：tool 参数错误不应直接打断整个主流程。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    message = asyncio.run(control.spawn_agent("missing_agent", "research this"))
    assert "Unknown agent target" in message
    assert "background_research" in message
    assert "remote_code_wiki" in message


def test_build_tools_exposes_available_agent_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测工具描述：主 agent 是否知道可用 worker，很大程度取决于工具描述是否带出可选类型。
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)

    control = async_subagent_runtime.AgentControl(
        build_specs(),
        build_test_remote_refs(),
        checkpointer=object(),
        backend=object(),
    )

    tools = control.build_tools()
    spawn_tool = next(tool for tool in tools if tool.name == "spawn_agent")
    assert "background_research" in spawn_tool.description
    assert "remote_code_wiki" in spawn_tool.description
    assert "spawnable via remote gateway" in spawn_tool.description


def test_build_tools_for_agent_limits_spawn_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    main_spec = LocalWorkerSpec(
        name="main",
        description="main agent",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_targets=("background_research", "remote_code_wiki"),
    )
    background_spec = build_specs()["background_research"]
    extra_spec = LocalWorkerSpec(
        name="extra_worker",
        description="extra helper",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
    )
    remote_refs = build_test_remote_refs()
    other_remote = RemoteRef(
        name="other_remote",
        description="other remote",
        url="https://example.com/other",
        remote_agent_name="other",
    )
    control = async_subagent_runtime.AgentControl(
        {
            "main": main_spec,
            "background_research": background_spec,
            "extra_worker": extra_spec,
        },
        {
            **remote_refs,
            "other_remote": other_remote,
        },
        checkpointer=object(),
        backend=object(),
    )

    tools = control.build_tools_for("main")
    spawn_tool = next(tool for tool in tools if tool.name == "spawn_agent")
    wait_tool = next(tool for tool in tools if tool.name == "wait_agent")
    check_tool = next(tool for tool in tools if tool.name == "check_agent")
    send_tool = next(tool for tool in tools if tool.name == "send_input")
    cancel_tool = next(tool for tool in tools if tool.name == "cancel_agent")
    list_tool = next(tool for tool in tools if tool.name == "list_agents")

    assert "background_research" in spawn_tool.description
    assert "remote_code_wiki" in spawn_tool.description
    assert "extra_worker" not in spawn_tool.description
    assert "other_remote" not in spawn_tool.description

    denied = asyncio.run(
        spawn_tool.ainvoke({"agent_name": "extra_worker", "task": "do this"})
    )
    out_of_scope_record = asyncio.run(control.spawn_task("extra_worker", "hidden task"))
    other_thread_record = asyncio.run(
        control.spawn_task(
            "background_research",
            "other visible type hidden owner",
            parent_thread_id="other-thread",
        )
    )
    allowed_record = asyncio.run(
        control.spawn_task(
            "background_research",
            "visible task",
            parent_thread_id="main-thread",
        )
    )
    main_config = {"configurable": {"thread_id": "main-thread"}}
    listing = asyncio.run(list_tool.ainvoke({}, config=main_config))
    wait_denied = asyncio.run(
        wait_tool.ainvoke({"task_id": out_of_scope_record.task_id}, config=main_config)
    )
    check_denied = asyncio.run(
        check_tool.ainvoke({"task_id": out_of_scope_record.task_id}, config=main_config)
    )
    same_target_wait_denied = asyncio.run(
        wait_tool.ainvoke({"task_id": other_thread_record.task_id}, config=main_config)
    )
    send_denied = asyncio.run(
        send_tool.ainvoke(
            {
                "task_id": out_of_scope_record.task_id,
                "message": "follow up",
            },
            config=main_config,
        )
    )
    cancel_denied = asyncio.run(
        cancel_tool.ainvoke(
            {"task_id": out_of_scope_record.task_id},
            config=main_config,
        )
    )

    assert "not allowed for 'main'" in denied
    assert "background_research" in denied
    assert "extra_worker" not in listing
    assert "other_remote" not in listing
    assert out_of_scope_record.task_id not in listing
    assert other_thread_record.task_id not in listing
    assert allowed_record.task_id in listing
    assert "wait_agent may target only your direct children" in wait_denied
    assert "check_agent may target only your direct children" in check_denied
    assert "wait_agent may target only your direct children" in same_target_wait_denied
    assert (
        "send_input may target only your direct parent or direct children"
        in send_denied
    )
    assert "cancel_agent may target only your direct children" in cancel_denied
    assert allowed_record.task_id in wait_denied


def test_compiling_agent_resolves_declared_scope_in_target_order(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict = {}

    def capture_factory(**kwargs):
        captured.update(kwargs)
        return FakeAgent()

    monkeypatch.setattr(
        async_subagent_runtime,
        "create_runtime_agent",
        capture_factory,
    )
    def local_spec(name: str) -> LocalWorkerSpec:
        return LocalWorkerSpec(
            name=name,
            description=name,
            system_prompt="prompt",
            model=object(),
            tools=[],
            memory=[],
            skills=[],
        )

    local_first = local_spec("local_first")
    local_second = local_spec("local_second")
    extra = local_spec("extra")
    remote_first = RemoteRef(
        name="remote_first",
        description="remote first",
        url="https://example.com/first",
        remote_agent_name="first",
    )
    remote_second = RemoteRef(
        name="remote_second",
        description="remote second",
        url="https://example.com/second",
        remote_agent_name="second",
    )
    main = LocalWorkerSpec(
        name="main",
        description="main",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_targets=(
            "local_first",
            "remote_first",
            "local_second",
            "remote_second",
            "local_first",
            "remote_first",
        ),
        system_tools=frozenset({"spawn_agent", "list_agents"}),
    )
    control = async_subagent_runtime.AgentControl(
        {
            "main": main,
            "local_second": local_second,
            "local_first": local_first,
            "extra": extra,
        },
        {
            "remote_second": remote_second,
            "remote_first": remote_first,
        },
        checkpointer=object(),
        backend=object(),
    )

    control._get_or_create_agent("main")

    assert list(captured["local_worker_specs"]) == ["local_first", "local_second"]
    assert list(captured["remote_refs"]) == ["remote_first", "remote_second"]
    assert {tool.name for tool in captured["worker_tools"]} == {
        "spawn_agent",
        "list_agents",
    }


def test_declared_but_unavailable_target_returns_clear_error() -> None:
    main = LocalWorkerSpec(
        name="main",
        description="main",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_targets=("unavailable_worker",),
    )
    control = async_subagent_runtime.AgentControl(
        {"main": main},
        {},
        checkpointer=object(),
        backend=object(),
        unavailable_agents={
            "unavailable_worker": "missing provider credential",
        },
    )
    spawn_tool = next(
        tool for tool in control.build_tools_for("main") if tool.name == "spawn_agent"
    )

    message = asyncio.run(
        spawn_tool.ainvoke(
            {"agent_name": "unavailable_worker", "task": "try unavailable"}
        )
    )

    assert "Agent target 'unavailable_worker' is unavailable" in message
    assert "missing provider credential" in message
    assert "Available:" in message


def test_child_task_can_send_input_to_direct_parent_but_cannot_cancel_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(async_subagent_runtime, "create_runtime_agent", factory)
    parent_spec = LocalWorkerSpec(
        name="parent",
        description="parent",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        delegation_targets=("child",),
    )
    child_spec = LocalWorkerSpec(
        name="child",
        description="child",
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
        system_tools=frozenset({"send_input", "list_agents"}),
    )
    control = async_subagent_runtime.AgentControl(
        {"parent": parent_spec, "child": child_spec},
        {},
        checkpointer=object(),
        backend=object(),
    )

    async def scenario() -> tuple[str, str, str, str]:
        parent = await control.spawn_task("parent", "parent run")
        assert control.get_live_run(parent.task_id) is not None
        await control.get_live_run(parent.task_id)
        child = await control.spawn_task(
            "child",
            "child run",
            parent_task_id=parent.task_id,
            parent_thread_id=parent.thread_id,
        )
        assert control.get_live_run(child.task_id) is not None
        await control.get_live_run(child.task_id)
        tools = control.build_tools_for("child")
        send_tool = next(tool for tool in tools if tool.name == "send_input")
        list_tool = next(tool for tool in tools if tool.name == "list_agents")
        config = {
            "configurable": {
                "task_id": child.task_id,
                "thread_id": child.thread_id,
                "parent_task_id": parent.task_id,
            }
        }
        listing = await list_tool.ainvoke({}, config=config)
        sent = await send_tool.ainvoke(
            {"task_id": parent.task_id, "message": "need clarification"},
            config=config,
        )
        # Use the same scoped tool set with cancel enabled to exercise direction
        # authorization independently from system-tool filtering.
        scoped_cancel = next(
            tool
            for tool in control._build_tools(
                allowed_targets=set(),
                caller_agent_name="child",
            )
            if tool.name == "cancel_agent"
        )
        denied = await scoped_cancel.ainvoke({"task_id": parent.task_id}, config=config)
        return parent.task_id, listing, sent, denied

    parent_id, listing, sent, denied = asyncio.run(scenario())

    assert parent_id in listing
    assert f"task_id={parent_id}" in sent
    assert "cancel_agent may target only your direct children" in denied
