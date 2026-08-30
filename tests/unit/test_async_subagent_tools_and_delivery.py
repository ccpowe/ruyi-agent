from __future__ import annotations

import asyncio
import pytest
from langchain_core.tools import StructuredTool

import ruyi_agent.runtime.delegation.async_runtime as async_subagent_runtime
import ruyi_agent.runtime.agent_factory as agent_factory_module
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.storage.task_store import TaskStore
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.runtime.delegation.notifications import SettledRunNotifier
from ruyi_agent.runtime.delegation.policy import DelegationPolicy
from ruyi_agent.runtime.delegation.registry import AgentRegistry
from ruyi_agent.runtime.delegation.run_supervisor import RuntimeClosingError
from ruyi_agent.runtime.delegation.task_manager import TaskManager
from ruyi_agent.runtime.delegation.tools import DelegationTools
from ruyi_agent.runtime.delegation.contracts import (
    UnknownAgentTargetError,
    UnavailableAgentTargetError,
)
from ruyi_agent.task_models import TaskRecord

from tests.support.async_subagent_runtime import (
    FakeAgent,
    FakeAgentFactory,
    build_specs,
    build_test_remote_refs,
    wait_for_task_state,
)


class _CommandPort:
    """Ephemeral four-method command port used by direct tool tests."""

    def __init__(self, manager: TaskManager) -> None:
        self.manager = manager
        self.calls: list[tuple[str, str]] = []
        self.spawn_error: BaseException | None = None

    async def spawn_task(
        self,
        agent_name: str,
        task: str,
        *,
        parent_task_id: str | None = None,
        parent_thread_id: str | None = None,
    ) -> TaskRecord:
        del agent_name, task, parent_task_id, parent_thread_id
        if self.spawn_error is not None:
            raise self.spawn_error
        raise AssertionError("scope test must reject spawn before invoking its port")

    async def refresh_task(self, task_id: str) -> TaskRecord:
        self.calls.append(("refresh", task_id))
        return self.manager.get_task(task_id)

    async def send_task_input(self, task_id: str, message: str) -> TaskRecord:
        self.calls.append(("send", message))
        return self.manager.get_task(task_id)

    async def cancel_task(self, task_id: str) -> TaskRecord:
        self.calls.append(("cancel", task_id))
        return self.manager.get_task(task_id)


def _direct_tools(
    specs: dict[str, LocalWorkerSpec] | None = None,
    remote_refs: dict[str, RemoteRef] | None = None,
    *,
    unavailable_agents: dict[str, str] | None = None,
) -> tuple[TaskManager, DelegationTools, _CommandPort]:
    manager = TaskManager()
    registry = AgentRegistry(
        specs if specs is not None else build_specs(),
        remote_refs if remote_refs is not None else build_test_remote_refs(),
        unavailable_agents,
    )
    policy = DelegationPolicy(
        manager,
        node_id="node-test",
        max_delegation_depth=3,
        max_tasks_per_root=20,
        permission_default_profile="",
    )
    notifier = SettledRunNotifier(manager, None)
    command_port = _CommandPort(manager)
    return manager, DelegationTools(registry, manager, policy, notifier), command_port


def test_list_agents_returns_current_tasks() -> None:
    # 为什么测列表能力：主 agent 需要知道当前 runtime 里有哪些异步子任务正在被管理。
    manager, tools, command_port = _direct_tools()
    record = manager.create_task_record(
        "task-1",
        "background_research",
        parent_task_id=None,
        root_task_id="task-1",
        depth=1,
        parent_thread_id="main-thread",
    )
    list_tool = next(
        tool
        for tool in tools.build_tools(command_port=command_port)
        if tool.name == "list_agents"
    )
    listing = asyncio.run(
        list_tool.ainvoke({}, config={"configurable": {"thread_id": "main-thread"}})
    )

    assert f"task_id={record.task_id}" in listing
    assert "agent=background_research" in listing
    assert "name=remote_code_wiki" in listing
    assert "kind=remote_ref" in listing


def test_background_local_task_publishes_terminal_message_to_mailbox(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
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
        await wait_for_task_state(control, record.task_id, states={"completed"})
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
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
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
        await wait_for_task_state(control, record.task_id, states={"completed"})
        first_messages = mailbox.drain("main-thread")

        followup = await control.send_task_input(record.task_id, "second run")
        mailbox.drain(record.thread_id)
        await wait_for_task_state(control, followup.task_id, states={"completed"})
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
        agent_factory_module,
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
        await wait_for_task_state(control, record.task_id, states={"completed"})
        return continued_state, messages

    state, messages = asyncio.run(scenario())

    assert state == "running"
    assert [message.content for message in messages] == ["new constraint"]
    assert messages[0].child_task_id is None


def test_send_input_to_settled_task_wakes_mailbox_run(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mailbox = AgentMailbox()

    class TwoStageAgent(FakeAgent):
        def __init__(self) -> None:
            super().__init__()
            self.second_started = asyncio.Event()
            self.allow_second_finish = asyncio.Event()
            self.consumed: list = []

        async def ainvoke(self, payload, *, config, version):
            self.calls.append(
                {"payload": payload, "config": config, "version": version}
            )
            if len(self.calls) == 2:
                assert payload == {"messages": []}
                task_id = config["configurable"]["task_id"]
                thread_id = config["configurable"]["thread_id"]
                self.consumed.extend(
                    mailbox.claim(
                        recipient_task_id=task_id,
                        recipient_thread_id=thread_id,
                    )
                )
                assert [message.content for message in self.consumed] == ["follow up"]
                mailbox.acknowledge([message.message_id for message in self.consumed])
                self.second_started.set()
                await self.allow_second_finish.wait()
            return {"messages": [{"role": "assistant", "content": "done"}]}

    agent = TwoStageAgent()
    monkeypatch.setattr(
        agent_factory_module,
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

    async def scenario() -> tuple[int, list[dict], list, list]:
        record = await control.spawn_task("background_research", "first")
        await wait_for_task_state(control, record.task_id, states={"completed"})
        continued = await control.send_task_input(record.task_id, "follow up")
        await asyncio.wait_for(agent.second_started.wait(), timeout=1)
        assert len(agent.calls) == 2
        assert agent.consumed
        empty = mailbox.claim(
            recipient_task_id=record.task_id,
            recipient_thread_id=record.thread_id,
        )
        agent.allow_second_finish.set()
        continued = await wait_for_task_state(
            control, continued.task_id, states={"completed"}
        )
        await control.close()
        return continued.run_count, agent.calls, agent.consumed, empty

    run_count, calls, consumed, empty = asyncio.run(scenario())

    assert run_count == 2
    assert calls[1]["payload"] == {"messages": []}
    assert len(calls) == 2
    assert len(consumed) == 1
    assert empty == []


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
        agent_factory_module,
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
        await wait_for_task_state(first_control, record.task_id, states={"completed"})
        await first_control.close()
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
            second_mailbox.acknowledge([message.message_id for message in messages])
            await asyncio.sleep(0)
            return {"messages": [{"role": "assistant", "content": "resumed"}]}

    restarted_agent = RestartConsumingAgent()
    monkeypatch.setattr(
        agent_factory_module,
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
        assert second_task_store.get_task(task_id).run_count == 1
        await second_control.wake_pending_mailbox_tasks()
        record = await wait_for_task_state(
            second_control, task_id, states={"completed"}
        )
        assert len(restarted_agent.calls) == 1
        return record.run_count

    try:
        assert asyncio.run(recover()) == 2
    finally:
        second_mailbox_store.close()
        second_task_store.close()


def test_wait_agent_suppresses_mailbox_delivery() -> None:
    mailbox = AgentMailbox()
    manager = TaskManager()
    registry = AgentRegistry(build_specs(), build_test_remote_refs())
    policy = DelegationPolicy(
        manager,
        node_id="node-test",
        max_delegation_depth=3,
        max_tasks_per_root=20,
        permission_default_profile="",
    )
    notifier = SettledRunNotifier(manager, mailbox)
    delegation_tools = DelegationTools(registry, manager, policy, notifier)
    command_port = _CommandPort(manager)
    record = manager.create_task_record(
        "child-task",
        "background_research",
        parent_task_id="parent-task",
        root_task_id="parent-task",
        depth=2,
        parent_thread_id="main-thread",
    )
    manager.mark_completed(record.task_id, "done")
    mailbox.publish_settled(
        recipient_thread_id=record.parent_thread_id,
        recipient_task_id=record.parent_task_id,
        child_task_id=record.task_id,
        child_agent_name=record.agent_name,
        run_count=record.run_count,
        status=record.state,
        content=record.result or "",
    )
    wait_tool = next(
        tool
        for tool in delegation_tools.build_tools(command_port=command_port)
        if tool.name == "wait_agent"
    )
    result = asyncio.run(
        wait_tool.ainvoke(
            {"task_id": record.task_id},
            config={"configurable": {"thread_id": "main-thread"}},
        )
    )
    messages = mailbox.drain("main-thread")

    assert "state=completed" in result
    assert messages == []
    assert manager.get_task(record.task_id).mailbox_suppressed is True


def test_mailbox_delivery_flags_are_persisted(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    factory = FakeAgentFactory()
    monkeypatch.setattr(agent_factory_module, "create_runtime_agent", factory)
    db_path = tmp_path / "tasks.sqlite"
    task_store = TaskStore(str(db_path))
    mailbox_store = MailboxStore(str(db_path))
    mailbox = AgentMailbox(mailbox_store)
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
        await wait_for_task_state(control, delivered.task_id, states={"completed"})

        suppressed = await control.spawn_task(
            "background_research",
            "wait for result",
            parent_thread_id="main-thread",
        )
        await wait_for_task_state(control, suppressed.task_id, states={"completed"})
        return delivered.task_id, suppressed.task_id

    delivered_task_id, suppressed_task_id = asyncio.run(scenario())
    manager = TaskManager(task_store, settled_outbox_enabled=True)
    notifier = SettledRunNotifier(manager, mailbox)
    notifier.suppress_mailbox_delivery(manager.get_task(suppressed_task_id))
    mailbox_store.close()
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
    del monkeypatch
    _manager, tools, command_port = _direct_tools()
    command_port.spawn_error = UnknownAgentTargetError(
        "Unknown agent target: missing_agent"
    )
    spawn_tool = next(
        tool
        for tool in tools.build_tools(command_port=command_port)
        if tool.name == "spawn_agent"
    )
    message = asyncio.run(
        spawn_tool.ainvoke({"agent_name": "missing_agent", "task": "research this"})
    )
    assert "Unknown agent target" in message
    assert "background_research" in message
    assert "remote_code_wiki" in message


def test_build_tools_exposes_available_agent_names(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # 为什么测工具描述：主 agent 是否知道可用 worker，很大程度取决于工具描述是否带出可选类型。
    del monkeypatch
    _manager, delegation_tools, command_port = _direct_tools()
    spawn_tool = next(
        tool
        for tool in delegation_tools.build_tools(command_port=command_port)
        if tool.name == "spawn_agent"
    )
    assert "background_research" in spawn_tool.description
    assert "remote_code_wiki" in spawn_tool.description
    assert "spawnable via remote gateway" in spawn_tool.description


def test_build_tools_for_agent_limits_spawn_scope() -> None:
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
    manager, delegation_tools, command_port = _direct_tools(
        {
            "main": main_spec,
            "background_research": background_spec,
            "extra_worker": extra_spec,
        },
        {**remote_refs, "other_remote": other_remote},
    )

    tools = delegation_tools.build_tools_for("main", command_port=command_port)
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
    out_of_scope_record = manager.create_task_record(
        "extra-task",
        "extra_worker",
        parent_task_id=None,
        root_task_id="extra-task",
        depth=1,
        parent_thread_id="main-thread",
    )
    other_thread_record = manager.create_task_record(
        "other-task",
        "background_research",
        parent_task_id=None,
        root_task_id="other-task",
        depth=1,
        parent_thread_id="other-thread",
    )
    allowed_record = manager.create_task_record(
        "allowed-task",
        "background_research",
        parent_task_id=None,
        root_task_id="allowed-task",
        depth=1,
        parent_thread_id="main-thread",
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
        system_tools=frozenset({"spawn_agent", "check_agent", "list_agents"}),
    )
    specs = {
        "main": main,
        "local_second": local_second,
        "local_first": local_first,
        "extra": extra,
    }
    refs = {
        "remote_second": remote_second,
        "remote_first": remote_first,
    }

    class CountingRemoteClient:
        def __init__(self) -> None:
            self.get_calls = 0

        async def get_task(self, remote_ref: RemoteRef, *, task_id: str) -> dict:
            del remote_ref, task_id
            self.get_calls += 1
            raise AssertionError("closed runtime must not call remote get")

    remote_client = CountingRemoteClient()
    monkeypatch.setattr(
        agent_factory_module,
        "create_runtime_agent",
        capture_factory,
    )
    control = async_subagent_runtime.AgentControl(
        specs,
        refs,
        checkpointer=object(),
        backend=object(),
        a2a_client=remote_client,
    )

    async def scenario() -> tuple[TaskRecord, TaskRecord]:
        record = await control.spawn_task(
            "main",
            "compile",
            task_id="main-task",
            parent_thread_id="main-thread",
        )
        record = await wait_for_task_state(
            control,
            record.task_id,
            states={"completed"},
        )
        remote_record = control.ensure_remote_task_record(
            agent_name="remote_first",
            task_id="remote-task",
            upstream_task_id="upstream-task",
        )
        await control.close()
        with pytest.raises(RuntimeClosingError, match="closing"):
            await control.refresh_task(remote_record.task_id)
        return record, remote_record

    record, remote_record = asyncio.run(scenario())

    assert list(captured["local_worker_specs"]) == ["local_first", "local_second"]
    assert list(captured["remote_refs"]) == ["remote_first", "remote_second"]
    assert {tool.name for tool in captured["worker_tools"]} == {
        "spawn_agent",
        "check_agent",
        "list_agents",
    }
    assert all(isinstance(tool, StructuredTool) for tool in captured["worker_tools"])
    spawn_tool = next(
        tool for tool in captured["worker_tools"] if tool.name == "spawn_agent"
    )
    check_tool = next(
        tool for tool in captured["worker_tools"] if tool.name == "check_agent"
    )
    assert "remote_first" in spawn_tool.description
    assert isinstance(check_tool, StructuredTool)
    assert record.state == "completed"
    assert remote_record.route_kind == "remote_ref"
    assert remote_client.get_calls == 0


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
    _manager, delegation_tools, command_port = _direct_tools(
        {"main": main},
        {},
        unavailable_agents={"unavailable_worker": "missing provider credential"},
    )
    command_port.spawn_error = UnavailableAgentTargetError(
        "Agent target 'unavailable_worker' is unavailable: missing provider credential"
    )
    spawn_tool = next(
        tool
        for tool in delegation_tools.build_tools_for("main", command_port=command_port)
        if tool.name == "spawn_agent"
    )

    message = asyncio.run(
        spawn_tool.ainvoke(
            {"agent_name": "unavailable_worker", "task": "try unavailable"}
        )
    )

    assert "Agent target 'unavailable_worker' is unavailable" in message
    assert "missing provider credential" in message
    assert "Available:" in message


def test_child_task_can_send_input_to_direct_parent_but_cannot_cancel_it() -> None:
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
        system_tools=frozenset({"send_input", "list_agents", "cancel_agent"}),
    )
    manager, delegation_tools, command_port = _direct_tools(
        {"parent": parent_spec, "child": child_spec}, {}
    )

    parent = manager.create_task_record(
        "parent-task",
        "parent",
        parent_task_id=None,
        root_task_id="parent-task",
        depth=1,
    )
    child = manager.create_task_record(
        "child-task",
        "child",
        parent_task_id=parent.task_id,
        parent_thread_id=parent.thread_id,
        root_task_id=parent.root_task_id,
        depth=2,
    )
    tools = delegation_tools.build_tools_for("child", command_port=command_port)
    send_tool = next(tool for tool in tools if tool.name == "send_input")
    list_tool = next(tool for tool in tools if tool.name == "list_agents")
    scoped_cancel = next(tool for tool in tools if tool.name == "cancel_agent")
    config = {
        "configurable": {
            "task_id": child.task_id,
            "thread_id": child.thread_id,
            "parent_task_id": parent.task_id,
        }
    }
    listing = asyncio.run(list_tool.ainvoke({}, config=config))
    sent = asyncio.run(
        send_tool.ainvoke(
            {"task_id": parent.task_id, "message": "need clarification"},
            config=config,
        )
    )
    denied = asyncio.run(
        scoped_cancel.ainvoke({"task_id": parent.task_id}, config=config)
    )
    parent_id = parent.task_id

    assert parent_id in listing
    assert f"task_id={parent_id}" in sent
    assert "cancel_agent may target only your direct children" in denied
