from __future__ import annotations

import asyncio

import httpx
from fastapi import FastAPI

import ruyi_agent.runtime.delegation.async_runtime as async_runtime
from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.config.loader import LocalWorkerSpec, RemoteRef
from ruyi_agent.gateway.tasks import GatewayTaskModule
from ruyi_agent.integrations.a2a.client import A2AClient
from ruyi_agent.runtime.mailbox.service import AgentMailbox
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from ruyi_agent.storage.mailbox_store import MailboxStore
from ruyi_agent.storage.task_store import TaskStore


class RecordingAgent:
    def __init__(self, mailbox: AgentMailbox | None) -> None:
        self.inputs: list[str] = []
        self._mailbox = mailbox

    async def ainvoke(self, payload, *, config, version):
        del version
        messages = payload["messages"]
        if messages:
            content = str(messages[0]["content"])
        else:
            assert self._mailbox is not None
            configurable = config["configurable"]
            claimed = self._mailbox.claim(
                recipient_task_id=str(configurable["task_id"]),
                recipient_thread_id=str(configurable["thread_id"]),
            )
            content = "\n".join(message.content for message in claimed)
        self.inputs.append(content)
        await asyncio.sleep(0.01)
        return {"messages": [{"role": "assistant", "content": f"done: {content}"}]}


class DropCommittedResponseTransport(httpx.AsyncBaseTransport):
    """Drop one successful create and input response after the app handled it."""

    def __init__(self, app: FastAPI) -> None:
        self._inner = httpx.ASGITransport(app=app)
        self.dropped_create = False
        self.dropped_input = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        response = await self._inner.handle_async_request(request)
        is_create = request.method == "POST" and request.url.path.endswith(
            "/agents/code_wiki/tasks"
        )
        is_input = request.method == "POST" and request.url.path.endswith("/input")
        should_drop = response.is_success and (
            (is_create and not self.dropped_create)
            or (is_input and not self.dropped_input)
        )
        if not should_drop:
            return response
        if is_create:
            self.dropped_create = True
        if is_input:
            self.dropped_input = True
        await response.aread()
        await response.aclose()
        raise httpx.ReadError(
            "simulated connection loss after downstream commit",
            request=request,
        )

    async def aclose(self) -> None:
        await self._inner.aclose()


def _local_spec(name: str) -> LocalWorkerSpec:
    return LocalWorkerSpec(
        name=name,
        description=name,
        system_prompt="prompt",
        model=object(),
        tools=[],
        memory=[],
        skills=[],
    )


def test_remote_gateway_retry_survives_lost_create_and_input_responses(
    monkeypatch,
    tmp_path,
) -> None:
    agents: dict[str, RecordingAgent] = {}
    mailboxes: dict[str, AgentMailbox] = {}

    def create_agent(**kwargs):
        name = str(kwargs["name"])
        return agents.setdefault(name, RecordingAgent(mailboxes.get(name)))

    monkeypatch.setattr(async_runtime, "create_runtime_agent", create_agent)
    monkeypatch.setenv("REMOTE_E2E_TOKEN", "remote-secret")

    downstream_db = str(tmp_path / "downstream-tasks.sqlite")
    downstream_task_store = TaskStore(downstream_db)
    downstream_mailbox_store = MailboxStore(downstream_db)
    downstream_mailbox = AgentMailbox(downstream_mailbox_store)
    mailboxes["code_wiki"] = downstream_mailbox
    downstream_command_store = GatewayCommandStore(downstream_db)
    downstream_route_store = GatewayRouteStore(
        str(tmp_path / "downstream-routes.sqlite")
    )
    downstream_control = async_runtime.AgentControl(
        {"code_wiki": _local_spec("code_wiki")},
        checkpointer=object(),
        backend=object(),
        task_store=downstream_task_store,
        mailbox=downstream_mailbox,
        node_id="node-b",
    )
    downstream_service = GatewayTaskModule(
        main_agent_name="code_wiki",
        agent_configs={
            "code_wiki": {
                "kind": "local",
                "public": True,
                "name": "code_wiki",
                "description": "remote worker",
            }
        },
        control=downstream_control,
        route_store=downstream_route_store,
        command_store=downstream_command_store,
    )
    downstream_app = create_gateway_app(
        service=downstream_service,
        bearer_token="remote-secret",
    )
    downstream_root = FastAPI()
    downstream_root.mount("/a2a", downstream_app)
    dropping_transport = DropCommittedResponseTransport(downstream_root)

    remote_ref = RemoteRef(
        name="remote_code_wiki",
        description="remote worker",
        url="https://node-b.test/a2a",
        remote_agent_name="code_wiki",
        auth={"type": "bearer", "token_env": "REMOTE_E2E_TOKEN"},
    )
    upstream_db = str(tmp_path / "upstream-tasks.sqlite")
    upstream_task_store = TaskStore(upstream_db)
    upstream_command_store = GatewayCommandStore(upstream_db)
    upstream_route_store = GatewayRouteStore(str(tmp_path / "upstream-routes.sqlite"))
    upstream_control = async_runtime.AgentControl(
        {"main": _local_spec("main")},
        {"remote_code_wiki": remote_ref},
        checkpointer=object(),
        backend=object(),
        task_store=upstream_task_store,
        a2a_client=A2AClient(
            transports={"https://node-b.test/a2a": dropping_transport}
        ),
        node_id="node-a",
    )
    upstream_service = GatewayTaskModule(
        main_agent_name="main",
        agent_configs={
            "main": {
                "kind": "local",
                "public": True,
                "name": "main",
                "description": "main",
            },
            "remote_code_wiki": {
                "kind": "remote_ref",
                "public": True,
                "name": "remote_code_wiki",
                "description": "remote worker",
            },
        },
        control=upstream_control,
        route_store=upstream_route_store,
        command_store=upstream_command_store,
    )
    upstream_app = create_gateway_app(
        service=upstream_service,
        bearer_token="upstream-secret",
    )

    async def scenario() -> tuple[str, str]:
        transport = httpx.ASGITransport(app=upstream_app)
        headers = {
            "Authorization": "Bearer upstream-secret",
            "Idempotency-Key": "external-create-1",
        }
        create_body = {"input": {"content": "first"}, "metadata": {}}
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://upstream.test",
        ) as client:
            lost_create = await client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=create_body,
            )
            recovered_create = await client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=create_body,
            )
            replayed_create = await client.post(
                "/agents/remote_code_wiki/tasks",
                headers=headers,
                json=create_body,
            )

            assert lost_create.status_code == 502
            assert recovered_create.status_code == 201
            assert replayed_create.status_code == 201
            assert replayed_create.json() == recovered_create.json()
            assert replayed_create.headers["idempotency-replayed"] == "true"
            proxy_task_id = str(recovered_create.json()["task_id"])

            downstream_records = downstream_control.list_persisted_task_records()
            assert len(downstream_records) == 1
            downstream_record = downstream_records[0]
            if downstream_control.get_live_run(downstream_record.task_id) is not None:
                await downstream_control.get_live_run(downstream_record.task_id)

            input_headers = {
                "Authorization": "Bearer upstream-secret",
                "Idempotency-Key": "external-input-1",
            }
            input_body = {"input": {"content": "second"}}
            lost_input = await client.post(
                f"/tasks/{proxy_task_id}/input",
                headers=input_headers,
                json=input_body,
            )
            recovered_input = await client.post(
                f"/tasks/{proxy_task_id}/input",
                headers=input_headers,
                json=input_body,
            )
            replayed_input = await client.post(
                f"/tasks/{proxy_task_id}/input",
                headers=input_headers,
                json=input_body,
            )

            assert lost_input.status_code == 502
            assert recovered_input.status_code == 202
            assert replayed_input.status_code == 202
            assert replayed_input.json() == recovered_input.json()
            assert replayed_input.headers["idempotency-replayed"] == "true"
            downstream_record = downstream_control.list_persisted_task_records()[0]
            if downstream_control.get_live_run(downstream_record.task_id) is not None:
                await downstream_control.get_live_run(downstream_record.task_id)
            return proxy_task_id, downstream_record.task_id

    try:
        proxy_task_id, downstream_task_id = asyncio.run(scenario())
        assert dropping_transport.dropped_create is True
        assert dropping_transport.dropped_input is True
        assert upstream_command_store.count_commands() == 2
        assert downstream_command_store.count_commands() == 2
        assert len(upstream_control.list_persisted_task_records()) == 1
        assert len(downstream_control.list_persisted_task_records()) == 1
        assert upstream_control.get_task_record(proxy_task_id).upstream_task_id == (
            downstream_task_id
        )
        assert downstream_control.get_task_record(downstream_task_id).run_count == 2
        assert agents["code_wiki"].inputs == ["first", "second"]
    finally:
        upstream_command_store.close()
        upstream_route_store.close()
        upstream_task_store.close()
        downstream_command_store.close()
        downstream_route_store.close()
        downstream_mailbox_store.close()
        downstream_task_store.close()
