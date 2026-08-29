from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from ruyi_agent.integrations.a2a.client import A2AClientError
from ruyi_agent.storage.gateway_command_store import GatewayCommandStore
from ruyi_agent.storage.gateway_route_store import GatewayRouteStore
from tests.unit.gateway_http_support import (
    ReviewRemoteA2AClient,
    StaticRemoteA2AClient,
    auth_headers,
    build_app,
)


class ReplaySafeCancelledRemoteA2AClient(StaticRemoteA2AClient):
    def __init__(self) -> None:
        super().__init__()
        self.effect_started = asyncio.Event()
        self.actual_effects = 0
        self._responses: dict[str, dict[str, object]] = {}

    async def create_task(
        self,
        remote_ref,
        *,
        input_content,
        metadata,
        idempotency_key=None,
        **kwargs,
    ):
        assert isinstance(idempotency_key, str) and idempotency_key
        self.created_idempotency_keys.append(idempotency_key)
        if idempotency_key in self._responses:
            return dict(self._responses[idempotency_key])
        self.actual_effects += 1
        self.created_inputs.append(input_content)
        response = {
            "task_id": "safe-upstream-1",
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": f"remote done: {input_content}",
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
        }
        self._responses[idempotency_key] = response
        self.effect_started.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class NoKeyCancelledRemoteA2AClient(StaticRemoteA2AClient):
    """Expose effects while blocking only the first no-header create attempt."""

    def __init__(self) -> None:
        super().__init__()
        self.effect_started = asyncio.Event()
        self.actual_effects = 0
        self._responses: dict[str, dict[str, object]] = {}

    async def create_task(
        self,
        remote_ref,
        *,
        input_content,
        metadata,
        idempotency_key=None,
        **kwargs,
    ):
        del metadata, kwargs
        assert isinstance(idempotency_key, str) and idempotency_key
        self.created_idempotency_keys.append(idempotency_key)
        if idempotency_key in self._responses:
            return dict(self._responses[idempotency_key])
        self.actual_effects += 1
        self.created_inputs.append(input_content)
        response = {
            "task_id": f"no-key-upstream-{self.actual_effects}",
            "agent_name": remote_ref.name,
            "status": "completed",
            "last_result": f"remote done: {input_content}",
            "error": None,
            "run_count": 1,
            "created_at": "2026-04-23T00:00:00Z",
            "updated_at": "2026-04-23T00:00:01Z",
        }
        self._responses[idempotency_key] = response
        if self.actual_effects == 1:
            self.effect_started.set()
            await asyncio.Event().wait()
            raise AssertionError("unreachable")
        return dict(response)


class LeakingRemoteA2AClient(ReviewRemoteA2AClient):
    private_task_id = "upstream-review-task"

    def __init__(self) -> None:
        super().__init__()
        self.fail_operation: str | None = None

    def _raise_private_error(self) -> None:
        raise A2AClientError(
            status_code=502,
            code="upstream_gateway_error",
            message=(
                "private task upstream-review-task failed at "
                "https://remote.invalid/tasks/upstream-review-task"
            ),
            details={
                "task_id": self.private_task_id,
                "task_url": f"/tasks/{self.private_task_id}",
                "nested": {"task_id": self.private_task_id},
            },
        )

    async def create_task(self, *args, **kwargs):
        if self.fail_operation == "create":
            self._raise_private_error()
        return await super().create_task(*args, **kwargs)

    async def get_task(self, *args, **kwargs):
        if self.fail_operation == "get":
            self._raise_private_error()
        return await super().get_task(*args, **kwargs)

    async def send_input(self, *args, **kwargs):
        if self.fail_operation == "send":
            self._raise_private_error()
        return await super().send_input(*args, **kwargs)

    async def cancel_task(self, *args, **kwargs):
        if self.fail_operation == "cancel":
            self._raise_private_error()
        return await super().cancel_task(*args, **kwargs)

    async def submit_review_decision(self, *args, **kwargs):
        if self.fail_operation == "review":
            self._raise_private_error()
        return await super().submit_review_decision(*args, **kwargs)

    async def list_task_messages(self, *args, **kwargs):
        del args, kwargs
        self._raise_private_error()

    @asynccontextmanager
    async def open_task_event_stream(self, *args, **kwargs):
        del args, kwargs
        self._raise_private_error()
        yield  # pragma: no cover - required by the async contextmanager protocol


def test_declared_safe_cancelled_create_replays_same_downstream_key_after_restart(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote = ReplaySafeCancelledRemoteA2AClient()
    route_path = str(tmp_path / "safe-cancelled-routes.sqlite")
    command_path = str(tmp_path / "safe-cancelled-commands.sqlite")
    headers = {**auth_headers(), "Idempotency-Key": "safe-cancelled-create"}
    body = {"input": {"content": "create once"}, "metadata": {}}
    first_routes = GatewayRouteStore(route_path)
    first_commands = GatewayCommandStore(command_path)
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=first_routes,
        command_store=first_commands,
        remote_create_idempotency="ruyi_gateway_v1",
    )

    async def cancel_request() -> None:
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gateway.test",
        ) as client:
            request = asyncio.create_task(
                client.post(
                    "/agents/remote_code_wiki/tasks",
                    headers=headers,
                    json=body,
                )
            )
            await asyncio.wait_for(remote.effect_started.wait(), timeout=1)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request

    asyncio.run(cancel_request())
    [reserved] = first_routes.list_routes()
    assert reserved.route_state == "pending"
    evidence = first_routes.get_create_evidence(reserved.task_id)
    assert evidence is not None
    assert (evidence.key_scope, evidence.replay_policy, evidence.effect_boundary) == (
        "external",
        "ruyi_gateway_v1",
        "started",
    )
    first_routes.close()
    first_commands.close()

    second_routes = GatewayRouteStore(route_path)
    second_commands = GatewayCommandStore(command_path)
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=second_routes,
        command_store=second_commands,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    try:
        with TestClient(second_app) as client:
            recovered = client.post(
                "/agents/remote_code_wiki/tasks", headers=headers, json=body
            )
            replay = client.post(
                "/agents/remote_code_wiki/tasks", headers=headers, json=body
            )
            queried = client.get(
                f"/tasks/{recovered.json()['task_id']}", headers=auth_headers()
            )

        assert recovered.status_code == replay.status_code == 201
        assert recovered.json() == replay.json()
        assert queried.status_code == 200
        assert queried.json()["task_id"] == recovered.json()["task_id"]
        assert queried.json()["status"] == "completed"
        assert replay.headers["idempotency-replayed"] == "true"
        assert remote.actual_effects == 1
        assert remote.created_inputs == ["create once"]
        assert remote.created_idempotency_keys == [
            "safe-cancelled-create",
            "safe-cancelled-create",
        ]
        route = second_routes.get_route(recovered.json()["task_id"])
        assert route is not None and route.route_state == "active"
        assert route.upstream_task_id == "safe-upstream-1"
    finally:
        second_routes.close()
        second_commands.close()


def test_declared_safe_no_key_cancel_is_not_replayed_and_next_post_is_new(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    remote = NoKeyCancelledRemoteA2AClient()
    route_path = str(tmp_path / "no-key-cancelled-routes.sqlite")
    first_routes = GatewayRouteStore(route_path)
    first_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=first_routes,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    body = {"input": {"content": "independent no-key create"}, "metadata": {}}

    async def cancel_request() -> None:
        transport = httpx.ASGITransport(app=first_app)
        async with httpx.AsyncClient(
            transport=transport,
            base_url="http://gateway.test",
        ) as client:
            request = asyncio.create_task(
                client.post(
                    "/agents/remote_code_wiki/tasks",
                    headers=auth_headers(),
                    json=body,
                )
            )
            await asyncio.wait_for(remote.effect_started.wait(), timeout=1)
            request.cancel()
            with pytest.raises(asyncio.CancelledError):
                await request

    asyncio.run(cancel_request())
    [old_route] = first_routes.list_routes()
    old_task_id = old_route.task_id
    assert old_route.route_state == "uncertain"
    assert remote.actual_effects == 1
    first_routes.close()

    second_routes = GatewayRouteStore(route_path)
    second_app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=second_routes,
        remote_create_idempotency="ruyi_gateway_v1",
    )
    try:
        assert remote.actual_effects == 1
        with TestClient(second_app) as client:
            old = client.get(f"/tasks/{old_task_id}", headers=auth_headers())
            old_send = client.post(
                f"/tasks/{old_task_id}/input",
                headers=auth_headers(),
                json={"input": {"content": "must not replay"}},
            )
            assert remote.actual_effects == 1
            created = client.post(
                "/agents/remote_code_wiki/tasks",
                headers=auth_headers(),
                json=body,
            )

        assert old.status_code == 200
        assert old.json()["task_id"] == old_task_id
        assert old_send.status_code == 409
        assert old_send.json()["error"]["details"]["route_state"] == "uncertain"
        assert created.status_code == 201
        assert created.json()["task_id"] != old_task_id
        assert remote.actual_effects == 2
        assert len(set(remote.created_idempotency_keys)) == 2
        persisted_old = second_routes.get_route(old_task_id)
        assert persisted_old is not None
        assert persisted_old.route_state == "uncertain"
    finally:
        second_routes.close()


def test_remote_create_error_and_persisted_route_never_leak_downstream_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = LeakingRemoteA2AClient()
    remote.fail_operation = "create"
    routes = GatewayRouteStore(":memory:")
    app, _ = build_app(
        monkeypatch,
        a2a_client=remote,  # type: ignore[arg-type]
        route_store=routes,
    )

    with TestClient(app) as client:
        created = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "private failure"}, "metadata": {}},
        )
        public_task_id = created.json()["error"]["details"]["task_id"]
        queried = client.get(f"/tasks/{public_task_id}", headers=auth_headers())
        listed = client.get("/tasks", headers=auth_headers())

    assert created.status_code == 502
    assert queried.status_code == listed.status_code == 200
    assert public_task_id != remote.private_task_id
    for response in (created, queried, listed):
        assert remote.private_task_id not in response.text
        assert "remote.invalid" not in response.text
    route = routes.get_route(public_task_id)
    assert route is not None
    assert remote.private_task_id not in (route.route_error or "")


def test_all_remote_proxy_http_errors_rewrite_to_public_route_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    remote = LeakingRemoteA2AClient()
    app, _ = build_app(monkeypatch, a2a_client=remote)  # type: ignore[arg-type]

    with TestClient(app) as client:
        created = client.post(
            "/agents/remote_code_wiki/tasks",
            headers=auth_headers(),
            json={"input": {"content": "pending review"}, "metadata": {}},
        )
        public_task_id = created.json()["task_id"]
        assert public_task_id != remote.private_task_id

        requests = []
        remote.fail_operation = "get"
        requests.append(client.get(f"/tasks/{public_task_id}", headers=auth_headers()))
        remote.fail_operation = "send"
        requests.append(
            client.post(
                f"/tasks/{public_task_id}/input",
                headers=auth_headers(),
                json={"input": {"content": "continue"}},
            )
        )
        remote.fail_operation = "cancel"
        requests.append(
            client.post(f"/tasks/{public_task_id}/cancel", headers=auth_headers())
        )
        remote.fail_operation = "messages"
        requests.append(
            client.get(f"/tasks/{public_task_id}/messages", headers=auth_headers())
        )
        remote.fail_operation = "review"
        requests.append(
            client.post(
                f"/tasks/{public_task_id}/reviews/remote-review-1/decision",
                headers=auth_headers(),
                json={"decisions": [{"type": "approve"}]},
            )
        )
        remote.fail_operation = "events"
        requests.append(
            client.get(
                f"/tasks/{public_task_id}/events?run_count=1",
                headers=auth_headers(),
            )
        )

    assert {response.status_code for response in requests} == {502}
    for response in requests:
        assert remote.private_task_id not in response.text
        assert "remote.invalid" not in response.text
        assert response.json()["error"]["details"]["task_id"] == public_task_id
        assert response.json()["error"]["details"]["task_url"] == (
            f"/tasks/{public_task_id}"
        )
