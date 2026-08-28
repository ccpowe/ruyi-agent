from __future__ import annotations

import asyncio
import socket
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Any

import httpx
import uvicorn

from ruyi_agent.channels.http.routes import create_gateway_app
from ruyi_agent.gateway.models import AgentRefResponse


class _ConsoleService:
    def list_agents(self) -> list[AgentRefResponse]:
        return [
            AgentRefResponse(
                name="main",
                kind="local",
                public=True,
                description="main agent",
                is_default=True,
            )
        ]


@asynccontextmanager
async def _serve(app: Any) -> AsyncIterator[str]:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen()
    host, port = listener.getsockname()
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            log_level="error",
            lifespan="off",
            proxy_headers=False,
        )
    )
    server_task = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        for _ in range(500):
            if server.started:
                break
            if server_task.done():
                await server_task
            await asyncio.sleep(0.01)
        else:
            raise RuntimeError("Uvicorn did not start")
        yield f"http://{host}:{port}"
    finally:
        server.should_exit = True
        with suppress(TimeoutError):
            await asyncio.wait_for(server_task, timeout=5)
        listener.close()


def test_real_http_team_console_login_cookie_api_and_logout() -> None:
    async def scenario() -> None:
        app = create_gateway_app(
            service=_ConsoleService(),  # type: ignore[arg-type]
            bearer_token="console-e2e-secret",
        )
        async with _serve(app) as base_url:
            async with httpx.AsyncClient(
                base_url=base_url,
                follow_redirects=False,
                timeout=5,
            ) as client:
                anonymous = await client.get("/debug/team")
                assert anonymous.status_code == 303
                assert anonymous.headers["location"] == "/debug/team/login"

                login_page = await client.get("/debug/team/login")
                assert login_page.status_code == 200
                assert "console-e2e-secret" not in login_page.text

                origin_headers = {
                    "Origin": base_url,
                    "Sec-Fetch-Site": "same-origin",
                }
                invalid = await client.post(
                    "/debug/team/login",
                    content="token=invalid",
                    headers={
                        **origin_headers,
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )
                assert invalid.status_code == 401
                assert "set-cookie" not in invalid.headers

                logged_in = await client.post(
                    "/debug/team/login",
                    content="token=console-e2e-secret",
                    headers={
                        **origin_headers,
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )
                assert logged_in.status_code == 303
                assert "HttpOnly" in logged_in.headers["set-cookie"]
                assert "SameSite=strict" in logged_in.headers["set-cookie"]

                console = await client.get("/debug/team")
                styles = await client.get("/debug/team/app.css")
                script = await client.get("/debug/team/app.js")
                assert console.status_code == 200
                assert styles.status_code == 200
                assert script.status_code == 200
                assert console.headers["cache-control"] == "no-store"

                console_api_headers = {
                    "X-Ruyi-Team-Console": "1",
                    "Sec-Fetch-Site": "same-origin",
                    "Referer": f"{base_url}/debug/team",
                }
                agents = await client.get("/agents", headers=console_api_headers)
                assert agents.status_code == 200
                assert agents.json()["items"][0]["name"] == "main"
                assert agents.headers["cache-control"] == "no-store"

                bearer = await client.get(
                    "/agents",
                    headers={"Authorization": "Bearer console-e2e-secret"},
                )
                assert bearer.status_code == 200

                no_marker = await client.get(
                    "/agents",
                    headers={"Sec-Fetch-Site": "same-origin"},
                )
                assert no_marker.status_code == 401

                logout = await client.post(
                    "/debug/team/logout",
                    headers={
                        **console_api_headers,
                        "Origin": base_url,
                    },
                )
                assert logout.status_code == 303
                assert "Max-Age=0" in logout.headers["set-cookie"]

                after_logout = await client.get("/debug/team")
                assert after_logout.status_code == 303

                forged_tls = await client.post(
                    "/debug/team/login",
                    content="token=console-e2e-secret",
                    headers={
                        "Content-Type": "application/x-www-form-urlencoded",
                        "Host": "gateway.example",
                        "Origin": "http://gateway.example",
                        "Sec-Fetch-Site": "same-origin",
                        "X-Forwarded-Proto": "https",
                    },
                )
                assert forged_tls.status_code == 400
                assert "set-cookie" not in forged_tls.headers

    asyncio.run(scenario())
