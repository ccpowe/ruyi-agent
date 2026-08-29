"""Dependencies shared by the Gateway HTTP subrouters."""

from __future__ import annotations

from collections.abc import Callable

from fastapi import Request

from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.tasks import GatewayTaskModule

from .team_console_auth import TeamConsoleAuthenticator


class GatewayHttpContext:
    """Resolve authentication and the request-scoped Gateway facade."""

    def __init__(
        self,
        *,
        service_getter: Callable[[Request], GatewayTaskModule],
        bearer_token: str,
    ) -> None:
        self._service_getter = service_getter
        self.console_auth = TeamConsoleAuthenticator(bearer_token)

    def service(self, request: Request) -> GatewayTaskModule:
        return self._service_getter(request)

    async def require_bearer(self, request: Request) -> None:
        auth_source = self.console_auth.authenticate_api_request(request.scope)
        if auth_source is None:
            raise GatewayTaskError(
                code="unauthorized",
                message="Missing or invalid bearer token",
            )
        if auth_source == "console_session":
            self.console_auth.mark_console_api_authenticated(request.scope)

    def console_session_is_valid(self, request: Request) -> bool:
        return self.console_auth.console_transport_allowed(
            request.scope
        ) and self.console_auth.authenticate_console_session(request.scope)
