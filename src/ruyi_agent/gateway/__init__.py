"""Gateway Task Module."""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ruyi_agent.gateway.tasks import GatewayTaskModule

__all__ = ["GatewayTaskModule"]


def __getattr__(name: str) -> Any:
    if name == "GatewayTaskModule":
        from ruyi_agent.gateway.tasks import GatewayTaskModule

        return GatewayTaskModule
    raise AttributeError(name)
