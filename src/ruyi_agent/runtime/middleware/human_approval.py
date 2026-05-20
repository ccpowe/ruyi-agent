from __future__ import annotations

import json
import uuid
from typing import Any

from langchain.agents.middleware.types import AgentMiddleware, ContextT, ResponseT
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.config import get_config
from langgraph.types import interrupt

from ruyi_agent.control_plane.permissions import (
    PermissionContext,
    PermissionDecision,
    PermissionPolicy,
    PermissionResult,
)
from ruyi_agent.storage.review_audit import ReviewAuditStore


def _tool_call_signature(tool_call: dict[str, Any]) -> str:
    try:
        args = json.dumps(tool_call.get("args", {}), sort_keys=True, default=str)
    except TypeError:
        args = repr(tool_call.get("args", {}))
    return f"{tool_call.get('id')}:{tool_call.get('name')}:{args}"


def _get_configurable() -> dict[str, Any]:
    try:
        config = get_config()
    except RuntimeError:
        return {}
    configurable = config.get("configurable") or {}
    return configurable if isinstance(configurable, dict) else {}


def _copy_tool_call_with_args(
    tool_call: dict[str, Any],
    *,
    args: dict[str, Any],
) -> dict[str, Any]:
    revised = dict(tool_call)
    revised["args"] = args
    revised.setdefault("type", "tool_call")
    return revised


class HumanApprovalMiddleware(AgentMiddleware[object, ContextT, ResponseT]):
    """Review model-proposed tool calls before execution."""

    def __init__(
        self,
        *,
        policy: PermissionPolicy,
        backend_kind: str,
        workspace_root: str,
        agent_name: str | None = None,
        permission_profile: str | None = None,
        audit_store: ReviewAuditStore | None = None,
    ) -> None:
        super().__init__()
        self._policy = policy
        self._backend_kind = backend_kind
        self._workspace_root = workspace_root
        self._agent_name = agent_name
        self._permission_profile = permission_profile
        self._audit_store = audit_store

    def _context(self) -> PermissionContext:
        configurable = _get_configurable()
        agent_name = configurable.get("agent_name") or self._agent_name
        profile_name = (
            configurable.get("permission_profile") or self._permission_profile
        )
        thread_id = configurable.get("thread_id")
        task_id = configurable.get("task_id")
        return PermissionContext(
            agent_name=agent_name if isinstance(agent_name, str) else None,
            profile_name=profile_name if isinstance(profile_name, str) else None,
            backend_kind=self._backend_kind,
            workspace_root=self._workspace_root,
            thread_id=thread_id if isinstance(thread_id, str) else None,
            task_id=task_id if isinstance(task_id, str) else None,
        )

    def _evaluate(
        self,
        tool_call: dict[str, Any],
        context: PermissionContext,
    ) -> PermissionResult:
        args = tool_call.get("args")
        return self._policy.evaluate(
            tool_name=str(tool_call.get("name") or ""),
            tool_args=args if isinstance(args, dict) else {},
            context=context,
        )

    def _audit(
        self,
        event_type: str,
        *,
        context: PermissionContext,
        tool_call: dict[str, Any] | None = None,
        result: PermissionResult | None = None,
        review_id: str | None = None,
        payload: dict[str, Any] | None = None,
    ) -> None:
        if self._audit_store is None:
            return
        self._audit_store.append(
            event_type,
            source="local",
            review_id=review_id,
            task_id=context.task_id,
            thread_id=context.thread_id,
            agent_name=context.agent_name,
            profile_name=(
                result.profile_name
                if result is not None
                else context.profile_name
            ),
            backend_kind=context.backend_kind,
            workspace_root=context.workspace_root,
            tool_name=(
                str(tool_call.get("name"))
                if tool_call is not None and tool_call.get("name") is not None
                else None
            ),
            tool_call_id=(
                str(tool_call.get("id"))
                if tool_call is not None and tool_call.get("id") is not None
                else None
            ),
            policy_decision=(
                result.decision.value if result is not None else None
            ),
            risk=result.risk if result is not None else None,
            reason=result.reason if result is not None else None,
            payload=payload,
        )

    def _action_request(
        self,
        tool_call: dict[str, Any],
        result: PermissionResult,
        context: PermissionContext,
    ) -> dict[str, Any]:
        description = "\n".join(
            [
                "Tool execution requires approval.",
                f"agent={context.agent_name or 'unknown'}",
                f"profile={result.profile_name or context.profile_name or 'default'}",
                f"backend={context.backend_kind}",
                f"workspace={context.workspace_root}",
                f"risk={result.risk}",
                f"reason={result.reason}",
            ]
        )
        return {
            "name": tool_call["name"],
            "args": tool_call.get("args", {}),
            "description": description,
            "risk": result.risk,
            "reason": result.reason,
            "profile_name": result.profile_name,
            "backend_kind": context.backend_kind,
            "workspace_root": context.workspace_root,
            "agent_name": context.agent_name,
        }

    def _review_config(
        self,
        tool_call: dict[str, Any],
        result: PermissionResult,
    ) -> dict[str, Any]:
        return {
            "action_name": tool_call["name"],
            "allowed_decisions": result.allowed_decisions,
        }

    def _blocked_message(
        self,
        tool_call: dict[str, Any],
        *,
        content: str,
    ) -> ToolMessage:
        return ToolMessage(
            content=content,
            name=str(tool_call.get("name") or "tool"),
            tool_call_id=str(tool_call.get("id") or "unknown"),
            status="error",
        )

    def after_model(self, state: dict[str, Any], runtime: Any) -> dict[str, Any] | None:  # noqa: ARG002
        messages = state.get("messages") or []
        last_ai_msg = next(
            (msg for msg in reversed(messages) if isinstance(msg, AIMessage)),
            None,
        )
        if last_ai_msg is None or not last_ai_msg.tool_calls:
            return None

        context = self._context()
        tool_calls: list[dict[str, Any]] = [dict(call) for call in last_ai_msg.tool_calls]
        approved: set[str] = set()
        blocked: set[str] = set()
        artificial_tool_messages: list[ToolMessage] = []
        changed = False

        while True:
            action_requests: list[dict[str, Any]] = []
            review_configs: list[dict[str, Any]] = []
            interrupt_indices: list[int] = []
            interrupt_results: list[PermissionResult] = []

            for idx, tool_call in enumerate(tool_calls):
                signature = _tool_call_signature(tool_call)
                if signature in approved or signature in blocked:
                    continue
                result = self._evaluate(tool_call, context)
                self._audit(
                    "permission_evaluated",
                    context=context,
                    tool_call=tool_call,
                    result=result,
                    payload={"args": tool_call.get("args", {})},
                )
                if result.decision == PermissionDecision.ALLOW:
                    continue
                if result.decision == PermissionDecision.DENY:
                    blocked.add(signature)
                    self._audit(
                        "tool_denied",
                        context=context,
                        tool_call=tool_call,
                        result=result,
                        payload={"args": tool_call.get("args", {})},
                    )
                    artificial_tool_messages.append(
                        self._blocked_message(
                            tool_call,
                            content=(
                                "Tool call denied by permission policy: "
                                f"{result.reason}"
                            ),
                        )
                    )
                    changed = True
                    continue
                action_requests.append(
                    self._action_request(tool_call, result, context)
                )
                review_configs.append(self._review_config(tool_call, result))
                interrupt_indices.append(idx)
                interrupt_results.append(result)

            if not action_requests:
                break

            review_id = str(uuid.uuid4())
            review_payload = {
                "review_id": review_id,
                "action_requests": action_requests,
                "review_configs": review_configs,
            }
            for idx, result in zip(interrupt_indices, interrupt_results, strict=True):
                self._audit(
                    "review_requested",
                    context=context,
                    tool_call=tool_calls[idx],
                    result=result,
                    review_id=review_id,
                    payload={
                        "action_request": action_requests[
                            interrupt_indices.index(idx)
                        ],
                        "review_config": review_configs[
                            interrupt_indices.index(idx)
                        ],
                    },
                )
            response = interrupt(
                review_payload
            )
            decisions = response.get("decisions") if isinstance(response, dict) else None
            if not isinstance(decisions, list):
                raise ValueError("Human approval resume payload must include decisions")
            if len(decisions) != len(interrupt_indices):
                raise ValueError(
                    "Number of human decisions does not match interrupted tool calls"
                )

            for decision, idx, result in zip(
                decisions,
                interrupt_indices,
                interrupt_results,
                strict=True,
            ):
                if not isinstance(decision, dict):
                    raise ValueError("Human approval decision must be a dict")
                decision_type = decision.get("type")
                tool_call = tool_calls[idx]
                allowed = result.allowed_decisions
                if decision_type not in allowed:
                    raise ValueError(
                        f"Decision '{decision_type}' is not allowed for "
                        f"tool '{tool_call.get('name')}'"
                    )
                if decision_type == "approve":
                    self._audit(
                        "review_decision",
                        context=context,
                        tool_call=tool_call,
                        result=result,
                        review_id=review_id,
                        payload={"decision": decision},
                    )
                    approved.add(_tool_call_signature(tool_call))
                    changed = True
                    continue
                if decision_type == "reject":
                    self._audit(
                        "review_decision",
                        context=context,
                        tool_call=tool_call,
                        result=result,
                        review_id=review_id,
                        payload={"decision": decision},
                    )
                    blocked.add(_tool_call_signature(tool_call))
                    artificial_tool_messages.append(
                        self._blocked_message(
                            tool_call,
                            content=str(
                                decision.get("message")
                                or "User rejected the tool call."
                            ),
                        )
                    )
                    changed = True
                    continue
                if decision_type == "edit":
                    edited_action = decision.get("edited_action")
                    if not isinstance(edited_action, dict):
                        raise ValueError("Edit decision must include edited_action")
                    edited_name = edited_action.get("name", tool_call.get("name"))
                    if edited_name != tool_call.get("name"):
                        raise ValueError("Edit may change tool args, not tool name")
                    edited_args = edited_action.get("args")
                    if not isinstance(edited_args, dict):
                        raise ValueError("Edit decision args must be a dict")
                    self._audit(
                        "review_decision",
                        context=context,
                        tool_call=tool_call,
                        result=result,
                        review_id=review_id,
                        payload={"decision": decision},
                    )
                    tool_calls[idx] = _copy_tool_call_with_args(
                        tool_call,
                        args=edited_args,
                    )
                    changed = True
                    continue
                raise ValueError(f"Unsupported human decision: {decision_type}")

        if not changed:
            return None
        last_ai_msg.tool_calls = tool_calls
        return {"messages": [last_ai_msg, *artificial_tool_messages]}

    async def aafter_model(
        self,
        state: dict[str, Any],
        runtime: Any,
    ) -> dict[str, Any] | None:
        return self.after_model(state, runtime)
