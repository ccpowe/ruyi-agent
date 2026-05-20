from __future__ import annotations

import json
from typing import Any

from prompt_toolkit import PromptSession

from ruyi_agent.channels.cli.renderer import InteractiveRenderer
from ruyi_agent.control_plane.contracts import (
    ReviewDecision,
    ReviewDecisionKind,
    ReviewSnapshot,
)


class ApprovalPresenter:
    def __init__(
        self,
        renderer: InteractiveRenderer,
        *,
        prompt_session: PromptSession | None = None,
    ) -> None:
        self._renderer = renderer
        self._prompt_session = prompt_session or PromptSession()
        # Keep approval prompts out of the terminal scrollback after a decision.
        self._prompt_session.app.erase_when_done = True

    async def request_decisions(
        self,
        review: ReviewSnapshot,
    ) -> list[ReviewDecision] | None:
        decisions: list[ReviewDecision] = []
        for action in review.actions:
            allowed = action.allowed_decisions or [
                ReviewDecisionKind.APPROVE,
                ReviewDecisionKind.REJECT,
            ]
            allowed_values = {item.value for item in allowed}
            while True:
                choice = await self._prompt_decision(
                    review=review,
                    tool_name=action.tool_name,
                    allowed_values=allowed_values,
                )
                if choice in {"?", "details"}:
                    self._renderer.render_review_details(review)
                    continue
                if choice in {"a", "approve"} and "approve" in allowed_values:
                    decisions.append(
                        ReviewDecision(
                            action_id=action.action_id,
                            decision=ReviewDecisionKind.APPROVE,
                        )
                    )
                    break
                if choice in {"r", "reject"} and "reject" in allowed_values:
                    message = await self._prompt_session.prompt_async(
                        "reject reason: ",
                        default="User rejected the tool call.",
                    )
                    decisions.append(
                        ReviewDecision(
                            action_id=action.action_id,
                            decision=ReviewDecisionKind.REJECT,
                            message=message,
                        )
                    )
                    break
                if choice in {"e", "edit"} and "edit" in allowed_values:
                    self._renderer.render_review_details(review)
                    edited_args = _read_json_object("edited args JSON: ", self._renderer)
                    if edited_args is None:
                        continue
                    decisions.append(
                        ReviewDecision(
                            action_id=action.action_id,
                            decision=ReviewDecisionKind.EDIT,
                            edited_args=edited_args,
                        )
                    )
                    break
                if choice in {"s", "skip", ""}:
                    return None
                self._renderer.error(f"unsupported decision: {choice}")
        return decisions

    async def _prompt_decision(
        self,
        *,
        review: ReviewSnapshot,
        tool_name: str,
        allowed_values: set[str],
    ) -> str:
        options = ["a=approve", "r=reject", "s=skip", "?=details"]
        if "edit" in allowed_values:
            options.append("e=edit")
        choice = await self._prompt_session.prompt_async(
            f"decision for {tool_name} [default=s]: ",
            default="s",
            bottom_toolbar=_format_review_toolbar(review, options),
        )
        return choice.strip().lower()


def _format_review_toolbar(review: ReviewSnapshot, options: list[str]) -> str:
    tool_names = ", ".join(action.tool_name for action in review.actions) or "tool"
    source = f"task:{review.task_id[:8]}" if review.task_id else "root"
    parts = [f"Approval required: {tool_names}", f"source={source}"]
    if review.agent_name:
        parts.append(f"agent={review.agent_name}")
    reason = review.reason or review.risk
    if reason:
        parts.append(f"reason={reason}")
    parts.append(" | ".join(options))
    return " | ".join(parts)


def _read_json_object(
    prompt: str,
    renderer: InteractiveRenderer,
) -> dict[str, Any] | None:
    first_line = renderer.console.input(prompt)
    raw = first_line.strip()
    if not raw:
        return None
    lines = [first_line]
    while True:
        try:
            parsed = json.loads("\n".join(lines))
        except json.JSONDecodeError:
            if len(lines) == 1:
                renderer.info("Paste remaining JSON lines. Use END to cancel.")
            next_line = renderer.console.input()
            if next_line.strip() == "END":
                renderer.info("Edit cancelled.")
                return None
            lines.append(next_line)
            continue
        if not isinstance(parsed, dict):
            renderer.error("edited args must be a JSON object")
            return None
        return parsed
