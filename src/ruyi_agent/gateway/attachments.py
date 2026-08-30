"""Gateway attachment preparation and runtime workspace boundary."""

from __future__ import annotations

import base64
import binascii
from pathlib import PurePosixPath
from typing import Any

from ruyi_agent.gateway.application import GatewayApplicationContext
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import PreparedInput
from ruyi_agent.gateway_protocol.dto import AttachmentInput
from ruyi_agent.task_models import MetadataScalar

ATTACHMENT_METADATA_KEY = "attachments"
ATTACHMENT_INBOX_SUBDIR = "inbox/gateway"
SAFE_ATTACHMENT_CHARS = set(
    "abcdefghijklmnopqrstuvwxyz"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    "0123456789"
    ".-_"
)


class GatewayAttachmentService:
    """Validate, persist, and project inbound attachments."""

    def __init__(self, context: GatewayApplicationContext) -> None:
        self._context = context

    async def prepare(
        self,
        content: str,
        attachments: list[AttachmentInput],
        *,
        batch_id: str,
    ) -> PreparedInput:
        if not attachments:
            return PreparedInput(content=content, attachment_metadata=[])
        workspace_root = self.normalized_workspace_root()
        if workspace_root is None:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message="Runtime workspace root is not configured",
            )

        uploads: list[tuple[str, bytes]] = []
        metadata: list[dict[str, str]] = []
        for index, attachment in enumerate(attachments, start=1):
            filename = sanitize_attachment_name(attachment.name)
            content_bytes = self._decode_attachment(attachment, filename=filename)
            path = str(
                PurePosixPath(workspace_root)
                / ATTACHMENT_INBOX_SUBDIR
                / batch_id
                / f"{index:02d}-{filename}"
            )
            self.ensure_workspace_path(path, kind="Attachment upload")
            if not PurePosixPath(path).is_relative_to(
                PurePosixPath(workspace_root) / ATTACHMENT_INBOX_SUBDIR
            ):
                raise GatewayTaskError(
                    code="runtime_unavailable",
                    message="Attachment upload path is outside the gateway inbox",
                )
            uploads.append((path, content_bytes))
            metadata.append(
                {
                    "name": filename,
                    "path": path,
                    "content_type": attachment.content_type or "",
                    "kind": attachment.kind,
                }
            )

        upload_result = await self._run_in_thread(
            self._context.control.upload_files,
            uploads,
        )
        if len(upload_result) != len(uploads):
            raise GatewayTaskError(
                code="attachment_upload_failed",
                message="Runtime returned an incomplete attachment upload result",
                kind="upstream_failure",
            )
        for item_metadata, result in zip(metadata, upload_result, strict=False):
            error = getattr(result, "error", None)
            if error:
                raise GatewayTaskError(
                    code="attachment_upload_failed",
                    message=(
                        f"Failed to upload attachment '{item_metadata['name']}': "
                        f"{error}"
                    ),
                )
        return PreparedInput(
            content=self._append_context(content, metadata),
            attachment_metadata=metadata,
        )

    def metadata_with_attachments(
        self,
        metadata: dict[str, MetadataScalar],
        attachments: list[dict[str, str]],
    ) -> dict[str, MetadataScalar]:
        if not attachments:
            return dict(metadata)
        result = dict(metadata)
        result[ATTACHMENT_METADATA_KEY] = "\n".join(
            "|".join(
                [item["name"], item["path"], item.get("content_type", ""), item["kind"]]
            )
            for item in attachments
        )
        return result

    def normalized_workspace_root(self) -> str | None:
        raw_root = self._context.control.workspace_root.strip()
        if not raw_root:
            return None
        if raw_root != "/":
            raw_root = raw_root.rstrip("/")
        root = PurePosixPath(raw_root)
        if not root.is_absolute() or ".." in root.parts:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message="Runtime workspace root must be an absolute normalized path",
            )
        return str(root)

    def ensure_workspace_path(self, path: str, *, kind: str) -> None:
        root_text = self.normalized_workspace_root()
        if root_text is None:
            raise GatewayTaskError(
                code="runtime_unavailable",
                message="Runtime workspace root is not configured",
            )
        candidate = PurePosixPath(path)
        if (
            not candidate.is_absolute()
            or ".." in candidate.parts
            or not candidate.is_relative_to(PurePosixPath(root_text))
        ):
            raise GatewayTaskError(
                code="workspace_path_forbidden",
                message=f"{kind} path is outside the runtime workspace",
            )

    def _decode_attachment(self, attachment: AttachmentInput, *, filename: str) -> bytes:
        try:
            content = base64.b64decode(attachment.data_base64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise GatewayTaskError(
                code="invalid_attachment",
                message=f"Attachment '{attachment.name}' is not valid base64",
            ) from exc
        if len(content) > self._context.attachment_max_bytes:
            raise GatewayTaskError(
                code="attachment_too_large",
                message=f"Attachment '{attachment.name}' exceeds max size",
            )
        return content

    async def _run_in_thread(self, function: Any, *args: Any) -> Any:
        import asyncio

        return await asyncio.to_thread(function, *args)

    def _append_context(
        self,
        content: str,
        attachments: list[dict[str, str]],
    ) -> str:
        lines = ["", "Uploaded attachments:"]
        for attachment in attachments:
            details = [
                f"name={attachment['name']}",
                f"path={attachment['path']}",
            ]
            if attachment["content_type"]:
                details.append(f"content_type={attachment['content_type']}")
            details.append(f"kind={attachment['kind']}")
            lines.append(f"- {' | '.join(details)}")
        return content.rstrip() + "\n" + "\n".join(lines)


def sanitize_attachment_name(name: str) -> str:
    basename = PurePosixPath(name.replace("\\", "/")).name.strip()
    if not basename or basename in {".", ".."}:
        basename = "attachment"
    sanitized = "".join(
        char if char in SAFE_ATTACHMENT_CHARS else "_" for char in basename
    ).strip("._")
    return sanitized or "attachment"
