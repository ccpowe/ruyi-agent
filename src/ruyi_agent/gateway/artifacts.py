"""Gateway artifact access policy and runtime download service."""

from __future__ import annotations

import asyncio
from pathlib import PurePosixPath

from ruyi_agent.gateway.application import GatewayApplicationContext
from ruyi_agent.gateway.attachments import GatewayAttachmentService
from ruyi_agent.gateway.errors import GatewayTaskError
from ruyi_agent.gateway.models import GatewayArtifact
from ruyi_agent.task_models import PublishedArtifact


class GatewayArtifactService:
    """Authorize workspace paths and return bounded artifact bytes."""

    def __init__(
        self,
        context: GatewayApplicationContext,
        attachments: GatewayAttachmentService,
    ) -> None:
        self._context = context
        self._attachments = attachments

    async def download(self, path: str) -> GatewayArtifact:
        self._attachments.ensure_workspace_path(path, kind="Artifact")
        result = await asyncio.to_thread(self._context.control.download_files, [path])
        if not result:
            raise self._not_found(path)
        item = result[0]
        error = getattr(item, "error", None)
        content = getattr(item, "content", None)
        if error or content is None:
            raise self._not_found(path)
        if len(content) > self._context.artifact_max_bytes:
            raise GatewayTaskError(
                code="artifact_too_large",
                message=f"Artifact exceeds max size: {path}",
            )
        filename = PurePosixPath(path).name or "artifact"
        return GatewayArtifact(
            path=path,
            filename=filename,
            content=content,
            content_type=guess_content_type(filename),
        )

    async def download_task_artifact(
        self,
        *,
        task_id: str,
        artifact_id: str,
    ) -> GatewayArtifact:
        route = await self._context.router.get_route(task_id)
        record = await self._context.router.get_record(route)
        artifact = find_artifact(record.artifacts, artifact_id)
        if artifact is None:
            raise self._not_found(artifact_id)
        downloaded = await self.download(artifact.path)
        return GatewayArtifact(
            path=artifact.path,
            filename=artifact.name,
            content=downloaded.content,
            content_type=artifact.content_type,
        )

    def _not_found(self, path: str) -> GatewayTaskError:
        return GatewayTaskError(
            code="artifact_not_found",
            message=f"Artifact is not readable: {path}",
        )


def guess_content_type(filename: str) -> str:
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".gif": "image/gif",
        ".webp": "image/webp",
        ".pdf": "application/pdf",
        ".txt": "text/plain",
        ".md": "text/markdown",
        ".csv": "text/csv",
        ".json": "application/json",
        ".html": "text/html",
        ".zip": "application/zip",
        ".mp3": "audio/mpeg",
        ".wav": "audio/wav",
        ".ogg": "audio/ogg",
        ".mp4": "video/mp4",
    }.get(PurePosixPath(filename).suffix.lower(), "application/octet-stream")


def find_artifact(
    artifacts: list[PublishedArtifact],
    artifact_id: str,
) -> PublishedArtifact | None:
    return next(
        (artifact for artifact in artifacts if artifact.artifact_id == artifact_id),
        None,
    )
