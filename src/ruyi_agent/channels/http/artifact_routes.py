"""Gateway artifact download HTTP routes and safe response headers."""

from __future__ import annotations

from pathlib import PurePosixPath
from urllib.parse import quote

from fastapi import Depends, FastAPI, Request
from fastapi.responses import Response

from .context import GatewayHttpContext
from .schemas import ArtifactDownloadRequest


def attach_artifact_routes(app: FastAPI, context: GatewayHttpContext) -> None:
    @app.post("/artifacts/download")
    async def download_artifact(
        request: Request,
        payload: ArtifactDownloadRequest,
        _: None = Depends(context.require_bearer),
    ) -> Response:
        artifact = await context.service(request).download_artifact(payload.path)
        return Response(
            content=artifact.content,
            media_type=artifact.content_type,
            headers={
                "Content-Disposition": attachment_content_disposition(
                    artifact.filename
                ),
                "X-Artifact-Path": header_path_value(artifact.path),
            },
        )

    @app.get("/tasks/{task_id}/artifacts/{artifact_id}/download")
    async def download_task_artifact(
        request: Request,
        task_id: str,
        artifact_id: str,
        _: None = Depends(context.require_bearer),
    ) -> Response:
        artifact = await context.service(request).download_task_artifact(
            task_id=task_id,
            artifact_id=artifact_id,
        )
        return Response(
            content=artifact.content,
            media_type=artifact.content_type,
            headers={
                "Content-Disposition": attachment_content_disposition(
                    artifact.filename
                ),
                "X-Artifact-Path": header_path_value(artifact.path),
                "X-Artifact-Id": artifact_id,
            },
        )


def attachment_content_disposition(filename: str) -> str:
    safe_filename = header_filename(filename)
    if safe_filename.isascii():
        return f'attachment; filename="{quote_header_filename(safe_filename)}"'
    fallback = ascii_filename_fallback(safe_filename)
    encoded = quote(safe_filename, safe="")
    return (
        f'attachment; filename="{quote_header_filename(fallback)}"; '
        f"filename*=UTF-8''{encoded}"
    )


def header_filename(filename: str) -> str:
    cleaned = PurePosixPath(filename.replace("\\", "/")).name.strip()
    cleaned = cleaned.replace("\r", "").replace("\n", "")
    return cleaned or "artifact"


def quote_header_filename(filename: str) -> str:
    return filename.replace("\\", "\\\\").replace('"', '\\"')


def ascii_filename_fallback(filename: str) -> str:
    suffix = PurePosixPath(filename).suffix
    if not suffix.isascii():
        suffix = ""
    return f"artifact{suffix}" if suffix else "artifact"


def header_path_value(path: str) -> str:
    return quote(path, safe="/-._~")
