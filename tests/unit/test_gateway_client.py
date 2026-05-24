from __future__ import annotations

import asyncio
from typing import Any

import httpx

from ruyi_agent.channels.gateway_client import (
    GatewayHTTPClient,
    _filename_from_content_disposition,
)


class FakeGatewayHTTPClient(GatewayHTTPClient):
    def __init__(self, response: httpx.Response) -> None:
        super().__init__(base_url="http://gateway.test", bearer_token="token")
        self.response = response
        self.requests: list[dict[str, Any]] = []

    async def _request_raw(
        self,
        method: str,
        path: str,
        *,
        json: dict[str, Any] | None = None,
    ) -> httpx.Response:
        self.requests.append({"method": method, "path": path, "json": json})
        return self.response


def test_filename_from_content_disposition_parses_quoted_and_bare_values() -> None:
    assert _filename_from_content_disposition('attachment; filename="report.pdf"') == "report.pdf"
    assert _filename_from_content_disposition("attachment; filename=report.pdf") == "report.pdf"
    assert _filename_from_content_disposition("attachment") is None


def test_download_artifact_uses_content_disposition_filename() -> None:
    client = FakeGatewayHTTPClient(
        httpx.Response(
            200,
            content=b"artifact",
            headers={
                "content-disposition": 'attachment; filename="report.html"',
                "content-type": "text/html",
            },
        )
    )

    artifact = asyncio.run(client.download_artifact(path="/workspace/out/index.html"))

    assert artifact.filename == "report.html"
    assert artifact.content_type == "text/html"
    assert artifact.content == b"artifact"
    assert client.requests == [
        {
            "method": "POST",
            "path": "/artifacts/download",
            "json": {"path": "/workspace/out/index.html"},
        }
    ]


def test_download_artifact_falls_back_to_path_name_without_header() -> None:
    client = FakeGatewayHTTPClient(httpx.Response(200, content=b"artifact"))

    artifact = asyncio.run(client.download_artifact(path="/workspace/out/index.html"))

    assert artifact.filename == "index.html"
    assert artifact.content == b"artifact"


def test_download_task_artifact_uses_task_scoped_endpoint() -> None:
    client = FakeGatewayHTTPClient(
        httpx.Response(
            200,
            content=b"artifact",
            headers={
                "content-disposition": 'attachment; filename="report.html"',
                "content-type": "text/html",
            },
        )
    )

    artifact = asyncio.run(
        client.download_task_artifact(task_id="task-1", artifact_id="art_1")
    )

    assert artifact.filename == "report.html"
    assert artifact.content_type == "text/html"
    assert artifact.content == b"artifact"
    assert client.requests == [
        {
            "method": "GET",
            "path": "/tasks/task-1/artifacts/art_1/download",
            "json": None,
        }
    ]
