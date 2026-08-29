from __future__ import annotations

from pathlib import Path

from ruyi_agent.channels.gateway_client import GatewayTaskClient
from ruyi_agent.channels.gateway_dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.media import MediaLimitError
from ruyi_agent.channels.telegram.client import (
    IMAGE_ATTACHMENT_EXTENSIONS,
    TelegramClient,
)


class TelegramArtifactDelivery:
    """Bounded Gateway artifact download and Telegram upload transport."""

    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        telegram_client: TelegramClient,
        max_bytes: int,
    ) -> None:
        self._gateway_client = gateway_client
        self._telegram_client = telegram_client
        self._max_bytes = max_bytes

    async def send(
        self,
        task: GatewayTask,
        artifact: GatewayPublishedArtifact,
        *,
        chat_id: int,
    ) -> None:
        filename = artifact.name or artifact.artifact_id
        try:
            downloaded = await self._gateway_client.download_task_artifact(
                task_id=task.task_id,
                artifact_id=artifact.artifact_id,
            )
        except MediaLimitError as error:
            await self._send_limit_warning(
                chat_id=chat_id, filename=filename, error=error
            )
            return
        if len(downloaded.content) > self._max_bytes:
            error = MediaLimitError(
                f"media download rejected: payload exceeds {self._max_bytes} bytes"
            )
            await self._send_limit_warning(
                chat_id=chat_id, filename=filename, error=error
            )
            return
        kind = (
            "photo"
            if Path(downloaded.filename).suffix.lower() in IMAGE_ATTACHMENT_EXTENSIONS
            else "document"
        )
        kwargs = {
            "chat_id": chat_id,
            "filename": downloaded.filename or "artifact",
            "content": downloaded.content,
            "caption": artifact.caption or filename,
            "parse_mode": None,
        }
        if kind == "photo":
            await self._telegram_client.send_photo(**kwargs)
        else:
            await self._telegram_client.send_document(**kwargs)

    async def _send_limit_warning(
        self,
        *,
        chat_id: int,
        filename: str,
        error: MediaLimitError,
    ) -> None:
        await self._telegram_client.send_message(
            chat_id=chat_id,
            text=f"文件发送失败：{filename}\n{error}",
            parse_mode=None,
        )
