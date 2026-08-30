from __future__ import annotations

from ruyi_agent.channels.feishu.client import FeishuClient
from ruyi_agent.channels.gateway_client import GatewayTaskClient
from ruyi_agent.gateway_protocol.dto import GatewayPublishedArtifact, GatewayTask
from ruyi_agent.channels.media import MediaLimitError


class FeishuArtifactDelivery:
    """Bounded Gateway artifact download and Feishu upload transport."""

    def __init__(
        self,
        *,
        gateway_client: GatewayTaskClient,
        feishu_client: FeishuClient,
        max_bytes: int,
    ) -> None:
        self._gateway_client = gateway_client
        self._feishu_client = feishu_client
        self._max_bytes = max_bytes

    async def send(
        self,
        task: GatewayTask,
        artifact: GatewayPublishedArtifact,
        *,
        chat_id: str,
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
        await self._feishu_client.send_file(
            chat_id=chat_id,
            filename=downloaded.filename or "artifact",
            content=downloaded.content,
        )

    async def _send_limit_warning(
        self,
        *,
        chat_id: str,
        filename: str,
        error: MediaLimitError,
    ) -> None:
        await self._feishu_client.send_message(
            chat_id=chat_id,
            text=f"文件发送失败：{filename}\n{error}",
        )
