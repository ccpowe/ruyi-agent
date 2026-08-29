from __future__ import annotations

import re

from ruyi_agent.channels.telegram.client import (
    KrokiMermaidRenderer,
    MermaidRenderError,
    TelegramAttachment,
)


async def extract_telegram_attachments(
    text: str,
    *,
    mermaid_renderer: KrokiMermaidRenderer,
) -> tuple[str, list[TelegramAttachment]]:
    """Render Telegram-specific inline diagrams into outbound attachments."""

    attachments: list[TelegramAttachment] = []
    pattern = re.compile(
        r"```mermaid\n(?P<body>.*?)```",
        re.DOTALL | re.IGNORECASE,
    )
    parts: list[str] = []
    last_end = 0
    rendered_count = 0
    for match in pattern.finditer(text):
        parts.append(text[last_end : match.start()])
        source = match.group("body").strip()
        try:
            image_bytes = await mermaid_renderer.render_png(source)
        except MermaidRenderError:
            parts.append(match.group(0))
            last_end = match.end()
            continue
        rendered_count += 1
        attachments.append(
            TelegramAttachment(
                kind="photo",
                filename=f"mermaid_{rendered_count}.png",
                content=image_bytes,
                caption=f"Mermaid diagram {rendered_count}",
            )
        )
        parts.append(f"[Mermaid diagram {rendered_count}]")
        last_end = match.end()
    parts.append(text[last_end:])
    return re.sub(r"\n{3,}", "\n\n", "".join(parts)).strip(), attachments
