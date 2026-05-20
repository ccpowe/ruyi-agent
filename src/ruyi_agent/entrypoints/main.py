from __future__ import annotations

import asyncio
import os
import sys

from ruyi_agent.channels.cli.interactive import run_interactive
from ruyi_agent.channels.feishu.adapter import run_feishu_adapter
from ruyi_agent.channels.telegram.adapter import run_telegram_adapter
from ruyi_agent.runtime.bootstrap import (
    DEFAULT_GATEWAY_HOST,
    DEFAULT_GATEWAY_PORT,
    create_bootstrapped_gateway_app,
)

app = create_bootstrapped_gateway_app()


def run_gateway() -> None:
    import uvicorn

    host = os.getenv("GATEWAY_HOST", DEFAULT_GATEWAY_HOST)
    port = int(os.getenv("GATEWAY_PORT", str(DEFAULT_GATEWAY_PORT)))
    uvicorn.run(app, host=host, port=port)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else os.getenv("APP_MODE", "gateway")
    if mode == "gateway":
        run_gateway()
        return
    if mode == "telegram":
        asyncio.run(run_telegram_adapter())
        return
    if mode == "feishu":
        asyncio.run(run_feishu_adapter())
        return
    if mode == "tui":
        from ruyi_agent.channels.cli.tui import app as tui_app

        sys.argv = [sys.argv[0], *sys.argv[2:]]
        tui_app()
        return
    if mode == "interactive":
        asyncio.run(run_interactive())
        return
    raise SystemExit(
        "Unsupported mode: "
        f"{mode!r}. Expected 'gateway', 'telegram', 'feishu', 'tui' "
        "or 'interactive'."
    )
