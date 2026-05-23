from __future__ import annotations

import argparse
import asyncio
import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass

from ruyi_agent.config.runtime_settings import configure_runtime_environment

ChannelSet = tuple[str, ...]
DEFAULT_GATEWAY_HOST = "127.0.0.1"
DEFAULT_GATEWAY_PORT = 8000


@dataclass(frozen=True, slots=True)
class CliOptions:
    workspace: str | None
    channels: ChannelSet | None
    init_only: bool = False
    init_force: bool = False
    all_channels: bool = False


CHANNEL_ENV_REQUIREMENTS: dict[str, tuple[str, ...]] = {
    "telegram": ("TELEGRAM_BOT_TOKEN",),
    "feishu": ("FEISHU_APP_ID", "FEISHU_APP_SECRET"),
}


class EntrypointRunner:
    def run_tui(self) -> None:
        from ruyi_agent.channels.cli.tui import run_interactive

        asyncio.run(run_interactive())

    def run_channels(self, channels: ChannelSet) -> None:
        if channels == ("gateway",):
            run_gateway()
            return
        asyncio.run(run_channels(channels))


def create_app():
    from ruyi_agent.runtime.bootstrap import create_bootstrapped_gateway_app

    return create_bootstrapped_gateway_app()


def run_gateway() -> None:
    import uvicorn

    host = os.getenv("GATEWAY_HOST", DEFAULT_GATEWAY_HOST)
    port = int(os.getenv("GATEWAY_PORT", str(DEFAULT_GATEWAY_PORT)))
    uvicorn.run(create_app(), host=host, port=port)


async def run_channels(channels: ChannelSet) -> None:
    async with asyncio.TaskGroup() as task_group:
        if "gateway" in channels:
            task_group.create_task(_run_gateway_async())
        if "telegram" in channels:
            from ruyi_agent.channels.telegram.adapter import run_telegram_adapter

            task_group.create_task(run_telegram_adapter())
        if "feishu" in channels:
            from ruyi_agent.channels.feishu.adapter import run_feishu_adapter

            task_group.create_task(run_feishu_adapter())


async def _run_gateway_async() -> None:
    import uvicorn

    host = os.getenv("GATEWAY_HOST", DEFAULT_GATEWAY_HOST)
    port = int(os.getenv("GATEWAY_PORT", str(DEFAULT_GATEWAY_PORT)))
    config = uvicorn.Config(create_app(), host=host, port=port)
    server = uvicorn.Server(config)
    await server.serve()


def parse_cli_options(argv: Sequence[str] | None = None) -> CliOptions:
    parser = argparse.ArgumentParser(
        prog="ruyi",
        usage=(
            "ruyi [-h] [--init] [--force] [--workspace WORKSPACE] [--tui] [--gateway] "
            "[--telegram] [--feishu] [--all] [mode]"
        ),
    )
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["gateway", "telegram", "feishu", "tui", "interactive"],
        metavar="mode",
        help="Legacy positional mode: gateway, telegram, feishu, tui, interactive.",
    )
    parser.add_argument("--init", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="With --init, overwrite existing generated config templates.",
    )
    parser.add_argument("--workspace", type=str, default=None)
    parser.add_argument("--tui", action="store_true")
    parser.add_argument("--gateway", action="store_true")
    parser.add_argument("--telegram", action="store_true")
    parser.add_argument("--feishu", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)
    if args.force and not args.init:
        parser.error("--force can only be used with --init")
    return CliOptions(
        workspace=args.workspace,
        init_only=args.init,
        init_force=args.force,
        all_channels=args.all,
        channels=_select_channels(
            mode=args.mode,
            tui=args.tui,
            gateway=args.gateway,
            telegram=args.telegram,
            feishu=args.feishu,
            all_channels=args.all,
        ),
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: EntrypointRunner | None = None,
) -> None:
    options = parse_cli_options(argv)
    try:
        configure_runtime_environment(
            workspace=options.workspace,
            init_force=options.init_force,
            init_templates=options.init_only,
        )
    except ValueError as exc:
        print(f"ruyi: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    if options.init_only:
        return
    active_runner = runner or EntrypointRunner()
    if options.channels is None:
        active_runner.run_tui()
        return
    channels = (
        _filter_configured_channels(options.channels)
        if options.all_channels
        else options.channels
    )
    active_runner.run_channels(channels)


def _filter_configured_channels(
    channels: ChannelSet,
    *,
    getenv=os.getenv,
) -> ChannelSet:
    selected: list[str] = []
    for channel in channels:
        required_env = CHANNEL_ENV_REQUIREMENTS.get(channel)
        if required_env is None or all(getenv(name) for name in required_env):
            selected.append(channel)
    return tuple(selected)


def _select_channels(
    *,
    mode: str | None,
    tui: bool,
    gateway: bool,
    telegram: bool,
    feishu: bool,
    all_channels: bool,
) -> ChannelSet | None:
    if tui or mode in {"tui", "interactive"}:
        return None
    if gateway or mode == "gateway":
        return ("gateway",)

    selected_adapters: list[str] = []
    if all_channels:
        selected_adapters.extend(["telegram", "feishu"])
    else:
        if mode == "telegram" or telegram:
            selected_adapters.append("telegram")
        if mode == "feishu" or feishu:
            selected_adapters.append("feishu")
    if not selected_adapters:
        return None
    return ("gateway", *selected_adapters)


__all__ = [
    "CliOptions",
    "EntrypointRunner",
    "create_app",
    "main",
    "parse_cli_options",
    "run_channels",
    "run_gateway",
    "_filter_configured_channels",
]
