from __future__ import annotations

import argparse
import asyncio
import sys
from collections.abc import Sequence
from dataclasses import dataclass

from ruyi_agent.config.runtime_settings import (
    RuntimeSettings,
    configure_runtime_environment,
)

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


class EntrypointRunner:
    def run_channels(
        self,
        channels: ChannelSet,
        settings: RuntimeSettings,
    ) -> None:
        if not isinstance(settings, RuntimeSettings):
            raise TypeError("EntrypointRunner.run_channels requires RuntimeSettings")
        if channels == ("gateway",):
            run_gateway(settings)
            return
        asyncio.run(run_channels(channels, settings))


def create_app(settings: RuntimeSettings | None = None):
    from ruyi_agent.runtime.bootstrap import create_bootstrapped_gateway_app

    active_settings = configure_runtime_environment() if settings is None else settings
    if not isinstance(active_settings, RuntimeSettings):
        raise TypeError(
            "create_app requires configure_runtime_environment to return "
            "RuntimeSettings"
        )
    return create_bootstrapped_gateway_app(active_settings)


def run_gateway(settings: RuntimeSettings) -> None:
    import uvicorn

    if not isinstance(settings, RuntimeSettings):
        raise TypeError("run_gateway requires RuntimeSettings")
    uvicorn.run(
        create_app(settings),
        host=settings.gateway.host,
        port=settings.gateway.port,
    )


async def run_channels(
    channels: ChannelSet,
    settings: RuntimeSettings,
) -> None:
    if not isinstance(settings, RuntimeSettings):
        raise TypeError("run_channels requires RuntimeSettings")
    async with asyncio.TaskGroup() as task_group:
        if "gateway" in channels:
            task_group.create_task(_run_gateway_async(settings))
        if "telegram" in channels:
            from ruyi_agent.channels.telegram.adapter import run_telegram_adapter

            task_group.create_task(run_telegram_adapter(settings))
        if "feishu" in channels:
            from ruyi_agent.channels.feishu.adapter import run_feishu_adapter

            task_group.create_task(run_feishu_adapter(settings))


async def _run_gateway_async(settings: RuntimeSettings) -> None:
    import uvicorn

    if not isinstance(settings, RuntimeSettings):
        raise TypeError("_run_gateway_async requires RuntimeSettings")
    config = uvicorn.Config(
        create_app(settings),
        host=settings.gateway.host,
        port=settings.gateway.port,
    )
    server = uvicorn.Server(config)
    await server.serve()


def parse_cli_options(argv: Sequence[str] | None = None) -> CliOptions:
    parser = argparse.ArgumentParser(
        prog="ruyi",
        usage=(
            "ruyi [-h] [--init] [--force] [--workspace WORKSPACE] [--gateway] "
            "[--telegram] [--feishu] [--all] [mode]"
        ),
    )
    parser.add_argument(
        "mode",
        nargs="?",
        choices=["gateway", "telegram", "feishu"],
        metavar="mode",
        help="Legacy positional mode: gateway, telegram, or feishu.",
    )
    parser.add_argument("--init", action="store_true")
    parser.add_argument(
        "--force",
        action="store_true",
        help="With --init, overwrite existing generated config templates.",
    )
    parser.add_argument("--workspace", type=str, default=None)
    parser.add_argument("--gateway", action="store_true")
    parser.add_argument("--telegram", action="store_true")
    parser.add_argument("--feishu", action="store_true")
    parser.add_argument("--all", action="store_true")
    args = parser.parse_args(argv)
    if args.force and not args.init:
        parser.error("--force can only be used with --init")
    channels = _select_channels(
        mode=args.mode,
        gateway=args.gateway,
        telegram=args.telegram,
        feishu=args.feishu,
        all_channels=args.all,
    )
    if channels is None and not args.init:
        parser.error("select an entrypoint: --gateway, --telegram, --feishu, or --all")
    return CliOptions(
        workspace=args.workspace,
        init_only=args.init,
        init_force=args.force,
        all_channels=args.all,
        channels=channels,
    )


def main(
    argv: Sequence[str] | None = None,
    *,
    runner: EntrypointRunner | None = None,
) -> None:
    options = parse_cli_options(argv)
    try:
        settings = configure_runtime_environment(
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
    assert options.channels is not None
    channels = (
        _filter_configured_channels(options.channels, settings=settings)
        if options.all_channels
        else options.channels
    )
    active_runner.run_channels(channels, settings=settings)


def _filter_configured_channels(
    channels: ChannelSet,
    *,
    settings: RuntimeSettings,
) -> ChannelSet:
    configured = {
        "telegram": bool(settings.channels.telegram.bot_token),
        "feishu": bool(
            settings.channels.feishu.app_id and settings.channels.feishu.app_secret
        ),
    }
    return tuple(
        channel
        for channel in channels
        if channel == "gateway" or configured.get(channel, False)
    )


def _select_channels(
    *,
    mode: str | None,
    gateway: bool,
    telegram: bool,
    feishu: bool,
    all_channels: bool,
) -> ChannelSet | None:
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
