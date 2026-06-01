from __future__ import annotations

import argparse
import base64
import os
import struct
import zlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage

from reasoning_chat_openai import ReasoningChatOpenAI, extract_reasoning_text


@dataclass(frozen=True)
class ProviderProbe:
    name: str
    key_envs: tuple[str, ...]
    base_url_envs: tuple[str, ...]
    default_base_url: str
    model_env: str
    default_model: str


PROVIDERS = {
    "deepseek": ProviderProbe(
        name="deepseek",
        key_envs=("DEEPSEEK_API_KEY", "deepseek_api_key"),
        base_url_envs=("DEEPSEEK_BASE_URL", "deepseek_base_url"),
        default_base_url="https://api.deepseek.com",
        model_env="DEEPSEEK_TEST_MODEL",
        default_model="deepseek-reasoner",
    ),
    "kimi": ProviderProbe(
        name="kimi",
        key_envs=("KIMI_API_KEY", "kimi_api_key"),
        base_url_envs=("KIMI_BASE_URL", "kimi_base_url"),
        default_base_url="https://api.moonshot.cn/v1",
        model_env="KIMI_TEST_MODEL",
        default_model="kimi-k2.6",
    ),
    "glm": ProviderProbe(
        name="glm",
        key_envs=("GLM_API_KEY", "glm_api_key", "ZAI_API_KEY", "zai_api_key"),
        base_url_envs=("GLM_BASE_URL", "glm_base_url", "ZAI_BASE_URL", "zai_base_url"),
        default_base_url="https://open.bigmodel.cn/api/paas/v4/",
        model_env="GLM_TEST_MODEL",
        default_model="glm-4.5",
    ),
}


def _png_chunk(kind: bytes, data: bytes) -> bytes:
    return (
        struct.pack(">I", len(data))
        + kind
        + data
        + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
    )


def _rgb_png_data_url(width: int = 16, height: int = 16) -> str:
    raw_rows = []
    for y in range(height):
        row = bytearray([0])
        for x in range(width):
            row.extend((240, 80 + (x * 8) % 120, 40 + (y * 8) % 120))
        raw_rows.append(bytes(row))
    png = (
        b"\x89PNG\r\n\x1a\n"
        + _png_chunk(
            b"IHDR",
            struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0),
        )
        + _png_chunk(b"IDAT", zlib.compress(b"".join(raw_rows)))
        + _png_chunk(b"IEND", b"")
    )
    return "data:image/png;base64," + base64.b64encode(png).decode("ascii")


def _first_env(names: tuple[str, ...], default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def _make_messages(*, with_image: bool) -> list[HumanMessage]:
    if not with_image:
        return [
            HumanMessage(
                content=(
                    "请用一句中文回答：9.11 和 9.8 哪个更大？"
                    "如果你有 reasoning_content 字段，请正常返回。"
                )
            )
        ]
    return [
        HumanMessage(
            content=[
                {
                    "type": "text",
                    "text": "这是一张 1x1 PNG 测试图。请用一句中文描述你看到了什么。",
                },
                {"type": "image_url", "image_url": {"url": _rgb_png_data_url()}},
            ]
        )
    ]


def _content_preview(content: Any, *, limit: int = 180) -> str:
    text = content if isinstance(content, str) else repr(content)
    return text.replace("\n", " ")[:limit]


def run_probe(
    provider_name: str,
    *,
    with_image: bool,
    show_reasoning: bool,
    max_tokens: int,
) -> bool:
    probe = PROVIDERS[provider_name]
    api_key = _first_env(probe.key_envs)
    if not api_key:
        print(f"[{provider_name}] skipped: missing API key env")
        return False

    base_url = _first_env(probe.base_url_envs, probe.default_base_url)
    model_name = os.getenv(probe.model_env, probe.default_model)
    chat = ReasoningChatOpenAI.for_provider(
        provider_name,  # type: ignore[arg-type]
        model=model_name,
        api_key=api_key,
        base_url=base_url,
        max_completion_tokens=max_tokens,
        timeout=60,
    )

    try:
        message = chat.invoke(_make_messages(with_image=with_image))
    except Exception as exc:  # noqa: BLE001
        print(f"[{provider_name}] failed: {exc.__class__.__name__}: {exc}")
        return False

    reasoning = extract_reasoning_text(message)
    reasoning_keys = [
        key
        for key in ("reasoning_content", "reasoning", "reasoning_details", "reasoning_text")
        if key in message.additional_kwargs
    ]
    print(
        f"[{provider_name}] ok model={model_name} "
        f"reasoning_chars={len(reasoning)} keys={reasoning_keys}"
    )
    print(f"[{provider_name}] content={_content_preview(message.content)}")
    if show_reasoning and reasoning:
        print(f"[{provider_name}] reasoning={_content_preview(reasoning, limit=600)}")
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--provider",
        choices=[*PROVIDERS.keys(), "all"],
        default="all",
    )
    parser.add_argument("--env-file", default=str(Path(__file__).with_name(".env.test")))
    parser.add_argument("--with-image", action="store_true")
    parser.add_argument("--show-reasoning", action="store_true")
    parser.add_argument("--max-tokens", type=int, default=512)
    args = parser.parse_args()

    load_dotenv(args.env_file, override=False)
    provider_names = (
        list(PROVIDERS)
        if args.provider == "all"
        else [args.provider]
    )
    results = [
        run_probe(
            provider,
            with_image=args.with_image,
            show_reasoning=args.show_reasoning,
            max_tokens=args.max_tokens,
        )
        for provider in provider_names
    ]
    if not any(results):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
