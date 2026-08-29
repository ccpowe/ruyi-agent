from __future__ import annotations

import re
from collections.abc import Callable

TELEGRAM_MAX_MESSAGE_LENGTH = 4096
TELEGRAM_MDV2_SPECIAL_CHARS = r"_*[]()~`>#+-=|{}.!"
FENCED_CODE_PATTERN = re.compile(r"```(?P<lang>[^\n`]*)\n?(?P<body>.*?)```", re.DOTALL)
INLINE_CODE_PATTERN = re.compile(r"`([^`\n]+)`")
LINK_PATTERN = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
HEADER_PATTERN = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)
BOLD_PATTERN = re.compile(r"\*\*(.+?)\*\*", re.DOTALL)
ITALIC_PATTERN = re.compile(r"(?<!\*)\*(?!\*)(.+?)(?<!\*)\*(?!\*)", re.DOTALL)
STRIKE_PATTERN = re.compile(r"~~(.+?)~~", re.DOTALL)
SPOILER_PATTERN = re.compile(r"\|\|(.+?)\|\|", re.DOTALL)
BLOCKQUOTE_PATTERN = re.compile(r"^(> ?.*)$", re.MULTILINE)
TABLE_SEPARATOR_PATTERN = re.compile(r"^\s*\|?[\s:-]+(?:\|[\s:-]+)+\|?\s*$")


def _utf16_len(text: str) -> int:
    return len(text.encode("utf-16-le")) // 2


def _escape_mdv2(text: str) -> str:
    escaped: list[str] = []
    for char in text:
        if char == "\\" or char in TELEGRAM_MDV2_SPECIAL_CHARS:
            escaped.append(f"\\{char}")
        else:
            escaped.append(char)
    return "".join(escaped)


def _strip_mdv2(text: str) -> str:
    text = re.sub(r"\\([\\_*[\]()~`>#+\-=|{}.!])", r"\1", text)
    text = text.replace("*", "").replace("_", "").replace("~", "")
    text = text.replace("||", "").replace("`", "")
    return text


def _protect_segments(
    text: str,
    pattern: re.Pattern[str],
    renderer: Callable[[re.Match[str]], str],
    placeholders: list[str],
) -> str:
    def replace(match: re.Match[str]) -> str:
        placeholder = f"\u0000TG{len(placeholders)}\u0000"
        placeholders.append(renderer(match))
        return placeholder

    return pattern.sub(replace, text)


def _restore_placeholders(text: str, placeholders: list[str]) -> str:
    for index in range(len(placeholders) - 1, -1, -1):
        text = text.replace(f"\u0000TG{index}\u0000", placeholders[index])
    return text


def _parse_pipe_row(line: str) -> list[str]:
    stripped = line.strip()
    if stripped.startswith("|"):
        stripped = stripped[1:]
    if stripped.endswith("|"):
        stripped = stripped[:-1]
    return [cell.strip() for cell in stripped.split("|")]


def _wrap_markdown_tables(text: str) -> str:
    lines = text.splitlines()
    result: list[str] = []
    index = 0
    in_code_fence = False
    while index < len(lines):
        line = lines[index]
        if line.strip().startswith("```"):
            in_code_fence = not in_code_fence
            result.append(line)
            index += 1
            continue
        if (
            not in_code_fence
            and index + 1 < len(lines)
            and "|" in line
            and "|" in lines[index + 1]
            and TABLE_SEPARATOR_PATTERN.match(lines[index + 1])
        ):
            headers = _parse_pipe_row(line)
            index += 2
            rows: list[list[str]] = []
            while index < len(lines) and "|" in lines[index] and lines[index].strip():
                rows.append(_parse_pipe_row(lines[index]))
                index += 1
            if headers and rows:
                for row in rows:
                    title = row[0] if row else "row"
                    result.append(f"**{title}**")
                    pairs = zip(headers[1:], row[1:], strict=False)
                    for header, value in pairs:
                        result.append(f"- {header}: {value}")
                    result.append("")
                if result and result[-1] == "":
                    result.pop()
                continue
        result.append(line)
        index += 1
    return "\n".join(result)


def _format_telegram_markdown_v2(text: str) -> str:
    text = _wrap_markdown_tables(text)
    placeholders: list[str] = []

    def stash(rendered: str) -> str:
        placeholder = f"\u0000TG{len(placeholders)}\u0000"
        placeholders.append(rendered)
        return placeholder

    def render_fence(match: re.Match[str]) -> str:
        lang = match.group("lang")
        body = match.group("body").replace("\\", "\\\\").replace("`", "\\`")
        return f"```{lang}\n{body}```"

    text = _protect_segments(text, FENCED_CODE_PATTERN, render_fence, placeholders)
    text = _protect_segments(
        text,
        INLINE_CODE_PATTERN,
        lambda match: f"`{match.group(1).replace('\\', '\\\\')}`",
        placeholders,
    )
    text = _protect_segments(
        text,
        LINK_PATTERN,
        lambda match: (
            f"[{_escape_mdv2(match.group(1))}]"
            f"({match.group(2).replace('\\', '\\\\').replace(')', '\\)')})"
        ),
        placeholders,
    )

    text = HEADER_PATTERN.sub(
        lambda match: stash(f"*{_escape_mdv2(match.group(2).strip('* '))}*"),
        text,
    )
    text = BOLD_PATTERN.sub(
        lambda match: stash(f"*{_escape_mdv2(match.group(1))}*"),
        text,
    )
    text = STRIKE_PATTERN.sub(
        lambda match: stash(f"~{_escape_mdv2(match.group(1))}~"),
        text,
    )
    text = SPOILER_PATTERN.sub(
        lambda match: stash(f"||{_escape_mdv2(match.group(1))}||"),
        text,
    )
    text = ITALIC_PATTERN.sub(
        lambda match: stash(f"_{_escape_mdv2(match.group(1))}_"),
        text,
    )
    text = BLOCKQUOTE_PATTERN.sub(
        lambda match: stash(f"> {_escape_mdv2(match.group(1)[1:].lstrip())}"),
        text,
    )

    text = _escape_mdv2(text)
    return _restore_placeholders(text, placeholders)


def _split_telegram_message(
    text: str, limit: int = TELEGRAM_MAX_MESSAGE_LENGTH
) -> list[str]:
    if _utf16_len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if _utf16_len(current + line) <= limit:
            current += line
            continue
        if current:
            chunks.append(current)
            current = ""
        if _utf16_len(line) <= limit:
            current = line
            continue
        for char in line:
            if _utf16_len(current + char) > limit and current:
                chunks.append(current)
                current = char
            else:
                current += char
    if current:
        chunks.append(current)
    return chunks
