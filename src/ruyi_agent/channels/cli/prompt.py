from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
from pathlib import Path

from prompt_toolkit import PromptSession
from prompt_toolkit.application.run_in_terminal import run_in_terminal
from prompt_toolkit.completion import Completer, Completion, PathCompleter
from prompt_toolkit.document import Document
from prompt_toolkit.history import FileHistory, InMemoryHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.patch_stdout import patch_stdout


class CodingCliCompleter(Completer):
    def __init__(self, command_names: list[str]) -> None:
        self._command_names = sorted(command_names)
        self._path_completer = PathCompleter(
            only_directories=False,
            expanduser=True,
        )

    def get_completions(self, document: Document, complete_event):
        text_before_cursor = document.text_before_cursor
        current_word = document.get_word_before_cursor(WORD=True)
        stripped = text_before_cursor.lstrip()
        if stripped.startswith("/") and " " not in stripped:
            token = stripped
            for command in self._command_names:
                if command.startswith(token):
                    yield Completion(
                        command,
                        start_position=-len(token),
                        display=command,
                    )
            return

        at_index = current_word.rfind("@")
        if at_index == -1:
            return
        path_fragment = current_word[at_index + 1 :]
        path_document = Document(path_fragment, cursor_position=len(path_fragment))
        for completion in self._path_completer.get_completions(
            path_document,
            complete_event,
        ):
            yield Completion(
                "@" + completion.text,
                start_position=-(len(path_fragment) + 1),
                display="@" + str(completion.display_text),
            )


class InteractivePrompt:
    def __init__(
        self,
        *,
        command_names: list[str],
        history_path: Path | None = None,
    ) -> None:
        self._kb = self._build_key_bindings()
        self._session = PromptSession(
            completer=CodingCliCompleter(command_names),
            history=self._build_history(history_path),
            key_bindings=self._kb,
            multiline=True,
            prompt_continuation="... ",
            complete_while_typing=True,
        )

    async def read(self, *, agent_name: str, thread_id: str) -> str:
        prompt = f"{agent_name}:{thread_id[:8]}> "
        with patch_stdout():
            return await self._session.prompt_async(prompt)

    def _build_history(self, history_path: Path | None):
        path = history_path or self._default_history_path()
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
        except OSError:
            return InMemoryHistory()
        return FileHistory(str(path))

    def _default_history_path(self) -> Path:
        state_home = os.getenv("XDG_STATE_HOME")
        if state_home:
            return Path(state_home) / "ruyi_agent" / "interactive_history"
        return Path.home() / ".local" / "state" / "ruyi_agent" / "interactive_history"

    def _build_key_bindings(self) -> KeyBindings:
        bindings = KeyBindings()

        @bindings.add("enter")
        def _(event) -> None:
            event.current_buffer.validate_and_handle()

        @bindings.add("c-j")
        def _(event) -> None:
            event.current_buffer.insert_text("\n")

        @bindings.add("escape", "enter")
        def _(event) -> None:
            event.current_buffer.insert_text("\n")

        @bindings.add("c-d")
        def _(event) -> None:
            if event.current_buffer.text:
                event.current_buffer.delete()
                return
            event.app.exit(exception=EOFError)

        @bindings.add("c-o")
        def _(event) -> None:
            buffer = event.current_buffer

            def open_editor() -> None:
                edited = self._edit_text(buffer.text)
                if edited is not None:
                    buffer.text = edited
                    buffer.cursor_position = len(edited)

            run_in_terminal(open_editor)

        return bindings

    def _edit_text(self, current_text: str) -> str | None:
        editor = (
            os.getenv("VISUAL")
            or os.getenv("EDITOR")
            or self._first_available_editor()
        )
        if not editor:
            return None
        with tempfile.NamedTemporaryFile(
            mode="w+",
            suffix=".md",
            encoding="utf-8",
        ) as tmp:
            tmp.write(current_text)
            tmp.flush()
            try:
                subprocess.run([*shlex.split(editor), tmp.name], check=False)
            except OSError:
                return None
            tmp.seek(0)
            return tmp.read().rstrip("\n")

    def _first_available_editor(self) -> str | None:
        for candidate in ("code", "vim", "nano", "vi"):
            if _which(candidate) is not None:
                return candidate
        return None


def _which(command: str) -> str | None:
    path = os.getenv("PATH", "")
    for item in path.split(os.pathsep):
        candidate = Path(item) / command
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None
