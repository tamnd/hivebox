"""verl tools that act in the trajectory's hivebox cell: `bash`, `str_replace_editor` and
`submit`. They follow verl's BaseTool, and each call finds its trajectory's cell, so the three
share one cell and one shell however verl makes and releases its tool instances."""

from __future__ import annotations

import posixpath
import time
from typing import Any

from verl.tools.base_tool import BaseTool
from verl.tools.schemas import OpenAIFunctionToolSchema, ToolResponse

from .. import _errors
from . import _trajectory

# How much of a command's output or a file view the model sees.
MAX_OUTPUT = 16 << 10
SNIPPET_LINES = 4


def _schema(name: str, description: str, properties: dict, required: list[str]) -> OpenAIFunctionToolSchema:
    return OpenAIFunctionToolSchema.model_validate({
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {"type": "object", "properties": properties, "required": required},
        },
    })


def _clip(text: str) -> str:
    if len(text) <= MAX_OUTPUT:
        return text
    half = MAX_OUTPUT // 2
    return text[:half] + f"\n... ({len(text) - MAX_OUTPUT} characters left out) ...\n" + text[-half:]


class _HiveTool(BaseTool):
    """What the three tools share. The config takes `endpoint`, `token` and `project`, which
    default to $HIVE_ENDPOINT, $HIVE_TOKEN and $HIVE_PROJECT, and `task`, the defaults for
    every sample in the same shape as a hive-pollen task."""

    def __init__(self, config: dict, tool_schema: OpenAIFunctionToolSchema | None = None):
        self._create_kwargs: dict[str, dict] = {}
        super().__init__(config or {}, tool_schema)

    async def create(self, instance_id: str | None = None, **kwargs) -> tuple[str, ToolResponse]:
        instance_id, response = await super().create(instance_id)
        self._create_kwargs[instance_id] = kwargs.get("create_kwargs") or {}
        return instance_id, response

    async def release(self, instance_id: str, **kwargs) -> None:
        self._create_kwargs.pop(instance_id, None)

    def trajectory(self, instance_id: str, kwargs: dict) -> _trajectory.Trajectory:
        return _trajectory.find(self.config, kwargs, self._create_kwargs.get(instance_id))


class BashTool(_HiveTool):
    """Runs a command in the trajectory's shell, which keeps its directory and variables."""

    def get_openai_tool_schema(self) -> OpenAIFunctionToolSchema:
        return _schema(
            "bash",
            "Run a bash command in the repository. The shell keeps its working directory and variables between calls.",
            {"command": {"type": "string", "description": "The command to run."}},
            ["command"],
        )

    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        command = parameters.get("command")
        if not isinstance(command, str) or not command.strip():
            return ToolResponse(text="bash needs a command."), 0.0, {}
        t = self.trajectory(instance_id, kwargs)
        start = time.monotonic()
        try:
            out, code, timed_out = await t.shell(command, t.command_timeout)
        except _errors.HiveError as e:
            return ToolResponse(text=f"The command could not be run: {e}"), 0.0, {"hive_error": e.reason}
        text = _clip(out)
        if timed_out:
            text += f"\n(the command was stopped after {t.command_timeout:.0f} s)"
        elif code != 0:
            text += f"\n(exit code {code})"
        return ToolResponse(text=text or "(no output)"), 0.0, {"exit_code": code, "ms": int((time.monotonic() - start) * 1000)}


class EditorTool(_HiveTool):
    """Views, creates and edits files, as SWE-agent's str_replace_editor does."""

    def get_openai_tool_schema(self) -> OpenAIFunctionToolSchema:
        return _schema(
            "str_replace_editor",
            "View, create and edit files. `view` shows a file with line numbers or lists a directory. `create` writes a new file. "
            "`str_replace` replaces old_str, which must appear exactly once, with new_str. `insert` adds new_str after line "
            "insert_line. `undo_edit` takes back the last edit to the file. Relative paths are in the repository.",
            {
                "command": {"type": "string", "enum": ["view", "create", "str_replace", "insert", "undo_edit"]},
                "path": {"type": "string", "description": "The file or directory."},
                "file_text": {"type": "string", "description": "The new file's contents, for create."},
                "old_str": {"type": "string", "description": "The text to replace, for str_replace."},
                "new_str": {"type": "string", "description": "The new text, for str_replace and insert."},
                "insert_line": {"type": "integer", "description": "The line new_str goes after, for insert. 0 is the top."},
                "view_range": {"type": "array", "items": {"type": "integer"}, "description": "First and last line to view, -1 for the end."},
            },
            ["command", "path"],
        )

    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        t = self.trajectory(instance_id, kwargs)
        command, path = parameters.get("command"), parameters.get("path")
        if not isinstance(path, str) or not path:
            return ToolResponse(text="str_replace_editor needs a path."), 0.0, {}
        path = posixpath.normpath(posixpath.join(t.workdir, path))
        try:
            cell = await t.get_cell()
            text = await self._run(t, cell, command, path, parameters)
        except _errors.FileError as e:
            text = f"{path}: {e}"
        except _errors.HiveError as e:
            return ToolResponse(text=f"The edit could not be made: {e}"), 0.0, {"hive_error": e.reason}
        return ToolResponse(text=_clip(text)), 0.0, {}

    async def _run(self, t, cell, command, path, p) -> str:
        if command == "view":
            info = await cell.files.stat(path)
            if info.type == "dir":
                r = await cell.run(["find", path, "-maxdepth", "2", "-not", "-path", "*/.*"], timeout=30)
                return r.stdout.decode("utf-8", "replace")
            text = await cell.files.read_text(path)
            lines = text.removesuffix("\n").split("\n")
            first, last = 1, len(lines)
            if p.get("view_range"):
                first, last = int(p["view_range"][0]), int(p["view_range"][1])
                if last == -1:
                    last = len(lines)
                if not 1 <= first <= last <= len(lines):
                    return f"view_range should be within 1 to {len(lines)}."
            return _numbered(lines[first - 1:last], first)
        if command == "create":
            if not isinstance(p.get("file_text"), str):
                return "create needs file_text."
            if await cell.files.exists(path):
                return f"{path} already exists. Use str_replace to change it."
            await cell.files.write(path, p["file_text"])
            t.history.setdefault(path, []).append(None)
            return f"Created {path}."
        if command in ("str_replace", "insert"):
            old = await cell.files.read_text(path)
            new_str = p.get("new_str") or ""
            if command == "str_replace":
                old_str = p.get("old_str")
                if not old_str:
                    return "str_replace needs old_str."
                n = old.count(old_str)
                if n != 1:
                    return f"old_str appears {n} times in {path}, and it has to appear exactly once."
                new = old.replace(old_str, new_str)
                at = old[:old.index(old_str)].count("\n")
            else:
                lines = old.split("\n")
                at = int(p.get("insert_line", -1))
                if not 0 <= at <= len(lines):
                    return f"insert_line should be within 0 to {len(lines)}."
                new = "\n".join(lines[:at] + new_str.split("\n") + lines[at:])
            await cell.files.write(path, new)
            t.history.setdefault(path, []).append(old)
            lines = new.split("\n")
            first = max(at - SNIPPET_LINES, 0)
            last = min(at + new_str.count("\n") + SNIPPET_LINES + 1, len(lines))
            return f"Edited {path}. Here are the lines around the edit:\n" + _numbered(lines[first:last], first + 1)
        if command == "undo_edit":
            if not t.history.get(path):
                return f"There is no edit to {path} to undo."
            before = t.history[path].pop()
            if before is None:
                await cell.files.remove(path)
                return f"Removed {path}, which create had made."
            await cell.files.write(path, before)
            return f"Took back the last edit to {path}."
        return f"Unknown command {command!r}. Use view, create, str_replace, insert or undo_edit."


def _numbered(lines: list[str], first: int) -> str:
    return "\n".join(f"{i:6}\t{line}" for i, line in enumerate(lines, first))


class SubmitTool(_HiveTool):
    """Ends the work: the trajectory's changes are checked in a verifier cell of their own. The
    model only hears that its changes were submitted, and the reward goes to the trainer as the
    tool's reward and, with HiveAgentLoop, as the sample's reward score."""

    def get_openai_tool_schema(self) -> OpenAIFunctionToolSchema:
        return _schema("submit", "Submit your changes when the task is done. Call it once, at the end.", {}, [])

    async def execute(self, instance_id: str, parameters: dict[str, Any], **kwargs) -> tuple[ToolResponse, float, dict]:
        t = self.trajectory(instance_id, kwargs)
        if t.submitted:
            return ToolResponse(text="Your changes were already submitted."), 0.0, {}
        t.submitted = True
        await t.verify()
        reward = t.reward
        if _trajectory.current.get() is None:
            # Without HiveAgentLoop nothing else stops the cell.
            await t.close()
        return ToolResponse(text="Your changes were submitted."), reward or 0.0, t.fields()
