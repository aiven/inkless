#!/usr/bin/env python3

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Any, Final, TypedDict

RUNNER: Final = ".vale/inkless.py"
COMMIT_RE: Final = re.compile(r"\bgit\b[^;|&\n]*\bcommit\b")


class ClaudeEvent(TypedDict, total=False):
    hook_event_name: str
    tool_name: str
    tool_input: dict[str, Any]
    stop_hook_active: bool


class CursorEvent(TypedDict, total=False):
    hook_event_name: str
    command: str
    workspace_roots: list[str]
    status: str
    loop_count: int


class Harness[E](ABC):
    @abstractmethod
    def project_dir(self, event: E) -> str: ...

    @abstractmethod
    def action(self, event: E) -> str | None: ...

    @abstractmethod
    def report(self, event: E, message: str) -> int: ...


class ClaudeHarness(Harness[ClaudeEvent]):
    def project_dir(self, event: ClaudeEvent) -> str:
        return os.environ.get("CLAUDE_PROJECT_DIR", ".")

    def action(self, event: ClaudeEvent) -> str | None:
        match event.get("hook_event_name"):
            case "PreToolUse":
                if event.get("tool_name") != "Bash":
                    return None
                command = event.get("tool_input", {}).get("command", "")
                return "Commit rejected" if COMMIT_RE.search(command) else None
            case "Stop":
                return None if event.get("stop_hook_active") else "Don't stop yet"
            case _:
                return None

    def report(self, event: ClaudeEvent, message: str) -> int:
        print(message, file=sys.stderr)
        return 2


class CursorHarness(Harness[CursorEvent]):
    def project_dir(self, event: CursorEvent) -> str:
        roots = event.get("workspace_roots") or ["."]
        return os.environ.get("CURSOR_PROJECT_DIR", roots[0])

    def action(self, event: CursorEvent) -> str | None:
        match event.get("hook_event_name"):
            case "beforeShellExecution":
                command = event.get("command", "")
                return "Commit rejected" if COMMIT_RE.search(command) else None
            case "stop":
                completed = event.get("status") == "completed"
                first_pass = event.get("loop_count", 0) == 0
                return "Don't stop yet" if completed and first_pass else None
            case _:
                return None

    def report(self, event: CursorEvent, message: str) -> int:
        if event.get("hook_event_name") == "stop":
            payload = {"followup_message": message}
        else:
            payload = {
                "permission": "deny",
                "user_message": message,
                "agent_message": message,
            }
        print(json.dumps(payload))
        return 0


class HarnessType(StrEnum):
    CLAUDE = "claude"
    CURSOR = "cursor"


HARNESSES: Final[dict[HarnessType, Harness[Any]]] = {
    HarnessType.CLAUDE: ClaudeHarness(),
    HarnessType.CURSOR: CursorHarness(),
}


def run_inkless_vale() -> tuple[int, str]:
    result = subprocess.run([sys.executable, RUNNER], capture_output=True, text=True)
    return result.returncode, (result.stdout + result.stderr).strip()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--harness",
        type=HarnessType,
        choices=list(HarnessType),
        required=True,
    )
    args = parser.parse_args()
    harness = HARNESSES[args.harness]

    if shutil.which("vale") is None:
        print(
            "Vale is not installed. Install it from https://vale.sh/docs/install",
            file=sys.stderr,
        )
        return 0

    event = json.load(sys.stdin)
    os.chdir(harness.project_dir(event))

    action = harness.action(event)
    if action is None:
        return 0

    code, output = run_inkless_vale()
    if code == 0:
        return 0

    return harness.report(
        event,
        f"{action}: Vale found alerts on added prose.\n{output}\n"
        "Fix the alerts (see the inkless-vale skill), rerun `make vale`, "
        "and retry.",
    )


if __name__ == "__main__":
    sys.exit(main())
