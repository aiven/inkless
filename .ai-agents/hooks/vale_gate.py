#!/usr/bin/env python3

import json
import os
import re
import shutil
import subprocess
import sys
from typing import Any, Final, TypedDict

RUNNER: Final = ".vale/inkless.py"
COMMIT_RE: Final = re.compile(r"\bgit\b[^;|&\n]*\bcommit\b")


class ClaudeEvent(TypedDict, total=False):
    hook_event_name: str
    tool_name: str
    tool_input: dict[str, Any]
    stop_hook_active: bool


def run_inkless_vale() -> tuple[int, str]:
    result = subprocess.run([sys.executable, RUNNER], capture_output=True, text=True)
    return result.returncode, (result.stdout + result.stderr).strip()


def pre_tool_use_action(event: ClaudeEvent) -> str | None:
    if event.get("tool_name") != "Bash":
        return None
    command = event.get("tool_input", {}).get("command", "")
    if not COMMIT_RE.search(command):
        return None
    return "Commit rejected"


def stop_action(event: ClaudeEvent) -> str | None:
    if event.get("stop_hook_active"):
        return None
    return "Don't stop yet"


def main() -> int:
    if shutil.which("vale") is None:
        print(
            "Vale is not installed. Install it from https://vale.sh/docs/install",
            file=sys.stderr,
        )
        return 0

    claude_project_dir = os.environ.get("CLAUDE_PROJECT_DIR", ".")
    os.chdir(claude_project_dir)

    event: ClaudeEvent = json.load(sys.stdin)
    match event.get("hook_event_name"):
        case "PreToolUse":
            action = pre_tool_use_action(event)
        case "Stop":
            action = stop_action(event)
        case _:
            action = None

    if action is None:
        return 0

    code, output = run_inkless_vale()
    if code == 0:
        return 0

    print(
        f"{action}: Vale found alerts on added prose.\n{output}\n"
        "Fix the alerts (see the inkless-vale skill), rerun `make vale`, "
        "and retry.",
        file=sys.stderr,
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
