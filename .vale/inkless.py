#!/usr/bin/env python3
"""Vale wrapper for inkless-owned files.

Supported modes:
  - Lint everything in full (--all)
  - Lint only changed lines, compared to an optional base (--base <ref>)

Usage:
  inkless.py [--base <ref>] [--all]
"""

import argparse
import json
import os
import subprocess
import sys
from collections.abc import Iterable
from typing import Final, TypedDict

OWNER: Final = "@aiven/inkless"
VALE_ALERTS_FOUND_EXIT_CODE: Final = 1


class Alert(TypedDict, total=False):
    Check: str
    Line: int
    Message: str
    Severity: str
    Span: list[int]


def _run(cmd: str, *args: str) -> str:
    result = subprocess.run([cmd, *args], capture_output=True, text=True, check=True)
    return result.stdout


def git(*args: str) -> str:
    return _run("git", *args)


def vale(*args: str) -> str:
    return _run("vale", *args)


def owned_pathspecs() -> list[str]:
    pathspecs = []
    with open("INKLESS_OWNERSHIP", encoding="utf-8") as manifest:
        for line in manifest:
            fields = line.split("#", 1)[0].split()
            if len(fields) == 2 and fields[1] == OWNER:
                pathspecs.append(":(glob)" + fields[0])
    if not pathspecs:
        sys.exit("No OWNED entries found in INKLESS_OWNERSHIP")
    return pathspecs


def resolve_base(base_arg: str | None) -> str:
    candidates = [base_arg] if base_arg else ["origin/main", "main"]
    for ref in candidates:
        try:
            return git("merge-base", "HEAD", ref).strip()
        except subprocess.CalledProcessError:
            continue

    # merge-base fails on shallow clones
    # fall back to the ref itself
    if base_arg:
        try:
            return git("rev-parse", "--verify", f"{base_arg}^{{commit}}").strip()
        except subprocess.CalledProcessError:
            sys.exit(f"--base {base_arg!r} doesn't resolve to a commit")
    sys.exit("Cannot resolve a base ref; pass one with --base")


def drop_symlinks(paths: Iterable[str]) -> list[str]:
    return [path for path in paths if os.path.isfile(path) and not os.path.islink(path)]


def changed_files(base: str, pathspecs: list[str]) -> list[str]:
    tracked = git(
        "diff",
        "--name-only",
        "--diff-filter=d",
        base,
        "--",
        *pathspecs,
    ).splitlines()
    untracked = git(
        "ls-files",
        "--others",
        "--exclude-standard",
        "--",
        *pathspecs,
    ).splitlines()
    files = sorted(set(tracked) | set(untracked))
    return drop_symlinks(files)


def added_lines(base: str, path: str) -> set[int] | None:
    if not git("ls-files", "--", path).strip():
        return None

    lines: set[int] = set()
    diff = git(
        "diff",
        "--no-ext-diff",
        "--no-color",
        "-U0",
        base,
        "--",
        path,
    )
    for line in diff.splitlines():
        if not line.startswith("@@"):
            continue
        added = line.split()[2]  # "+start[,count]"
        start, _, count = added.lstrip("+").partition(",")
        lines.update(range(int(start), int(start) + int(count or 1)))
    return lines


def run_vale(files: list[str]) -> dict[str, list[Alert]]:
    try:
        output = vale("--output=JSON", *files)
    except subprocess.CalledProcessError as error:
        if error.returncode != VALE_ALERTS_FOUND_EXIT_CODE:
            sys.exit(f"vale failed:\n{error.stderr}")
        output = error.stdout

    if not output.strip():
        sys.exit("vale produced no output")

    alerts: dict[str, list[Alert]] = json.loads(output)
    return alerts


def report(path: str, alert: Alert) -> None:
    line, col = alert["Line"], alert["Span"][0]
    severity, check, msg = alert["Severity"], alert["Check"], alert["Message"]
    print(f"{path}:{line}:{col} {severity} [{check}] {msg}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--base",
        help="base ref to diff against",
    )
    scope.add_argument(
        "--all",
        action="store_true",
        help="lint all owned files in full",
    )
    args = parser.parse_args()

    pathspecs = owned_pathspecs()
    base: str | None
    if args.all:
        git_ls_files = git("ls-files", "--", *pathspecs).splitlines()
        base, files = None, drop_symlinks(git_ls_files)
    else:
        base = resolve_base(args.base)
        files = changed_files(base, pathspecs)

    if not files:
        print("No changed inkless-owned files to lint.")
        return 0

    count = 0
    for path, alerts in run_vale(files).items():
        new_lines = None if base is None else added_lines(base, path)
        for alert in alerts:
            if new_lines is None or alert["Line"] in new_lines:
                report(path, alert)
                count += 1

    if count:
        print(f"\n{count} Vale alert(s) on added lines. Fix them before committing.")
        return 1

    print(f"Vale: no alerts on added lines across {len(files)} file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
