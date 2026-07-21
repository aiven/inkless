#!/usr/bin/env python3
"""Generate an Inkless changelog draft between two inkless-release tags.

Sources, in order of reliability:
  1. Conventional-commit log (backbone) -- categorized by type/scope.
  2. configs.rst delta                  -- RELIABLE (config keys are stable).
  3. metrics.rst delta                  -- NOISY, marked REVIEW (see caveat below).

Caveat on metrics: docs/inkless/metrics.rst is produced by MetricsDocs.main(),
a hand-maintained list of metric registries. When a registry is added to that
list, its metrics appear as "added" in the diff even though the code shipped
earlier (e.g. the 3->16 mbean jump at 0.39 was backfilled tooling, not 13 new
metrics). Always curate the metrics block before publishing.

Usage:
  gen-changelog.py                     # latest two inkless-release tags
  gen-changelog.py --version 0.44      # 0.43 -> 0.44
  gen-changelog.py --from inkless-release-0.43 --to inkless-release-0.44
"""
import argparse
import re
import subprocess
import sys

TYPE_LABELS = [
    ("feat", "Features"),
    ("fix", "Fixes"),
    ("refactor", "Refactors"),
    ("test", "Tests"),
    ("docs", "Docs"),
    ("chore", "Chores"),
]
# Types surfaced in the curated GH-release-notes summary (feat/fix only).
SUMMARY_TYPES = {"feat", "fix"}


def git(*args):
    r = subprocess.run(["git", *args], capture_output=True, text=True)
    if r.returncode != 0:
        if r.stderr.strip():
            print(f"warning: git {' '.join(args)}: {r.stderr.strip()}", file=sys.stderr)
        return None
    return r.stdout


def release_tags():
    out = git("tag") or ""
    tags = [t for t in out.split() if re.match(r"^inkless-release-0\.\d+$", t)]
    return sorted(tags, key=lambda t: int(t.rsplit(".", 1)[-1]))


def kafka_base_tags(version):
    out = git("tag") or ""
    tags = [t for t in out.split()
            if re.match(rf"^inkless-4\.\d+\.\d+-{re.escape(version)}$", t)]
    # extract the kafka version portion "4.1.2"
    return sorted({t[len("inkless-"):-(len(version) + 1)] for t in tags})


def show(tag, path):
    return git("show", f"{tag}:{path}")


def gradle_version(tag):
    """Return (kafka_base, scala) from gradle.properties at tag.

    The version string is like `4.2.0-inkless-SNAPSHOT`; `-inkless` is stripped so
    the Kafka base reads `4.2.0-SNAPSHOT`.
    """
    text = show(tag, "gradle.properties") or ""
    ver = scala = None
    for line in text.splitlines():
        if line.startswith("version="):
            ver = line[len("version="):].strip().replace("-inkless", "")
        elif line.startswith("scalaVersion="):
            scala = line[len("scalaVersion="):].strip()
    return ver, scala


def upstream_syncs(frm, to):
    """Merge commits in range that merged apache/kafka trunk (an upstream sync)."""
    out = git("log", "--merges", "--format=%s", f"{frm}..{to}") or ""
    return [s.strip() for s in out.splitlines()
            if re.search(r"apache/kafka|sync/upstream", s) or s.startswith("merge: apache")]


def upstream_note(frm, to):
    """One-line note on upstream syncs / main base version move in the range, or None."""
    fv, fs = gradle_version(frm)
    tv, ts = gradle_version(to)
    synced = bool(upstream_syncs(frm, to))
    if tv and fv and tv != fv:
        note = f"Upstream sync: main development base moved to Kafka {tv} (from {fv})"
        if ts and fs and ts != fs:
            note += f", Scala {fs} -> {ts}"
        return note + "."
    if synced:
        return "Upstream sync: merged apache/kafka trunk (no base version change)."
    return None


def parse_configs(text):
    keys, prefix = set(), ""
    if not text:
        return keys
    for line in text.splitlines():
        m = re.match(r"^Under ``([^`]*)``", line)
        if m:
            prefix = m.group(1)
            continue
        m = re.match(r"^``([^`]+)``\s*$", line)
        if m:
            keys.add(prefix + m.group(1))
    return keys


def parse_metrics(text):
    mbeans, attrs, cur = set(), set(), None
    if not text:
        return mbeans, attrs
    for line in text.splitlines():
        m = re.match(r"^(io\.aiven\.inkless[^\s]+)", line)
        if m:
            cur = m.group(1)
            mbeans.add(cur)
            continue
        m = re.match(r"^([A-Za-z][a-zA-Z0-9\-\._]+)\s{2,}\S", line)
        if m and cur:
            attrs.add(f"{cur} :: {m.group(1)}")
    return mbeans, attrs


def parse_commit(subject):
    """Return (type, scope, description) for a conventional commit, else None."""
    m = re.match(r"^(\w+)(?:\(([^)]*)\))?!?:\s*(.+)$", subject)
    if not m:
        return None
    return m.group(1).lower(), (m.group(2) or "").lower(), m.group(3).strip()


def collect_commits(frm, to):
    # --first-parent follows only the mainline of PR squash-merges, excluding the
    # thousands of individual upstream commits pulled in by apache/kafka merge commits.
    out = git("log", "--first-parent", "--no-merges", "--format=%s", f"{frm}..{to}") or ""
    buckets = {t: [] for t, _ in TYPE_LABELS}
    other = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        parsed = parse_commit(line)
        if not parsed:
            continue  # skip Merge/KAFKA-/MINOR upstream noise
        typ, scope, desc = parsed
        # keep only inkless-relevant scopes; drop pure sync bookkeeping
        if scope.startswith("sync") or scope == "sync":
            continue
        entry = (scope, desc)
        if typ in buckets:
            buckets[typ].append(entry)
        else:
            other.append(entry)
    return buckets, other


def fmt_entry(scope, desc):
    return f"- {'(' + scope + ') ' if scope else ''}{desc}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--version")
    ap.add_argument("--from", dest="frm")
    ap.add_argument("--to")
    ap.add_argument("--summary", action="store_true",
                    help="emit only the curated GH-release-notes summary (feat/fix + config "
                         "changes; metric deltas are omitted -- too noisy to auto-publish, see "
                         "module docstring)")
    args = ap.parse_args()

    tags = release_tags()
    if args.frm and args.to:
        frm, to = args.frm, args.to
    elif args.version:
        to = f"inkless-release-{args.version}"
        if to not in tags:
            ap.error(f"tag {to} not found; known: {', '.join(tags) or '(none)'}")
        idx = tags.index(to)
        if idx == 0:
            ap.error(f"{to} is the earliest inkless-release tag; nothing precedes it")
        frm = tags[idx - 1]
    else:
        if len(tags) < 2:
            ap.error(f"need at least two inkless-release tags, found {len(tags)}")
        frm, to = tags[-2], tags[-1]

    version = to.rsplit("-", 1)[-1]
    kafka = kafka_base_tags(version)

    ck_add = sorted(parse_configs(show(to, "docs/inkless/configs.rst"))
                    - parse_configs(show(frm, "docs/inkless/configs.rst")))
    ck_rm = sorted(parse_configs(show(frm, "docs/inkless/configs.rst"))
                   - parse_configs(show(to, "docs/inkless/configs.rst")))
    mb_to, ma_to = parse_metrics(show(to, "docs/inkless/metrics.rst"))
    mb_fr, ma_fr = parse_metrics(show(frm, "docs/inkless/metrics.rst"))
    mb_add, mb_rm = sorted(mb_to - mb_fr), sorted(mb_fr - mb_to)
    ma_add, ma_rm = sorted(ma_to - ma_fr), sorted(ma_fr - ma_to)

    buckets, other = collect_commits(frm, to)

    p = print
    header = f"## {version}"
    if kafka:
        header += f" (Kafka {', '.join(kafka)})"
    p(header)
    p("")

    note = upstream_note(frm, to)
    if note:
        p(f"> {note}")
        p("")

    if args.summary:
        for typ, label in TYPE_LABELS:
            if typ not in SUMMARY_TYPES:
                continue
            if buckets[typ]:
                p(f"### {label}")
                for scope, desc in buckets[typ]:
                    p(fmt_entry(scope, desc))
                p("")
        if ck_add or ck_rm:
            p("### Configuration")
            for c in ck_add:
                p(f"- Added `{c}`")
            for c in ck_rm:
                p(f"- Removed `{c}`")
            p("")
        return

    for typ, label in TYPE_LABELS:
        if buckets[typ]:
            p(f"### {label}")
            for scope, desc in buckets[typ]:
                p(fmt_entry(scope, desc))
            p("")
    if other:
        p("### Other")
        for scope, desc in other:
            p(fmt_entry(scope, desc))
        p("")

    p("### Config & metric changes")
    if ck_add or ck_rm:
        for c in ck_add:
            p(f"- config added: `{c}`")
        for c in ck_rm:
            p(f"- config removed: `{c}`")
    else:
        p("- no config changes")
    if mb_add or mb_rm or ma_add or ma_rm:
        p("")
        p("<!-- REVIEW metrics: metrics.rst is a hand-maintained registry list;"
          " a delta here may be tooling backfill, not a shipped metric. Curate before publishing. -->")
        for m in mb_add:
            p(f"- metric mbean added: `{m}`")
        for m in mb_rm:
            p(f"- metric mbean removed: `{m}`")
        for a in ma_add:
            p(f"- metric attr added: `{a}`")
        for a in ma_rm:
            p(f"- metric attr removed: `{a}`")
    p("")


if __name__ == "__main__":
    main()
