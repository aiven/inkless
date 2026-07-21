---
name: inkless-upstream-sync
description: Keep the Inkless fork current with upstream Apache Kafka by merging upstream changes. Covers main sync (merge apache/kafka trunk into main) and release sync (merge an upstream patch release such as 4.1.2 into an inkless-4.x release branch), plus checking how far behind a branch is. Use when asked to sync main with apache/kafka trunk, merge upstream, catch a release branch up to a Kafka patch release, or check how far behind upstream a branch is. For backporting inkless commits to release branches or cutting an increment, use the inkless-release-prep skill instead.
---

# Inkless Upstream Sync

Merge upstream Apache Kafka into the fork. Two workflows, both merge-based (never
rebase), following [VERSIONING-STRATEGY.md](../../../docs/inkless/VERSIONING-STRATEGY.md).

The tooling lives in [`inkless-sync/`](../../../inkless-sync/) (scripts, guides,
`lib/common.sh`, session templates, archived sessions). This skill is the agent
entry point; run scripts from the repo root.

| Workflow | When | Script | Guide |
| --- | --- | --- | --- |
| Main sync | Catch `main` up to apache/kafka trunk (every 2-3 months, or before a new Kafka minor) | `main-sync.sh` | [MAIN-SYNC-ACTION-PLAN.md](../../../inkless-sync/MAIN-SYNC-ACTION-PLAN.md) |
| Release sync | Merge an upstream patch (e.g. 4.1.2) into `inkless-4.1` | `release-sync.sh` | [RELEASE-SYNC-GUIDE.md](../../../inkless-sync/RELEASE-SYNC-GUIDE.md), [RELEASE-SYNC-ACTION-PLAN.md](../../../inkless-sync/RELEASE-SYNC-ACTION-PLAN.md) |
| Status check | How far behind upstream is a branch? | `sync-status.sh` | -- |

Conflict handling for both: [CONFLICT-RESOLUTION-STRATEGY.md](../../../inkless-sync/CONFLICT-RESOLUTION-STRATEGY.md).
Full tooling reference: [inkless-sync/README.md](../../../inkless-sync/README.md).

## Prerequisite

```bash
git remote add apache https://github.com/apache/kafka.git   # once
git fetch apache
```

## Main sync

```bash
./inkless-sync/main-sync.sh --dry-run          # preview
./inkless-sync/main-sync.sh                    # to apache/trunk HEAD
./inkless-sync/main-sync.sh --before-version 4.3
```

The script fetches upstream, creates a `sync/upstream-YYYYMMDD` branch, merges,
and categorizes conflicts: OWNED inkless paths auto-resolve "ours"; INTERLEAVED
files stop for manual review. Follow the action plan's file-by-file playbook.

## Release sync

```bash
./inkless-sync/release-sync.sh inkless-4.1 --list-tags
./inkless-sync/release-sync.sh inkless-4.1 --to-tag 4.1.2 --dry-run
./inkless-sync/release-sync.sh inkless-4.1 --to-tag 4.1.2
```

Resolve version-file conflicts with the `{upstream_version}-inkless` pattern
(keep upstream versions in `streams/quickstart` POMs). See the guide.

## Status check

```bash
./inkless-sync/sync-status.sh main
./inkless-sync/sync-status.sh --all
```

## Every sync

1. Track progress in a session file (templates in `inkless-sync/`):
   ```bash
   cp inkless-sync/MAIN-SYNC-SESSION-TEMPLATE.md    .inkless-sync/SESSION-$(date +%Y-%m-%d).md
   cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .inkless-sync/RELEASE-SESSION-<branch>-$(date +%Y-%m-%d).md
   ```
2. Resolve conflicts per the strategy doc; show diffs and wait for approval on
   INTERLEAVED files before committing.
3. Verify: `make build` then `make test`.
4. Commit with sync prefixes (`merge:`, `sync(compile):`, `sync(test):`, `sync(config):`, `sync(verify):`).
5. Archive the session:
   ```bash
   mv .inkless-sync/SESSION-*.md inkless-sync/sessions/
   ```

## Guardrails

- Merge, never rebase. Commit incrementally; keep behavioral and test-only fixes separate.
- When restoring a file upstream removed for inkless reasons, restore its test too and add an `INKLESS NOTE` comment.
- If blocked on a conflict, record it in the session file and ask rather than forcing it.
