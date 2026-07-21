---
name: inkless-release-prep
description: Prepare a new Inkless increment by aligning the active release branches (inkless-4.1, inkless-4.2, ...) with the inkless commits on main. Covers checking which inkless commits are missing from a release branch, cherry-picking (backporting) them main->release, and creating a new release branch for a new Kafka minor. This is the manual prep that precedes the automated GitHub release. Use when asked to prepare/cut an inkless increment, backport or cherry-pick inkless commits to release branches, check release-branch consistency with main, or create a new inkless-4.x release branch. For merging upstream apache/kafka changes, use the inkless-upstream-sync skill instead.
---

# Inkless Release Prep

Assemble a new Inkless increment (`inkless-release-<N>`). Inkless features land on
`main` first; before a release, each active release branch must be brought level
by cherry-picking the missing inkless commits. This skill covers that prep; the
release itself (tags, Docker images, binaries, GitHub Release) is automated -- see
[RELEASES.md](../../../docs/inkless/RELEASES.md).

The tooling lives in [`inkless-sync/`](../../../inkless-sync/). This skill is the
agent entry point; run scripts from the repo root.

## Worktree layout

Prep runs across dedicated git worktrees, one per branch you touch. The active
set is `main` plus each active release branch (currently 4.1 and 4.2 -- released
minors like 4.0 are excluded):

| Worktree           | Branch        | Role                                     |
| ------------------ | ------------- | ---------------------------------------- |
| `../inkless-main`  | `main`        | Cherry-pick source (features land here). |
| `../inkless-4.1`   | `inkless-4.1` | Active release branch (pick target).     |
| `../inkless-4.2`   | `inkless-4.2` | Active release branch (pick target).     |

Run `cherry-pick-to-release.sh <branch>` from the worktree that already has
`<branch>` checked out. The script checks out the target branch if HEAD isn't
already on it (`cherry-pick-to-release.sh:294`); using the matching worktree
avoids branch-switching churn in a shared checkout and the "branch already
checked out in another worktree" error, and keeps `main` available in
`../inkless-main` as the pick source.

### Prepare/update worktrees (pre-req)

Before cherry-picking, ensure every active worktree exists and is level with
`origin`. Offer to create missing ones and fast-forward the rest -- the tooling
diffs `origin/*` refs, so a stale local branch yields misleading results.

```bash
git fetch origin --prune

# branch -> worktree (one per active branch)
update_worktree() {
  local branch="$1" wt="$2"
  if [ -d "$wt" ]; then
    git -C "$wt" merge --ff-only "origin/$branch"   # update existing
  else
    git worktree add "$wt" "$branch"                # create missing
  fi
}

update_worktree main        ../inkless-main
update_worktree inkless-4.1 ../inkless-4.1
update_worktree inkless-4.2 ../inkless-4.2
```

If a worktree has local commits that block a fast-forward, stop and surface it
rather than forcing the update.

| Step | Script | Guide |
| --- | --- | --- |
| Which inkless commits are missing from a release branch? | `branch-consistency.sh` | -- |
| Backport (cherry-pick) missing commits main->release | `cherry-pick-to-release.sh` | [CHERRY-PICK-SYNC-GUIDE.md](../../../inkless-sync/CHERRY-PICK-SYNC-GUIDE.md) |
| Create a release branch for a new Kafka minor | `create-release-branch.sh` | -- |

Conflict handling: [CONFLICT-RESOLUTION-STRATEGY.md](../../../inkless-sync/CONFLICT-RESOLUTION-STRATEGY.md)
plus the divergence tables in the cherry-pick guide. Full tooling reference:
[inkless-sync/README.md](../../../inkless-sync/README.md).

### Aligning back to branch expectations (cherry-pick + revert)

Some main commits should not take effect on a release branch (e.g. a JDK/CI change
scoped to a newer Kafka line -- 4.1 mirrors apache/4.1 on JDK 23, 4.2 on JDK 25).
Do NOT skip them with a side list. Instead keep the decision in history:

1. **Cherry-pick the commit** so the branch stays in sync (it is then "present" by
   PR number for `branch-consistency.sh`).
2. **Add a follow-up commit that reverts or adjusts it** back to the branch's
   expectation, prefixed `sync(revert):` (clean revert) or `sync(align):` (partial
   adjust). Explain why in the message.

`sync(...)` commits are excluded by `branch-consistency.sh` (`is_excluded_commit`),
so they are never treated as missing elsewhere and never re-picked. Net result: the
branch reads as in sync, and the rollback is explicit and auditable -- no hidden
skip file. Example: `sync(revert): keep JDK 23 on inkless-4.1 (revert #502; 4.1
mirrors apache/4.1)`.

For the specific case of version strings (`gradle.properties`/`Makefile`/`.env`),
`cherry-pick-to-release.sh` already automates this: it restores version-owned files
to the branch value after every pick, so version-bump commits need no manual revert.

## Increment workflow

0. **Log the previous increment** (if not already done). The changelog entry is
   diffed from `inkless-release-*` tags, so the just-released increment can only
   be written *after* its tag exists -- i.e. at the start of the next prep.
   Generate it and land it on `main` via a normal PR (the increment number lives
   only in tags, never in `gradle.properties`):
   ```bash
   .ai-agents/skills/inkless-changelog/gen-changelog.py --version <prev-N>
   # prepend the output under the '---' in docs/inkless/CHANGELOG.md, then PR to main
   ```
   See the [`inkless-changelog`](../inkless-changelog/SKILL.md) skill for curation.

1. **Find what's missing** on each active release branch:
   ```bash
   ./inkless-sync/branch-consistency.sh inkless-4.1 --missing
   ```
2. **Track progress** in a session file:
   ```bash
   cp inkless-sync/CHERRY-PICK-SESSION-TEMPLATE.md .inkless-sync/CHERRY-PICK-SESSION-$(date +%Y-%m-%d).md
   ```
3. **Cherry-pick** (oldest-first; interactive prompts before each commit). Run
   from that branch's worktree (e.g. `../inkless-4.1`), not `../inkless-main`:
   ```bash
   ./inkless-sync/cherry-pick-to-release.sh inkless-4.1 --dry-run
   ./inkless-sync/cherry-pick-to-release.sh inkless-4.1
   ```
4. **Resolve conflicts** using the divergence tables in the cherry-pick guide
   (main vs release-branch API differences: `TopicPartition` vs `TopicIdPartition`,
   share coordinator presence, `Option` vs `Optional`, etc.).
5. **Compile per commit** (fast path clean picks, full path after conflicts):
   ```bash
   ./gradlew :storage:inkless:compileJava :core:compileScala
   ```
   Run a full `./gradlew compileJava compileScala compileTestJava compileTestScala`
   every 5-10 picks to catch cross-module regressions.
6. **Verify:** `make build` then `make test`.
7. **Archive the session:**
   ```bash
   mv .inkless-sync/CHERRY-PICK-SESSION-*.md inkless-sync/sessions/
   ```

If branches diverge such that a commit cannot be cleanly backported, record it in
the session file and defer it rather than forcing it (branches may end up one
increment apart -- see VERSIONING-STRATEGY.md).

## Creating a new release branch

When Apache ships a new minor (e.g. 4.3.0):

```bash
./inkless-sync/create-release-branch.sh 4.3 --dry-run
./inkless-sync/create-release-branch.sh 4.3
```

Then set the release version by merging the upstream tag with the
`inkless-upstream-sync` skill (`release-sync.sh inkless-4.3 --to-tag 4.3.0`).

## Then: push, release, changelog

Prep ends once branches build+test clean. The rest is the release ceremony,
which is automated -- do NOT create tags by hand (the workflow owns tag
creation, version math, and pushing). Order matters:

1. **Push the release branches** to `origin` (`git push origin inkless-4.1`,
   etc.). This is the gate: the release workflow validates against `origin/*`
   and aborts if a branch is behind. Nothing is released until branches are
   pushed.
2. **Trigger the release** per [RELEASES.md](../../../docs/inkless/RELEASES.md)
   (GitHub Actions -> Inkless Release). It validates, creates+pushes tags, builds
   images/binaries, and publishes the GitHub Release.
3. **Changelog** is generated FROM the new `inkless-release-<N>` tag, so it
   happens *after* the tag exists -- not during prep. The release workflow now
   auto-injects the curated summary into the Release body via the
   [`inkless-changelog`](../inkless-changelog/SKILL.md) generator. The detailed
   `docs/inkless/CHANGELOG.md` entry is committed to `main` via a normal PR --
   done at the START of the next increment (step 0 above), since the tag must
   exist first.

## Guardrails

- Cherry-pick in batches; compile between batches. Reset and defer a commit whose fix is non-trivial.
- Keep cherry-picked commit messages intact; add `sync(compile):` / `sync(test):` commits (or amend) for adaptation fixes.
- `branch-consistency.sh` is also the gate the release CI uses -- a branch must report zero missing commits before release.
- **Versions are set only by upstream syncs.** A release branch's version string
  (`gradle.properties` `version=`, `Makefile` `VERSION`, docker `.env`
  `KAFKA_VERSION`) is owned by `release-sync.sh` (`--to-tag`), never by a
  cherry-pick from main. A main version-bump commit (e.g. `MINOR: update Kafka
  version variables`) still gets cherry-picked so the branch stays in sync, but
  `cherry-pick-to-release.sh` restores those version-owned files to the branch's
  value afterward (the bump can apply with no conflict, so this runs after every
  pick, not just on conflict). Net effect: the commit lands (present for
  branch-consistency) without changing the branch's output version. For other
  branch-specific divergences, use the cherry-pick + `sync(revert):` pattern above.
