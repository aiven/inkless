---
name: inkless-changelog
description: Generate or update the Inkless changelog and GitHub release notes for an Inkless release increment. Diffs conventional-commit history and the auto-generated configs.rst/metrics.rst between two inkless-release tags, categorizes changes, and produces a detailed changelog entry plus a curated release-notes summary. Use when cutting an Inkless release, refreshing docs/inkless/CHANGELOG.md, or preparing GitHub release notes for an inkless-release-<N> tag.
---

# Inkless Changelog & Release Notes

Produce two artifacts for an Inkless increment (`inkless-release-<N-1>` -> `inkless-release-<N>`):

1. A **detailed changelog entry** added to `docs/inkless/CHANGELOG.md` (newest first; see the placement step below).
2. A **curated release-notes summary** for the GitHub Release of `inkless-release-<N>`.

Both derive from the same sources; the summary is a filtered subset of the detailed entry.

## Sources and their reliability

| Source | Command | Reliability |
| --- | --- | --- |
| Commits | `git log --first-parent --no-merges` between the two tags | High. `--first-parent` excludes upstream commits dragged in by `apache/kafka` merge commits, leaving inkless PR squash-merges. |
| Config changes | diff of `docs/inkless/configs.rst` between tags | High. Config keys are stable. |
| Metric changes | diff of `docs/inkless/metrics.rst` between tags | Low. `metrics.rst` is produced by a hand-maintained registry list in `MetricsDocs.main()`, so a delta may be documentation catch-up, not a shipped metric (e.g. the 3->16 mbean jump at 0.39). ALWAYS curate. |
| Upstream sync | `apache/kafka` merge commits in range + `gradle.properties` version delta | High. Emitted as a blockquote note under the heading when `main`'s Kafka base moves (e.g. 4.1.0 -> 4.2.0-SNAPSHOT at 0.35). |

## Steps

1. **Locate the repo and tags.** Run from the inkless worktree. Confirm the target tag exists:
   ```bash
   git tag | grep '^inkless-release-' | sort -V | tail -5
   ```

2. **Generate the draft** with the helper (run from repo root):
   ```bash
   .ai-agents/skills/inkless-changelog/gen-changelog.py --version <N>
   ```
   Options:
   - `--version <N>` -> uses `inkless-release-<N-1>..inkless-release-<N>`.
   - `--from <tag> --to <tag>` -> explicit range.
   - no args -> latest two `inkless-release-*` tags.
   - `--summary` -> emit only the curated release-notes summary (features + fixes + config changes).

3. **Curate the metrics block.** The draft marks metric deltas with a
   `<!-- REVIEW metrics ... -->` comment. Cross-check each entry against the
   feature/fix commits in the same range. Keep metrics that a commit introduced;
   collapse documentation catch-up into a short note (see the 0.39 entry in
   `CHANGELOG.md` as the reference pattern). Remove the REVIEW comment before publishing.

4. **Prepend the entry** to `docs/inkless/CHANGELOG.md` directly under the
   `---` separator (newest first). Keep the Kafka-version list in the heading
   accurate:
   ```bash
   git tag | grep -E "^inkless-4\.[0-9]+\.[0-9]+-<N>$"
   ```

5. **Produce the GitHub release notes** from `--summary` output. Drop
   `chore`/`test`/`docs`/`refactor`; keep features, fixes, and config/metric
   changes an operator cares about. Tighten wording; strip PR numbers only if
   the release UI already links them.

## Constraints

- The only file this skill writes in the repo is `docs/inkless/CHANGELOG.md`.
  Release notes are handed to the operator / GitHub Release UI, not committed.
- Do not invent metric or config names; every entry must trace to a commit or a
  `configs.rst`/`metrics.rst` diff.
- Never publish raw metric deltas without the curation step.

## Related

- `docs/inkless/RELEASES.md` -- release process; the "Cutting a Release" section references this skill.
- `docs/inkless/VERSIONING-STRATEGY.md` -- what an increment is.
- `storage/inkless/src/main/java/io/aiven/inkless/doc/MetricsDocs.java` -- the
  registry list behind `metrics.rst`; incomplete coverage here is the root cause
  of noisy metric deltas.
