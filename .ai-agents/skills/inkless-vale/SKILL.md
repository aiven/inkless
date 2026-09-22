---
name: inkless-vale
description: Lint the prose a change adds on inkless-owned paths with Vale and fix the alerts. Use after writing or editing docs, code comments, or Javadoc, and before committing or reporting a task as complete.
---

# Inkless prose lint

Run Vale on the prose this change adds and fix every alert before you commit.
The check enforces the Style section of AGENTS.md mechanically: the Google
package covers the developer-documentation style guide, and the ai-tells package
catches the fingerprints of AI-written text.

## Scope

The runner (`.vale/inkless.py`) limits alerts to:

- Files on inkless-OWNED paths, derived from the single-owner `@aiven/inkless`
  entries in `INKLESS_OWNERSHIP`. Upstream Kafka files and INTERLEAVED files are
  never checked.
- Lines added relative to the merge base with `main`, so pre-existing prose
  doesn't surface.
- Markdown files plus comments in `.java`, `.scala`, and `.py` files.

License headers (AGPL and Apache) are exempt through `BlockIgnores` in
`.vale.ini`. Don't reword them.

## Run it

```sh
make vale
```

The target syncs the Vale packages on first use and exits nonzero when
it finds an alert on an added line. Useful variants:

- `python3 .vale/inkless.py --base <ref>`: diff against a specific ref instead
  of the merge base with `main`.
- `python3 .vale/inkless.py --all`: audit every owned file in full, ignoring the
  diff.

## Fix alerts

- Reword the prose. Don't weaken `.vale.ini`, turn off a rule, or add an inline
  skip comment.
- If Vale flags an existing Kafka or Inkless identifier, write it in backticks.
  Vale skips code spans.
- If a backticked term still alerts, add it to
  `.vale/styles/config/vocabularies/Inkless/accept.txt` and mention the addition
  in the PR body.
- Rerun `make vale` until the run is clean.
