# Release Branch Sync Guide

This guide explains how to sync inkless release branches (e.g., `inkless-4.0`, `inkless-4.1`) with upstream Apache Kafka releases.

## Overview

Inkless release branches track specific Apache Kafka release series. When Apache releases a patch version (e.g., 4.0.1), the corresponding inkless branch needs to be synced to incorporate those fixes.

### Version Pattern

```
{upstream_version}-inkless
```

Examples:
- Upstream `4.0.0` → Inkless `4.0.0-inkless`
- Upstream `4.0.1` → Inkless `4.0.1-inkless`

## Quick Start

### List Available Tags

See what upstream releases are available:

```bash
./inkless-sync/release-sync.sh inkless-4.0 --list-tags
```

Output:
```
Release tags for 4.0.x series:
=======================================

  4.0.1           (b31ce61fd6) <- latest
  4.0.0           (985bc99521) <- current base

Current inkless version: 4.0.0-inkless
Current base version:    4.0.0

INFO: There are 76 commits available to sync (from 4.0.0 to 4.0.1)
```

### Preview Conflicts (Dry Run)

Before syncing, preview what conflicts might occur:

```bash
./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1 --dry-run
```

### Sync to a Tag

Sync to a specific upstream release:

```bash
./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1
```

### Sync to HEAD (Unreleased Fixes)

For critical fixes that haven't been released yet:

```bash
./inkless-sync/release-sync.sh inkless-4.0 --to-head --dry-run
./inkless-sync/release-sync.sh inkless-4.0 --to-head
```

## Prerequisites

1. **Git remotes configured**: The script expects either `apache` or `upstream` remote pointing to `apache/kafka`
2. **Clean working tree**: No uncommitted changes
3. **On the correct branch**: Script will checkout the target branch if needed

### Setting Up Remotes

```bash
# Add Apache Kafka as upstream (if not already configured)
git remote add apache git@github.com:apache/kafka.git
git fetch apache --tags
```

## Workflow Example

### Syncing inkless-4.0 to Apache Kafka 4.0.1

1. **Check current state**:
   ```bash
   ./inkless-sync/release-sync.sh inkless-4.0 --list-tags
   ```

2. **Create a worktree** (recommended for isolation):
   ```bash
   git worktree add ../inkless-sync-4.0.1 -b inkless-4.0-sync-4.0.1 origin/inkless-4.0
   cp -r inkless-sync ../inkless-sync-4.0.1/
   cd ../inkless-sync-4.0.1
   ```

3. **Preview conflicts**:
   ```bash
   ./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1 --branch inkless-4.0-sync-4.0.1 --dry-run
   ```

4. **Perform sync**:
   ```bash
   ./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1 --branch inkless-4.0-sync-4.0.1
   ```

5. **Resolve conflicts** (if any):
   ```bash
   # Edit conflicted files following resolution patterns below
   git add <resolved-files>
   git commit -m "Merge upstream 4.0.1 into inkless-4.0-sync-4.0.1"
   ```

6. **Create session file** for tracking:
   ```bash
   mkdir -p .inkless-sync
   cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .inkless-sync/RELEASE-SESSION-inkless-4.0-$(date +%Y-%m-%d).md
   ```

7. **Verify build**:
   ```bash
   make build
   make test
   ```

8. **Archive session file**:
   ```bash
   mv .inkless-sync/RELEASE-SESSION-*.md inkless-sync/sessions/
   ```

9. **Push for review**, with the `upstream` label on the PR:
   ```bash
   git push origin inkless-4.0-sync-4.0.1
   gh pr create --base inkless-4.0 --label upstream --title "chore(inkless): sync inkless-4.0 with upstream Apache Kafka 4.0.1"
   ```
   The `Inkless PR labels` workflow adds the label on its own for `inkless-*-sync-*` branches
   and for PRs whose merge commit has an `apache/kafka` parent, as long as the workflow file
   exists on the release branch.

10. **Land the sync**. After CI is green and a reviewer approves the PR, follow [Land the sync](#land-the-sync).

## Land the sync

Land a sync PR with **Create a merge commit**. Don't use **Squash and merge** or **Rebase and merge**.

### Why only the merge commit

The next sync finds its starting point with `git merge-base` between the base branch and the upstream tag. That search needs the upstream commits to keep their SHAs.

- **Squash and merge** replaces the upstream commits with one new commit. The next sync can't see which upstream commits are already merged.
- **Rebase and merge** gives each upstream commit a new SHA. The merge base moves back, and the same commits conflict again.
- **Create a merge commit** adds one commit and leaves every existing commit unchanged.

### The extra merge commit

The branch already holds a `Merge upstream <tag>` commit. The button adds a second commit, `Merge pull request #<n> from ...`, on top of it. This extra commit is harmless:

- The `Merge upstream <tag>` commit stays on the second-parent side. Its upstream parents keep their SHAs, so `git merge-base` and `sync-status.sh` give the same result as with one merge.
- The extra commit doesn't change any file. It only records the PR number.
- Don't avoid it with a direct push. A direct push doesn't link the PR and needs a manual check that the base branch didn't move.

The team merged PRs #657 (4.2.1) and #771 (main sync) with the button and pushed 4.3.1 and 4.2.2 directly. Both ways keep the merge base. New syncs use the button, so the process is the same every time.

### Steps

1. Check that the PR shows the text `This branch has no conflicts with the base branch`. If the base branch moved, merge it into the sync branch and wait for CI.
2. Choose **Create a merge commit** in the merge dropdown.
3. Fetch the base branch. Tag the new head with `inkless-<upstream version>-<inkless release>`. The tag for upstream 4.3.2 and Inkless 0.51 is `inkless-4.3.2-0.51`. Push the tag. Check `docs/inkless/RELEASES.md` for what the tag builds.

## Handling Conflicts

Common conflict areas when syncing release branches:

| File | Reason | Resolution |
|------|--------|------------|
| `gradle.properties` | Version changes | Use `{version}-inkless` pattern |
| `tests/kafkatest/__init__.py` | Version changes | Use `{version}+inkless` (PEP 440 local version label) |
| `tests/kafkatest/version.py` | DEV_VERSION | Use `{version}-inkless` (matches `gradle.properties` exactly) |
| `docs/js/templateData.js` | Version strings | Use inkless version pattern |
| `committer-tools/kafka-merge-pr.py` | DEFAULT_FIX_VERSION | Use inkless version |
| `gradle/dependencies.gradle` | Dependency changes | Add new upstream deps, verify inkless deps are used |
| `streams/quickstart/**/pom.xml` | Maven versions | Use `{version}-inkless` (matches `gradle.properties` exactly) |
| `*.scala` / `*.java` | Import organization | Accept upstream, remove duplicates |
| `.gitignore` | Inkless-specific entries | Keep both inkless and upstream |

### Conflict Resolution Patterns

#### Version Files (use `inkless` suffix)

```properties
# gradle.properties
version=4.0.1-inkless
```

```python
# tests/kafkatest/__init__.py
# This value must stay PEP 440 (https://peps.python.org/pep-0440/) compliant:
# Use "+inkless" as a local version label.
__version__ = '4.0.1+inkless'

# tests/kafkatest/version.py
# Must match gradle.properties exactly (verified by the ':verifyVersionConsistency'
# Gradle task); don't add an extra "-SNAPSHOT" suffix on top of "-inkless".
DEV_VERSION = KafkaVersion("4.0.1-inkless")
```

```javascript
// docs/js/templateData.js
var context={
    "version": "40inkless",
    "dotVersion": "4.0-inkless",
    "fullDotVersion": "4.0.1-inkless",
    "scalaVersion": "2.13"
};
```

#### POM files (use the `-inkless` version)

The `streams/quickstart` POMs must match `gradle.properties` exactly; the
`:verifyVersionConsistency` Gradle task enforces this:
```xml
<version>4.0.1-inkless</version>
```

#### Dependencies (merge and verify)

```gradle
// gradle/dependencies.gradle - add new upstream deps, verify inkless-specific deps are still used
commonsBeanutils: "1.11.0",          // new from upstream
commonsLang: "3.18.0",               // new from upstream
// Note: verify unused dependencies (search for imports before keeping)
```

#### Import Organization

For Scala/Java files with import conflicts:
1. Accept upstream's import ordering
2. Remove any duplicate imports that appear twice after merge

## Script Options

```
Usage: release-sync.sh [OPTIONS] <inkless-branch>

Arguments:
  inkless-branch    The inkless release branch (e.g., inkless-4.0)

Options:
  --to-tag TAG      Sync to specific upstream tag (e.g., 4.0.1)
  --to-head         Sync to HEAD of upstream branch
  --list-tags       List available upstream tags
  --include-rc      Include release candidates when listing tags
  --dry-run         Preview conflicts without merging
  -h, --help        Show this help
```

## Files Modified by Sync

After a successful merge, the script automatically updates the main Gradle version:

- `gradle.properties` - Main version property (updated by the script)

Python version files are **not** modified by the script and should be updated manually if needed:

- `tests/kafkatest/__init__.py` - Python test version (update manually if required)

## Troubleshooting

### "Tag not found"

```bash
# Fetch latest tags from upstream
git fetch apache --tags
```

### "Branch does not exist"

```bash
# Check available inkless branches
git branch -a | grep inkless
```

### "Merge has conflicts"

This is expected for non-trivial syncs. Resolve conflicts manually:

```bash
# See conflicted files
git status

# After resolving
git add <files>
git commit
```

### "Not newer than current base"

The script prevents syncing to an older version. Check your current version:

```bash
grep "^version=" gradle.properties
```

## Session Files

Track sync progress using session files in `.inkless-sync/` (gitignored working directory):

```bash
mkdir -p .inkless-sync
cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .inkless-sync/RELEASE-SESSION-inkless-4.0-$(date +%Y-%m-%d).md
```

After sync completes, archive the session file to committed history:

```bash
mv .inkless-sync/RELEASE-SESSION-*.md inkless-sync/sessions/
```

See `inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md` for the full template with sections for:
- Session info and status tracking
- Conflict summary by category
- Version file resolutions
- Build verification checklist

## Verification

After completing the merge, verify the build:

```bash
# Build inkless components
make build

# Run inkless tests
make test
```

### What `make build` does

```bash
./gradlew :core:build :storage:inkless:build :metadata:build -x test -x generateJooqClasses
```

### What `make test` does

```bash
./gradlew :storage:inkless:test :storage:inkless:integrationTest -x generateJooqClasses
./gradlew :metadata:test --tests "org.apache.kafka.controller.*"
./gradlew :core:test --tests "*Inkless*"
```

## Release the sync under the current Inkless version

Use this procedure when the upstream sync is the only change on the release branch. The branch has no new Inkless commits. The Inkless version stays the same. Example: `inkless-4.3` synced to Kafka 4.3.2 after release 0.51 included Kafka 4.3.1.

Keep the Inkless version for an upstream-only change. See [VERSIONING-STRATEGY.md](../docs/inkless/VERSIONING-STRATEGY.md) for the version rules.

If a new Inkless release follows soon, skip these steps. The next release tags the synced branch head.

Sync the upstream Kafka patch release first. Then sync the Inkless commits. Open one pull request for each sync.

The first case was `inkless-4.2.2-0.49`. The team tagged it after release 0.49, and the release page doesn't list it. The 4.3.2 sync is the first one added to the published release.

### Steps

1. Merge the sync PR into the release branch. Check that `gradle.properties` shows the new Kafka version.
2. Create the Kafka-base tag on the merge commit, with the existing tag message format:

   ```bash
   git fetch origin
   git tag -a inkless-4.3.2-0.51 origin/inkless-4.3 -m "Inkless release 0.51 on Kafka 4.3.2"
   ```

3. Check the tag target with `git show inkless-4.3.2-0.51`. A pushed tag starts a public build and publishes a Docker image.

   ```bash
   git push origin inkless-4.3.2-0.51
   ```

4. Wait for the `Inkless Publish` run that the tag push starts. It builds the `4.3.2-0.51` image and moves the `4.3-latest` alias. The run leaves the GitHub Release unchanged.
5. Run the `Inkless Release` workflow with `inkless_version` set to `0.51` and `resume` set to `true`. The workflow skips tagging. It rebuilds the images and binaries. Then it regenerates the release notes and uploads the binaries to `inkless-release-0.51`.

The `CHANGELOG.md` heading of the release keeps the old Kafka version. The entry of the next release lists the new version.

### What the `resume` run does

- For each active branch, the workflow builds the highest-patch tag for the version. For `inkless-4.3` it builds `inkless-4.3.2-0.51` and not `inkless-4.3.1-0.51`.
- The release notes list every `inkless-<kafka-version>-0.51` tag, so they name Kafka 4.3.1 and Kafka 4.3.2. The workflow replaces any manual edit of the notes.
- The upload uses `--clobber`, so the old `kafka_2.13-4.3.1-inkless.tgz` asset stays on the release. Keep it. Users can still download the earlier Kafka patch version.
- The workflow rebuilds the `0.51` and `latest` Docker images from `inkless-release-0.51` on `main`, with the same content.
- The branches `inkless-4.1` and `inkless-4.2` are rebuilt from their existing tags.

### Verify

```bash
gh release view inkless-release-0.51 --json body,assets --jq '.assets[].name'
docker buildx imagetools inspect ghcr.io/aiven/inkless:4.3.2-0.51
```

The asset list must include `kafka_2.13-4.3.2-inkless.tgz`. The release notes must link `inkless-4.3.2-0.51`.

> The first run of this procedure, for Kafka 4.3.2 on release 0.51, matched these effects.

## AI-Assisted Sync

For AI-assisted release syncs, see [RELEASE-SYNC-PROMPT.md](RELEASE-SYNC-PROMPT.md) for a ready-to-use prompt.
