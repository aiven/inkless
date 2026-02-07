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
   mkdir -p .sync
   cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .sync/RELEASE-SESSION-inkless-4.0-$(date +%Y-%m-%d).md
   ```

7. **Verify build**:
   ```bash
   make build
   make test
   ```

8. **Push for review**:
   ```bash
   git push origin inkless-4.0-sync-4.0.1
   # Create PR to merge into inkless-4.0
   ```

## Handling Conflicts

Common conflict areas when syncing release branches:

| File | Reason | Resolution |
|------|--------|------------|
| `gradle.properties` | Version changes | Use `{version}-inkless` pattern |
| `tests/kafkatest/__init__.py` | Version changes | Use `{version}.inkless` pattern |
| `tests/kafkatest/version.py` | DEV_VERSION | Use `{version}-inkless-SNAPSHOT` |
| `docs/js/templateData.js` | Version strings | Use inkless version pattern |
| `committer-tools/kafka-merge-pr.py` | DEFAULT_FIX_VERSION | Use inkless version |
| `gradle/dependencies.gradle` | Dependency changes | Add new upstream deps, verify inkless deps are used |
| `streams/quickstart/*.pom` | Maven versions | Keep upstream version (no -inkless) |
| `*.scala` / `*.java` | Import organization | Accept upstream, remove duplicates |
| `.gitignore` | Inkless-specific entries | Keep both inkless and upstream |

### Conflict Resolution Patterns

#### Version Files (use `-inkless` suffix)

```properties
# gradle.properties
version=4.0.1-inkless
```

```python
# tests/kafkatest/__init__.py
__version__ = '4.0.1.inkless'

# tests/kafkatest/version.py
DEV_VERSION = KafkaVersion("4.0.1-inkless-SNAPSHOT")
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

#### POM Files (keep upstream version)

The `streams/quickstart` POMs use standard Apache versioning for Maven Central compatibility:
```xml
<version>4.0.1</version>  <!-- NOT 4.0.1-inkless -->
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

Track sync progress using session files in `.sync/`:

```bash
mkdir -p .sync
cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .sync/RELEASE-SESSION-inkless-4.0-$(date +%Y-%m-%d).md
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

## AI-Assisted Sync

For AI-assisted release syncs, see [RELEASE-SYNC-PROMPT.md](RELEASE-SYNC-PROMPT.md) for a ready-to-use prompt.
