# Inkless Upstream Sync Tooling

This directory contains scripts and configuration for syncing inkless with upstream Apache Kafka.

## Overview

The sync process follows the [Versioning Strategy](../docs/inkless/VERSIONING-STRATEGY.md):
- Uses **merge commits** (not rebase) for velocity
- Preserves inkless-specific features and configurations
- Creates structured commits for different types of adaptations

## AI-Assisted Sync (Recommended)

Start a sync session with Claude Code using **[SYNC-PROMPT.md](SYNC-PROMPT.md)**.

The agent will guide you to the appropriate workflow:
- **Main Sync** → [MAIN-SYNC-PROMPT.md](MAIN-SYNC-PROMPT.md)
- **Release Sync** → [RELEASE-SYNC-PROMPT.md](RELEASE-SYNC-PROMPT.md)

## Scripts Overview

| Script | Purpose |
|--------|---------|
| `main-sync.sh` | Sync main branch with Apache Kafka trunk |
| `release-sync.sh` | Sync release branches with upstream patch releases |
| `sync-status.sh` | Check how far behind branches are from upstream |
| `branch-consistency.sh` | Check if inkless commits from main are in release branches |
| `create-release-branch.sh` | Create new inkless release branches |
| `cherry-pick-to-release.sh` | Cherry-pick inkless commits to release branches |

## Two Types of Sync

| Type | Branch | Script | Use Case |
|------|--------|--------|----------|
| **Main Sync** | `main` | `main-sync.sh` | Weekly/biweekly sync with Apache Kafka trunk |
| **Release Sync** | `inkless-4.0`, etc. | `release-sync.sh` | Sync release branches with upstream patch releases |

### When to Use Each

- **Main Sync**: Regular development sync to keep up with Apache Kafka trunk
- **Release Sync**: When Apache releases a patch (e.g., 4.0.1) and we need to incorporate fixes into our release branch

## Quick Start

### Regular Weekly/Biweekly Sync

```bash
# Sync with latest apache/kafka trunk
./inkless-sync/main-sync.sh
```

### Sync Before New Kafka Version

```bash
# Sync to the commit just before a version tag (e.g., before 4.3)
./inkless-sync/main-sync.sh --before-version 4.3
```

### Dry Run (Preview)

```bash
# See what would happen without making changes
./inkless-sync/main-sync.sh --dry-run
```

### Release Branch Sync

```bash
# List available upstream release tags
./inkless-sync/release-sync.sh inkless-4.0 --list-tags

# Sync inkless-4.0 to Apache Kafka 4.0.1
./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1
```

For detailed release sync workflow, see [RELEASE-SYNC-GUIDE.md](RELEASE-SYNC-GUIDE.md).

### Check Sync Status

```bash
# Check how far behind main is from upstream trunk
./inkless-sync/sync-status.sh main

# Check release branch status
./inkless-sync/sync-status.sh inkless-4.0

# Check all branches
./inkless-sync/sync-status.sh --all
```

### Branch Consistency Check

Check if inkless commits from main have been cherry-picked to release branches:

```bash
# Check inkless-4.0 consistency with main
./inkless-sync/branch-consistency.sh inkless-4.0

# Show missing commits with cherry-pick commands
./inkless-sync/branch-consistency.sh inkless-4.0 --missing

# Show ALL missing commits (including old ones that were intentionally skipped)
./inkless-sync/branch-consistency.sh inkless-4.0 --missing --all
```

### Create New Release Branch

```bash
# Create inkless-4.2 from main (main must be at 4.2.0-inkless-SNAPSHOT or later)
./inkless-sync/create-release-branch.sh 4.2

# Preview what would happen
./inkless-sync/create-release-branch.sh 4.2 --dry-run

# Force creation even if version doesn't match
./inkless-sync/create-release-branch.sh 4.2 --force
```

After creating the branch, sync with upstream to set the release version:
```bash
./inkless-sync/release-sync.sh inkless-4.2 --to-tag 4.2.0
```

### Cherry-pick to Release Branches

```bash
# Cherry-pick all missing inkless commits to inkless-4.0
./inkless-sync/cherry-pick-to-release.sh inkless-4.0

# Preview what would be cherry-picked
./inkless-sync/cherry-pick-to-release.sh inkless-4.0 --dry-run

# Cherry-pick specific commits
./inkless-sync/cherry-pick-to-release.sh inkless-4.0 abc123 def456
```

## How It Works (Main Sync)

### Phase 1: Preparation
1. Fetches upstream apache/kafka
2. Determines sync target (trunk HEAD or before-version)
3. Generates "inkless manifest" - list of files we've modified
4. Creates sync branch: `sync/upstream-YYYYMMDD`

### Phase 2: Merge
1. Executes `git merge` from upstream
2. Categorizes conflicts:
   - **Protected**: Files in `storage/inkless/`, `docs/inkless/`, etc. → Use "ours"
   - **Auto-resolvable**: Import conflicts, trivial changes → Auto-resolve
   - **Manual**: Complex conflicts → Stop and ask for human help
3. Completes merge commit or reports conflicts needing attention

### Phase 3: Adaptation
1. Compiles the codebase
2. Analyzes any compilation errors
3. Errors should be fixed and committed as: `sync(compile): fix compilation errors`

### Phase 4: Verification
1. Runs inkless-specific tests
2. Verifies inkless features are preserved (manifest check)
3. Generates sync report

## File Structure

```
inkless-sync/
├── main-sync.sh              # Main branch sync script
├── release-sync.sh               # Release branch sync script
├── sync-status.sh                # Check sync status vs upstream
├── branch-consistency.sh         # Check cherry-pick consistency
├── create-release-branch.sh      # Create new release branches
├── cherry-pick-to-release.sh     # Cherry-pick commits to releases
├── README.md                     # This file
├── SYNC-PROMPT.md                # Entry point prompt for AI-assisted sync
├── RELEASE-SYNC-GUIDE.md         # Release sync documentation
├── RELEASE-SYNC-PROMPT.md        # AI prompt for release syncs
├── CONFLICT-RESOLUTION-STRATEGY.md     # Conflict resolution guidance
├── MAIN-SYNC-PROMPT.md                 # AI prompt for main branch syncs
├── MAIN-SYNC-SESSION-TEMPLATE.md       # Session template for main syncs
├── MAIN-SYNC-ACTION-PLAN.md            # Action plan for main syncs
├── RELEASE-SYNC-SESSION-TEMPLATE.md    # Session template for release syncs
├── RELEASE-SYNC-ACTION-PLAN.md         # Action plan for release syncs
├── lib/
│   └── common.sh                 # Shared utility functions
└── config/
    └── protected-patterns.txt    # Files to protect during merge
```

During sync, a `.sync/` directory is created with:
```
.sync/
├── sync-info.txt                 # Sync metadata
├── manifest-files.txt            # Files modified by inkless
├── manifest-stats.txt            # Diff stats from merge base
├── conflicted-files.txt          # Files with conflicts
├── conflicts-protected.txt       # Protected file conflicts
├── conflicts-auto-resolvable.txt # Auto-resolvable conflicts
├── conflicts-manual.txt          # Conflicts needing manual resolution
├── compile-output.txt            # Compilation output
├── compile-errors.txt            # Extracted compilation errors
└── SYNC-REPORT.md               # Final sync report
```

## Configuration

### Protected Patterns (`config/protected-patterns.txt`)

Files matching these patterns are auto-resolved with "ours" (inkless version) during conflicts.
Only truly inkless-owned files should be listed here; version files and core Kafka files
with inkless modifications are intentionally excluded so they fall into manual review:

```
# Inkless-owned directories (safe to auto-resolve)
storage/inkless/**
docs/inkless/**
docker/inkless/**
config/inkless/**
tests/kafkatest/tests/inkless/**
.github/workflows/inkless*.yml

# Inkless test files
core/src/test/java/kafka/server/Inkless*.java
```

### Adding Apache Remote

If you don't have the apache remote configured:

```bash
git remote add apache https://github.com/apache/kafka.git
git fetch apache
```

## Commit Convention

After sync, use these commit message prefixes:

| Prefix | Description |
|--------|-------------|
| `merge:` | The merge commit itself |
| `sync(compile):` | Fixing compilation errors from API changes |
| `sync(test):` | Fixing test infrastructure changes |
| `sync(config):` | Preserving inkless configurations |
| `sync(verify):` | Verification and manifest updates |

Example:
```bash
git commit -m "sync(compile): adapt to KafkaMetricsGroup constructor change"
git commit -m "sync(test): add NoOpRemoteLogMetadataManager to test config"
```

## Troubleshooting

### Manual Conflicts

If the script reports manual conflicts:

1. Check `.sync/conflicts-manual.txt` for the list
2. Resolve each conflict manually
3. `git add <resolved-files>`
4. `git commit` to complete the merge
5. Continue with adaptation phase

### Compilation Errors

1. Check `.sync/compile-errors.txt` for error list
2. Fix errors in logical groups
3. Commit each group: `git commit -m "sync(compile): <description>"`

### Test Failures

1. Check test output in `.sync/test-*-output.txt`
2. Fix failures
3. Commit: `git commit -m "sync(test): <description>"`

## Agent Usage

This script is designed to be run by an automated agent (e.g., Claude):

1. Agent runs `./inkless-sync/main-sync.sh`
2. If manual conflicts: Agent reports to human via PR/issue
3. If compilation errors: Agent attempts fixes, commits separately
4. If test failures: Agent analyzes and fixes or reports
5. Agent creates PR with sync report

The structured commit approach makes it easy to:
- Review what changed and why
- Revert specific fixes if needed
- Understand the sync process

## Related Documentation

- [Versioning Strategy](../docs/inkless/VERSIONING-STRATEGY.md)
- [Inkless README](../docs/inkless/README.md)
- [Release Sync Guide](RELEASE-SYNC-GUIDE.md)
- [Conflict Resolution Strategy](CONFLICT-RESOLUTION-STRATEGY.md)
- [Main Sync Prompt](MAIN-SYNC-PROMPT.md)
