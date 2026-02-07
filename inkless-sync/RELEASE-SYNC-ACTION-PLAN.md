# Release Sync Action Plan

## Overview

This document outlines the workflow for syncing inkless release branches (e.g., `inkless-4.0`, `inkless-4.1`) with upstream Apache Kafka patch releases.

## Key Differences from Main Sync

| Aspect | Main Sync | Release Sync |
|--------|-----------|--------------|
| Target | Apache Kafka trunk (HEAD) | Specific release tags (e.g., 4.0.1) |
| Frequency | Weekly/biweekly | When upstream releases patches |
| Conflicts | Core files with inkless modifications | Mostly version files and dependencies |
| Complexity | High (API changes, new features) | Lower (bug fixes, backports) |

## Recommended Workflow

### Step 1: Discovery

Check what upstream releases are available:

```bash
./inkless-sync/release-sync.sh inkless-4.0 --list-tags
```

This shows:
- Available upstream tags
- Current inkless version
- Number of commits to sync

### Step 2: Setup Worktree

Create an isolated environment for the sync:

```bash
# Create worktree from release branch
git worktree add ../inkless-sync-4.0.1 -b inkless-4.0-sync-4.0.1 origin/inkless-4.0

# Copy sync scripts
cp -r inkless-sync ../inkless-sync-4.0.1/

# Change to worktree
cd ../inkless-sync-4.0.1
```

### Step 3: Create Session File

```bash
mkdir -p .sync
cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .sync/RELEASE-SESSION-inkless-4.0-$(date +%Y-%m-%d).md
```

### Step 4: Preview Conflicts

```bash
./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1 --branch inkless-4.0-sync-4.0.1 --dry-run
```

Or manually:

```bash
git merge --no-commit 4.0.1
git diff --name-only --diff-filter=U
git merge --abort
```

### Step 5: Execute Merge

```bash
git merge 4.0.1
```

### Step 6: Resolve Conflicts

Follow the resolution patterns below for each conflict type.

### Step 7: Verify Build

```bash
make build
make test
```

### Step 8: Push and Create PR

```bash
git push -u origin inkless-4.0-sync-4.0.1
gh pr create --base inkless-4.0 --title "Sync inkless-4.0 to upstream 4.0.1"
```

## Conflict Resolution Playbook

### Version Files

These files need the `-inkless` suffix pattern:

#### gradle.properties
```properties
version=4.0.1-inkless
```

#### tests/kafkatest/__init__.py
```python
__version__ = '4.0.1.inkless'
```

#### tests/kafkatest/version.py
```python
DEV_VERSION = KafkaVersion("4.0.1-inkless-SNAPSHOT")
```

#### docs/js/templateData.js
```javascript
var context={
    "version": "40inkless",
    "dotVersion": "4.0-inkless",
    "fullDotVersion": "4.0.1-inkless",
    "scalaVersion": "2.13"
};
```

#### committer-tools/kafka-merge-pr.py
```python
DEFAULT_FIX_VERSION = "4.0.1-inkless"
```

### POM Files (Keep Upstream Version)

These files use standard Apache Kafka versioning for Maven Central compatibility:

- `streams/quickstart/pom.xml`
- `streams/quickstart/java/pom.xml`
- `streams/quickstart/java/src/main/resources/archetype-resources/pom.xml`

```xml
<version>4.0.1</version>  <!-- NOT 4.0.1-inkless -->
```

### Dependencies (gradle/dependencies.gradle)

1. **Accept upstream's new dependencies** - They fixed bugs, we need those fixes
2. **Verify inkless-specific dependencies** - Search codebase for imports before removing
3. **Remove truly unused dependencies** - If upstream removed and we don't use

Example resolution:
```gradle
// New from upstream 4.0.1
commonsBeanutils: "1.11.0",
commonsLang: "3.18.0",

// Inkless-specific (verify still used)
jooq: "3.19.16",
flyway: "11.1.0",
```

### Import Organization (Scala/Java files)

When files have import conflicts due to reorganization:

1. Accept upstream import ordering
2. Remove duplicate imports after merge
3. Verify compilation: `./gradlew :module:compileScala`

### .gitignore

Keep both inkless and upstream entries:

```gitignore
# Upstream entries
...

# Inkless-specific
.sync/
*.inkless.log
```

### CI/GitHub Workflows

- Usually accept upstream changes
- Verify inkless-specific workflows (`inkless*.yml`) are preserved
- Check for new required checks that might affect inkless

## Common Scenarios

### Scenario 1: Clean Merge (No Conflicts)

Rare but possible for small patch releases:

```bash
git merge 4.0.1
# No conflicts - just commit
git push -u origin inkless-4.0-sync-4.0.1
```

### Scenario 2: Version-Only Conflicts

Most common scenario:

1. Resolve version files with `-inkless` pattern
2. Commit merge
3. Verify build

### Scenario 3: Dependency Conflicts

When upstream updates dependencies:

1. Accept upstream dependency versions
2. Verify inkless-specific deps are still present
3. Search for imports to verify usage
4. Commit merge

### Scenario 4: Test File Conflicts

Usually import reorganization:

1. Accept upstream version
2. Check for duplicate imports
3. Verify tests compile

## Verification Checklist

Before pushing:

- [ ] `gradle.properties` has correct version (`X.Y.Z-inkless`)
- [ ] `make build` passes
- [ ] `make test` passes
- [ ] Inkless module builds: `./gradlew :storage:inkless:build`
- [ ] Key inkless files exist and unchanged:
  - [ ] `storage/inkless/src/main/java/io/aiven/inkless/InklessWriter.java`
  - [ ] `docs/inkless/README.md`
  - [ ] `.github/workflows/inkless.yml`

## After Merge

### Update Main Branch (if needed)

If there are fixes in the release that should go to main:

```bash
# Check what commits are in release but not main
git log main..inkless-4.0 --oneline
```

### Clean Up Worktree

```bash
cd ..
git worktree remove inkless-sync-4.0.1
```

## Troubleshooting

### Tag not found

```bash
git fetch apache --tags
```

### Merge shows unexpected conflicts

Verify you're on the correct branch:

```bash
git branch -vv
git log --oneline -5
```

### Build fails after merge

1. Check for missing dependencies
2. Verify version files are correct
3. Check for API changes (rare in patch releases)

### Tests fail after merge

1. Check test configuration files
2. Look for renamed test utilities
3. Verify inkless test fixtures are present
