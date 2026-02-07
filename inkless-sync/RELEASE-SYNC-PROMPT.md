# Release Sync AI Prompt

Use this prompt with Claude Code to sync an inkless release branch with upstream Apache Kafka releases.

## Quick Start

Copy and paste this prompt to start a release sync session:

---

**PROMPT:**

```
I need to sync the inkless release branch with an upstream Apache Kafka release.

## Context
- Release branch: inkless-4.0 (or inkless-4.1, etc.)
- Target: Sync to latest upstream release tag (e.g., 4.0.1, 4.0.2)

## Steps

1. **Discovery**: Run `./inkless-sync/release-sync.sh inkless-4.0 --list-tags` to see available upstream tags

2. **Create worktree**: Create a dedicated worktree for this sync:
   ```bash
   git worktree add ../inkless-sync-4.0.1 -b inkless-4.0-sync-4.0.1 origin/inkless-4.0
   ```

3. **Copy scripts**: Copy sync scripts to worktree:
   ```bash
   cp -r inkless-sync ../inkless-sync-4.0.1/
   ```

4. **Dry run**: Preview conflicts:
   ```bash
   cd ../inkless-sync-4.0.1
   ./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1 --branch inkless-4.0-sync-4.0.1 --dry-run
   ```

5. **Execute sync**: Run the actual sync:
   ```bash
   ./inkless-sync/release-sync.sh inkless-4.0 --to-tag 4.0.1 --branch inkless-4.0-sync-4.0.1
   ```

6. **Resolve conflicts**: When conflicts occur, resolve them following these patterns:
   - **Version files**: Use `{upstream_version}-inkless` pattern (e.g., `4.0.1-inkless`)
   - **gradle/dependencies.gradle**: Add upstream new deps, verify inkless deps are actually used
   - **streams/quickstart POMs**: Keep upstream version (no -inkless suffix)
   - **Test files**: Accept upstream changes, keep inkless-specific code
   - **.gitignore**: Keep both inkless and upstream entries

7. **Create session file**: Copy template and track progress:
   ```bash
   mkdir -p .sync
   cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .sync/RELEASE-SESSION-{branch}-{date}.md
   ```

8. **Verify build**:
   ```bash
   make build
   make test
   ```

9. **Push**: When verified, push the sync branch for PR review

Please help me execute this release sync process.
```

---

## Conflict Resolution Patterns

### Version Files

Files that need `-inkless` suffix:
- `gradle.properties` → `version=4.0.1-inkless`
- `tests/kafkatest/__init__.py` → `__version__ = '4.0.1.inkless'`
- `tests/kafkatest/version.py` → `DEV_VERSION = KafkaVersion("4.0.1-inkless-SNAPSHOT")`
- `docs/js/templateData.js` → `"fullDotVersion": "4.0.1-inkless"`
- `committer-tools/kafka-merge-pr.py` → `DEFAULT_FIX_VERSION = "4.0.1-inkless"`

### POM Files (Keep Upstream Version)

These files use standard Apache Kafka versioning:
- `streams/quickstart/pom.xml`
- `streams/quickstart/java/pom.xml`
- `streams/quickstart/java/src/main/resources/archetype-resources/pom.xml`

### Dependencies

In `gradle/dependencies.gradle`:
- Add new upstream dependencies
- Verify inkless-specific dependencies are actually used (search for imports)
- Remove unused dependencies that upstream removed

### Import Organization

Scala/Java files may have import reorganization conflicts:
- Accept upstream import ordering
- Remove duplicate imports after merge

## Session File Template

Use the template at `inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md`:

```bash
mkdir -p .sync
cp inkless-sync/RELEASE-SYNC-SESSION-TEMPLATE.md .sync/RELEASE-SESSION-inkless-4.0-$(date +%Y-%m-%d).md
```

The template includes sections for:
- Session info (branch, target, status)
- Conflict summary and resolution tracking
- Version file resolutions
- Build verification checklist

## Verification

After merge, run:

```bash
# Build core inkless components
make build

# Run inkless tests
make test
```

Expected: All builds and tests pass.

## Common Issues

### Tag not found
```bash
git fetch apache --tags
```

### Merge conflicts in test files
Usually import reorganization - accept upstream, remove duplicates.

### Build fails after merge
Check for:
- Missing dependencies (add to gradle/dependencies.gradle)
- API changes (may need inkless-specific fixes)
