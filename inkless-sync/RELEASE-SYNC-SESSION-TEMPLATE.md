# Release Sync Session: [BRANCH] to [TAG]

## Session Info
- **Date**: [YYYY-MM-DD]
- **Release Branch**: [inkless-X.Y]
- **Working Branch**: [inkless-X.Y-sync-X.Y.Z]
- **Target Tag**: [X.Y.Z]
- **Commits to merge**: [count]
- **Status**: [In Progress | Blocked | Complete]

---

## Phase 1: Discovery

### Current State
```bash
./inkless-sync/release-sync.sh [BRANCH] --list-tags
```

- Current inkless version:
- Current base version:
- Target version:

---

## Phase 2: Merge

### Merge Command
```bash
git merge [TAG]
```

### Conflict Summary
| Category | Count | Files |
|----------|-------|-------|
| Version files | | |
| Dependencies | | |
| Test files | | |
| Documentation | | |
| Other | | |

---

## Phase 3: Conflict Resolution

### Version Files
| # | File | Resolution | Status |
|---|------|------------|--------|
| 1 | gradle.properties | `version=[TAG]-inkless` | |
| 2 | tests/kafkatest/__init__.py | `__version__ = '[TAG].inkless'` | |
| 3 | tests/kafkatest/version.py | `DEV_VERSION = KafkaVersion("[TAG]-inkless-SNAPSHOT")` | |
| 4 | docs/js/templateData.js | Update version strings | |
| 5 | committer-tools/kafka-merge-pr.py | `DEFAULT_FIX_VERSION = "[TAG]-inkless"` | |

### Dependency Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| | gradle/dependencies.gradle | | |

### POM Files (Keep Upstream Version)
| # | File | Notes | Status |
|---|------|-------|--------|
| | streams/quickstart/pom.xml | No -inkless suffix | |
| | streams/quickstart/java/pom.xml | | |

### Test Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| | | Accept upstream, remove duplicate imports | |

### Other Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| | .gitignore | Keep both inkless and upstream entries | |

---

## Phase 4: Verification

### Build
```bash
make build
```
- [ ] Build passes

### Tests
```bash
make test
```
- [ ] Tests pass

### Checklist
- [ ] Version updated to `[TAG]-inkless` in gradle.properties
- [ ] Inkless module builds: `./gradlew :storage:inkless:build`
- [ ] Key inkless files unchanged:
  - [ ] `storage/inkless/src/main/java/io/aiven/inkless/InklessWriter.java`
  - [ ] `docs/inkless/README.md`

---

## Summary

### Merge Commit
```
[COMMIT_HASH] Merge upstream [TAG] into [WORKING_BRANCH]
```

### Files Modified
| Type | Count |
|------|-------|
| Version files | |
| Dependencies | |
| Test files | |
| Other | |

### Blockers (if any)
| Issue | Description | Action Needed |
|-------|-------------|---------------|
| | | |

---

## Notes
[Any additional observations or learnings from this sync session]
