# Sync Session: [DATE]

## Session Info
- **Target**: [commit SHA or tag]
- **Branch**: `sync/upstream-YYYYMMDD`
- **Started**: [timestamp]
- **Status**: [In Progress | Blocked | Complete]

---

## Phase 1: Pre-Merge Analysis

### Target Identification
```bash
# Target commit
git rev-parse [TARGET]
```

### Conflict Preview
```bash
git merge --no-commit [TARGET]
git diff --name-only --diff-filter=U
```

### Conflict Summary
| Category | Count | Files |
|----------|-------|-------|
| Protected (ours) | | |
| Config (merge both) | | |
| Core+Inkless (re-apply) | | |
| Deleted by upstream | | |
| Import only | | |

---

## Phase 2: Conflict Resolution

### Protected Files (Use Ours)
| # | File | Status |
|---|------|--------|
| | | |

### Configuration Files
| # | File | Resolution Notes | Status |
|---|------|------------------|--------|
| | gradle.properties | | |
| | build.gradle | | |
| | gradle/dependencies.gradle | | |

### Core Files with Inkless Modifications
| # | File | Inkless Additions Needed | Status |
|---|------|--------------------------|--------|
| | BrokerServer.scala | SharedState, pass to constructors | |
| | ReplicaManager.scala | inklessSharedState param, handlers | |
| | KafkaApis.scala | inklessSharedState param, transformer | |
| | ControllerServer.scala | InklessMetadataView for metrics | |
| | DelayedFetch.scala | Split partition status | |

### Files Deleted by Upstream
| # | File | Decision | Reason |
|---|------|----------|--------|
| | | Keep/Delete | |

---

## Phase 3: Compilation

### Build Command
```bash
make build
```

### Compilation Errors
| # | File:Line | Error Type | Description | Fix | Status |
|---|-----------|------------|-------------|-----|--------|
| | | | | | |

### Fix Commits
| Commit | Description |
|--------|-------------|
| `sync(compile): ...` | |

---

## Phase 4: Testing

### Test Command
```bash
make test
```

### Test Failures
| # | Test Class | Test Method | Error | Fix | Status |
|---|------------|-------------|-------|-----|--------|
| | | | | | |

### Fix Commits
| Commit | Description |
|--------|-------------|
| `sync(test): ...` | |

---

## Phase 5: Verification

### Checklist
- [ ] `make build` passes
- [ ] `make test` passes
- [ ] Inkless module compiles: `./gradlew :storage:inkless:build`
- [ ] Key inkless files exist:
  - [ ] `storage/inkless/src/main/java/io/aiven/inkless/InklessWriter.java`
  - [ ] `docs/inkless/README.md`
- [ ] Version preserved in `gradle.properties`

### Comparison with Previous Sync
```bash
# Compare file changes
git diff [previous-sync-branch]..HEAD --stat
```

---

## Summary

### Commits Created
| Type | Count | Example |
|------|-------|---------|
| merge: | 1 | `merge: apache/kafka trunk before X.Y` |
| sync(config): | | |
| sync(compile): | | |
| sync(test): | | |

### Blockers (if any)
| Issue | Description | Action Needed |
|-------|-------------|---------------|
| | | |

### Next Steps
1.
2.
3.

---

## Notes
[Any additional observations or learnings from this sync session]
