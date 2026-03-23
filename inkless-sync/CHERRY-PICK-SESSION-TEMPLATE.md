# Cherry-pick Session: [BRANCH]

## Session Info
- **Date**: [YYYY-MM-DD]
- **Source**: origin/main
- **Target**: [inkless-X.Y]
- **Base**: [commit hash] ([base description, e.g., "Merge tag '4.0.2' into inkless-4.0"])
- **Status**: [In Progress | Blocked | Complete]

---

## Commits to Cherry-pick

List commits in main branch order (oldest first). Exclude commits not applicable to the release branch (e.g., version bumps).

| # | Hash | PR | Subject | Status |
|---|------|-----|---------|--------|
| 1 | | # | | Pending |

---

## Conflict Resolution Log

Document every conflict resolution. This is critical for reproducibility and future reference.

### Template per commit:

```
### #NNN - [commit subject]

**File**: `path/to/file`
- **Conflict**: [describe what conflicted]
- **Resolution**: [what was done]
- **Reason**: [why this resolution is correct for the release branch]
```

---

## Compilation Errors

Track compilation errors found after cherry-picks and their fixes.

| # | File:Line | Error | Cherry-pick | Fix | Status |
|---|-----------|-------|-------------|-----|--------|
| | | | #NNN | | |

---

## Verification

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
- [ ] All cherry-picks applied
- [ ] All conflicts documented
- [ ] Compilation verified after each cherry-pick
- [ ] Full build passes
- [ ] Full tests pass

---

## Summary

### Results
| Result | Count |
|--------|-------|
| Successfully cherry-picked | |
| Cherry-picked with conflicts | |
| Skipped (not applicable) | |
| Failed | |

### Key Conflict Patterns

Document recurring patterns for future sessions:

| Pattern | Files | Resolution |
|---------|-------|------------|
| TopicPartition vs TopicIdPartition | produce path files | Keep release branch type, use TopicIdEnricher |
| Missing feature (e.g., share coordinator) | BrokerMetadataPublisher, etc. | Skip feature blocks |
| Interface method replacement | MetadataView.java, etc. | Keep existing + add new |
| Pool.forEach vs foreachEntry | ReplicaManager.scala | Use `foreachEntry` on 4.0 |
| Readable vs ByteBuffer (parse methods) | AbstractRequest, AbstractResponse | Use ByteBuffer + ByteBufferAccessor |
| createTestTopic missing Uuid param | ReplicationControlManagerTest | Add Uuid.randomUuid() as first arg |
| Enum expansion (ApiKeys, etc.) | ApiKeys.java, AbstractRequest/Response | Add only inkless entry, skip main-only features |
| Cross-module dependency | ConfigurationControlManager | Inline constants from inaccessible modules |
| Java records not available | ReplicationControlManager | Use static final class instead |
| PartitionListener package | InitDisklessLogManager | Use kafka.cluster on 4.0 |

### Deferred Commits

Commits that could not be cherry-picked and the reason:

| PR | Reason | Blocked By |
|----|--------|------------|
| | | |

---

## Notes
[Any additional observations or learnings from this session]
