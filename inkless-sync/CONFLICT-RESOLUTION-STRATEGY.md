# Inkless Upstream Sync: Conflict Resolution Strategy

## Overview

This document defines a structured approach for resolving merge conflicts during upstream Apache Kafka syncs. Based on learnings from sync testing.

## Conflict Categories

### Category 1: Pure Inkless Files (Protected)
**Strategy: Use OURS (inkless version)**

Files that are 100% inkless-specific and don't exist in upstream:
- `storage/inkless/**`
- `docs/inkless/**`
- `config/inkless/**`
- `.github/workflows/inkless*.yml`

**Resolution**: Automatic - keep ours.

### Category 2: Configuration Files
**Strategy: Intelligent merge - preserve both**

| File | Resolution |
|------|------------|
| `gradle.properties` | Keep inkless version string, accept upstream dependency versions |
| `build.gradle` | Keep inkless module config (`:storage:inkless`), accept upstream plugin versions |
| `gradle/dependencies.gradle` | Add inkless dependencies (assertj, testcontainers) to upstream |
| `settings.gradle` | Verify `storage:inkless` module is included |

**Resolution**: Manual review required - merge both sets of changes.

### Category 3: Core Files with Inkless Modifications
**Strategy: Apply inkless additions to upstream base**

These files have significant inkless additions to upstream Kafka code:

| File | Inkless Additions |
|------|-------------------|
| `core/.../BrokerServer.scala` | `SharedState` initialization, pass to ReplicaManager/KafkaApis |
| `core/.../ReplicaManager.scala` | `inklessSharedState`, `inklessMetadataView` params, inkless handlers |
| `core/.../KafkaApis.scala` | `inklessSharedState` param, `InklessTopicMetadataTransformer` |
| `core/.../ControllerServer.scala` | `InklessMetadataView` for metrics publisher |
| `core/.../DelayedFetch.scala` | Split partition status (classic vs diskless) |

**Resolution**:
1. Take upstream version as base
2. Re-apply inkless-specific additions
3. Adapt to new upstream APIs

### Category 4: Files Deleted by Upstream
**Strategy: Evaluate if inkless still needs them**

Files that upstream deleted:
- Accept deletion if truly obsolete
- Restore if inkless depends on them (e.g., `FlattenedIterator`)

**Resolution**: Case-by-case evaluation.

### Category 5: Import-Only Conflicts
**Strategy: Accept upstream, add inkless imports**

When only import statements conflict:
1. Accept upstream import changes (package moves, renames)
2. Add inkless-specific imports (`io.aiven.inkless.*`)

**Resolution**: Semi-automatic - follow package move patterns.

## Conflict Resolution Tracking

### Template for Each Sync Session

```markdown
## Sync Session: [DATE]
Target: [commit/tag]
Branch: sync/upstream-YYYYMMDD

### Merge Summary
- Total conflicts: X
- Auto-resolved: Y
- Manual resolution: Z

### Conflict Log

| # | File | Category | Resolution | Status |
|---|------|----------|------------|--------|
| 1 | gradle.properties | Config | Keep version, accept deps | Done |
| 2 | build.gradle | Config | Add inkless module | Done |
| 3 | BrokerServer.scala | Core+Inkless | Re-apply SharedState | In Progress |

### Compilation Errors
After merge, track compilation errors:

| # | File:Line | Error | Fix |
|---|-----------|-------|-----|
| 1 | ReplicaManager:284 | Missing import | Add io.aiven.inkless.* |

### Test Failures
After compilation, track test failures:

| # | Test | Error | Fix |
|---|------|-------|-----|
```

## Workflow

### Phase 1: Pre-Merge Analysis
1. Identify target commit
2. Preview conflicts: `git merge --no-commit TARGET`
3. Categorize each conflict
4. Create tracking document

### Phase 2: Conflict Resolution
For each conflict:
1. Identify category
2. Apply appropriate strategy
3. Document resolution
4. Mark status

### Phase 3: Compilation
1. Run `make build`
2. Track errors in document
3. Fix errors systematically
4. Commit fixes with `sync(compile):` prefix

### Phase 4: Testing
1. Run `make test`
2. Track failures
3. Fix tests
4. Commit with `sync(test):` prefix

### Phase 5: Verification
1. Verify inkless features work
2. Compare with previous sync
3. Document any regressions

## API Change Patterns

### Common Upstream Changes and Fixes

| Change Type | Example | Fix Pattern |
|-------------|---------|-------------|
| Class moved package | `DelegationTokenManager` | Update import |
| Constructor param added | `KafkaMetricsGroup` | Add required param |
| Method signature changed | `maybeRecordAndGetThrottleTimeMs` | Adapt call site |
| Class renamed | `ClientMetricsReceiverPlugin` → `ClientTelemetryExporterPlugin` | Update references |
| Type changed | `Seq` → `LinkedHashMap` | Convert data structures |

## Files Requiring Re-Application

After each sync, these files typically need inkless additions re-applied:

1. **ReplicaManager.scala**
   - Import: inkless handlers
   - Constructor: `inklessSharedState`, `inklessMetadataView`
   - Fields: handlers, metadata view
   - Methods: `findDisklessBatches`, `fetchDisklessMessages`, `fetchParamsWithNewMaxBytes`

2. **BrokerServer.scala**
   - Import: `SharedState`
   - Startup: `inklessSharedState` initialization
   - Constructor calls: pass `inklessSharedState` to ReplicaManager, KafkaApis

3. **KafkaApis.scala**
   - Import: `SharedState`, `InklessTopicMetadataTransformer`
   - Constructor: `inklessSharedState`
   - Field: `inklessTopicMetadataTransformer`

4. **ControllerServer.scala**
   - Import: `InklessMetadataView`
   - Field: `inklessMetadataView`
   - Usage: pass to `ControllerMetadataMetricsPublisher`

5. **DelayedFetch.scala**
   - Constructor: split `classicFetchPartitionStatus`/`disklessFetchPartitionStatus`
   - Methods: `tryCompleteDiskless`

## Cherry-pick Sync Conflicts

Cherry-picking inkless commits from main to release branches produces a distinct class of conflicts caused by **divergence between main and the release branch**, rather than upstream changes.

### Category 6: Type Divergence (TopicPartition vs TopicIdPartition)
**Strategy: Keep release branch's type, adapt cherry-pick logic**

On main, the produce path uses `TopicIdPartition` directly. On release branches (4.0, 4.1), it uses `TopicPartition` with `TopicIdEnricher` to convert.

**Resolution**:
1. Keep the release branch's `TopicPartition`-based method signatures
2. Use `TopicIdEnricher.enrich()` where topic IDs are needed
3. Pass enriched `Set<TopicIdPartition>` to internal methods that require it

### Category 7: Missing Features on Release Branch
**Strategy: Skip feature blocks that don't exist on the release branch**

Cherry-picks from main may include code for features not present on the release branch (e.g., share coordinator on 4.0, KRaft-only APIs).

**Resolution**:
1. Identify blocks related to the missing feature
2. Skip those blocks entirely during conflict resolution
3. Keep only the inkless-specific additions

### Category 8: Interface/API Expansion
**Strategy: Add new methods, don't replace existing ones**

Cherry-picks may replace interface methods that are still called by existing code on the release branch.

**Resolution**:
1. Keep ALL existing methods that have callers on the release branch
2. Add new methods from the cherry-pick as additions
3. Verify no callers are broken: `grep -r "methodName" --include="*.java" --include="*.scala"`

### Category 9: Class/Object Coexistence
**Strategy: Keep both when main split or moved code that still exists on the release branch**

Cherry-picks may assume code was refactored (e.g., `DynamicThreadPool` moved to separate class on main but still exists as object on 4.0).

**Resolution**:
1. Keep the existing class/object on the release branch
2. Add the new class/code from the cherry-pick alongside it

### Category 10: Cross-Module Dependency
**Strategy: Inline constants or restructure to avoid the dependency**

Cherry-picks from main may introduce imports that cross module boundaries differently than on the release branch. For example, `metadata` module importing `ServerConfigs` from `server` module works on main but not on 4.0.

**Resolution**:
1. Identify the constant or class being referenced
2. If it's a simple constant (string, boolean), inline the value directly
3. If it's a class, check if an equivalent exists in an accessible module
4. Document the inlining so it can be revisited when module structure changes

### Category 11: Enum/Switch Expansion
**Strategy: Add only the new inkless entry, skip entries from features not on the release branch**

Cherry-picks that add new entries to enums or switch statements (e.g., `ApiKeys`, `AbstractRequest.parseRequest`, `AbstractResponse.parseResponse`) may carry additional entries from features not present on the release branch.

**Resolution**:
1. Keep all existing entries from HEAD (the release branch)
2. Add ONLY the new inkless entry (e.g., `INIT_DISKLESS_LOG`)
3. Drop entries for features not on the release branch (e.g., `STREAMS_GROUP_HEARTBEAT`, `DESCRIBE_SHARE_GROUP_OFFSETS` on 4.0)
4. Ensure parameter types match the release branch (e.g., `ByteBuffer` not `Readable`)

### Category 12: Java Language Feature Divergence
**Strategy: Downgrade to the release branch's Java version features**

Main may use newer Java features (records, sealed classes, pattern matching) that don't exist on the release branch's Java version.

**Resolution**:
1. Replace `record` declarations with `static final class` + explicit fields/constructor
2. Replace `sealed` interfaces with regular interfaces
3. Replace pattern matching switches with if/else chains
4. Keep the same logic and field names for minimal divergence

### Cherry-pick Conflict Tracking

Use `.inkless-sync/CHERRY-PICK-SESSION-*.md` files to track:
- Each commit's conflict details
- Resolution decisions and reasoning
- Compilation errors and fixes

See [CHERRY-PICK-SYNC-GUIDE.md](CHERRY-PICK-SYNC-GUIDE.md) for the full workflow and session template.

## Guiding Principles

### Principle 1: Minimize Future Conflicts
**Prefer upstream patterns over inkless-specific patterns when both work.**

Example: If upstream uses `util.LinkedHashMap` and inkless uses `Seq`, prefer `util.LinkedHashMap` even if `Seq` is more Scala-idiomatic. This reduces conflicts in future syncs.

Concrete example from sync 2025-11-21:
- `DelayedFetch` constructor was changed from `Seq[(TopicIdPartition, FetchPartitionStatus)]` to `util.LinkedHashMap[TopicIdPartition, FetchPartitionStatus]`
- Upstream uses `new util.LinkedHashMap[...]()` and `.put()` - follow this pattern, don't introduce helper methods or Scala conversions
- In tests, create the map inline: `val map = new util.LinkedHashMap[...]; map.put(k, v)`

### Principle 2: Restore Tests with Restored Classes
**When restoring a class removed by upstream, also restore its associated tests.**

If inkless needs a class that upstream deleted (e.g., `FlattenedIterator`), restore both the class AND its test file to maintain test coverage.

### Principle 3: Document Restored Files
**When restoring removed files, add INKLESS NOTE documentation.**

Add a Javadoc/comment explaining:
- Why the file was retained for inkless
- The upstream commit that removed it
- A TODO for future migration away from the dependency

### Principle 4: Conservative Test Approach
**For test files, prefer upstream's version unless inkless-specific tests are critical.**

Test files from upstream tend to be stable. Adding inkless-specific tests to upstream test files creates merge conflicts. Consider:
- Keeping upstream tests as-is
- Adding inkless-specific tests in separate test classes (e.g., `InklessXxxTest`)

### Principle 5: Compare with Previous Syncs
**Before finalizing, compare with previous sync branches to catch regressions.**

Use `git diff previous-sync..current-sync` to identify differences and validate that changes are intentional.

## Success Criteria

A sync is successful when:
- [ ] All conflicts resolved and documented
- [ ] `make build` passes
- [ ] `make test` passes
- [ ] Inkless-specific tests pass
- [ ] No regressions in existing functionality
- [ ] Compared with previous sync branch for unexpected differences
