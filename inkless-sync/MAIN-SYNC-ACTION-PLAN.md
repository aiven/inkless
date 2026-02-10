# Actionable Sync Strategy

## Key Learnings from Sync Test

### What Works Well
1. **Merge phase** - The merge itself works correctly
2. **Protected file detection** - Pattern-based identification works
3. **Commit convention** - `sync(type):` prefixes provide clear history

### Key Challenges Identified

1. **Ours/Theirs is insufficient** - Need intelligent merge that:
   - Takes upstream API changes
   - Re-applies inkless additions

2. **Core files with inkless modifications** - These files require:
   - Understanding what inkless adds
   - Understanding what upstream changed
   - Combining both sets of changes

3. **API changes cascade** - A single upstream API change can require:
   - Import updates
   - Constructor changes
   - Method signature updates

## Recommended Workflow

### For Each Sync Session

#### Step 1: Setup (5 min)
```bash
# Create worktree for clean environment (e.g., for 2026-02-05)
git worktree add ../inkless-sync-20260205 -b sync/upstream-20260205
cd ../inkless-sync-20260205

# Create .sync directory and copy session template
mkdir -p .sync
cp inkless-sync/MAIN-SYNC-SESSION-TEMPLATE.md .sync/SESSION.md
```

#### Step 2: Preview & Categorize (15 min)
```bash
# Preview merge
git merge --no-commit [TARGET]

# List all conflicts
git diff --name-only --diff-filter=U > .sync/conflicts.txt

# Count and categorize conflicts
wc -l .sync/conflicts.txt
```

Fill in the conflict summary table in SESSION.md.

#### Step 3: Resolve Protected Files (5 min)
```bash
# For each protected file (storage/inkless/*, docs/inkless/*, etc.)
git checkout --ours [file]
git add [file]
```

#### Step 4: Resolve Configuration Files (10-15 min)
For each config file (gradle.properties, build.gradle, etc.):
1. View both versions
2. Merge manually: upstream changes + inkless config
3. Test with `./gradlew tasks` to verify Gradle works

#### Step 5: Resolve Core Files (30-60 min)
For each core file with inkless modifications:

1. **Take upstream as base**
   ```bash
   git checkout --theirs [file]
   ```

2. **Identify inkless additions needed** (from strategy doc)

3. **Apply inkless additions**
   - Add imports
   - Add constructor parameters
   - Add fields
   - Add methods

4. **Verify syntax**
   ```bash
   ./gradlew :core:compileScala -x test
   ```

#### Step 6: Complete Merge (5 min)
```bash
git commit -m "merge: apache/kafka trunk [TARGET_INFO]"
```

#### Step 7: Fix Compilation Errors (15-30 min)
```bash
make build
# Fix errors iteratively
git commit -m "sync(compile): [description]"
```

#### Step 8: Fix Test Failures (15-30 min)
```bash
make test
# Fix failures iteratively
git commit -m "sync(test): [description]"
```

#### Step 9: Verify & Document (10 min)
- Run full verification checklist
- Complete SESSION.md
- Note any learnings

## File-by-File Playbook

### ReplicaManager.scala
1. Take upstream version
2. Add imports:
   ```scala
   import io.aiven.inkless.common.SharedState
   import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
   import io.aiven.inkless.control_plane.{BatchInfo, FindBatchRequest, FindBatchResponse, MetadataView}
   import io.aiven.inkless.delete.{DeleteRecordsInterceptor, FileCleaner, RetentionEnforcer}
   import io.aiven.inkless.merge.FileMerger
   import io.aiven.inkless.produce.AppendHandler
   import kafka.server.metadata.{InklessMetadataView, KRaftMetadataCache}
   ```
3. Add constructor params (at end):
   ```scala
   inklessSharedState: Option[SharedState] = None,
   inklessMetadataView: Option[MetadataView] = None
   ```
4. Add fields (after other private vals)
5. Add methods (before closing brace)
6. Update any calls to changed upstream APIs

### BrokerServer.scala
1. Take upstream version
2. Add import: `import io.aiven.inkless.common.SharedState`
3. In startup():
   - Create `inklessSharedState`
   - Pass to ReplicaManager constructor
   - Pass to KafkaApis constructor

### KafkaApis.scala
1. Take upstream version
2. Add imports:
   ```scala
   import io.aiven.inkless.common.SharedState
   import io.aiven.inkless.metadata.InklessTopicMetadataTransformer
   ```
3. Add constructor param: `inklessSharedState: Option[SharedState] = None`
4. Add field: `val inklessTopicMetadataTransformer = ...`

### ControllerServer.scala
1. Take upstream version
2. Add import: `InklessMetadataView` to metadata import
3. Create `inklessMetadataView` before ControllerMetadataMetricsPublisher
4. Pass `isDisklessTopic` function to publisher

### DelayedFetch.scala
Evaluate: May be able to use upstream if inkless-specific fetch handling is in ReplicaManager.

## Testing New Session

To test this strategy in a clean context:

1. **Create new worktree** (use today's date, e.g., 20260205)
   ```bash
   git worktree add ../inkless-sync-20260205 -b sync/upstream-20260205
   cd ../inkless-sync-20260205
   ```

2. **Setup tracking**
   ```bash
   mkdir -p .sync
   cp inkless-sync/MAIN-SYNC-SESSION-TEMPLATE.md .sync/SESSION.md
   ```

3. **Run sync following this plan**
   - Document each step in .sync/SESSION.md
   - Note any deviations
   - Record time spent

4. **Compare results**
   - With `upstream-sync-before-4.3` branch
   - Note differences

## Iteration

After each sync session:
1. Update conflict resolution strategy based on learnings
2. Add new API change patterns discovered
3. Refine file playbooks
4. Improve automation where possible
