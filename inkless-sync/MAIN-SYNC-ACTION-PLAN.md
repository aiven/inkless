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

# Create .inkless-sync directory and copy session template
mkdir -p .inkless-sync
cp inkless-sync/MAIN-SYNC-SESSION-TEMPLATE.md .inkless-sync/SESSION-$(date +%Y-%m-%d).md
```

#### Step 2: Preview & Categorize (15 min)
```bash
# Preview merge
git merge --no-commit [TARGET]

# List all conflicts
git diff --name-only --diff-filter=U > .inkless-sync/conflicts.txt

# Count and categorize conflicts
wc -l .inkless-sync/conflicts.txt
```

Fill in the conflict summary table in the session file.

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
- Complete session file
- Note any learnings

#### Step 10: Merge the sync PR (10 min)

**A repository admin has to merge a sync PR.** Both settings this step changes, the `main` ruleset
and the repository's merge options, are admin-only. The `maintain` role can't reach them. If you
ran the sync and aren't an admin, take the PR to review as usual and then hand the merge to an
admin. Ask internally who currently holds the role.

A sync PR shows **Merging is blocked** with **Commits must have verified signatures**, and the
merge-commit button is missing. Two rules in the `main` ruleset cause this. Turn both off for the
merge, then restore them.

Why each one fires:

- **Require signed commits**: upstream Apache commits are occasionally unsigned. Signing one rewrites
  its SHA, which destroys the merge base with `apache/kafka` and makes every later sync conflict, so
  the rule has to come off instead. To see which commits are at fault, open the PR's **Commits** tab
  and look for the **Unverified** badge. Expect a small number out of several hundred, all upstream.
- **Require linear history**: this forbids the merge commit, so no merge-commit button appears. The
  ruleset's own merge-method list still shows **Merge**, which is misleading. Linear history
  overrides it.

To unblock the merge:

1. Go to **Settings > Rules > Rulesets** and open the ruleset targeting the default branch (`main`).
2. Under **Rules**, clear **Require signed commits** and **Require linear history**, then choose
   **Save changes**.
3. Go to **Settings > General > Pull Requests** and confirm **Allow merge commits** is checked. This
   toggle is separate from the ruleset, and both have to be right before the button appears.
4. Reload the PR. It now reads **This branch has no conflicts with the base branch**.
5. Choose **Create a merge commit** from the merge dropdown. Squash and rebase flatten the upstream
   commits and drop the `merge:` commit, which breaks the merge base the next sync reads.

After the merge, return to the ruleset and re-check **Require signed commits** and **Require linear
history**. Leaving the signature rule off also drops the check on Inkless-authored work.

A repository admin can merge through **Require signed commits** using the ruleset's bypass list
instead of clearing the rule for everyone. That is the narrower lever, and worth setting up if
unsigned upstream commits keep appearing.

## File-by-File Playbook

### ReplicaManager.scala
1. Take upstream version
2. Add imports:
   ```scala
   import io.aiven.inkless.common.SharedState
   import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
   import io.aiven.inkless.control_plane.{BatchInfo, FindBatchRequest, FindBatchResponse, MetadataView}
   import io.aiven.inkless.delete.{DeleteRecordsInterceptor, FileCleaner, RetentionEnforcer}
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
   mkdir -p .inkless-sync
   cp inkless-sync/MAIN-SYNC-SESSION-TEMPLATE.md .inkless-sync/SESSION-$(date +%Y-%m-%d).md
   ```

3. **Run sync following this plan**
   - Document each step in the session file in .inkless-sync/
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
