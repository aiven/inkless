# Cherry-pick Sync Guide

This guide explains how to cherry-pick inkless-specific commits from `main` to release branches (e.g., `inkless-4.1`, `inkless-4.2`).

## Overview

Unlike [Release Sync](RELEASE-SYNC-GUIDE.md) (which merges upstream Apache Kafka patches), cherry-pick sync propagates **inkless feature commits** from `main` to release branches. This is needed because inkless features land on `main` first, and must be backported to active release branches.

### When to Use

- After new inkless features or fixes are merged to `main`
- Before cutting a new inkless release from a release branch
- Periodically to keep release branches up to date with inkless development

## Quick Start

### Check What's Missing

```bash
./inkless-sync/branch-consistency.sh inkless-4.1 --missing
```

### Preview Cherry-picks

```bash
./inkless-sync/cherry-pick-to-release.sh inkless-4.1 --dry-run
```

### Execute Cherry-picks

```bash
./inkless-sync/cherry-pick-to-release.sh inkless-4.1
```

## Workflow

### Step 1: Discovery

Run the consistency check to see which commits are missing:

```bash
./inkless-sync/branch-consistency.sh inkless-4.1 --missing
```

This uses `--first-parent` on main to identify commits landed directly via PR (squash-merged), excluding upstream content brought in via merge commits. Commits are matched to the release branch by PR number (`(#NNN)` pattern).

To also see old commits that were intentionally skipped:

```bash
./inkless-sync/branch-consistency.sh inkless-4.1 --missing --all
```

### Step 2: Create Session File

Track progress in a session file:

```bash
mkdir -p .inkless-sync
cp inkless-sync/CHERRY-PICK-SESSION-TEMPLATE.md \
   .inkless-sync/CHERRY-PICK-SESSION-$(date +%Y-%m-%d).md
```

### Step 3: Review Commits

Before cherry-picking, review the list of commits. Consider:

- **Ordering**: Commits should be applied oldest-first (main branch order). The script handles this automatically when using `branch-consistency.sh` output.
- **Dependencies**: Some commits depend on earlier ones. Check PR descriptions for prerequisite PRs.
- **Exclusions**: Skip commits that are not applicable to the release branch (e.g., version bumps like `MINOR: update Kafka version variables`).

### Step 4: Cherry-pick

#### Interactive Mode (Recommended)

```bash
./inkless-sync/cherry-pick-to-release.sh inkless-4.1
```

The script prompts before each commit. Options: `Y`(proceed), `n`(skip), `s`(skip), `q`(quit).

#### Non-interactive Mode

```bash
./inkless-sync/cherry-pick-to-release.sh inkless-4.1 --no-interactive
```

Stops on first conflict. Resolve manually, then `git cherry-pick --continue` and re-run the script.

#### Specific Commits

```bash
./inkless-sync/cherry-pick-to-release.sh inkless-4.1 abc123 def456
```

When providing specific commits, they are applied in the order given (not reversed).

### Step 5: Resolve Conflicts

When a cherry-pick has conflicts, resolve them following the patterns in [Conflict Resolution Patterns](#conflict-resolution-patterns) below.

**After resolving each conflict:**

1. Complete the cherry-pick: `git cherry-pick --continue`
2. Document the resolution in the session file
3. Then follow the **conflict path** in Step 6 below

### Step 6: Verify After Each Cherry-pick

Use the fast path when the cherry-pick applied cleanly; use the full path when it required conflict resolution.

#### Fast path (no conflicts)

```bash
# Core inkless modules
./gradlew :storage:inkless:compileJava :core:compileScala

# If the commit touches tests
./gradlew :storage:inkless:compileTestJava :core:compileTestScala

# If the commit touches metadata, clients, or other modules
./gradlew :metadata:compileJava :clients:compileJava
```

#### Full path (conflict resolution required)

```bash
# 1. Apply formatting — conflict resolution often leaves unused imports or style drift
make fmt

# 2. Amend the cherry-pick with any fmt changes before proceeding
git add -p   # stage only fmt-related changes
git commit --amend --no-edit

# 3. Full build — catches cross-module regressions the fast path misses
make build

# 4. Re-run the tests introduced or modified by this commit
#    Identify them from the commit diff, then run specifically, e.g.:
./gradlew :storage:inkless:test --tests "io.aiven.inkless.SomeNewTest"
```

Fix any compilation errors before proceeding to the next commit. Commit fixes as amendments to the cherry-pick (if the cherry-pick introduced the broken code) or as separate `sync(compile):` commits.

**Important**: Commits touching `clients/` (e.g., `ApiKeys.java`, `AbstractRequest.java`) or `metadata/` (e.g., `ReplicationControlManager.java`) need their respective modules compiled — not just `:core` and `:storage:inkless`.

### Step 7: Periodic Full Compilation

After every 5-10 cherry-picks, run a full project compilation to catch cross-module issues early:

```bash
./gradlew compileJava compileScala compileTestJava compileTestScala
```

This catches issues like cross-module dependency problems (e.g., `metadata` module importing from `server` module) that per-module checks miss.

### Step 8: Verify Build

After all cherry-picks are complete:

```bash
make build
make test
```

### Step 9: Archive Session

```bash
mv .inkless-sync/CHERRY-PICK-SESSION-*.md inkless-sync/sessions/
```

## Conflict Resolution Patterns

Cherry-pick conflicts differ from merge conflicts because they arise from **divergence between main and the release branch**, not from upstream changes.

### Common Divergence Points

The following are examples of API divergences encountered during cherry-pick syncs. As release branches progress, some entries may no longer apply — verify against your target branch.

| Area                                       | main                                              | Release branch                                               | Resolution Pattern                                        | Affected versions |
| ------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------ | --------------------------------------------------------- | ----------------- |
| Partition types                            | `TopicIdPartition` throughout produce path        | `TopicPartition` + `TopicIdEnricher`                         | Keep release branch's type, adapt cherry-pick logic       | 4.0, 4.1          |
| Share coordinator                          | Present (Kafka 4.2+ feature)                      | Absent                                                       | Skip share coordinator blocks                             | 4.0, 4.1          |
| KRaft/ZK compat                            | KRaft-only (ZK removed)                           | ZK compatibility code present                                | Keep both where needed                                    | 4.0 only          |
| `DynamicThreadPool`                        | Moved to separate class                           | Object in `DynamicBrokerConfig`                              | Keep existing + add new code                              | 4.0               |
| `Pool` class                               | `forEach` method                                  | `foreachEntry` method                                        | Use whichever method exists                               | 4.0               |
| `KRaftMetadataCache.getTopicName()`        | Returns `java.util.Optional[String]`              | Returns `Option[String]`                                     | Use `Some(name)` not `Optional.of(name)`                  | 4.0               |
| `KRaftMetadataCache.getAliveBrokerNodes()` | Returns `java.util.List[Node]`                    | Returns `Seq[Node]`                                          | Add `.asJava` when implementing Java interfaces           | 4.0               |
| `FeatureControlManager`                    | `metadataVersionOrThrow()`                        | `metadataVersion()`                                          | Check which method exists                                 | 4.0               |
| `AbstractRequest/Response.parse`           | Uses `Readable` parameter                         | Uses `ByteBuffer` + `ByteBufferAccessor`                     | Adapt parse methods to match                              | 4.0               |
| `BrokerMetadataPublisher` ctor             | Takes `ShareCoordinator`, `SharePartitionManager` | Takes `Option[ShareCoordinator]`, no `SharePartitionManager` | Wrap in `Some()`, drop `SharePartitionManager`            | 4.0, 4.1          |
| `Partition.makeLeader/makeFollower`        | Takes `PartitionRegistration`                     | Takes `LeaderAndIsrRequest.PartitionState`                   | Convert via `toLeaderAndIsrPartitionState()`              | 4.0               |
| `PartitionListener`                        | Package `org.apache.kafka.server.partition`       | Package `kafka.cluster`                                      | Use the package that exists on target                     | 4.0               |
| `UnifiedLog.producerStateManager`          | Method call `producerStateManager()`              | Field access (Scala val)                                     | Drop parens if it's a val                                 | 4.0               |
| Java records                               | Used (e.g., `record IneligibleReplica`)           | Not available (Java 11)                                      | Use static final class with explicit fields               | 4.0 only          |
| `ApiKeys` enum                             | Includes STREAMS_GROUP, SHARE_GROUP_OFFSETS, etc. | Fewer entries                                                | Add only INIT_DISKLESS_LOG, don't carry main-only entries | 4.0, 4.1          |

### Resolution by File Type

#### Pure Inkless Files (`storage/inkless/**`)

Usually apply cleanly. If not, the conflict is typically in:

- **Import changes**: Accept the cherry-pick's imports, remove any that don't exist on the release branch
- **API differences**: Adapt to the release branch's API surface

#### Core Kafka Files (`core/src/main/scala/**`)

These are the most conflict-prone. Common patterns:

**Imports**: Cherry-pick may add imports for classes that don't exist on the release branch or are in different packages.

- Check if the imported class exists: `git log --oneline --all -- '**/ClassName.java'`
- If it doesn't exist, skip the import and adapt the code

**Constructor parameters**: Cherry-pick may reference constructor params or methods that differ.

- Compare the class signature on main vs release branch
- Adapt the cherry-pick to use the release branch's API

**Feature blocks**: Cherry-pick may include code for features not present on the release branch (e.g., share coordinator).

- Skip the entire feature block
- Keep only the inkless-specific additions

#### Interface/API Files (`MetadataView.java`, etc.)

Cherry-pick may replace methods that are still in use on the release branch.

- **Keep existing methods** that other code still calls
- **Add new methods** from the cherry-pick as additions, not replacements
- Verify no callers are broken: `grep -r "methodName" --include="*.java" --include="*.scala"`

#### Test Files

- Accept cherry-pick's test changes when they simplify test setup
- Verify test constructors match the production code on the release branch
- If a test references a class/constructor that doesn't exist on the release branch, adapt it

### TopicPartition vs TopicIdPartition Pattern

This is a frequent conflict source when the release branch still uses `TopicPartition` in the produce path while main has moved to `TopicIdPartition`. If the release branch uses `TopicIdEnricher`, follow this pattern:

**When cherry-picking produce path changes:**

1. Keep the release branch's `TopicPartition`-based signatures
2. Use `TopicIdEnricher.enrich(metadata, entriesPerPartition)` to convert when needed
3. Pass `entriesPerPartitionEnriched.keySet()` (which is `Set<TopicIdPartition>`) to methods that need topic IDs

**Example adaptation** (AppendHandler):

```java
// main version: handle() receives Map<TopicIdPartition, MemoryRecords> directly
// release branch: handle() receives Map<TopicPartition, MemoryRecords>, enriches internally
public CompletableFuture<...> handle(Map<TopicPartition, MemoryRecords> entriesPerPartition, ...) {
    // Enrich with topic IDs (release-branch-specific step)
    Map<TopicIdPartition, MemoryRecords> entriesPerPartitionEnriched =
        TopicIdEnricher.enrich(metadata, entriesPerPartition);
    // Now use entriesPerPartitionEnriched for methods that need TopicIdPartition
    return writer.write(entriesPerPartitionEnriched,
        getLogConfigs(entriesPerPartitionEnriched.keySet()), requestLocal);
}
```

## Commit Convention

Cherry-picked commits retain their original message. When additional fixes are needed:

| Prefix           | Use                                                |
| ---------------- | -------------------------------------------------- |
| `sync(compile):` | Fixing compilation errors after cherry-pick        |
| `sync(test):`    | Fixing test compilation/failures after cherry-pick |

Alternatively, amend the cherry-pick commit with the fixes if they are small and directly related.

## Troubleshooting

### "Commit not found"

The commit hash may have been rebased on main. Use the PR number to find the current hash:

```bash
git log --oneline origin/main --grep="(#NNN)"
```

### Cherry-pick Applies But Doesn't Compile

This is expected when main and the release branch have diverged significantly. Common causes:

- Missing imports (class in different package or doesn't exist on release branch)
- Method signature differences
- Type mismatches (TopicPartition vs TopicIdPartition)
- Cross-module dependency (e.g., `ServerConfigs` is in `server` module, not accessible from `metadata` module on release branch)
- Java version differences (main may use newer Java features not available on the release branch)
- Scala/Java API surface differences (`ByteBuffer` vs `Readable`, `Option` vs `Optional`)

Fix compilation errors, then either amend the cherry-pick or create a separate `sync(compile):` commit.

**Concrete examples from inkless-4.1 sync:**

| Error                                        | Cause                                                                                        | Fix                                           |
| -------------------------------------------- | -------------------------------------------------------------------------------------------- | --------------------------------------------- |
| `forEach` not a member of `Pool`             | Release branch uses `foreachEntry`                                                           | Change to `foreachEntry`                      |
| `metadataVersionOrThrow()` not found         | Release branch uses `metadataVersion()`                                                      | Drop `OrThrow`                                |
| `ByteBuffer` cannot convert to `Readable`    | `InitDisklessLogRequest.parse` written for main's API                                        | Change to `ByteBuffer` + `ByteBufferAccessor` |
| `ServerConfigs` not found in metadata module | `metadata` module can't depend on `server` on release branch                                 | Inline the constant values                    |
| `PartitionListener` not found                | Different package on release branch (`kafka.cluster` vs `org.apache.kafka.server.partition`) | Fix import                                    |
| `producerStateManager()` not a member        | Scala val on release branch, not a method                                                    | Remove parens                                 |
| `String cannot be converted to Uuid`         | `createTestTopic` takes `Uuid` first on release branch                                       | Add `Uuid.randomUuid()`                       |

### Too Many Conflicts

If a commit has extensive conflicts, consider:

1. Cherry-picking the commit's changes manually (read the PR diff on GitHub)
2. Skipping the commit if it's not critical for the release branch
3. Creating a release-branch-specific implementation

### Knowing When to Stop

Not all commits can be cleanly cherry-picked. **Defer a commit** when it:

- Introduces code that depends on APIs fundamentally different between main and the release branch (e.g., `org.apache.kafka.server.partition.PartitionListener` on main vs `kafka.cluster.PartitionListener` on release branch, with different method signatures or lifecycles)
- Requires cross-module dependency changes (e.g., `metadata` module importing from `server` module)
- Needs multiple deep adaptations across different subsystems simultaneously

**Strategy**: Cherry-pick in batches. Compile after each batch. If a commit breaks compilation and the fix is non-trivial, reset to the last good state and defer the problematic commit. Record it in the session file with the reason for deferral, so the next session can pick it up with a focused effort.

## AI-Assisted Cherry-pick Sync

For AI-assisted sessions, use this prompt:

```
I need to cherry-pick inkless commits from main to a release branch.

## Context
- Target branch: inkless-4.1 (or inkless-4.2, etc.)
- Source: origin/main

## Steps

1. Run `./inkless-sync/branch-consistency.sh inkless-4.1 --missing` to find missing commits
2. Create session file from inkless-sync/CHERRY-PICK-SESSION-TEMPLATE.md
3. Present each commit for approval before cherry-picking
4. Resolve conflicts following inkless-sync/CHERRY-PICK-SYNC-GUIDE.md
5. Compile after each cherry-pick: `./gradlew :storage:inkless:compileJava :core:compileScala`
6. Document all conflict resolutions in the session file
7. Run full build and tests after all cherry-picks

Reference files:
- inkless-sync/CHERRY-PICK-SYNC-GUIDE.md
- inkless-sync/CHERRY-PICK-SESSION-TEMPLATE.md
- inkless-sync/CONFLICT-RESOLUTION-STRATEGY.md
```

## Related Documentation

- [Branch Consistency Check](README.md#branch-consistency-check) - Finding missing commits
- [Conflict Resolution Strategy](CONFLICT-RESOLUTION-STRATEGY.md) - General conflict patterns
- [Release Sync Guide](RELEASE-SYNC-GUIDE.md) - Upstream patch sync (different workflow)
