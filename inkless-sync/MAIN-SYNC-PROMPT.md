# Agent Prompts for Upstream Sync

This document provides prompt templates for guiding an AI agent through the upstream sync process.

## Initial Sync Prompt

Use this prompt to start a new sync session:

```
I need to sync inkless with upstream Apache Kafka.

Target: [commit SHA, tag, or "trunk" for latest]

Please follow the sync process documented in inkless-sync/:
1. Create a worktree with branch sync/upstream-YYYYMMDD
2. Preview and categorize conflicts
3. Resolve conflicts following CONFLICT-RESOLUTION-STRATEGY.md
4. Track progress in .sync/SESSION.md using the template
5. Fix compilation errors
6. Run tests

Reference files:
- inkless-sync/CONFLICT-RESOLUTION-STRATEGY.md
- inkless-sync/MAIN-SYNC-ACTION-PLAN.md
- inkless-sync/MAIN-SYNC-SESSION-TEMPLATE.md

Start by reading these files and setting up the worktree.
```

## Phase-Specific Prompts

### Phase 1: Setup

```
Set up a new sync session targeting [TARGET].

1. Create worktree: ../inkless-sync-YYYYMMDD with branch sync/upstream-YYYYMMDD
2. Create .sync/ directory
3. Copy MAIN-SYNC-SESSION-TEMPLATE.md to .sync/SESSION.md
4. Fetch upstream and identify target commit

Report the setup status and target commit SHA.
```

### Phase 2: Preview Conflicts

```
Preview the merge and categorize conflicts.

1. Run: git merge --no-commit [TARGET]
2. List all conflicting files
3. Categorize each conflict using CONFLICT-RESOLUTION-STRATEGY.md:
   - Category 1: Protected (pure inkless files)
   - Category 2: Configuration files
   - Category 3: Core files with inkless modifications
   - Category 4: Files deleted by upstream
   - Category 5: Import-only conflicts

Update .sync/SESSION.md with the conflict summary.
Do NOT complete the merge yet - just preview and categorize.
```

### Phase 3: Resolve Protected Files

```
Resolve all Category 1 (Protected) conflicts.

For files matching these patterns, use "ours" (inkless version):
- storage/inkless/**
- docs/inkless/**
- config/inkless/**
- .github/workflows/inkless*.yml

Commands:
git checkout --ours [file]
git add [file]

Update .sync/SESSION.md with resolutions.
```

### Phase 4: Resolve Configuration Files

```
Resolve Category 2 (Configuration) conflicts.

For each config file (gradle.properties, build.gradle, gradle/dependencies.gradle):
1. Show both versions (ours and theirs)
2. Explain what each side has
3. Propose a merged version that:
   - Keeps inkless version string
   - Keeps inkless module configuration
   - Accepts upstream dependency/plugin versions

Wait for approval before applying changes.
```

### Phase 5: Resolve Core Files

```
Resolve Category 3 (Core files with inkless modifications).

For [FILE_NAME], follow the playbook in MAIN-SYNC-ACTION-PLAN.md:
1. Take upstream version as base: git checkout --theirs [FILE]
2. Identify inkless additions needed (from strategy doc)
3. Apply inkless additions:
   - Add imports
   - Add constructor parameters
   - Add fields
   - Add methods
4. Show the diff of changes made
5. Verify syntax compiles

Wait for approval before moving to next file.
```

### Phase 6: Complete Merge

```
Complete the merge commit.

1. Verify all conflicts are resolved: git diff --name-only --diff-filter=U
2. If clean, create merge commit:
   git commit -m "merge: apache/kafka trunk [TARGET_INFO]"
3. Show commit summary

Report any remaining issues.
```

### Phase 7: Fix Compilation

```
Fix compilation errors.

1. Run: make build
2. Collect all compilation errors
3. For each error:
   - Identify the cause (missing import, API change, etc.)
   - Propose a fix
   - Apply the fix
4. Re-run build until clean
5. Commit fixes: git commit -m "sync(compile): [description]"

Update .sync/SESSION.md with error log and fixes.
```

### Phase 8: Fix Tests

```
Fix test failures.

1. Run: make test
2. Collect failing tests
3. For each failure:
   - Analyze the error
   - Identify if it's due to API changes or missing config
   - Propose and apply fix
4. Re-run tests until green
5. Commit fixes: git commit -m "sync(test): [description]"

Update .sync/SESSION.md with test log and fixes.
```

### Phase 9: Verify and Report

```
Complete verification and generate report.

1. Run verification checklist:
   - make build passes
   - make test passes
   - Key inkless files exist
   - Version preserved in gradle.properties

2. Generate summary:
   - Total conflicts resolved
   - Commits created
   - Time spent (if tracked)
   - Any blockers or issues

3. Update .sync/SESSION.md with final status

4. If successful, provide PR description draft.
```

## Error Recovery Prompts

### When Blocked on Conflict

```
I'm blocked on resolving [FILE].

The conflict is:
[paste conflict markers]

Inkless needs: [what inkless adds]
Upstream changed: [what upstream changed]

Please help resolve this by:
1. Explaining what both sides are doing
2. Proposing a merged solution
3. Showing the exact code to use
```

### When Compilation Fails

```
Compilation failed with these errors:
[paste errors]

Please:
1. Identify the root cause for each error
2. Explain the upstream API change that caused it
3. Provide the fix for each error
4. Show me the exact edits needed
```

### When Tests Fail

```
These tests are failing:
[paste test failures]

Please:
1. Analyze each failure
2. Determine if it's a real bug or test infrastructure issue
3. Propose fixes
4. Show the exact changes needed
```

## Continuation Prompt

If a session is interrupted:

```
Continue the sync session on branch [sync/upstream-YYYYMMDD].

Current status:
- Phase: [current phase]
- Last completed step: [step]
- Blockers: [if any]

Read .sync/SESSION.md for full context and continue from where we left off.
```

## Best Practices for Agent Usage

1. **Be specific about targets** - Always provide exact commit SHA or tag
2. **Work incrementally** - Complete one phase before moving to next
3. **Document everything** - Keep .sync/SESSION.md updated
4. **Wait for approval** - On complex merges, show diff before committing
5. **Track blockers** - If stuck, clearly describe the issue
6. **Commit often** - Small, focused commits are easier to review/revert
7. **Restore tests with restored classes** - When restoring a class removed by upstream for inkless functionality, also restore its associated test file to maintain test coverage
8. **Add INKLESS NOTE** - When restoring removed files, add a Javadoc/comment explaining why the file was retained for inkless, with a TODO for future migration
