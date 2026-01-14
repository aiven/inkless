# Inkless Versioning Strategy

## Overview

Inkless is developed as a fork of Apache Kafka. It:
- Is proposed upstream via KIP-1150/1163, but maintains its own implementation
- Iterates independently on top of Kafka versions
- Uses a special release strategy optimized for open development and progress tracking

**Historical context:**
- Previously used per-branch `rc` tags. For example: `inkless-4.0.0-rc32` (Kafka 4.0) and `inkless-4.1.1-rc1` (Kafka 4.1).
- Starting with the next release, Inkless iterations use a global counter so versions are comparable across branches. The next iteration will be `0.33`.
  - Tag the release on `main` as `inkless-release-0.33`
  - Tag the corresponding Kafka-base builds as `inkless-4.0.0-0.33`, `inkless-4.1.1-0.33`

**Key principle:** Inkless versions show progress and Kafka compatibility, not production readiness marketing.

---

## Version Format

Inkless uses two tag families:

1. **Inkless release tags** (on `main`):
   - `inkless-release-<inkless-version>`

2. **Kafka-base tags** (on Inkless release branches):
   - `inkless-<kafka-version>-<inkless-version>`

```
Inkless release tag:
inkless-release-0.33

Kafka-base tags:
inkless-4.0.0-0.33
inkless-4.1.1-0.33
```

The `inkless-release-*` tag is the canonical identifier for an Inkless iteration; the `inkless-<kafka-version>-*` tags track the same Inkless iteration applied to specific Kafka versions.

### Components

**`inkless-release-<inkless-version>`** (e.g., `inkless-release-0.33`)
- Tag on `main` identifying an Inkless iteration
- **Canonical release identifier** used for changelog/release notes
- Represents the Inkless iteration independent of Kafka base

**`inkless-<kafka-version>-<inkless-version>`** (e.g., `inkless-4.1.1-0.33`)
- Tag on a release branch (e.g. `inkless-4.1`)
- Shows which Kafka version this Inkless iteration is based on
- Allows users to select compatibility with a specific Kafka version

**`<kafka-version>`** (e.g., `4.0.0`, `4.1.1`)
- Exact Apache Kafka version from upstream tag
- Taken from Kafka release branches (e.g., tag `4.0.0` on branch `4.0`)

**`<inkless-version>`** (e.g., `0.32`, `0.33`)
- Inkless iteration number
- **Global counter** across all Kafka versions
- Increments with each Inkless release from trunk
- Same Inkless version across Kafka versions indicates the same Inkless change set (modulo backport feasibility)

---

## Why This Format?

### Problem with Previous Approach

```
Old format:
inkless-4.0.0-rc32   ← Looks very mature (32 iterations!)
inkless-4.1.1-rc1    ← Looks brand new (only 1 iteration!)

Reality: Both contain the SAME Inkless commits, just on different Kafka bases!
```

The rc number was per-branch, making versions look unrelated when they were actually aligned.

### Solution: Global Inkless Version

```
New format:
inkless-release-0.32  ← Canonical Inkless iteration 0.32 (tag on `main`)
inkless-4.0.0-0.32    ← Kafka 4.0.0 + Inkless iteration 0.32
inkless-4.1.1-0.32    ← Kafka 4.1.1 + Inkless iteration 0.32
inkless-4.2.0-0.32    ← Kafka 4.2.0 + Inkless iteration 0.32
```

**Benefits:**
- Same Inkless version across Kafka versions is immediately visible
- Users can see alignment: "These all have Inkless 0.32"
- Kafka version is explicit and unambiguous
- Simple comparison: higher Inkless version = newer features
- No confusing "rc" terminology

---

## Development and Release Workflow

### Branch Structure

```
apache/kafka trunk
    ↓ merge commits (favor velocity, avoid repeated rebase conflicts)
    
inkless trunk/main (active development)
    • Inkless feature commits
    • Merge commits from apache/kafka
    • Fast iteration on ideas
    |
    ├─→ inkless-4.0 (release branch)
    |     ↓ Based on Kafka tag 4.0.0 from Kafka branch 4.0
    |     ↓ Cherry-pick Inkless commits from trunk (manual)
    |     ↓ Merge commits to align with Kafka 4.0.1, 4.0.2, etc.
    |     •─→ inkless-4.0.0-0.30
    |     •─→ inkless-4.0.0-0.31
    |     •─→ inkless-4.0.0-0.32
    |     • (merge Kafka 4.0.1)
    |     •─→ inkless-4.0.1-0.32
    |     •─→ inkless-4.0.1-0.33
    |
    ├─→ inkless-4.1 (release branch)
    |     ↓ Based on Kafka tag 4.1.0 from Kafka branch 4.1
    |     ↓ Cherry-pick Inkless commits from trunk (manual)
    |     ↓ Merge commits to align with Kafka 4.1.1, 4.1.2, etc.
    |     •─→ inkless-4.1.0-0.32
    |     • (merge Kafka 4.1.1)
    |     •─→ inkless-4.1.1-0.32
    |     •─→ inkless-4.1.1-0.33
    |
    └─→ (future inkless-4.2, inkless-4.3, etc.)
```

### Key Workflow Principles

1. **Trunk development uses merge commits** from apache/kafka
   - Favors velocity over clean history
   - Allows quick iteration on ideas
   - Avoids resolving rebase conflicts multiple times

2. **Release branches are independent** (do not converge)
   - Similar to Kafka's own release branch strategy
   - Each branch based on specific Kafka version tag
   - No cross-branch merging

3. **Cherry-picking to release branches**
   - Manual process to select Inkless commits from trunk
   - Applied to all active release branches
   - Uses same Inkless version number across branches

4. **Kafka updates via merge commits**
   - When Kafka releases 4.0.1, 4.0.2, etc.
   - Merge those tags into corresponding Inkless release branch
   - Keeps Inkless changes on top of newer Kafka patches

---

## Release Process

### 1. Development on Trunk

**Where:** `inkless` trunk/main branch

**Process:**
```bash
# Regular development
git checkout main
# ... make Inkless changes ...
git commit -m "inkless: Add feature X"

# Sync with Apache Kafka (merge commits)
git fetch apache
git merge apache/trunk
```

**Notes:**
- Merge commits from apache/kafka for velocity
- No need for clean rebase history
- Focus on fast iteration

---

### 2. Creating a New Release Branch

**When:** Apache Kafka releases a new minor version (e.g., 4.1.0, 4.2.0)

**Process:**
```bash
# Fetch Kafka upstream
git fetch apache

# Create Inkless release branch from Kafka tag
git checkout -b inkless-4.1 refs/tags/4.1.0
git push origin inkless-4.1

# Now ready for cherry-picking Inkless commits
```

**Example branches:**
- `inkless-4.0` - based on Kafka 4.0.0 tag
- `inkless-4.1` - based on Kafka 4.1.0 tag
- `inkless-4.2` - based on Kafka 4.2.0 tag

---

### 3. Creating an Inkless Release (Cherry-Picking)

**When:** Ready to release a new Inkless iteration

**Process:**
1. Tag the release on `main` as `inkless-release-<inkless-version>`
2. Cherry-pick the release commit set to each active release branch
3. Tag each branch head as `inkless-<kafka-version>-<inkless-version>`

#### Step 1: Determine next Inkless version
```bash
# Look at latest Inkless release tag on main
git tag | grep "^inkless-release-" | sort -V | tail -n 5

# Example output:
# inkless-release-0.30
# inkless-release-0.31
# inkless-release-0.32

# Next version: 0.33
```

#### Step 2: Tag the release on `main`, then cherry-pick to each active release branch

First, tag the canonical Inkless release on `main`:

```bash
# On main
git checkout main

git tag -a inkless-release-0.33 -m "Inkless release 0.33"
git push origin inkless-release-0.33
```

Then, apply the release to each active Kafka base:

```bash
# For inkless-4.0 branch
git checkout inkless-4.0

# Cherry-pick the commits included in inkless-release-0.33
# (manual selection)
git cherry-pick <commit-hash-1>
git cherry-pick <commit-hash-2>
git cherry-pick <commit-hash-3>

# Resolve conflicts if any
# Test the build

git tag -a inkless-4.0.0-0.33 -m "Inkless release 0.33 on Kafka 4.0.0"
git push origin inkless-4.0.0-0.33
```

```bash
# For inkless-4.1 branch
git checkout inkless-4.1

# Cherry-pick the SAME commits included in inkless-release-0.33
git cherry-pick <commit-hash-1>
git cherry-pick <commit-hash-2>
git cherry-pick <commit-hash-3>

# Resolve conflicts if any
# Test the build

git tag -a inkless-4.1.1-0.33 -m "Inkless release 0.33 on Kafka 4.1.1"
git push origin inkless-4.1.1-0.33
```

**Result:**
- `inkless-release-0.33` identifies the Inkless change set
- `inkless-4.0.0-0.33` and `inkless-4.1.1-0.33` apply it to specific Kafka versions

---

### 4. Merging Kafka Patch Releases

**When:** Apache Kafka releases a patch version (e.g., 4.1.1, 4.1.2)

**Process:**
```bash
# On inkless-4.1 branch
git checkout inkless-4.1

# Fetch new Kafka tag
git fetch apache

# Merge Kafka patch release (merge commit)
git merge refs/tags/4.1.1
# Resolve conflicts if any

git push origin inkless-4.1

# Continue with same Inkless iteration (if no Inkless changes)
git tag -a inkless-4.1.1-0.33 -m "Inkless release 0.33 on Kafka 4.1.1"
git push origin inkless-4.1.1-0.33
```

**Notes:**
- Merge commits are used (not rebase)
- Keeps Inkless changes on top
- Kafka patch number updates in version tag

---

## Version Increment Rules

### When to Increment Inkless Version

**Increment `0.32` → `0.33` when:**
- New Inkless features added to trunk
- Bug fixes to Inkless implementation
- Ready to release new iteration to branches
- After manual cherry-picking to release branches

**Same Inkless version `0.33` when:**
- Just merged Kafka patch update (4.1.0 → 4.1.1)
- Same Inkless commits on different Kafka versions
- No new Inkless functionality

### Examples

**Scenario 1: New Inkless Feature**
```bash
# On trunk: develop feature
git commit -m "inkless: Add AZ alignment improvements"

# Cherry-pick to branches
inkless-4.0.0-0.33  ← was 0.32, now 0.33 (new feature)
inkless-4.1.1-0.33  ← was 0.32, now 0.33 (same feature)
```

**Scenario 2: Kafka Patch Update**
```bash
# Merge Kafka 4.1.1 into inkless-4.1 branch
git merge refs/tags/4.1.1

# Tag with same Inkless version
inkless-4.1.1-0.33  ← still 0.33 (just Kafka update)
```

**Scenario 3: New Kafka Minor Version**
```bash
# Create new branch from Kafka 4.2.0
git checkout -b inkless-4.2 refs/tags/4.2.0

# Cherry-pick latest Inkless commits
git cherry-pick ...

# Use current Inkless version
inkless-4.2.0-0.33  ← same 0.33 as other branches
```

---

## User-Facing Documentation

### How Users Choose a Version

**To use latest Inkless features:**
```bash
# Find highest Inkless version number
git tag | grep "inkless" | sort -V | tail -n 1

# Example output: inkless-4.1.1-0.33
# This shows version 0.33 is latest
```

**To find latest for specific Kafka version:**
```bash
# Find latest Inkless version for your Kafka version
git tag | grep "inkless-4.1" | sort -V | tail -n 1

# Example output: inkless-4.1.1-0.33
```

**To list all versions for a Kafka version:**
```bash
# See all Inkless iterations on Kafka 4.1.x
git tag | grep "inkless-4.1"
```

### Version Comparison

**Understanding versions:**
```
inkless-4.0.0-0.32  vs  inkless-4.1.1-0.32
    ↑              ↑         ↑              ↑
    Kafka 4.0.0    Inkless   Kafka 4.1.1    Same Inkless!
                   0.32                     0.32

inkless-4.1.1-0.32  vs  inkless-4.1.1-0.33
    ↑              ↑         ↑              ↑
    Same Kafka     Older     Same Kafka     Newer
    4.1.1          Inkless   4.1.1          Inkless
```

**Rules:**
- Higher Inkless version (`0.33` > `0.32`) = newer Inkless features
- Higher Kafka version (`4.1.1` > `4.0.0`) = newer Kafka base
- Same Inkless version across Kafka versions = same Inkless code

---

## Migration from Current Versioning

This repository historically used per-branch `rc` numbers (e.g. `inkless-4.0.0-rc32` vs `inkless-4.1.1-rc1`). Going forward, Inkless releases are identified by `inkless-release-<inkless-version>` tags on `main` and use a global Inkless iteration number so versions are comparable across Kafka branches.

**Migration approach (tag mapping):**

```bash
inkless-4.0.0-rc32  →  inkless-4.0.0-0.32
inkless-4.1.1-rc1   →  inkless-4.1.1-0.32

# Next release (start from 0.33)
inkless-release-0.33
inkless-4.0.0-0.33
inkless-4.1.1-0.33
```

This makes it clear that `inkless-4.0.0-0.32` and `inkless-4.1.1-0.32` contain the same Inkless commits.

---

## FAQ

### Q: Why global Inkless version instead of per-branch?

**A:** So users can see when versions are aligned:
- `inkless-4.0.0-0.33` and `inkless-4.1.1-0.33` have the same Inkless code
- Makes it clear which versions are compatible feature-wise
- Easier to track what Inkless improvements are in each release

### Q: What if cherry-picks differ between branches?

**A:** Use different Inkless version numbers:
- `inkless-4.0.0-0.33` - includes feature A
- `inkless-4.1.1-0.34` - includes feature A + feature B (couldn't backport)

But try to keep them aligned when possible.

### Q: When do we update Kafka version in the tag?

**A:** When merging Kafka patch releases:
- Merge Kafka tag `4.1.1` → update tag to `inkless-4.1.1-0.33`
- Merge Kafka tag `4.1.2` → update tag to `inkless-4.1.2-0.33`

### Q: Do we ever do a "stable" release without version?

**A:** No. Every release has a version number. The version itself shows progress and alignment. There's no special "stable" marketing because:
- Inkless is an open development fork
- Focus is on progress, not production marketing
- Each release on a branch represents stable progress

### Q: How do we communicate which version users should use?

**A:** Through documentation:
- README.md points to latest Inkless version
- Release notes describe what's in each version
- Users can query git tags to find versions
- Users choose based on Kafka compatibility needs

---

## Summary

**Versioning format:**
- `inkless-release-<inkless-version>`
- `inkless-<kafka-version>-<inkless-version>`

**Key principles:**
1. Kafka version tracks upstream exactly
2. Inkless iteration is global across branches
3. Same Inkless iteration = same Inkless commits
4. Manual cherry-picking to release branches
5. Merge commits for Kafka updates
6. Focus on progress and alignment, not production marketing

**Benefits:**
- Clear Kafka compatibility
- Visible alignment across versions
- Simple comparison and selection
- Supports Aiven's development workflow
- Baseline for commercial product
- Not intended as long-term fork, but may evolve depending on upstream process
