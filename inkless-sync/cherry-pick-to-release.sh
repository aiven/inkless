#!/usr/bin/env bash
#
# cherry-pick-to-release.sh - Cherry-pick inkless commits from main to release branches
#
# Uses branch-consistency.sh to identify missing commits and cherry-picks them.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# Default values
RELEASE_BRANCH=""
DRY_RUN=false
INTERACTIVE=true
SPECIFIC_COMMITS=()

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] <release-branch> [commit...]

Cherry-pick inkless commits from main to a release branch.

Arguments:
  release-branch    The target release branch (e.g., inkless-4.0)
  commit...         Optional: specific commit hashes to cherry-pick
                    If not specified, uses branch-consistency.sh to find missing commits

Options:
  --dry-run         Show what would be cherry-picked without doing it
  --no-interactive  Don't prompt for confirmation before each commit
  -h, --help        Show this help

Examples:
  # Cherry-pick all missing inkless commits to inkless-4.0
  $(basename "$0") inkless-4.0

  # Preview what would be cherry-picked
  $(basename "$0") inkless-4.0 --dry-run

  # Cherry-pick specific commits
  $(basename "$0") inkless-4.0 abc123 def456

  # Non-interactive mode (for automation)
  $(basename "$0") inkless-4.0 --no-interactive
EOF
}

# Parse arguments
parse_args() {
    local positional=()

    while [[ $# -gt 0 ]]; do
        case $1 in
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --no-interactive)
                INTERACTIVE=false
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            -*)
                error "Unknown option: $1"
                usage
                exit 1
                ;;
            *)
                positional+=("$1")
                shift
                ;;
        esac
    done

    if [[ ${#positional[@]} -lt 1 ]]; then
        error "Missing required argument: release-branch"
        usage
        exit 1
    fi

    RELEASE_BRANCH="${positional[0]}"

    # Remaining positional args are specific commits
    if [[ ${#positional[@]} -gt 1 ]]; then
        SPECIFIC_COMMITS=("${positional[@]:1}")
    fi
}

# Get missing commits using branch-consistency.sh
get_missing_commits() {
    local release_branch="$1"

    # Run branch-consistency and extract missing commit hashes
    "$SCRIPT_DIR/branch-consistency.sh" "$release_branch" --missing 2>/dev/null | \
        grep -E "^- [a-f0-9]+" | \
        sed 's/^- //' | \
        cut -d' ' -f1
}

# Validate that a given hash/ref is a reachable commit object.
# Returns 0 if valid, non-zero otherwise.
validate_commit() {
    local commit="$1"

    # Use git cat-file to test for a valid commit without triggering set -e.
    if ! git cat-file -e "${commit}^{commit}" 2>/dev/null; then
        error "Invalid or unknown commit: $commit"
        return 1
    fi

    return 0
}

# Check if a commit is inkless-specific (should be cherry-picked)
is_applicable_commit() {
    local commit="$1"
    local msg

    # Ensure the commit exists and is a valid commit object before using git log.
    if ! validate_commit "$commit"; then
        # Treat invalid commits as not applicable so callers can skip them.
        return 1
    fi

    msg=$(git log --format="%s" -1 "$commit")

    # Skip merge conflict resolution commits (they may not apply cleanly)
    if [[ "$msg" =~ ^fix\(inkless\):\ (merge\ conflicts|resolve.*merge) ]]; then
        return 1
    fi

    return 0
}

# Version-carrying files whose content is OWNED by the release branch (set by
# release-sync.sh), never by a cherry-pick from main. A version-bump commit from
# main must still land on the branch (so it is "present" for branch-consistency),
# but it must NOT change the branch's output version.
#
# Note: a bump can apply either WITH a conflict (branch version differs from the
# commit's expected "from" line) or cleanly (they happen to match) -- in the clean
# case there is no conflict to resolve, yet the version is silently changed. So we
# do not rely on conflict resolution; instead we RESTORE these files to the
# branch's pre-pick content after the pick, covering both cases.
#
# Keep this list in sync with what release-sync.sh's update_version_files touches.
VERSION_OWNED_FILES=(
    "gradle.properties"
    "Makefile"
    "docker/examples/docker-compose-files/inkless/.env"
)

# Print the version-owned files (if any) that a commit modifies. Used by --dry-run
# to report which picks would trigger version restoration, and is purely
# informational (a static diff inspection, no working-tree changes).
commit_touches_version_owned_files() {
    local commit="$1"
    local changed
    changed=$(git show --name-only --format="" "$commit" 2>/dev/null)
    local vf
    for vf in "${VERSION_OWNED_FILES[@]}"; do
        grep -qxF "$vf" <<< "$changed" && echo "$vf"
    done
}

# Restore version-owned files to their pre-pick (HEAD-before-pick) content and, if
# that changed the tree, amend the just-created cherry-pick commit. Args: the
# pre-pick commit SHA (HEAD before the pick). Emits a note when it restores.
restore_version_owned_files() {
    local prepick="$1"
    local restored=false
    local f
    for f in "${VERSION_OWNED_FILES[@]}"; do
        # Only touch files that exist at the pre-pick HEAD.
        git cat-file -e "${prepick}:${f}" 2>/dev/null || continue
        # If the current file differs from the pre-pick version, restore it.
        if ! git diff --quiet "${prepick}" -- "$f" 2>/dev/null; then
            git checkout "$prepick" -- "$f"
            git add -- "$f"
            echo "  ↳ kept branch version (restored $f)"
            restored=true
        fi
    done
    if [[ "$restored" == "true" ]]; then
        # Fold the restore into the cherry-pick commit; keep its message.
        GIT_EDITOR=true git commit --amend --no-edit >/dev/null 2>&1
    fi
}

# If the in-progress cherry-pick's ONLY conflicts are version-owned files, resolve
# them to "ours", stage, and continue. Returns 0 if fully resolved, 1 otherwise
# (leaving the conflict in place for manual handling).
try_autoresolve_version_conflict() {
    local conflicted
    conflicted=$(git diff --name-only --diff-filter=U)

    # No conflicts recorded -> nothing to auto-resolve here.
    [[ -z "$conflicted" ]] && return 1

    # Every conflicted path must be version-owned; otherwise there is a real
    # conflict and we must not mask it.
    local f vf owned
    while IFS= read -r f; do
        owned=false
        for vf in "${VERSION_OWNED_FILES[@]}"; do
            [[ "$f" == "$vf" ]] && { owned=true; break; }
        done
        [[ "$owned" != "true" ]] && return 1
    done <<< "$conflicted"

    # All conflicts are version-owned: keep the branch's version (ours).
    while IFS= read -r f; do
        git checkout --ours -- "$f"
        git add -- "$f"
        echo "  ↳ kept branch version for version-owned file: $f"
    done <<< "$conflicted"

    # Continue the cherry-pick with the resolved tree (keep the original message).
    GIT_EDITOR=true git cherry-pick --continue >/dev/null 2>&1
}

# Cherry-pick a single commit with error handling
cherry_pick_commit() {
    local commit="$1"
    local msg

    # Validate commit before attempting to cherry-pick
    if ! validate_commit "$commit"; then
        return 1
    fi

    msg=$(git log --format="%s" -1 "$commit")

    echo ""
    echo "Cherry-picking: $commit"
    echo "  Message: $msg"

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "  [DRY RUN] Would cherry-pick this commit"
        return 0
    fi

    if [[ "$INTERACTIVE" == "true" ]]; then
        echo ""
        read -p "  Proceed? [Y/n/s(kip)/q(uit)] " -n 1 -r
        echo ""
        case "$REPLY" in
            n|N)
                echo "  Skipped by user"
                return 0
                ;;
            s|S)
                echo "  Skipped"
                return 0
                ;;
            q|Q)
                echo "  Quitting"
                exit 0
                ;;
        esac
    fi

    local prepick
    prepick=$(git rev-parse HEAD)

    if git cherry-pick "$commit"; then
        # Versions are branch-owned (set only by upstream sync / release-sync.sh).
        # A pick may change a version-owned file with NO conflict, so restore
        # unconditionally after a clean apply too.
        restore_version_owned_files "$prepick"
        echo "  ✅ Successfully cherry-picked"
        return 0
    else
        # Version-owned files (gradle.properties/Makefile/.env) are set by
        # release-sync, not by picks. If they are the ONLY conflict, keep the
        # branch's version and continue so the commit still lands.
        if try_autoresolve_version_conflict; then
            restore_version_owned_files "$prepick"
            echo "  ✅ Cherry-picked (version kept at branch value)"
            return 0
        fi

        echo "  ⚠️  Cherry-pick failed (conflict or other issue)"
        echo ""
        echo "  The conflicted state has been left in place so you can resolve it in"
        echo "  dependency order. Do NOT skip ahead: applying later commits before this"
        echo "  one is resolved can silently break ordering."
        echo ""
        echo "  To continue:"
        echo "    1. Resolve conflicts, then: git cherry-pick --continue"
        echo "    2. Re-run this script to cherry-pick the remaining commits."
        echo "       (auto-detect mode skips the resolved commit automatically; with"
        echo "        explicit hashes, pass only the ones not yet applied.)"
        echo ""
        echo "  Or, to bail out entirely: git cherry-pick --abort"
        echo ""
        # Signal the caller to STOP the run (return code 2), leaving the conflict
        # in place. Never auto-abort-and-continue: that reorders the remaining
        # picks relative to their dependencies.
        return 2
    fi
}

# Main execution
main() {
    parse_args "$@"

    require_git_repo
    # Dry-run is read-only: it inspects commits and prints a plan but never
    # touches the working tree, so it does not require a clean worktree.
    if [[ "$DRY_RUN" != "true" ]]; then
        require_clean_worktree
    fi
    fetch_upstream
    git fetch origin --prune

    echo "# Cherry-pick to Release Branch"
    echo ""
    echo "Target branch: $RELEASE_BRANCH"
    echo ""

    # Check if branch exists
    if ! git show-ref --verify --quiet "refs/remotes/origin/$RELEASE_BRANCH"; then
        error "Branch 'origin/$RELEASE_BRANCH' not found"
        exit 1
    fi

    # Get commits to cherry-pick
    local commits=()

    local reverse_order=true

    if [[ ${#SPECIFIC_COMMITS[@]} -gt 0 ]]; then
        commits=("${SPECIFIC_COMMITS[@]}")
        reverse_order=false  # User-provided order is respected as-is
        echo "Specific commits provided: ${#commits[@]}"
    else
        info "Finding missing commits using branch-consistency..."
        while IFS= read -r commit; do
            if [[ -n "$commit" ]]; then
                commits+=("$commit")
            fi
        done < <(get_missing_commits "$RELEASE_BRANCH")
        echo "Found ${#commits[@]} missing commit(s)"
    fi

    if [[ ${#commits[@]} -eq 0 ]]; then
        echo ""
        echo "Status: ✅ No commits to cherry-pick"
        return
    fi

    # Build ordered list for display and execution
    local ordered=()
    if [[ "$reverse_order" == "true" ]]; then
        for ((i=${#commits[@]}-1; i>=0; i--)); do
            ordered+=("${commits[$i]}")
        done
    else
        ordered=("${commits[@]}")
    fi

    echo ""
    if [[ "$reverse_order" == "true" ]]; then
        echo "## Commits to cherry-pick (oldest first)"
    else
        echo "## Commits to cherry-pick (provided order)"
    fi
    echo ""
    for commit in "${ordered[@]}"; do
        local msg
        msg=$(git log --format="%s" -1 "$commit" 2>/dev/null || echo "unknown")
        local applicable="✓"
        if ! is_applicable_commit "$commit"; then
            applicable="⚠ (merge resolution - may not apply)"
        fi
        echo "- $commit $msg $applicable"
        # Flag commits that touch version-owned files: on a real run their version
        # changes are restored to the branch value after the pick.
        local touched
        touched=$(commit_touches_version_owned_files "$commit")
        if [[ -n "$touched" ]]; then
            echo "    ↳ touches version-owned file(s); branch version will be kept: $(echo "$touched" | tr '\n' ' ')"
        fi
    done

    if [[ "$DRY_RUN" == "true" ]]; then
        echo ""
        echo "[DRY RUN] Would cherry-pick ${#commits[@]} commit(s)"
        echo "Run without --dry-run to execute"
        return
    fi

    echo ""

    # Checkout the release branch
    local current_branch
    current_branch=$(git rev-parse --abbrev-ref HEAD)

    if [[ "$current_branch" != "$RELEASE_BRANCH" ]]; then
        info "Checking out $RELEASE_BRANCH..."
        if git show-ref --verify --quiet "refs/heads/$RELEASE_BRANCH"; then
            git checkout "$RELEASE_BRANCH"
        else
            git checkout -b "$RELEASE_BRANCH" "origin/$RELEASE_BRANCH"
        fi
    fi

    # Cherry-pick each commit
    local success_count=0
    local skip_count=0
    local stopped_on=""
    local remaining=0

    for commit in "${ordered[@]}"; do
        if [[ -n "$stopped_on" ]]; then
            remaining=$((remaining + 1))
            continue
        fi

        if ! is_applicable_commit "$commit"; then
            echo ""
            echo "Skipping merge resolution commit: $commit"
            skip_count=$((skip_count + 1))
            continue
        fi

        cherry_pick_commit "$commit" && rc=0 || rc=$?
        if [[ $rc -eq 0 ]]; then
            success_count=$((success_count + 1))
        else
            # rc 2 (conflict) or any other failure: stop immediately, leaving the
            # conflict in place, so the remaining picks keep their dependency order.
            stopped_on="$commit"
        fi
    done

    echo ""
    echo "## Summary"
    echo ""
    echo "| Result | Count |"
    echo "|--------|-------|"
    echo "| Successfully cherry-picked | $success_count |"
    echo "| Skipped (merge resolution) | $skip_count |"
    echo "| Stopped at (conflict) | ${stopped_on:-none} |"
    echo "| Remaining (not attempted) | $remaining |"
    echo ""

    if [[ -n "$stopped_on" ]]; then
        echo "⛔ Stopped at $stopped_on due to a conflict. Resolve it (git cherry-pick"
        echo "   --continue), then re-run this script to apply the $remaining remaining commit(s)."
        return 1
    elif [[ $success_count -gt 0 ]]; then
        echo "✅ Cherry-pick complete"
        echo ""
        echo "Next steps:"
        echo "  1. Verify build: make build && make test"
        echo "  2. Push changes: git push origin $RELEASE_BRANCH"
    fi
}

main "$@"
