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

    if git cherry-pick "$commit"; then
        echo "  ✅ Successfully cherry-picked"
        return 0
    else
        echo "  ⚠️  Cherry-pick failed (conflict or other issue)"
        echo ""
        echo "  Options:"
        echo "    1. Resolve conflicts, then: git cherry-pick --continue"
        echo "    2. Abort this cherry-pick (you can re-run this script to continue): git cherry-pick --abort"
        echo ""

        if [[ "$INTERACTIVE" == "true" ]]; then
            read -p "  Abort and continue with next? [Y/n] " -n 1 -r
            echo ""
            if [[ ! "$REPLY" =~ ^[Nn]$ ]]; then
                git cherry-pick --abort 2>/dev/null || true
                return 1
            else
                echo "  Leaving in conflicted state for manual resolution"
                exit 1
            fi
        else
            git cherry-pick --abort 2>/dev/null || true
            return 1
        fi
    fi
}

# Main execution
main() {
    parse_args "$@"

    require_git_repo
    require_clean_worktree
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

    if [[ ${#SPECIFIC_COMMITS[@]} -gt 0 ]]; then
        commits=("${SPECIFIC_COMMITS[@]}")
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

    echo ""
    echo "## Commits to cherry-pick"
    echo ""
    for commit in "${commits[@]}"; do
        local msg
        msg=$(git log --format="%s" -1 "$commit" 2>/dev/null || echo "unknown")
        local applicable="✓"
        if ! is_applicable_commit "$commit"; then
            applicable="⚠ (merge resolution - may not apply)"
        fi
        echo "- $commit $msg $applicable"
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
    local fail_count=0

    for commit in "${commits[@]}"; do
        if ! is_applicable_commit "$commit"; then
            echo ""
            echo "Skipping merge resolution commit: $commit"
            skip_count=$((skip_count + 1))
            continue
        fi

        if cherry_pick_commit "$commit"; then
            success_count=$((success_count + 1))
        else
            fail_count=$((fail_count + 1))
        fi
    done

    echo ""
    echo "## Summary"
    echo ""
    echo "| Result | Count |"
    echo "|--------|-------|"
    echo "| Successfully cherry-picked | $success_count |"
    echo "| Skipped (merge resolution) | $skip_count |"
    echo "| Failed (conflicts) | $fail_count |"
    echo ""

    if [[ $fail_count -gt 0 ]]; then
        echo "⚠️  Some commits failed to cherry-pick. Manual resolution may be needed."
    elif [[ $success_count -gt 0 ]]; then
        echo "✅ Cherry-pick complete"
        echo ""
        echo "Next steps:"
        echo "  1. Verify build: make build && make test"
        echo "  2. Push changes: git push origin $RELEASE_BRANCH"
    fi
}

main "$@"
