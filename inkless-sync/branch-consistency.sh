#!/usr/bin/env bash
#
# branch-consistency.sh - Check if inkless commits from main are in release branches
#
# Identifies inkless-specific commits on main that should be cherry-picked
# to release branches.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# Default values
RELEASE_BRANCH=""
VERBOSE=false
SHOW_MISSING=false
SHOW_ALL=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] <release-branch>

Check if inkless-specific commits from main are present in a release branch.

Arguments:
  release-branch    The release branch to check (e.g., inkless-4.0)

Options:
  --missing         Show only missing commits (not yet cherry-picked)
  --all             Show ALL missing commits (including old ones)
                    By default, only shows commits newer than last cherry-picked
  --verbose         Show detailed commit information
  -h, --help        Show this help

Examples:
  # Check inkless-4.0 consistency with main
  $(basename "$0") inkless-4.0

  # Show only missing commits
  $(basename "$0") inkless-4.0 --missing

  # Verbose output with commit details
  $(basename "$0") inkless-4.0 --verbose
EOF
}

# Parse arguments
parse_args() {
    local positional=()

    while [[ $# -gt 0 ]]; do
        case $1 in
            --missing)
                SHOW_MISSING=true
                shift
                ;;
            --all)
                SHOW_ALL=true
                shift
                ;;
            --verbose)
                VERBOSE=true
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
}

# Check if commit is inkless-specific (not a sync/merge commit)
# Only considers commits that:
# 1. Have a PR number (squash-merged PRs)
# 2. Mention inkless/diskless
# 3. Are not merge or sync commits
is_inkless_commit() {
    local commit_msg="$1"

    # MUST have a PR number - this filters out merge conflict resolution commits
    # PR merges have pattern (#XXX)
    if [[ ! "$commit_msg" =~ \(#[0-9]+\) ]]; then
        return 1
    fi

    # Exclude merge commits
    if [[ "$commit_msg" =~ ^[Mm]erge ]]; then
        return 1
    fi

    # Exclude sync-specific commits (these are main-trunk sync related)
    if [[ "$commit_msg" =~ ^(sync\(|fix\(sync\)|docs\(sync\)|refactor\(sync\)) ]]; then
        return 1
    fi

    # Include commits that mention inkless or diskless
    if [[ "$commit_msg" =~ (inkless|diskless|Inkless|Diskless) ]]; then
        return 0
    fi

    # Include commits with inkless-related prefixes
    if [[ "$commit_msg" =~ ^(feat|fix|refactor|chore|docs|test)\((inkless|diskless|storage:inkless|metadata:diskless) ]]; then
        return 0
    fi

    return 1
}

# Get the point where release branch diverged from main
get_branch_diverge_point() {
    local release_branch="$1"
    git merge-base "origin/main" "origin/$release_branch" 2>/dev/null
}

# Check if a commit exists in release branch using PR number
commit_in_release_by_pr() {
    local commit_msg="$1"
    local release_branch="$2"

    # Extract PR number if present - pattern (#XXX)
    if [[ "$commit_msg" =~ \(#([0-9]+)\) ]]; then
        local pr_num="${BASH_REMATCH[1]}"
        # Search for exact PR pattern "(#XXX)" in release branch
        git log --oneline --fixed-strings "origin/$release_branch" --grep="(#${pr_num})" 2>/dev/null | head -1
    fi
}

# Main consistency check
check_consistency() {
    local release_branch="$1"

    echo "# Branch Consistency Check"
    echo ""
    echo "Comparing: main → $release_branch"
    echo "Generated: $(date)"
    echo ""

    # Check if branches exist
    if ! git show-ref --verify --quiet "refs/remotes/origin/main"; then
        error "Branch 'origin/main' not found"
        exit 1
    fi

    if ! git show-ref --verify --quiet "refs/remotes/origin/$release_branch"; then
        error "Branch 'origin/$release_branch' not found"
        exit 1
    fi

    # Get diverge point
    local diverge_point
    diverge_point=$(get_branch_diverge_point "$release_branch")

    if [[ -z "$diverge_point" ]]; then
        error "Could not find divergence point between main and $release_branch"
        exit 1
    fi

    local diverge_desc
    diverge_desc=$(git log --oneline -1 "$diverge_point")
    echo "Branch diverged at: $diverge_desc"
    echo ""

    # First, get all inkless commits on main since divergence
    info "Scanning main branch for inkless commits..."

    local inkless_commits_file
    inkless_commits_file=$(mktemp)
    # Use RETURN trap for function-scoped cleanup (doesn't persist after function returns)
    trap "rm -f '$inkless_commits_file'" RETURN

    while IFS= read -r line; do
        local msg="${line#* }"
        if is_inkless_commit "$msg"; then
            echo "$line" >> "$inkless_commits_file"
        fi
    done < <(git log --oneline --no-merges "$diverge_point..origin/main")

    local inkless_count
    inkless_count=$(wc -l < "$inkless_commits_file" | tr -d ' ')
    echo "Found $inkless_count inkless-specific commits on main"
    echo ""

    if [[ "$inkless_count" -eq 0 ]]; then
        echo "Status: ✅ No inkless commits to check"
        return
    fi

    # Now check each inkless commit against release branch
    info "Checking against $release_branch..."

    local found_count=0
    local missing_count=0
    local missing_list=()
    local missing_indices=()
    local found_list=()
    local first_found_index=-1  # Index of the newest cherry-picked commit
    local commit_index=0

    # Commits are in reverse chronological order (newest first)
    while IFS= read -r line; do
        local hash="${line%% *}"
        local msg="${line#* }"

        # Check by PR number (most reliable for cherry-picks)
        local found
        found=$(commit_in_release_by_pr "$msg" "$release_branch" || echo "")

        if [[ -n "$found" ]]; then
            found_count=$((found_count + 1))
            found_list+=("$line → $found")
            # Track the first (newest) found commit
            if [[ $first_found_index -eq -1 ]]; then
                first_found_index=$commit_index
            fi
        else
            missing_count=$((missing_count + 1))
            missing_list+=("$line")
            missing_indices+=("$commit_index")
        fi
        commit_index=$((commit_index + 1))
    done < "$inkless_commits_file"

    # Separate missing commits into actionable (newer than last cherry-pick) and old
    local actionable_commits=()
    local old_commits=()
    local i=0
    for commit in "${missing_list[@]}"; do
        local idx="${missing_indices[$i]}"
        if [[ $first_found_index -eq -1 ]] || [[ $idx -lt $first_found_index ]]; then
            # No cherry-picked commits found, or this commit is newer
            actionable_commits+=("$commit")
        else
            old_commits+=("$commit")
        fi
        i=$((i + 1))
    done

    local actionable_count=${#actionable_commits[@]}
    local old_count=${#old_commits[@]}

    # Summary
    echo ""
    echo "## Summary"
    echo ""
    echo "| Metric | Count |"
    echo "|--------|-------|"
    echo "| Inkless commits on main | $inkless_count |"
    echo "| Found in $release_branch | $found_count |"
    echo "| Missing (actionable) | $actionable_count |"
    echo "| Missing (old) | $old_count |"
    echo ""

    if [[ $missing_count -eq 0 ]]; then
        echo "Status: ✅ All inkless commits are present in $release_branch"
    elif [[ $actionable_count -eq 0 ]]; then
        echo "Status: ✅ No new commits to cherry-pick ($old_count old commits skipped)"
    else
        echo "Status: ⚠️  $actionable_count commit(s) need to be cherry-picked to $release_branch"
    fi

    # Show missing commits
    if [[ $missing_count -gt 0 ]] && { [[ "$SHOW_MISSING" == "true" ]] || [[ "$VERBOSE" == "true" ]] || [[ "$SHOW_ALL" == "true" ]]; }; then
        # Show actionable commits (newer than last cherry-pick)
        if [[ $actionable_count -gt 0 ]]; then
            echo ""
            echo "## Commits to Cherry-pick"
            echo ""
            echo "These commits are newer than the last cherry-picked commit and should be cherry-picked:"
            echo ""
            for commit in "${actionable_commits[@]}"; do
                echo "- $commit"
            done

            echo ""
            echo "### Cherry-pick commands"
            echo ""
            echo '```bash'
            echo "git checkout $release_branch"
            # Cherry-pick in reverse order (oldest first) for cleaner history
            for ((j=${#actionable_commits[@]}-1; j>=0; j--)); do
                local commit="${actionable_commits[$j]}"
                local hash="${commit%% *}"
                echo "git cherry-pick $hash"
            done
            echo '```'
            echo ""
            echo "**Note**: Some commits may not apply cleanly due to code differences between branches."
            echo "Review each commit to determine if it's applicable to the release branch."
        fi

        # Show old commits only if --all is specified
        if [[ $old_count -gt 0 ]]; then
            if [[ "$SHOW_ALL" == "true" ]]; then
                echo ""
                echo "## Old Missing Commits (informational)"
                echo ""
                echo "These commits are older than the last cherry-picked commit and were likely"
                echo "intentionally skipped or not applicable to $release_branch:"
                echo ""
                for commit in "${old_commits[@]}"; do
                    echo "- $commit"
                done
            else
                echo ""
                echo "_$old_count old commit(s) not shown. Use --all to see them._"
            fi
        fi
    fi

    # Show found commits in verbose mode
    if [[ "$VERBOSE" == "true" ]] && [[ $found_count -gt 0 ]]; then
        echo ""
        echo "## Commits Already Present"
        echo ""
        for commit in "${found_list[@]}"; do
            echo "- $commit"
        done
    fi
}

# Main execution
main() {
    parse_args "$@"

    require_git_repo
    fetch_upstream
    git fetch origin --prune

    check_consistency "$RELEASE_BRANCH"
}

main "$@"
