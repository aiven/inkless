#!/usr/bin/env bash
#
# sync-status.sh - Show sync status for inkless branches
#
# Reports how far behind each branch is from upstream Apache Kafka.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# Default values
BRANCH=""
ALL_BRANCHES=false
VERBOSE=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] [branch]

Show sync status for inkless branches relative to upstream Apache Kafka.

Arguments:
  branch            Specific branch to check (e.g., main, inkless-4.0)
                    If not specified, shows status for main branch

Options:
  --all             Show status for all inkless branches
  --verbose         Show detailed commit information
  -h, --help        Show this help

Examples:
  # Check main branch sync status
  $(basename "$0")
  $(basename "$0") main

  # Check specific release branch
  $(basename "$0") inkless-4.0

  # Check all branches
  $(basename "$0") --all
EOF
}

# Parse arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --all)
                ALL_BRANCHES=true
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
                BRANCH="$1"
                shift
                ;;
        esac
    done
}

# Show status for main branch
show_main_status() {
    local branch="main"
    local remote
    remote=$(get_upstream_remote)
    local upstream_target="$remote/trunk"

    echo "## Main Branch Sync Status"
    echo ""

    # Check if branch exists
    if ! git show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        warn "Branch 'origin/$branch' not found"
        return
    fi

    # Get current version
    local current_version
    current_version=$(get_current_version "origin/$branch")
    echo "Current version: $current_version"

    # Get merge base with upstream
    local merge_base
    merge_base=$(git merge-base "origin/$branch" "$upstream_target" 2>/dev/null || echo "")

    if [[ -z "$merge_base" ]]; then
        warn "Could not find merge base with upstream"
        return
    fi

    # Show merge base info (commit hash and date)
    local base_hash base_date
    base_hash=$(git rev-parse --short "$merge_base")
    base_date=$(git log -1 --format="%ci" "$merge_base" | cut -d' ' -f1)
    echo "Last upstream sync: $base_hash ($base_date)"

    # Count commits behind
    local commits_behind
    commits_behind=$(git rev-list --count "$merge_base..$upstream_target" 2>/dev/null || echo "0")
    echo "Commits behind upstream: $commits_behind"

    # Get upstream version (from gradle.properties at HEAD)
    local upstream_version
    upstream_version=$(git show "$upstream_target:gradle.properties" 2>/dev/null | grep "^version=" | cut -d= -f2 || echo "unknown")
    echo "Upstream version: $upstream_version"

    if [[ "$commits_behind" -gt 0 ]]; then
        echo ""
        echo "Status: ⚠️  Behind upstream by $commits_behind commits"

        if [[ "$VERBOSE" == "true" ]]; then
            echo ""
            echo "Recent upstream commits not yet merged:"
            git log -n 10 --oneline "$merge_base..$upstream_target"
            local total
            total=$(git rev-list --count "$merge_base..$upstream_target")
            if [[ "$total" -gt 10 ]]; then
                echo "... and $((total - 10)) more commits"
            fi
        fi
    else
        echo ""
        echo "Status: ✅ Up to date with upstream"
    fi
}

# Show status for release branch
show_release_status() {
    local branch="$1"

    echo "## Release Branch: $branch"
    echo ""

    # Check if branch exists
    if ! git show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        warn "Branch 'origin/$branch' not found"
        return
    fi

    # Extract version from branch name
    local version
    version=$(get_upstream_branch "$branch")

    # Get current version
    local current_version
    current_version=$(get_current_version "origin/$branch")
    local base_version
    base_version=$(get_base_version "$current_version")
    echo "Current version: $current_version (base: $base_version)"

    # Get available tags
    local tags
    tags=$(list_release_tags "$version")

    if [[ -z "$tags" ]]; then
        warn "No upstream release tags found for ${version}.x"
        return
    fi

    local latest_tag
    latest_tag=$(echo "$tags" | head -1)
    echo "Latest upstream tag: $latest_tag"

    # Check if base_version is a non-standard version (e.g., from --to-head sync)
    if [[ "$base_version" == *"-HEAD"* ]] || [[ "$base_version" == *".x-"* ]]; then
        echo ""
        echo "Status: ℹ️  Synced to upstream HEAD (version: $base_version)"
        echo "         Cannot compare to release tags"
        return
    fi

    # Check if we're behind
    if version_less_than "$base_version" "$latest_tag"; then
        local commits_to_sync
        commits_to_sync=$(count_commits "$base_version" "$latest_tag")
        echo "Commits available: $commits_to_sync"
        echo ""
        echo "Status: ⚠️  Update available ($base_version → $latest_tag)"

        if [[ "$VERBOSE" == "true" ]]; then
            echo ""
            echo "Available tags:"
            echo "$tags" | while read -r tag; do
                if version_less_than "$base_version" "$tag"; then
                    echo "  - $tag (available)"
                else
                    echo "  - $tag (current or older)"
                fi
            done
        fi
    else
        echo ""
        echo "Status: ✅ Up to date with latest release ($latest_tag)"
    fi
}

# List all inkless release branches (inkless-X.Y pattern only)
list_inkless_branches() {
    git branch -r | grep -E "origin/inkless-[0-9]+\.[0-9]+$" | sed 's|origin/||' | tr -d ' '
}

# Main execution
main() {
    parse_args "$@"

    require_git_repo
    fetch_upstream
    git fetch origin --prune

    echo "# Inkless Sync Status"
    echo ""
    echo "Generated: $(date)"
    echo ""

    if [[ "$ALL_BRANCHES" == "true" ]]; then
        # Show main status
        show_main_status
        echo ""
        echo "---"
        echo ""

        # Show all release branches
        while IFS= read -r branch; do
            show_release_status "$branch"
            echo ""
            echo "---"
            echo ""
        done < <(list_inkless_branches)
    elif [[ -n "$BRANCH" ]]; then
        if [[ "$BRANCH" == "main" ]]; then
            show_main_status
        elif [[ "$BRANCH" =~ ^inkless- ]]; then
            show_release_status "$BRANCH"
        else
            error "Unknown branch type: $BRANCH"
            exit 1
        fi
    else
        # Default: show main status
        show_main_status
    fi
}

main "$@"
