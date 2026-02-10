#!/usr/bin/env bash
#
# release-sync.sh - Sync inkless release branches with upstream Apache Kafka releases
#
# This script helps sync inkless release branches (e.g., inkless-4.0, inkless-4.1)
# with upstream Apache Kafka release tags.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# Default values
DRY_RUN=false
LIST_TAGS=false
TO_HEAD=false
TARGET_TAG=""
INCLUDE_RC=false
WORKING_BRANCH=""
YES=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] <inkless-branch>

Sync an inkless release branch with upstream Apache Kafka releases.

Arguments:
  inkless-branch    The inkless release branch (e.g., inkless-4.0)

Options:
  --to-tag TAG      Sync to specific upstream tag (e.g., 4.0.1)
  --to-head         Sync to HEAD of upstream branch (for unreleased fixes)
  --list-tags       List available upstream tags for this release
  --include-rc      Include release candidates when listing tags
  --branch BRANCH   Working branch name (if different from inkless-branch)
  --dry-run         Preview conflicts without merging
  --yes, -y         Skip confirmation prompts (for non-interactive use)
  -h, --help        Show this help

Examples:
  # List available tags for 4.0 release series
  $(basename "$0") inkless-4.0 --list-tags

  # Preview conflicts when syncing to 4.0.1
  $(basename "$0") inkless-4.0 --to-tag 4.0.1 --dry-run

  # Sync to 4.0.1 release
  $(basename "$0") inkless-4.0 --to-tag 4.0.1

  # Sync on a different working branch
  $(basename "$0") inkless-4.0 --to-tag 4.0.1 --branch inkless-4.0-sync-4.0.1

  # Sync to HEAD of upstream 4.0 branch (for unreleased fixes)
  $(basename "$0") inkless-4.0 --to-head
EOF
}

# Parse arguments
parse_args() {
    local positional=()

    while [[ $# -gt 0 ]]; do
        case $1 in
            --to-tag)
                if [[ $# -lt 2 ]] || [[ "$2" == -* ]]; then
                    error "--to-tag requires a tag argument"
                    exit 1
                fi
                TARGET_TAG="$2"
                shift 2
                ;;
            --to-head)
                TO_HEAD=true
                shift
                ;;
            --list-tags)
                LIST_TAGS=true
                shift
                ;;
            --include-rc)
                INCLUDE_RC=true
                shift
                ;;
            --branch)
                if [[ $# -lt 2 ]] || [[ "$2" == -* ]]; then
                    error "--branch requires a branch name argument"
                    exit 1
                fi
                WORKING_BRANCH="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --yes|-y)
                YES=true
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
        error "Missing required argument: inkless-branch"
        usage
        exit 1
    fi

    INKLESS_BRANCH="${positional[0]}"
}

# Validate the inkless branch
validate_inkless_branch() {
    local branch="$1"

    # Check format
    if [[ ! "$branch" =~ ^inkless-[0-9]+\.[0-9]+$ ]]; then
        error "Invalid branch name format: $branch"
        error "Expected format: inkless-X.Y (e.g., inkless-4.0)"
        exit 1
    fi

    # Check if branch exists locally or remotely
    if ! git show-ref --verify --quiet "refs/heads/$branch" && \
       ! git show-ref --verify --quiet "refs/remotes/origin/$branch"; then
        error "Branch '$branch' does not exist locally or on origin"
        exit 1
    fi
}

# List tags for a release series
do_list_tags() {
    local inkless_branch="$1"
    local version
    version=$(get_upstream_branch "$inkless_branch")

    local current_version
    current_version=$(get_current_version "origin/$inkless_branch" 2>/dev/null || get_current_version "$inkless_branch")
    local base_version
    base_version=$(get_base_version "$current_version")

    echo ""
    echo "Release tags for ${version}.x series:"
    echo "======================================="
    echo ""

    local tags
    tags=$(list_release_tags "$version" "$INCLUDE_RC")

    if [[ -z "$tags" ]]; then
        warn "No release tags found for ${version}.x"
        return
    fi

    local latest_tag
    latest_tag=$(echo "$tags" | head -1)

    while IFS= read -r tag; do
        local tag_commit
        tag_commit=$(get_tag_commit "$tag")
        local short_commit="${tag_commit:0:10}"

        local marker=""
        if [[ "$tag" == "$base_version" ]]; then
            marker=" <- current base"
        elif [[ "$tag" == "$latest_tag" ]]; then
            marker=" <- latest"
        fi

        printf "  %-15s (%s)%s\n" "$tag" "$short_commit" "$marker"
    done <<< "$tags"

    echo ""
    echo "Current inkless version: $current_version"
    echo "Current base version:    $base_version"

    # Show commits available
    if [[ -n "$latest_tag" ]] && [[ "$latest_tag" != "$base_version" ]]; then
        # Only attempt to count commits if the base version is a valid git ref/commit.
        if git rev-parse --verify --quiet "$base_version^{commit}" >/dev/null 2>&1; then
            local commits
            commits=$(count_commits "$base_version" "$latest_tag")
            echo ""
            info "There are $commits commits available to sync (from $base_version to $latest_tag)"
        else
            echo ""
            warn "Base version '$base_version' is not a valid git ref; skipping commit count."
        fi
    fi
}

# Ensure working branch exists, creating from inkless branch if needed
ensure_working_branch() {
    local inkless_branch="$1"
    local working_branch="$2"

    # If working branch already exists locally, we're done
    if git show-ref --verify --quiet "refs/heads/$working_branch"; then
        return 0
    fi

    # If working branch is same as inkless branch, check it out from origin
    if [[ "$working_branch" == "$inkless_branch" ]]; then
        info "Creating local branch '$working_branch' from origin..."
        git checkout -b "$working_branch" "origin/$inkless_branch"
        return 0
    fi

    # Working branch is different - create from inkless branch
    info "Creating working branch '$working_branch' from '$inkless_branch'..."
    local base_ref
    if git show-ref --verify --quiet "refs/heads/$inkless_branch"; then
        base_ref="$inkless_branch"
    else
        base_ref="origin/$inkless_branch"
    fi
    git checkout -b "$working_branch" "$base_ref"
}

# Preview conflicts (dry run)
do_dry_run() {
    local inkless_branch="$1"
    local target="$2"
    local working_branch="$3"

    info "Dry run: previewing merge of $target into $working_branch"
    echo ""

    # Check if we can do a conflict preview (requires the branch to exist)
    local branch_exists=false
    if git show-ref --verify --quiet "refs/heads/$working_branch"; then
        branch_exists=true
    elif git show-ref --verify --quiet "refs/remotes/origin/$working_branch"; then
        branch_exists=true
    fi

    if [[ "$branch_exists" != "true" ]]; then
        # Branch doesn't exist - can't preview conflicts without creating it
        if [[ "$working_branch" == "$inkless_branch" ]]; then
            info "Branch '$working_branch' would be created from origin/$inkless_branch"
        else
            info "Working branch '$working_branch' would be created from '$inkless_branch'"
        fi
        echo ""
        warn "Cannot preview conflicts without creating the branch"
        info "Run without --dry-run to create the branch and perform the sync"
        return 0
    fi

    # Capture starting branch before any checkouts
    local starting_branch
    starting_branch=$(git branch --show-current)

    # Ensure we're on the working branch (but don't create it)
    local current_branch
    current_branch=$(git branch --show-current)
    if [[ "$current_branch" != "$working_branch" ]]; then
        info "Checking out $working_branch..."
        if git show-ref --verify --quiet "refs/heads/$working_branch"; then
            git checkout "$working_branch"
        elif git show-ref --verify --quiet "refs/remotes/origin/$working_branch"; then
            # Use a detached HEAD at the remote-tracking branch to avoid creating
            # a local branch during a dry run.
            git checkout --detach "origin/$working_branch"
        else
            warn "Branch '$working_branch' no longer exists locally or on origin; cannot preview merge."
            return 1
        fi
    fi

    # Attempt merge without committing
    info "Attempting merge (will be aborted)..."
    if git merge --no-commit --no-ff "$target" 2>/dev/null; then
        success "Merge would succeed cleanly!"
        git merge --abort 2>/dev/null || true
    else
        echo ""
        warn "Conflicts detected in the following files:"
        echo ""
        git diff --name-only --diff-filter=U 2>/dev/null || true
        echo ""

        # Show conflict details
        local conflict_count
        conflict_count=$(git diff --name-only --diff-filter=U 2>/dev/null | wc -l | tr -d ' ')
        info "Total files with conflicts: $conflict_count"

        git merge --abort 2>/dev/null || true
    fi

    # Restore starting branch if different from working branch
    if [[ "$starting_branch" != "$working_branch" ]] && [[ -n "$starting_branch" ]]; then
        git checkout "$starting_branch" 2>/dev/null || true
    fi
}

# Perform the actual sync
do_sync() {
    local inkless_branch="$1"
    local target="$2"
    local target_type="$3"  # "tag" or "head"
    local working_branch="$4"

    # Ensure working branch exists
    ensure_working_branch "$inkless_branch" "$working_branch"

    local current_version
    current_version=$(get_current_version "origin/$inkless_branch" 2>/dev/null || get_current_version "$working_branch")
    local base_version
    base_version=$(get_base_version "$current_version")

    # Determine target version for new inkless version
    local new_version
    if [[ "$target_type" == "tag" ]]; then
        new_version="${target}-inkless"
    else
        # For HEAD sync, keep current pattern but note it's ahead
        local version
        version=$(get_upstream_branch "$inkless_branch")
        new_version="${version}.x-HEAD-inkless"
    fi

    echo ""
    echo "Sync Summary"
    echo "============"
    echo "Release branch:  $inkless_branch"
    echo "Working branch:  $working_branch"
    echo "Current version: $current_version"
    echo "Target:          $target"
    echo "New version:     $new_version"
    echo ""

    # Validate target is newer
    if [[ "$target_type" == "tag" ]] && ! version_less_than "$base_version" "$target"; then
        error "Target tag '$target' is not newer than current base '$base_version'"
        exit 1
    fi

    # Count commits to merge
    local merge_base
    merge_base=$(get_merge_base "$working_branch" "$target")
    if [[ -z "$merge_base" ]]; then
        error "Could not find merge base between '$working_branch' and '$target'"
        error "The branches may have no common ancestor"
        exit 1
    fi
    local commits_to_merge
    commits_to_merge=$(count_commits "$merge_base" "$target")

    info "Commits to merge: $commits_to_merge"
    echo ""

    # Confirm before proceeding (skip if --yes was specified)
    if [[ "$YES" != "true" ]]; then
        read -p "Proceed with sync? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            info "Sync cancelled"
            exit 0
        fi
    fi

    # Checkout branch
    local current_branch
    current_branch=$(git branch --show-current)
    if [[ "$current_branch" != "$working_branch" ]]; then
        info "Checking out $working_branch..."
        git checkout "$working_branch"
    fi

    # Perform merge
    info "Merging $target..."
    if git merge --no-ff "$target" -m "Merge upstream $target into $working_branch"; then
        success "Merge completed successfully!"
    else
        echo ""
        warn "Merge has conflicts. Please resolve them manually."
        echo ""
        echo "Conflicted files:"
        git diff --name-only --diff-filter=U
        echo ""
        info "After resolving conflicts, run:"
        echo "  git add <resolved-files>"
        echo "  git commit"
        exit 1
    fi

    # Update version files
    info "Updating version to $new_version..."
    update_version_files "$new_version"

    success "Sync complete! New version: $new_version"
    echo ""
    info "Next steps:"
    echo "  1. Review the changes: git log --oneline -20"
    echo "  2. Run tests to verify: ./gradlew check"
    echo "  3. Push when ready: git push origin $working_branch"
}

# Update version files after sync
update_version_files() {
    local new_version="$1"

    # Update gradle.properties
    if [[ -f gradle.properties ]]; then
        sed -i.bak "s/^version=.*/version=$new_version/" gradle.properties
        rm -f gradle.properties.bak
        git add gradle.properties
        info "Updated gradle.properties"
    fi

    # Note: Python test version files (tests/kafkatest/__init__.py, tests/kafkatest/version.py)
    # are intentionally not updated automatically. They should be handled during conflict
    # resolution if upstream modifies them. Manual review recommended if test version issues arise.

    # Commit version updates only if there are staged changes
    if git diff --cached --quiet; then
        info "No version file changes to commit"
        return 0
    fi

    git commit -m "chore: bump version to $new_version after upstream sync"
}

# Main execution
main() {
    parse_args "$@"

    require_git_repo
    require_clean_worktree
    fetch_upstream
    git fetch origin --prune
    validate_inkless_branch "$INKLESS_BRANCH"

    # Use working branch if specified, otherwise use inkless branch
    if [[ -z "$WORKING_BRANCH" ]]; then
        WORKING_BRANCH="$INKLESS_BRANCH"
    fi

    if [[ "$LIST_TAGS" == "true" ]]; then
        do_list_tags "$INKLESS_BRANCH"
        exit 0
    fi

    # Determine target
    local target=""
    local target_type=""

    if [[ -n "$TARGET_TAG" ]]; then
        target="$TARGET_TAG"
        target_type="tag"

        # Verify tag exists
        if ! tag_exists "$target"; then
            error "Tag '$target' not found"
            info "Run with --list-tags to see available tags"
            exit 1
        fi
    elif [[ "$TO_HEAD" == "true" ]]; then
        local version
        version=$(get_upstream_branch "$INKLESS_BRANCH")
        local remote
        remote=$(get_upstream_remote)
        target="$remote/$version"
        target_type="head"

        # Verify branch exists
        if ! git show-ref --verify --quiet "refs/remotes/$target"; then
            error "Upstream branch '$target' not found"
            exit 1
        fi
    else
        error "Must specify either --to-tag or --to-head"
        usage
        exit 1
    fi

    if [[ "$DRY_RUN" == "true" ]]; then
        do_dry_run "$INKLESS_BRANCH" "$target" "$WORKING_BRANCH"
    else
        do_sync "$INKLESS_BRANCH" "$target" "$target_type" "$WORKING_BRANCH"
    fi
}

main "$@"
