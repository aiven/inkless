#!/usr/bin/env bash
#
# create-release-branch.sh - Create a new inkless release branch
#
# Creates a release branch from main when main is at the right version level.
# Main should be at X.Y.0-inkless-SNAPSHOT to create inkless-X.Y branch.
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

# Default values
VERSION=""
DRY_RUN=false
FORCE=false

usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] <version>

Create a new inkless release branch from main.

The script verifies that main is at the correct version level (X.Y.0-inkless-SNAPSHOT)
before creating the release branch. This ensures all inkless work is included.

Arguments:
  version           The release version (e.g., 4.2)

Options:
  --dry-run         Show what would be done without making changes
  --force           Create branch even if main version doesn't match
  -h, --help        Show this help

Examples:
  # Create inkless-4.2 branch (main must be at 4.2.0-inkless-SNAPSHOT)
  $(basename "$0") 4.2

  # Preview what would happen
  $(basename "$0") 4.2 --dry-run

Process:
  1. Verifies main is at {version}.0-inkless-SNAPSHOT (or later)
  2. Creates inkless-{version} branch from main (at the appropriate point)
  3. Version files are NOT modified (stays at SNAPSHOT)

After creation:
  - Push the branch: git push -u origin inkless-{version}
  - Sync with upstream release: release-sync.sh inkless-{version} --to-tag {version}.0
    (This updates the version to {version}.0-inkless)
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
            --force)
                FORCE=true
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
        error "Missing required argument: version"
        usage
        exit 1
    fi

    VERSION="${positional[0]}"
}

# Validate version format (X.Y)
validate_version() {
    local version="$1"
    if [[ ! "$version" =~ ^[0-9]+\.[0-9]+$ ]]; then
        error "Invalid version format: $version (expected X.Y, e.g., 4.2)"
        exit 1
    fi
}

# Get main's current version
get_main_version() {
    git show origin/main:gradle.properties 2>/dev/null | grep "^version=" | cut -d= -f2
}

# Extract major.minor from version string (e.g., "4.2.0-inkless-SNAPSHOT" -> "4.2")
get_major_minor() {
    local version="$1"
    echo "$version" | sed -E 's/^([0-9]+\.[0-9]+).*/\1/'
}

# Compare versions: returns 0 if v1 >= v2, 1 otherwise
version_gte() {
    local v1="$1"
    local v2="$2"
    # Use sort -V for version comparison
    local higher
    higher=$(printf '%s\n%s\n' "$v1" "$v2" | sort -V | tail -1)
    [[ "$higher" == "$v1" ]]
}

# Find the commit where main was at a specific version
find_version_commit() {
    local target_version="$1"
    local target_snapshot_pattern="version=${target_version}.0-inkless-SNAPSHOT"
    local target_release_pattern="version=${target_version}.0-inkless"

    # Prefer the SNAPSHOT version pattern (as used on main), fall back to release pattern
    local commit
    commit=$(git log --oneline origin/main -S "$target_snapshot_pattern" -- gradle.properties | head -1 | cut -d' ' -f1)
    local pattern="$target_snapshot_pattern"

    if [[ -z "${commit}" ]]; then
        commit=$(git log --oneline origin/main -S "$target_release_pattern" -- gradle.properties | head -1 | cut -d' ' -f1)
        pattern="$target_release_pattern"
    fi

    # Validate the commit actually contains the version (not a bump-away commit)
    # -S finds commits that add OR remove the string, so verify and use parent if needed
    if [[ -n "${commit}" ]]; then
        if ! git show "${commit}:gradle.properties" 2>/dev/null | grep -Fqx "${pattern}"; then
            # This commit removed the version, use its parent instead
            commit=$(git rev-parse "${commit}^" 2>/dev/null || echo "")
        fi
    fi

    echo "${commit}"
}


# Main execution
main() {
    parse_args "$@"
    validate_version "$VERSION"

    require_git_repo
    require_clean_worktree
    fetch_upstream
    git fetch origin --prune

    local branch_name="inkless-$VERSION"
    local expected_version="${VERSION}.0-inkless-SNAPSHOT"

    echo "# Create Release Branch"
    echo ""
    echo "Version: $VERSION"
    echo "Branch name: $branch_name"
    echo ""

    # Check if branch already exists
    if git show-ref --verify --quiet "refs/remotes/origin/$branch_name" 2>/dev/null; then
        error "Branch 'origin/$branch_name' already exists"
        exit 1
    fi

    if git show-ref --verify --quiet "refs/heads/$branch_name" 2>/dev/null; then
        error "Local branch '$branch_name' already exists"
        exit 1
    fi

    # Check main's version
    local main_version
    main_version=$(get_main_version)
    local main_major_minor
    main_major_minor=$(get_major_minor "$main_version")

    echo "Main branch version: $main_version"
    echo "Expected version:    $expected_version (or later)"
    echo ""

    local branch_point=""
    local branch_point_desc=""

    if [[ "$main_version" == "$expected_version" ]]; then
        # Exact match - branch from HEAD
        echo "✓ Main is at the exact version level"
        branch_point="origin/main"
        branch_point_desc=$(git log --oneline -1 origin/main)
    elif version_gte "$main_major_minor" "$VERSION"; then
        # Main is ahead - need to find the right point in history
        echo "Main is ahead of target version"
        info "Searching for commit where main was at ${VERSION}.0..."

        local version_commit
        version_commit=$(find_version_commit "$VERSION")

        if [[ -n "$version_commit" ]]; then
            branch_point="$version_commit"
            branch_point_desc=$(git log --oneline -1 "$version_commit")
            echo "Found: $branch_point_desc"
        else
            if [[ "$FORCE" == "true" ]]; then
                warn "Could not find version commit, using main HEAD due to --force"
                branch_point="origin/main"
                branch_point_desc=$(git log --oneline -1 origin/main)
            else
                error "Could not find commit where main was at ${VERSION}.0-inkless-SNAPSHOT"
                echo ""
                echo "This might happen if:"
                echo "  - Main was never at this version (skipped directly to a later version)"
                echo "  - The version commit is not in the searchable history"
                echo ""
                echo "Use --force to create from main HEAD anyway"
                exit 1
            fi
        fi
    else
        # Main is behind
        if [[ "$FORCE" == "true" ]]; then
            warn "Version mismatch ignored due to --force"
            branch_point="origin/main"
            branch_point_desc=$(git log --oneline -1 origin/main)
        else
            error "Main is behind the expected version level"
            echo ""
            echo "Main should be synced to upstream ${VERSION}.x before creating this branch."
            echo "Current main version: $main_version"
            echo "Expected:            $expected_version (or later)"
            echo ""
            echo "Options:"
            echo "  1. Sync main to upstream ${VERSION}.x first"
            echo "  2. Use --force to create anyway (not recommended)"
            exit 1
        fi
    fi

    echo ""
    echo "Branch point: $branch_point_desc"
    echo ""

    if [[ "$DRY_RUN" == "true" ]]; then
        echo "## Dry Run - Would execute:"
        echo ""
        echo "1. Create branch '$branch_name' from $branch_point"
        echo "   (Version will remain at $main_version until release-sync)"
        echo ""
        echo "To execute, run without --dry-run"
        return
    fi

    # Create branch (--no-track to avoid tracking main)
    info "Creating branch '$branch_name'..."
    git checkout --no-track -b "$branch_name" "$branch_point"

    # Note: Version files are NOT updated here - that happens during release-sync
    # when syncing with an actual upstream release tag

    echo ""
    echo "## Success"
    echo ""
    echo "Branch '$branch_name' created successfully from main."
    echo "Version remains at: $main_version"
    echo ""
    echo "Next steps:"
    echo "  1. Push the branch: git push -u origin $branch_name"
    echo "  2. Sync with upstream release: ./inkless-sync/release-sync.sh $branch_name --to-tag ${VERSION}.0"
    echo "     (This will update the version to ${VERSION}.0-inkless)"
    echo "  3. Verify build: make build && make test"
}

main "$@"
