#!/usr/bin/env bash
# Common utility functions for sync scripts

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
info() {
    echo -e "${BLUE}INFO:${NC} $*"
}

success() {
    echo -e "${GREEN}SUCCESS:${NC} $*"
}

warn() {
    echo -e "${YELLOW}WARNING:${NC} $*"
}

error() {
    echo -e "${RED}ERROR:${NC} $*" >&2
}

# Check if a command exists
require_command() {
    local cmd="$1"
    if ! command -v "$cmd" &> /dev/null; then
        error "Required command '$cmd' not found"
        exit 1
    fi
}

# Check if we're in a git repository
require_git_repo() {
    if ! git rev-parse --git-dir &> /dev/null; then
        error "Not in a git repository"
        exit 1
    fi
}

# Check if the working tree and index are clean
require_clean_worktree() {
    # Check for uncommitted changes (staged or unstaged)
    if ! git diff --quiet || ! git diff --cached --quiet; then
        error "Working tree has uncommitted changes"
        error "Please commit or stash your changes before proceeding"
        exit 1
    fi

    # Check for untracked files that might interfere
    local untracked
    untracked=$(git ls-files --others --exclude-standard 2>/dev/null | head -1)
    if [[ -n "$untracked" ]]; then
        warn "Working tree has untracked files (proceeding anyway)"
    fi
}

# Get the upstream remote name (apache or upstream)
get_upstream_remote() {
    if git remote | grep -q "^apache$"; then
        echo "apache"
    elif git remote | grep -q "^upstream$"; then
        echo "upstream"
    else
        error "No upstream remote found. Expected 'apache' or 'upstream'."
        exit 1
    fi
}

# Fetch from upstream remote
fetch_upstream() {
    local remote
    remote=$(get_upstream_remote)
    info "Fetching from $remote..."
    # Use --force to update tags that may have been moved (e.g., rc tags)
    if ! git fetch "$remote" --tags --force; then
        error "Failed to fetch from $remote. Check network connectivity and authentication."
        exit 1
    fi
}

# Get the upstream branch name from inkless branch
# inkless-4.0 -> 4.0
get_upstream_branch() {
    local inkless_branch="$1"
    echo "${inkless_branch#inkless-}"
}

# Get current version from gradle.properties
get_current_version() {
    local branch="${1:-HEAD}"
    git show "$branch:gradle.properties" 2>/dev/null | grep "^version=" | cut -d= -f2
}

# Extract base version (strip -SNAPSHOT and -inkless suffixes)
get_base_version() {
    local version="$1"
    # First remove a trailing -SNAPSHOT, then a trailing -inkless
    version="${version%-SNAPSHOT}"
    version="${version%-inkless}"
    echo "$version"
}

# Check if a tag exists
tag_exists() {
    local tag="$1"
    git rev-parse "$tag" &> /dev/null
}

# Get commit hash for a tag (dereference annotated tags)
get_tag_commit() {
    local tag="$1"
    git rev-parse "$tag^{commit}" 2>/dev/null
}

# Get merge base between two refs
get_merge_base() {
    local ref1="$1"
    local ref2="$2"
    git merge-base "$ref1" "$ref2" 2>/dev/null
}

# Compare versions (returns 0 if v1 < v2, 1 if v1 >= v2)
version_less_than() {
    local v1="$1"
    local v2="$2"

    # Remove -inkless suffix if present
    v1="${v1%-inkless}"
    v2="${v2%-inkless}"

    # Use sort -V for version comparison
    if [[ "$(printf '%s\n%s' "$v1" "$v2" | sort -V | head -n1)" == "$v1" ]] && [[ "$v1" != "$v2" ]]; then
        return 0  # v1 < v2
    else
        return 1  # v1 >= v2
    fi
}

# List release tags matching a version pattern (e.g., "4.0")
# Excludes inkless-specific tags and release candidates by default
list_release_tags() {
    local version="$1"
    local include_rc="${2:-false}"

    if [[ "$include_rc" == "true" ]]; then
        git tag -l "${version}.*" --sort=-v:refname | grep -v '\-inkless' || true
    else
        git tag -l "${version}.*" --sort=-v:refname | grep -v '\-rc' | grep -v '\-inkless' || true
    fi
}

# Get the latest release tag for a version
get_latest_tag() {
    local version="$1"
    list_release_tags "$version" | head -1
}

# Count commits between two refs
count_commits() {
    local from="$1"
    local to="$2"
    git rev-list --count "$from..$to" 2>/dev/null
}
