#!/usr/bin/env bash
#
# Inkless Upstream Sync Script
#
# This script automates syncing inkless:main with apache/kafka trunk
# It uses merge commits (per versioning strategy) and creates structured
# commits for different types of adaptations.
#
# Usage:
#   ./inkless-sync/main-sync.sh [OPTIONS]
#
# Options:
#   --target <ref>       Git ref to sync to (default: apache/trunk HEAD)
#   --before-version <v> Find last commit before version tag (e.g., "4.3")
#   --dry-run           Show what would be done without making changes
#   --help              Show this help message
#
# Environment:
#   APACHE_REMOTE       Name of apache kafka remote (default: apache)
#

set -euo pipefail

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"

CONFIG_DIR="${SCRIPT_DIR}/config"
PROTECTED_PATTERNS="${CONFIG_DIR}/protected-patterns.txt"
SYNC_DIR=".sync"

# Use shared upstream remote detection from lib/common.sh
APACHE_REMOTE="${APACHE_REMOTE:-$(get_upstream_remote)}"

# Default options
TARGET=""
BEFORE_VERSION=""
DRY_RUN=false

#------------------------------------------------------------------------------
# Utility Functions
#------------------------------------------------------------------------------

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

log_section() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN} $1${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
}

show_help() {
    head -30 "$0" | grep -E "^#" | sed 's/^# //' | sed 's/^#//'
    exit 0
}

#------------------------------------------------------------------------------
# Parse Arguments
#------------------------------------------------------------------------------

parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            --target)
                if [[ $# -lt 2 ]] || [[ "$2" == -* ]]; then
                    log_error "--target requires a git ref argument"
                    exit 1
                fi
                TARGET="$2"
                shift 2
                ;;
            --before-version)
                if [[ $# -lt 2 ]] || [[ "$2" == -* ]]; then
                    log_error "--before-version requires a version argument"
                    exit 1
                fi
                BEFORE_VERSION="$2"
                shift 2
                ;;
            --dry-run)
                DRY_RUN=true
                shift
                ;;
            --help|-h)
                show_help
                ;;
            *)
                log_error "Unknown option: $1"
                show_help
                ;;
        esac
    done
}

#------------------------------------------------------------------------------
# Helper: Load sync info safely (without executing as shell)
#------------------------------------------------------------------------------

load_sync_info() {
    local sync_file="${SYNC_DIR}/sync-info.txt"

    if [[ ! -f "$sync_file" ]]; then
        log_error "Sync info file not found: $sync_file"
        exit 1
    fi

    while IFS='=' read -r key value; do
        # Skip empty lines and comments
        [[ -z "$key" || "$key" == \#* ]] && continue
        case "$key" in
            TARGET_COMMIT) TARGET_COMMIT="$value" ;;
            MERGE_BASE) MERGE_BASE="$value" ;;
            BEFORE_VERSION) BEFORE_VERSION="$value" ;;
            DRY_RUN) DRY_RUN="$value" ;;
            TARGET_REF) TARGET_REF="$value" ;;
            SYNC_DATE) SYNC_DATE="$value" ;;
            SYNC_BRANCH) SYNC_BRANCH="$value" ;;
            *) ;; # Ignore unexpected keys
        esac
    done < "$sync_file"
}

#------------------------------------------------------------------------------
# Phase 1: Preparation
#------------------------------------------------------------------------------

phase_prepare() {
    log_section "Phase 1: Preparation"

    # Ensure we're in a git repository (uses shared function from lib/common.sh)
    require_git_repo

    # Ensure working tree is clean before any branching/merging
    require_clean_worktree

    # Ensure we're on main or a sync branch
    CURRENT_BRANCH=$(git branch --show-current)
    if [[ "$CURRENT_BRANCH" != "main" && ! "$CURRENT_BRANCH" =~ ^sync/ ]]; then
        log_error "Must be on 'main' or a 'sync/*' branch. Current: $CURRENT_BRANCH"
        exit 1
    fi

    # Fetch upstream (uses shared function from lib/common.sh)
    fetch_upstream

    # Determine sync target
    if [[ -n "$BEFORE_VERSION" ]]; then
        log_info "Finding last commit before version ${BEFORE_VERSION}..."
        # Find the commit just before the version tag
        if ! tag_exists "${BEFORE_VERSION}"; then
            log_error "Tag ${BEFORE_VERSION} not found"
            exit 1
        fi
        # Get parent of the version tag
        TARGET=$(git rev-parse "${BEFORE_VERSION}^")
        log_info "Target: ${TARGET} (parent of ${BEFORE_VERSION})"
    elif [[ -z "$TARGET" ]]; then
        TARGET="${APACHE_REMOTE}/trunk"
        log_info "Target: ${TARGET} (trunk HEAD)"
    fi

    # Resolve target to commit hash
    TARGET_COMMIT=$(git rev-parse "$TARGET")
    log_info "Target commit: ${TARGET_COMMIT}"

    # Generate inkless manifest (files we've modified)
    log_info "Generating inkless manifest..."
    MERGE_BASE=$(get_merge_base HEAD "$TARGET_COMMIT")
    if [[ -z "$MERGE_BASE" ]]; then
        log_error "Could not find merge base between HEAD and '$TARGET_COMMIT'"
        log_error "The branches may have no common ancestor or the target ref is not reachable"
        exit 1
    fi

    # Compute manifest count
    MANIFEST_COUNT=$(git diff "$MERGE_BASE"..HEAD --name-only | wc -l | tr -d ' ')
    log_info "Inkless manifest: ${MANIFEST_COUNT} files modified from merge base"

    # Identify protected files that might conflict
    log_info "Identifying protected inkless files..."
    PROTECTED_COUNT=0
    if [[ -f "$PROTECTED_PATTERNS" ]]; then
        local temp_patterns
        temp_patterns=$(grep -v "^#" "$PROTECTED_PATTERNS" | grep -v "^$" || true)
        if [[ -n "$temp_patterns" ]]; then
            # Disable globbing so patterns like "storage/inkless/**" are not
            # expanded by the shell before being passed to `find`.
            set -f
            while IFS= read -r pattern; do
                local count
                count=$(find . -path "./.git" -prune -o -path "./${pattern}" -print 2>/dev/null | wc -l | tr -d ' ')
                PROTECTED_COUNT=$((PROTECTED_COUNT + count))
            done <<< "$temp_patterns"
            # Re-enable globbing after processing patterns.
            set +f
        fi
        log_info "Protected files identified: ${PROTECTED_COUNT}"
    fi

    # Determine sync branch name
    SYNC_BRANCH="sync/upstream-$(date +%Y%m%d)"
    if [[ "$CURRENT_BRANCH" == "main" ]]; then
        # Check if branch already exists, add suffix if needed
        if git show-ref --verify --quiet "refs/heads/$SYNC_BRANCH"; then
            local suffix=2
            while git show-ref --verify --quiet "refs/heads/${SYNC_BRANCH}-${suffix}"; do
                suffix=$((suffix + 1))
            done
            SYNC_BRANCH="${SYNC_BRANCH}-${suffix}"
        fi
        log_info "Creating sync branch: ${SYNC_BRANCH}"
        if [[ "$DRY_RUN" == "true" ]]; then
            log_warn "[DRY-RUN] Would create branch: ${SYNC_BRANCH}"
        else
            git checkout -b "$SYNC_BRANCH"
        fi
    else
        SYNC_BRANCH="$CURRENT_BRANCH"
        log_info "Using existing sync branch: ${SYNC_BRANCH}"
    fi

    # Create sync metadata directory and files only if not dry-run
    if [[ "$DRY_RUN" != "true" ]]; then
        mkdir -p "$SYNC_DIR"

        # Write manifest files
        git diff "$MERGE_BASE"..HEAD --name-only > "${SYNC_DIR}/manifest-files.txt"
        git diff "$MERGE_BASE"..HEAD --stat > "${SYNC_DIR}/manifest-stats.txt"

        # Write protected patterns
        if [[ -f "$PROTECTED_PATTERNS" ]]; then
            grep -v "^#" "$PROTECTED_PATTERNS" | grep -v "^$" > "${SYNC_DIR}/protected-patterns-clean.txt" || true

            > "${SYNC_DIR}/protected-files.txt"
            # Disable globbing so patterns are passed to find, not expanded by shell
            set -f
            while IFS= read -r pattern; do
                find . -path "./.git" -prune -o -path "./${pattern}" -print 2>/dev/null >> "${SYNC_DIR}/protected-files.txt" || true
            done < "${SYNC_DIR}/protected-patterns-clean.txt"
            set +f
        fi

        # Save sync metadata
        cat > "${SYNC_DIR}/sync-info.txt" << EOF
SYNC_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
SYNC_BRANCH=${SYNC_BRANCH}
TARGET_REF=${TARGET}
TARGET_COMMIT=${TARGET_COMMIT}
MERGE_BASE=${MERGE_BASE}
BEFORE_VERSION=${BEFORE_VERSION}
EOF
    fi

    log_success "Preparation complete"
    echo ""
    echo "Sync Info:"
    echo "  SYNC_DATE=$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    echo "  SYNC_BRANCH=${SYNC_BRANCH}"
    echo "  TARGET_REF=${TARGET}"
    echo "  TARGET_COMMIT=${TARGET_COMMIT}"
    echo "  MERGE_BASE=${MERGE_BASE}"
    echo "  BEFORE_VERSION=${BEFORE_VERSION}"
}

#------------------------------------------------------------------------------
# Phase 2: Merge
#------------------------------------------------------------------------------

phase_merge() {
    log_section "Phase 2: Merge"

    # Load sync info safely
    load_sync_info

    log_info "Merging ${TARGET_COMMIT} into current branch..."

    if [[ "$DRY_RUN" == "true" ]]; then
        log_warn "[DRY-RUN] Would merge: ${TARGET_COMMIT}"
        # Show what would be merged
        COMMITS_TO_MERGE=$(count_commits "${MERGE_BASE}" "${TARGET_COMMIT}")
        log_info "Commits to merge: ${COMMITS_TO_MERGE}"
        return 0
    fi

    # Attempt merge
    MERGE_MSG="merge: apache/kafka trunk"
    if [[ -n "$BEFORE_VERSION" ]]; then
        MERGE_MSG="merge: apache/kafka trunk before ${BEFORE_VERSION}"
    fi

    if git merge "$TARGET_COMMIT" -m "$MERGE_MSG" --no-edit --no-ff; then
        log_success "Merge completed successfully (no conflicts)"
        echo "NO_CONFLICTS=true" >> "${SYNC_DIR}/sync-info.txt"
    else
        log_warn "Merge has conflicts - analyzing..."
        echo "NO_CONFLICTS=false" >> "${SYNC_DIR}/sync-info.txt"

        # Analyze conflicts
        analyze_conflicts
    fi
}

#------------------------------------------------------------------------------
# Conflict Analysis
#------------------------------------------------------------------------------

analyze_conflicts() {
    log_info "Analyzing conflicts..."

    # Get list of conflicted files
    git diff --name-only --diff-filter=U > "${SYNC_DIR}/conflicted-files.txt"

    CONFLICT_COUNT=$(wc -l < "${SYNC_DIR}/conflicted-files.txt" | tr -d ' ')
    log_info "Conflicted files: ${CONFLICT_COUNT}"

    # Categorize conflicts
    > "${SYNC_DIR}/conflicts-protected.txt"
    > "${SYNC_DIR}/conflicts-auto-resolvable.txt"
    > "${SYNC_DIR}/conflicts-manual.txt"

    while IFS= read -r file; do
        if is_protected_file "$file"; then
            echo "$file" >> "${SYNC_DIR}/conflicts-protected.txt"
        elif is_auto_resolvable "$file"; then
            echo "$file" >> "${SYNC_DIR}/conflicts-auto-resolvable.txt"
        else
            echo "$file" >> "${SYNC_DIR}/conflicts-manual.txt"
        fi
    done < "${SYNC_DIR}/conflicted-files.txt"

    PROTECTED=$(wc -l < "${SYNC_DIR}/conflicts-protected.txt" | tr -d ' ')
    AUTO=$(wc -l < "${SYNC_DIR}/conflicts-auto-resolvable.txt" | tr -d ' ')
    MANUAL=$(wc -l < "${SYNC_DIR}/conflicts-manual.txt" | tr -d ' ')

    echo ""
    echo "Conflict Summary:"
    echo "  Protected (use ours):     ${PROTECTED}"
    echo "  Auto-resolvable:          ${AUTO}"
    echo "  Manual resolution needed: ${MANUAL}"
    echo ""

    # Handle protected files - use ours
    if [[ "$PROTECTED" -gt 0 ]]; then
        log_info "Resolving protected files (using ours)..."
        while IFS= read -r file; do
            log_info "  Keeping ours: $file"
            git checkout --ours "$file"
            git add "$file"
        done < "${SYNC_DIR}/conflicts-protected.txt"
    fi

    # Handle auto-resolvable files
    if [[ "$AUTO" -gt 0 ]]; then
        log_info "Auto-resolving trivial conflicts..."
        while IFS= read -r file; do
            if auto_resolve_file "$file"; then
                log_info "  Auto-resolved: $file"
                git add "$file"
            else
                log_warn "  Could not auto-resolve: $file"
                echo "$file" >> "${SYNC_DIR}/conflicts-manual.txt"
            fi
        done < "${SYNC_DIR}/conflicts-auto-resolvable.txt"
    fi

    # Check if manual conflicts remain
    MANUAL_REMAINING=$(wc -l < "${SYNC_DIR}/conflicts-manual.txt" | tr -d ' ')
    if [[ "$MANUAL_REMAINING" -gt 0 ]]; then
        log_error "Manual conflict resolution required for ${MANUAL_REMAINING} files:"
        cat "${SYNC_DIR}/conflicts-manual.txt"
        echo ""
        log_error "Please resolve these conflicts manually, then run:"
        log_error "  git add <resolved-files>"
        log_error "  git commit"
        log_error "After committing, you can re-run ./inkless-sync/main-sync.sh to continue."
        exit 1
    fi

    # All conflicts resolved - complete the merge
    log_success "All conflicts resolved automatically"
    git commit -m "$MERGE_MSG"
}

is_protected_file() {
    local file="$1"

    if [[ ! -f "${SYNC_DIR}/protected-patterns-clean.txt" ]]; then
        return 1
    fi

    while IFS= read -r pattern; do
        # Simple glob matching
        if [[ "$file" == $pattern ]]; then
            return 0
        fi
        # Check if pattern is a directory prefix (e.g., "path/to/dir/")
        if [[ "$pattern" == */ && "$file" == "$pattern"* ]]; then
            return 0
        fi
        # Explicitly handle patterns that end with literal "/**" as recursive directory prefixes
        if [[ "$pattern" == *'/**' ]]; then
            local dir_prefix="${pattern%'/**'}/"
            if [[ "$file" == "$dir_prefix"* ]]; then
                return 0
            fi
            continue
        fi
        # Check with ** glob (other patterns containing **)
        if [[ "$pattern" == *"**"* ]]; then
            local prefix="${pattern%%\*\**}"
            if [[ "$file" == "$prefix"* ]]; then
                return 0
            fi
        fi
    done < "${SYNC_DIR}/protected-patterns-clean.txt"

    return 1
}

is_auto_resolvable() {
    local file="$1"

    # Check if conflict is only in imports or trivial changes
    # This is a simplified check - could be enhanced

    # For now, consider .java and .scala files with small conflicts as potentially auto-resolvable
    if [[ "$file" == *.java || "$file" == *.scala ]]; then
        # Count conflict markers
        local conflicts=$(grep -c "^<<<<<<< " "$file" 2>/dev/null || echo "0")
        if [[ "$conflicts" -le 3 ]]; then
            return 0
        fi
    fi

    return 1
}

auto_resolve_file() {
    local file="$1"

    # Try to auto-resolve using simple strategies
    # This is a basic implementation - could be enhanced

    # For import-only conflicts in Java/Scala, try to keep both
    if [[ "$file" == *.java || "$file" == *.scala ]]; then
        # Check if conflict is only in import section
        local in_imports=true
        while IFS= read -r line; do
            if [[ "$line" == "<<<<<<< "* ]]; then
                continue
            elif [[ "$line" == "=======" ]]; then
                continue
            elif [[ "$line" == ">>>>>>> "* ]]; then
                continue
            elif [[ "$line" == "import "* ]]; then
                continue
            elif [[ -n "$line" ]]; then
                in_imports=false
                break
            fi
        done < <(sed -n '/^<<<<<<< /,/^>>>>>>> /p' "$file")

        if [[ "$in_imports" == "true" ]]; then
            # Simple resolution: use theirs for imports (usually more up-to-date)
            git checkout --theirs "$file"
            return 0
        fi
    fi

    return 1
}

#------------------------------------------------------------------------------
# Phase 3: Adaptation
#------------------------------------------------------------------------------

phase_adapt() {
    log_section "Phase 3: Adaptation (Compile & Test)"

    # Try to compile
    log_info "Compiling..."
    if ./gradlew compileJava compileScala -x test -x generateJooqClasses 2>&1 | tee "${SYNC_DIR}/compile-output.txt"; then
        log_success "Compilation successful"
    else
        log_warn "Compilation failed - analyzing errors..."
        analyze_compile_errors
    fi
}

analyze_compile_errors() {
    log_info "Analyzing compilation errors..."

    # Extract errors from compile output
    grep -E "error:|Error:" "${SYNC_DIR}/compile-output.txt" > "${SYNC_DIR}/compile-errors.txt" || true

    ERROR_COUNT=$(wc -l < "${SYNC_DIR}/compile-errors.txt" | tr -d ' ')
    log_info "Compilation errors: ${ERROR_COUNT}"

    if [[ "$ERROR_COUNT" -gt 0 ]]; then
        echo ""
        echo "Compilation Errors:"
        cat "${SYNC_DIR}/compile-errors.txt"
        echo ""
        log_error "Please fix compilation errors and commit as:"
        log_error "  git commit -m 'sync(compile): fix compilation errors'"
    fi
}

#------------------------------------------------------------------------------
# Phase 4: Verification
#------------------------------------------------------------------------------

phase_verify() {
    log_section "Phase 4: Verification"

    log_info "Running inkless tests (using Makefile targets)..."

    # Run inkless tests using Makefile target
    if make test 2>&1 | tee "${SYNC_DIR}/test-output.txt"; then
        log_success "Inkless tests passed"
    else
        log_warn "Inkless tests failed - check ${SYNC_DIR}/test-output.txt"
    fi

    # Verify manifest (inkless features preserved)
    log_info "Verifying inkless features preserved..."
    verify_manifest

    # Generate sync report
    generate_report
}

verify_manifest() {
    local missing_files=0

    # Check that key inkless files still exist
    local key_files=(
        "storage/inkless/src/main/java/io/aiven/inkless/InklessWriter.java"
        "docs/inkless/README.md"
        "config/inkless/single-broker-0.properties"
        ".github/workflows/inkless.yml"
    )

    for file in "${key_files[@]}"; do
        if [[ ! -f "$file" ]]; then
            log_error "Missing key inkless file: $file"
            missing_files=$((missing_files + 1))
        fi
    done

    # Check version preserved
    if grep -q "inkless" gradle.properties; then
        log_success "Inkless version preserved in gradle.properties"
    else
        log_error "Inkless version NOT found in gradle.properties"
        missing_files=$((missing_files + 1))
    fi

    if [[ "$missing_files" -gt 0 ]]; then
        log_error "Manifest verification failed: ${missing_files} issues"
        return 1
    fi

    log_success "Manifest verification passed"
    return 0
}

#------------------------------------------------------------------------------
# Report Generation
#------------------------------------------------------------------------------

generate_report() {
    log_section "Generating Sync Report"

    # Load sync info safely
    load_sync_info

    cat > "${SYNC_DIR}/SYNC-REPORT.md" << EOF
# Upstream Sync Report

## Sync Information
- **Date:** ${SYNC_DATE}
- **Branch:** ${SYNC_BRANCH}
- **Target:** ${TARGET_REF}
- **Target Commit:** ${TARGET_COMMIT}
- **Before Version:** ${BEFORE_VERSION:-N/A}

## Conflict Resolution Summary
EOF

    if [[ -f "${SYNC_DIR}/conflicts-protected.txt" ]]; then
        echo "### Protected Files (used ours)" >> "${SYNC_DIR}/SYNC-REPORT.md"
        cat "${SYNC_DIR}/conflicts-protected.txt" >> "${SYNC_DIR}/SYNC-REPORT.md" 2>/dev/null || echo "None" >> "${SYNC_DIR}/SYNC-REPORT.md"
        echo "" >> "${SYNC_DIR}/SYNC-REPORT.md"
    fi

    if [[ -f "${SYNC_DIR}/conflicts-manual.txt" ]]; then
        echo "### Manual Resolutions Required" >> "${SYNC_DIR}/SYNC-REPORT.md"
        cat "${SYNC_DIR}/conflicts-manual.txt" >> "${SYNC_DIR}/SYNC-REPORT.md" 2>/dev/null || echo "None" >> "${SYNC_DIR}/SYNC-REPORT.md"
        echo "" >> "${SYNC_DIR}/SYNC-REPORT.md"
    fi

    cat >> "${SYNC_DIR}/SYNC-REPORT.md" << EOF

## Verification Results
- Compilation: $([ -f "${SYNC_DIR}/compile-errors.txt" ] && [ $(wc -l < "${SYNC_DIR}/compile-errors.txt") -gt 0 ] && echo "FAILED" || echo "PASSED")
- Inkless Tests: $([ -f "${SYNC_DIR}/test-output.txt" ] && grep -q "BUILD SUCCESSFUL" "${SYNC_DIR}/test-output.txt" && echo "PASSED" || echo "FAILED/NOT RUN")
- Manifest Check: $(verify_manifest 2>/dev/null && echo "PASSED" || echo "FAILED")

## Files Modified by Inkless
$(wc -l < "${SYNC_DIR}/manifest-files.txt" | tr -d ' ') files

## Next Steps
1. Review this report
2. Fix any failing tests
3. Create PR with this sync

---
*Generated by main-sync.sh*
EOF

    log_success "Report generated: ${SYNC_DIR}/SYNC-REPORT.md"
    echo ""
    cat "${SYNC_DIR}/SYNC-REPORT.md"
}

#------------------------------------------------------------------------------
# Main
#------------------------------------------------------------------------------

main() {
    parse_args "$@"

    log_section "Inkless Upstream Sync"

    if [[ "$DRY_RUN" == "true" ]]; then
        log_warn "DRY-RUN MODE - No changes will be made"
    fi

    phase_prepare

    # Short-circuit after preparation in dry-run mode
    if [[ "$DRY_RUN" == "true" ]]; then
        log_section "Dry Run Summary"
        COMMITS_TO_MERGE=$(count_commits "${MERGE_BASE}" "${TARGET_COMMIT}")
        log_info "Commits to merge: ${COMMITS_TO_MERGE}"
        log_info "Would create branch: ${SYNC_BRANCH}"
        log_info "Would merge: ${TARGET_COMMIT}"
        echo ""
        log_success "Dry run complete. Run without --dry-run to perform the sync."
        return 0
    fi

    phase_merge
    phase_adapt
    phase_verify

    log_section "Sync Complete"
    log_success "Sync completed. Review ${SYNC_DIR}/SYNC-REPORT.md"
    log_info "To create PR, run: gh pr create --title 'Upstream sync' --body-file ${SYNC_DIR}/SYNC-REPORT.md"
}

main "$@"
