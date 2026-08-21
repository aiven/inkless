#!/usr/bin/env bash
# Copyright (c) 2026 Aiven, Helsinki, Finland. https://aiven.io/
#
# Checks how the inkless release tag baked into kafka-version.properties resolves.
#
# Three regressions this guards against:
#   1. a source tree without .git must resolve to "unknown", not fail the build
#      (release packaging builds from a tarball, where git describe exits 128);
#   2. -PinklessTag must be honoured verbatim (it lands in the project's `ext`
#      namespace, which the ext block then shadows with the provider itself);
#   3. that same override must resolve at every subproject's createVersionFile
#      task, not only at root's printInklessTag (Gradle seeds -PinklessTag as an
#      extra property on every project, so a fix that only decouples root's own
#      read can still leave subprojects reading the raw CLI string instead of
#      the resolved provider).
#
# Usage: .github/scripts/test-inkless-tag.sh
# Run from the repository root of a git checkout.

set -euo pipefail

REPO_ROOT=$(git rev-parse --show-toplevel)
cd "$REPO_ROOT"

EXPORT_DIR=$(mktemp -d)
trap 'rm -rf "$EXPORT_DIR"' EXIT

failures=0

# Resolves the tag by running the real build logic. Extra args are passed to gradle.
resolve_tag() {
  local dir=$1
  shift
  local log
  log=$(mktemp)
  if ! (cd "$dir" && ./gradlew --console=plain --no-daemon "$@" printInklessTag) >"$log" 2>&1; then
    echo "gradle failed in $dir:" >&2
    cat "$log" >&2
    rm -f "$log"
    return 0
  fi
  sed -n 's/^INKLESS_TAG=//p' "$log" | tail -n1
  rm -f "$log"
}

# Resolves the tag baked into a subproject's kafka-version.properties by its own
# createVersionFile task, the call site that actually broke (root's printInklessTag
# never exercised it). Extra args are passed to gradle.
resolve_tag_via_create_version_file() {
  local dir=$1
  shift
  local log
  log=$(mktemp)
  if ! (cd "$dir" && ./gradlew --console=plain --no-daemon "$@" -x generateJooqClasses \
      :tools:tools-api:createVersionFile) >"$log" 2>&1; then
    echo "gradle failed in $dir:" >&2
    cat "$log" >&2
    rm -f "$log"
    return 0
  fi
  rm -f "$log"
  sed -n 's/^inklessTag=//p' "$dir/tools/tools-api/build/kafka/kafka-version.properties"
}

expect_eq() {
  local name=$1 expected=$2 actual=$3
  if [[ "$actual" == "$expected" ]]; then
    echo "ok   - $name (got '$actual')"
  else
    echo "FAIL - $name: expected '$expected', got '$actual'"
    failures=$((failures + 1))
  fi
}

expect_resolved() {
  local name=$1 actual=$2
  if [[ -n "$actual" && "$actual" != *CIRCULAR* ]]; then
    echo "ok   - $name (got '$actual')"
  else
    echo "FAIL - $name: expected a resolved tag, got '$actual'"
    failures=$((failures + 1))
  fi
}

echo "== git-less source tree (release packaging) =="
# Copy the tracked working tree (not `git archive HEAD`, so uncommitted changes to
# the resolution logic are exercised too), leaving out .git.
git ls-files -z | rsync -a --files-from=- --from0 ./ "$EXPORT_DIR"
expect_eq "no .git and no property falls back to unknown" \
  "unknown" "$(resolve_tag "$EXPORT_DIR")"
expect_eq "no .git honours -PinklessTag" \
  "inkless-release-9.99" "$(resolve_tag "$EXPORT_DIR" -PinklessTag=inkless-release-9.99)"
expect_eq "no .git honors -PinklessTag at a subproject's createVersionFile" \
  "inkless-release-9.99" "$(resolve_tag_via_create_version_file "$EXPORT_DIR" -PinklessTag=inkless-release-9.99)"

echo "== git checkout =="
expect_resolved "git checkout resolves from git describe" \
  "$(resolve_tag "$REPO_ROOT")"
expect_eq "-PinklessTag overrides git describe" \
  "inkless-release-9.99" "$(resolve_tag "$REPO_ROOT" -PinklessTag=inkless-release-9.99)"

if ((failures > 0)); then
  echo "$failures check(s) failed"
  exit 1
fi
echo "all checks passed"
