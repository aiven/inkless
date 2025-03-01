#!/usr/bin/env bash

set -euo pipefail
shopt -sq failglob

filter_script="./filter.py"

# This script is very makefile-esque, only rebuilding filtered files when the measured files changed.
# If someone figures out a way to do this directly in the Makefile concisely, go ahead :)
for file in measured/*/*/*/*/*.parquet;
do
  dest="filtered/$file"
  if [ ! "$dest" -nt "$file" ] || [ "$filter_script" -nt "$dest" ]; then
    mkdir -p "${dest%/*}"
    echo "$filter_script \"$file\" \"$dest\""
    "$filter_script" "$file" "$dest"
  fi
done