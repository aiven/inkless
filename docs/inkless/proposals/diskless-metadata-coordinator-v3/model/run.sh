#!/usr/bin/env bash
# PROTOTYPE. Runs TLC on one config: ./run.sh <spec> <cfg> [seconds]
# Prints the result and, on a violation, the action sequence of the trace.
set -u
cd "$(dirname "$0")"
spec=$1 cfg=$2 secs=${3:-600}
out=out/${cfg%.cfg}.txt
mkdir -p "$(dirname "$out")"
timeout "$secs" java -XX:+UseParallelGC -Xmx6g -cp tla2tools.jar tlc2.TLC \
  -workers auto -deadlock -config "$cfg" "$spec.tla" > "$out" 2>&1
code=$?
[ $code -eq 124 ] && echo "TIMEOUT after ${secs}s"
grep -E "^Error:|is violated|No error|states generated|distinct states|depth of the complete" "$out"
grep -E "^State [0-9]+:" "$out" | sed -E 's/ line [0-9]+, col [0-9]+ to line [0-9]+, col [0-9]+ of module [A-Za-z]+//'
echo "full output: model/$out"
