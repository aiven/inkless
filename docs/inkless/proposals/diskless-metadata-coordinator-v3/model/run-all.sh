#!/usr/bin/env bash
# PROTOTYPE. Runs every pinned scenario and the full-bounds simulations of
# FencesFloors without supervision, then writes runs/<timestamp>/summary.md.
# The console output is also saved to runs/<timestamp>/run-all.log.
#
# Usage:   ./run-all.sh
# Options, as environment variables:
#   TIME_PER_RUN  seconds per exhaustive TLC run (default 3600)
#   SIM_MINUTES   minutes per simulation run (default 90)
#   JAVA_OPTS     JVM options (default -Xmx8g)
#   WORKERS       TLC worker and GC threads (default half the CPU cores)
#   ONLY_SIM      1 runs only the every-fix-on simulation (default 0)
#
# Worst case runtime: 23 exhaustive runs x TIME_PER_RUN + 2 x SIM_MINUTES,
# about 26 hours with the defaults. Runs that are expected to fail stop
# within seconds.
set -u
cd "$(dirname "$0")"

TIME_PER_RUN=${TIME_PER_RUN:-3600}
SIM_MINUTES=${SIM_MINUTES:-90}
JAVA_OPTS=${JAVA_OPTS:--Xmx8g}
WORKERS=${WORKERS:-$(( $(getconf _NPROCESSORS_ONLN) / 2 > 0 ? $(getconf _NPROCESSORS_ONLN) / 2 : 1 ))}
ONLY_SIM=${ONLY_SIM:-0}

stamp=$(date +%Y%m%d-%H%M%S)
dir=runs/$stamp
mkdir -p "$dir"
summary=$dir/summary.md
exec > >(tee -a "$dir/run-all.log") 2>&1

total=25; [ "$ONLY_SIM" = 1 ] && total=1
n=0
log() { echo "$(date +%H:%M:%S) $*"; }

log "FencesFloors run $stamp"
log "  output:       model/$dir"
log "  TIME_PER_RUN: ${TIME_PER_RUN}s per exhaustive run"
log "  SIM_MINUTES:  ${SIM_MINUTES} min per simulation"
log "  JAVA_OPTS:    $JAVA_OPTS"
log "  WORKERS:      $WORKERS of $(getconf _NPROCESSORS_ONLN) cores"
log "  ONLY_SIM:     $ONLY_SIM"
log "  java:         $(java -version 2>&1 | head -1)"
log "  worst case:   about $(( (23 * TIME_PER_RUN + 2 * SIM_MINUTES * 60) / 3600 )) h for $total runs"

cat > "$summary" <<EOF
# FencesFloors run $stamp

TIME_PER_RUN=${TIME_PER_RUN}s, SIM_MINUTES=${SIM_MINUTES}, JAVA_OPTS=$JAVA_OPTS, WORKERS=$WORKERS

Partitions: p1 is the tracking partition t, p2 is t + 1, p3 is a plain participant.
Expected: "pass" means every fix is on and no invariant may fail. "fail" means
the fix for the scenario's race is off, and TLC must find a violation. In the
lost-tail runs, the partition that lost its tail is exempt from NoDeleteReferenced,
because ticket 10 accepts release inversion and resurrection there. A violation
means a lost tail hit another partition, a path ticket 10 says doesn't exist.
Verdict: "ok" matches the expectation, "partial" is a pass run that timed out
without a violation, and "CHECK" needs a look.

| Run | Expected | Result | Verdict | States | Depth | Seconds | Trace |
| --- | --- | --- | --- | --- | --- | --- | --- |
EOF

# cfg <file> <Scenario letter or ""> <unused> <files> <leases> <epochs> <ff> <fl> <fb> <fpw> <fmf> <faults>
cfg() {
  local f=$1
  {
    echo "CONSTANTS"
    echo "  P = {p1, p2, p3}"
    echo "  R = 2"
    echo "  MaxSeq = $(($5 * 2))"
    echo "  MaxEpoch = $6"
    echo "  MaxFiles = $4"
    echo "  FixFinish = $7"
    echo "  FixLease = $8"
    echo "  FixBrokerWide = $9"
    echo "  FixPlannedWait = ${10}"
    echo "  FixMissedFirst = ${11}"
    echo "  MaxFaults = ${12}"
    echo "  MaxLate = 1"
    if [ -n "$2" ]; then
      echo "  Scenario = \"$2\""
      echo "  p1 = p1"
      echo "  p2 = p2"
      echo "  p3 = p3"
    fi
    echo "SPECIFICATION Spec"
    [ -n "$2" ] && echo "ACTION_CONSTRAINT ScenarioOK"
    echo "INVARIANTS NoDeleteReferenced NoCutLive NoLeak EndedKnown"
    echo "CHECK_DEADLOCK FALSE"
    echo "ALIAS View"
  } > "$f"
  log "    config: $(grep -E 'Fix|MaxFaults|MaxEpoch|MaxFiles|Scenario' "$f" | tr "\n" " " | tr -s " ")"
}

# tlc <name> <module> <expected> <seconds> [extra TLC flags...]
tlc() {
  local name=$1 module=$2 expected=$3 secs=$4
  shift 4
  local out=$dir/$name.txt trace=$dir/$name.trace
  local start end code result states depth verdict
  log "    TLC on $module.tla, limit ${secs}s, expected: $expected, log: $name.txt"
  start=$(date +%s)
  timeout "$secs" java -XX:+UseParallelGC -XX:ParallelGCThreads=$WORKERS $JAVA_OPTS -cp tla2tools.jar tlc2.TLC \
    -workers $WORKERS -deadlock -metadir "$dir/states-$name" "$@" \
    -config "$dir/$name.cfg" "$module.tla" 2>&1 \
    | tee "$out" | grep --line-buffered -E "^Progress" \
    | sed -E 's/^Progress\(([0-9]+)\) at [^:]+:[0-9]+:[0-9]+: /      depth \1: /'
  code=${PIPESTATUS[0]}
  end=$(date +%s)
  rm -rf "$dir/states-$name"
  if grep -q "is violated" "$out"; then
    result="violation: $(grep -o 'Invariant [A-Za-z]* is violated' "$out" | head -1 | cut -d' ' -f2)"
    grep -E "^State [0-9]+:" "$out" \
      | sed -E 's/ line [0-9]+, col [0-9]+ to line [0-9]+, col [0-9]+ of module [A-Za-z]+//' > "$trace"
  elif grep -q "No error has been found" "$out"; then
    result="no error (complete)"
    trace=""
  elif [ $code -eq 124 ]; then
    result="timeout, no error so far"
    trace=""
  else
    result="TLC error, see $name.txt"
    trace=""
  fi
  case "$expected:$result" in
    pass:no\ error*|fail:violation*) verdict=ok ;;
    pass:timeout*) verdict=partial ;;
    *) verdict=CHECK ;;
  esac
  states=$(grep -oE '[0-9,]+ distinct states found' "$out" | tail -1 | cut -d' ' -f1)
  [ -z "$states" ] && states=$(grep -oE '[0-9,]+ states checked' "$out" | tail -1 | cut -d' ' -f1)
  depth=$(grep -oE 'Progress\([0-9]+\)' "$out" | tail -1 | grep -oE '[0-9]+')
  [ -z "$depth" ] && depth=$(grep -oE 'depth of the complete state graph search is [0-9]+' "$out" | grep -oE '[0-9]+$')
  echo "| $name | $expected | $result | $verdict | ${states:-?} | ${depth:-?} | $((end - start)) | ${trace:+$(basename "$trace")} |" >> "$summary"
  log "    result: $result, verdict: $verdict, ${states:-?} states, depth ${depth:-?}, $((end - start))s"
  [ -n "$trace" ] && log "    trace:  $(basename "$trace") ($(wc -l < "$trace" | tr -d " ") steps)"
}

# scenario <name> <Scenario letter> <unused> <files> <leases> <epochs> <fix to turn off or "">
scenario() {
  local name=$1 lp=$2 fp=$3 files=$4 leases=$5 epochs=$6 off=$7
  local ff=TRUE fl=TRUE fb=TRUE fpw=TRUE fmf=TRUE
  case $off in
    FixFinish) ff=FALSE ;;
    FixLease) fl=FALSE ;;
    FixBrokerWide) fb=FALSE ;;
    FixPlannedWait) fpw=FALSE ;;
    FixMissedFirst) fmf=FALSE ;;
  esac
  if [ -n "$off" ]; then
    n=$((n + 1)); log "[$n/$total] $name with $off off: TLC must find a violation"
    cfg "$dir/$name-no-$off.cfg" "$lp" "$fp" "$files" "$leases" "$epochs" $ff $fl $fb $fpw $fmf 0
    tlc "$name-no-$off" FFScenarios fail "$TIME_PER_RUN"
  else
    n=$((n + 1)); log "[$n/$total] $name with every fix on: no invariant may fail"
    cfg "$dir/$name-v3.cfg" "$lp" "$fp" "$files" "$leases" "$epochs" TRUE TRUE TRUE TRUE TRUE 0
    tlc "$name-v3" FFScenarios pass "$TIME_PER_RUN"
    n=$((n + 1)); log "[$n/$total] $name with one lost-tail fault: no other partition may be hit"
    cfg "$dir/$name-losttail.cfg" "$lp" "$fp" "$files" "$leases" "$epochs" TRUE TRUE TRUE TRUE TRUE 1
    tlc "$name-losttail" FFScenarios pass "$TIME_PER_RUN"
  fi
}

# Each scenario: name, letter, unused, files, lease requests, epochs. The lease
# and file plans live in FFScenarios.tla.
A=(A-empty-tracking-part A - 2 1 1)
B=(B-missed-sequence      B - 2 1 1)
C=(C-fallback-low-rate    C - 3 2 1)
D=(D-switch-back          D - 3 3 1)
E=(E-cross-partition      E - 4 3 1)
F=(F-lost-lease-reply     F - 2 2 1)
G=(G-epoch-end            G - 2 1 2)
H=(H-abandoned-range      H - 1 2 1)

if [ "$ONLY_SIM" != 1 ]; then
log "Phase 1 of 3: strength checks, each fix off must produce a violation"
scenario "${A[@]}" FixFinish
scenario "${B[@]}" FixFinish
scenario "${C[@]}" FixLease
scenario "${D[@]}" FixLease
scenario "${E[@]}" FixBrokerWide
scenario "${F[@]}" FixPlannedWait
scenario "${H[@]}" FixMissedFirst

log "Phase 2 of 3: v3 checks, every fix on, then with one lost-tail fault"
for s in A B C D E F G H; do
  eval "scenario \"\${$s[@]}\" \"\""
done
fi

log "Phase 3 of 3: full-bounds random simulations, ${SIM_MINUTES} min each"
n=$((n + 1)); log "[$n/$total] simulation with every fix on"
cfg "$dir/sim-v3.cfg" "" "" 4 3 2 TRUE TRUE TRUE TRUE TRUE 0
tlc sim-v3 FencesFloors pass $((SIM_MINUTES * 60)) -simulate -depth 200
if [ "$ONLY_SIM" != 1 ]; then
n=$((n + 1)); log "[$n/$total] simulation with one lost-tail fault"
cfg "$dir/sim-losttail.cfg" "" "" 4 3 2 TRUE TRUE TRUE TRUE TRUE 1
tlc sim-losttail FencesFloors pass $((SIM_MINUTES * 60)) -simulate -depth 200
fi

echo
log "Done. Summary: model/$summary"
log "Runs that need a look: $(grep -c '| CHECK |' "$summary"), partial: $(grep -c '| partial |' "$summary")"
cat "$summary"
