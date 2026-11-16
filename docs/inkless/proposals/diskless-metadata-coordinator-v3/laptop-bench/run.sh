#!/usr/bin/env bash
# Laptop scenarios for the coordinator risks against the Postgres backend. See README.md.
set -euo pipefail
cd "$(dirname "$0")"

RATE_MIB=${RATE_MIB:-8}          # diskless produce per broker, MiB/s
CLASSIC_RATE_MIB=${CLASSIC_RATE_MIB:-8}
WARMUP=${WARMUP:-120}
DURATION=${DURATION:-600}
RUNS=${RUNS:-1}
NS=${NS:-"1 2 4 8"}
SWEEP=${SWEEP:-"1 2 4"}
LADDER=${LADDER:-"1000000,10000000"}
PROMOTE_DELAY_S=${PROMOTE_DELAY_S:-0}
RESULTS=${RESULTS:-results/$(date +%Y%m%d-%H%M%S)}
BOOT=broker1:9092
mkdir -p "$RESULTS"

dc() { docker compose "$@"; }
kafka() { dc exec -T client "/opt/kafka/bin/$1" --bootstrap-server "$BOOT" "${@:2}"; }
tools() { dc exec -T tools python "$@"; }
log() { echo "[$(date +%T)] $*" >&2; }

netbytes() {  # rx+tx bytes on a container's eth0
  dc exec -T "$1" sh -c 'echo $(( $(cat /sys/class/net/eth0/statistics/rx_bytes) + $(cat /sys/class/net/eth0/statistics/tx_bytes) ))'
}

create_topic() {  # name partitions rf [configs...]
  local name=$1 parts=$2 rf=$3; shift 3
  local cfg=(); for c in "$@"; do cfg+=(--config "$c"); done
  kafka kafka-topics.sh --create --if-not-exists --topic "$name" --partitions "$parts" --replication-factor "$rf" "${cfg[@]}"
}

# One producer per AZ. 16 KiB sticky batches spread each 250 ms file over every coordinator partition.
produce() {  # topic rate_mib seconds label
  local topic=$1 rate=$2 secs=$3 label=$4 pids=()
  local rps=$((rate * 1024))
  for az in 1 2 3; do
    # A properties file, because --producer-props rejects the second '=' in the diskless_az client ID.
    printf '%s\n' "bootstrap.servers=broker$az:9092" acks=all linger.ms=100 batch.size=16384 \
      buffer.memory=128000000 max.request.size=64000000 "client.id=$label-az$az,diskless_az=az$az" \
      | dc exec -T client sh -c "cat > /tmp/$label-az$az.properties"
    dc exec -T client /opt/kafka/bin/kafka-producer-perf-test.sh --topic "$topic" \
      --throughput "$rps" --record-size 1024 --num-records $((rps * secs)) \
      --producer.config "/tmp/$label-az$az.properties" \
      > "$RESULTS/$label-producer-az$az.txt" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p" || true; done
}

# Files close every 250 ms or at 8 MiB. Each 16 KiB batch costs a batch row of about 100 bytes.
proxy() {  # n label rate_mib [extra args...]
  local n=$1 label=$2 rate=$3; shift 3
  local fps=$(( rate > 32 ? rate / 8 : 4 )) batches=$(( rate > 32 ? 512 : rate * 16 ))
  tools coord_proxy.py --topic "coord-proxy-n$n" --partitions "$n" --files-per-sec "$fps" \
    --part-bytes $(( batches * 100 / n )) --warmup "$WARMUP" --duration "$DURATION" --label "$label" "$@" \
    | tee "$RESULTS/$label-proxy.json"
}

renetem() { dc up -d --force-recreate --no-deps "netem-$1" >/dev/null; }

cmd_up() {
  dc up -d --build
  log "waiting for the cluster"
  until kafka kafka-topics.sh --list >/dev/null 2>&1; do sleep 2; done
  create_topic diskless-load 192 1 diskless.enable=true
  create_topic diskless-probe 1 1 diskless.enable=true
  create_topic classic-load 12 3 min.insync.replicas=2
  for n in 1 2 4 8 16; do
    create_topic "coord-proxy-n$n" "$n" 3 min.insync.replicas=2 flush.messages=1 unclean.leader.election.enable=false
  done
  log "ready"
}

cmd_down() { dc down -v --remove-orphans; }

cmd_preflight() {
  log "fsync latency of the Docker VM disk"
  dc exec -T -u postgres pg-primary pg_test_fsync -s 3 -f /var/lib/postgresql/data/fsync_test | tee "$RESULTS/preflight-fsync.txt"
}

# S1: commit latency against N at B=3. Also feeds S5 with network counters.
cmd_s1() {
  local secs=$((WARMUP + DURATION))
  for r in $(seq "$RUNS"); do
    log "S1 run $r: Postgres baseline"
    tools prom.py snap > "$RESULTS/s1-pg-r$r-before.json"
    local p0 s0; p0=$(netbytes pg-primary); s0=$(netbytes pg-standby)
    produce diskless-load "$RATE_MIB" "$secs" "s1-pg-r$r"
    tools prom.py snap > "$RESULTS/s1-pg-r$r-after.json"
    local p1 s1; p1=$(netbytes pg-primary); s1=$(netbytes pg-standby)
    tools prom.py interaz "$RESULTS/s1-pg-r$r-before.json" "$RESULTS/s1-pg-r$r-after.json" \
      --produced-bytes $((RATE_MIB * 3 * secs * 1048576)) \
      --pg-primary-bytes $((p1 - p0)) --pg-standby-bytes $((s1 - s0)) | tee "$RESULTS/s5-pg-r$r.json"

    for n in $NS; do
      log "S1 run $r: coordinator proxy N=$n, with the diskless load on the brokers"
      produce diskless-load "$RATE_MIB" "$secs" "s1-n$n-r$r" &
      local load=$!
      tools prom.py snap > "$RESULTS/s1-n$n-r$r-before.json"
      proxy "$n" "s1-n$n-r$r" "$RATE_MIB"
      tools prom.py snap > "$RESULTS/s1-n$n-r$r-after.json"
      wait "$load" || true
      local sent; sent=$(tools -c 'import json,sys;print(json.load(open(sys.argv[1]))["bytes_sent"])' "$RESULTS/s1-n$n-r$r-proxy.json")
      tools prom.py interaz "$RESULTS/s1-n$n-r$r-before.json" "$RESULTS/s1-n$n-r$r-after.json" \
        --produced-bytes $((RATE_MIB * 3 * secs * 1048576)) --proxy-bytes "$sent" | tee "$RESULTS/s5-n$n-r$r.json"
    done
  done
}

# S2: commit-throughput sweep. Saturation is the first step whose P99 exceeds twice the first step's.
cmd_s2() {
  local secs=$((WARMUP + DURATION))
  for k in $SWEEP; do
    local rate=$((RATE_MIB * k))
    log "S2 x$k: Postgres at $rate MiB/s per broker"
    produce diskless-load "$rate" "$secs" "s2-pg-x$k"
    tools prom.py snap > "$RESULTS/s2-pg-x$k.json"
    log "S2 x$k: proxy N=8 at $rate MiB/s per broker"
    produce diskless-load "$rate" "$secs" "s2-n8-x$k" &
    local load=$!
    proxy 8 "s2-n8-x$k" "$rate"
    wait "$load" || true
  done
}

# S3: co-tenancy. Classic produce latency without and with the coordinator proxy on the same brokers.
cmd_s3() {
  local secs=$((WARMUP + DURATION))
  log "S3 A: diskless (Postgres) plus classic"
  produce diskless-load "$RATE_MIB" "$secs" "s3a-diskless" &
  local a=$!
  produce classic-load "$CLASSIC_RATE_MIB" "$secs" "s3a-classic"
  wait "$a" || true
  tools prom.py snap > "$RESULTS/s3a.json"

  log "S3 B: diskless plus classic plus proxy N=8"
  produce diskless-load "$RATE_MIB" "$secs" "s3b-diskless" &
  local b1=$!
  produce classic-load "$CLASSIC_RATE_MIB" "$secs" "s3b-classic" &
  local b2=$!
  proxy 8 "s3b" "$RATE_MIB"
  wait "$b1" "$b2" || true
  tools prom.py snap > "$RESULTS/s3b.json"
}

leader_of_most() {  # topic: the broker that leads the most partitions
  kafka kafka-topics.sh --describe --topic "$1" | grep -o 'Leader: [0-9]*' | sort | uniq -c | sort -rn | head -1 | awk '{print "broker"$3}'
}

# S4: failover. The proxy's longest gap without a committed file, against Postgres promotion.
cmd_s4() {
  local victim p
  for mode in kill stop; do
    victim=$(leader_of_most coord-proxy-n8)
    log "S4 proxy: $mode $victim at 60 s"
    WARMUP=30 DURATION=180 proxy 8 "s4-proxy-$mode" "$RATE_MIB" &
    p=$!
    sleep 62
    if [[ $mode == kill ]]; then dc kill -s SIGKILL "$victim"; else dc stop -t 60 "$victim"; fi
    sleep 60
    dc start "$victim"; renetem "$victim"
    wait "$p" || true
    sleep 30
  done

  log "S4 Postgres: kill pg-primary at 60 s, promote the standby after ${PROMOTE_DELAY_S}s"
  tools coord_proxy.py --topic diskless-probe --partitions 1 --files-per-sec 20 --part-bytes 1024 \
    --warmup 30 --duration 180 --label s4-pg > "$RESULTS/s4-pg-probe.json" &
  p=$!
  sleep 62
  dc kill -s SIGKILL pg-primary
  sleep "$PROMOTE_DELAY_S"
  dc exec -T -u postgres pg-standby pg_ctl promote -D /var/lib/postgresql/data
  wait "$p" || true
  cat "$RESULTS/s4-pg-probe.json"
  log "pg-primary is gone; run ./run.sh down && ./run.sh up before the next scenario"
}

# S6: SQLite against Postgres on apply, findBatches, and backup across a row ladder.
cmd_s6() {
  tools db_bench.py --ladder "$LADDER" --measure "$DURATION" --rows-per-file $((RATE_MIB * 16)) \
    | tee "$RESULTS/s6-db.jsonl"
}

case "${1:-}" in
  up|down|preflight|s1|s2|s3|s4|s6) "cmd_$1" ;;
  all) cmd_preflight; cmd_s1; cmd_s2; cmd_s3; cmd_s6; cmd_s4 ;;
  *) echo "usage: $0 up|down|preflight|s1|s2|s3|s4|s6|all" >&2; exit 2 ;;
esac
log "results in $RESULTS"
