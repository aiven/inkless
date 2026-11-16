# Laptop scenarios for the coordinator risks

These Docker scenarios gauge the main performance risks of the diskless metadata coordinator against the Postgres backend on one laptop, before the coordinator exists. They don't replace the benchmarks in [Benchmark pass criteria](../issues/16-benchmark-pass-criteria.md). They tell you early whether a risk looks large enough to change the design.

## What runs

The Postgres side is real: Inkless brokers commit through `PostgresControlPlane` to a primary with an asynchronous streaming standby, which is the production-like baseline.

The coordinator side is a proxy, because the coordinator isn't implemented:

- Commit path: `coord_proxy.py` acts as each broker's commit delegate. Every 250 ms, it sends one part to each partition of `coord-proxy-n<N>`, a topic with replication factor 3, `min.insync.replicas=2`, and `flush.messages=1`. A part's acknowledgment then costs what a coordinator part costs before its SQLite apply: a leader flush, a follower fetch and flush, and the high-watermark advance. A file's latency is its slowest part, as for a produce request that touches every coordinator partition.
- SQLite: `db_bench.py` compares one coordinator partition's SQLite database with the shared Postgres database on the same rows, with the same apply shape and the same `findBatches` query.

## Environment

The environment has these services:

- `broker1` to `broker3`: Inkless brokers in racks `az1` to `az3`, with KRaft controllers, and data on named volumes.
- `pg-primary` and `pg-standby`: Postgres 17 with asynchronous streaming replication. The brokers use a multi-host JDBC URL, so they follow a promoted standby.
- `storage`: MinIO for WAL objects.
- `netem-*`: sidecars that add `AZ_DELAY_MS` (default 0.5 ms) to every packet each broker and the primary send, about 1 ms of round trip between AZs. The delay applies to traffic within an AZ too, on both backends.
- `client`: Kafka CLI tools and `kafka-producer-perf-test`.
- `tools`: Python with `confluent-kafka` and `psycopg`, which runs the proxy, the database micro-benchmark, and the metric reader.

Docker Desktop settings for an M4 MacBook Pro: 10 CPUs, 24 GB of memory, and 100 GB of disk. With less memory, lower `BROKER_HEAP` (default `3G`) and `PG_SHARED_BUFFERS` (default `2GB`).

## Run the scenarios

Start the environment and create the topics:

```shell
./run.sh up
./run.sh preflight
```

Run the scenarios one at a time:

```shell
./run.sh s1    # commit latency against N, and inter-AZ bytes
./run.sh s2    # commit-throughput sweep
./run.sh s3    # co-tenancy with a classic topic
./run.sh s6    # SQLite against Postgres across a row ladder
./run.sh s4    # failover; destroys pg-primary, so run it last
./run.sh down
```

Each run writes to `results/<timestamp>/`. The scripts read these environment variables:

| Variable | Default | Meaning |
| --- | --- | --- |
| `RATE_MIB` | 8 | Diskless produce per broker, in MiB/s. The ticket's base workload is 16. |
| `WARMUP`, `DURATION` | 120, 600 | Seconds per phase. The ticket's protocol is 600 and 1800. |
| `RUNS` | 1 | Repeats of S1. The ticket's protocol is 3, judged on the median. |
| `NS` | `1 2 4 8` | Coordinator partition counts for S1. |
| `SWEEP` | `1 2 4` | Rate multipliers for S2. |
| `LADDER` | `1000000,10000000` | Cluster-wide batch rows per S6 step. Add `100000000` if the disk has about 40 GB free. |
| `PROMOTE_DELAY_S` | 0 | Seconds between killing the primary and promoting the standby in S4. |

## Scenarios and how to read them

Apply the ticket's pass rule to each comparison: the coordinator's P50 and P99 within 1.1 times Postgres's.

### Preflight: disk flush latency

`preflight-fsync.txt` reports `pg_test_fsync` on the Docker VM disk. On the M4, `fdatasync` takes about 0.09 ms, against 0.5 to 2 ms on a cloud volume. The laptop therefore understates the main risk, the coordinator's second serial flush. Correct for it with this estimate of the gap on a cloud volume:

\[
\text{cloud gap} \approx \text{laptop gap} + (f_\text{cloud} - f_\text{laptop}) \times (2k - 1)
\]

Here \(f\) is the `fdatasync` latency, and \(k\) is the number of syncs in one Kafka segment flush: the segment, plus an `msync` and an `fsync` for each index, about 3 to 5. A coordinator part waits for two segment flushes in a row, and a Postgres commit waits for one WAL sync. With \(f_\text{cloud} = 1\) ms and \(k = 3\), the gap grows by about 4.5 ms.

### S1: commit latency against N at B=3

S1 covers the partition-count benchmark and the base-latency comparison.

- Postgres: `file_commit_ms_p50` and `file_commit_ms_p99` in `s1-pg-r<run>-after.json`, for each broker. `produce_total_ms_*` is the broker's `TotalTimeMs` for produce requests.
- Coordinator estimate: `file_ms` in `s1-n<N>-r<run>-proxy.json`, plus the SQLite `apply_ms` from S6 at the same percentile. The leader replies only after its apply commits, so the two add up.
- Compare the coordinator estimate with Postgres `FileCommitTime` at each `N`. The rest of a produce request, the 250 ms commit interval and the upload, is the same on both backends.
- `part_ms.p999` approximates a file's P99 when a file touches 8 partitions, because P99 of the slowest of 8 parts is close to P99.9 of one part.

### S5: inter-AZ bytes per produced MB

S1 also writes `s5-pg-r<run>.json` and `s5-n<N>-r<run>.json`:

- Postgres: two thirds of the traffic between the brokers and the primary, plus all replication to the standby. The traffic comes from `eth0` counters on both Postgres containers.
- Coordinator: two thirds of the part bytes the proxy sent, plus every follower copy, from `ReplicationBytesOutPerSec` on the brokers.

Compare `postgres_interaz_bytes_per_mb` with `coordinator_interaz_bytes_per_mb`. The proxy's parts are sized from the batch count, about 100 bytes per 16 KiB batch, so the coordinator figure is an estimate of the record size, not a measurement.

### S2: commit-throughput sweep

At each multiplier, compare P99 with the first step. A backend saturates at the first step whose P99 is more than twice its first step's. The proxy closes files on size above 32 MiB/s per broker, as Inkless does. Compare the multiplier at which each backend saturates. The laptop's shared disk and CPUs saturate long before 96 brokers would, so this shows the shape of the curve, not the limit.

### S3: co-tenancy

`s3a-classic-producer-az*.txt` and `s3b-classic-producer-az*.txt` end with the classic producer's P50 and P99. The B run adds the proxy on the same brokers. Its classic P50 and P99 should stay within 1.1 times the A run's. `log_flush_ms_*` in `s3a.json` and `s3b.json` shows the extra flush load. Every container shares one disk and one page cache, so this result is pessimistic for a cluster where each broker has its own disk.

### S4: failover

- Proxy: `s4-proxy-kill-proxy.json` and `s4-proxy-stop-proxy.json`. `longest_gap_s` is the longest time without a committed file, which is the time to first commit. `file_ms.max` is the slowest file, which is the time until stuck work completes.
- Postgres: `s4-pg-probe.json`, from a produce probe on a diskless topic, with the same two numbers.
- Managed Postgres detects a failed primary in tens of seconds, and the script promotes at once. Set `PROMOTE_DELAY_S` to your service's detection time, or read the Postgres numbers as a lower bound.
- The proxy omits the SQLite load and the high-watermark gate on a new leader, so its numbers are a lower bound too.

### S6: SQLite against Postgres

Each line of `s6-db.jsonl` is one ladder step:

- `apply_ms`: one commit. Postgres takes a whole file's rows, and SQLite takes the rows for one coordinator partition. Add SQLite's value to the proxy's `file_ms` in S1.
- `find_ms`: one `findBatches` call over 8 partitions, at a paced rate while commits run. Postgres takes every call, and SQLite takes 1/N of them.
- `backup_s` and `db_mib`: the SQLite snapshot backup time and the database size, for the state-size and snapshot benchmarks.

The tables and queries are simplified stand-ins for `commit_file` and `find_batches_v2`, so S6 compares engine and placement costs, not the real functions. The Python image's SQLite is older than the 3.51.3 floor from [SQLite apply and checkpoint off the shard thread](../issues/13-sqlite-apply-checkpoint.md). The WAL-reset bug that the floor fixes can corrupt data, but it doesn't change timings, so it doesn't affect this comparison.

## What the laptop can't test

| Benchmark | Why it can't run here |
| --- | --- |
| Partial failure | It needs the coordinator's fault hook and per-request completion. |
| 12 and 24 brokers | One laptop holds about 3 brokers with the rest of the stack. |
| Read-heavy load and cache storm on the coordinator | They need the read pool and the asynchronous `findBatches`. S6 covers the engine side of a read. |
| Snapshot transfer | It needs the coordinator's snapshot protocol. S6 records backup time and size. |
| State size at \(10^9\) rows, and election time | They need about 400 GB of disk and the coordinator's loader. |
| Retention burst and control traffic | They need release reports, leases, and fences. |
| Zero acknowledged loss | The proxy topic isn't checked by a verifying consumer. |

## Known distortions

- One disk and one page cache serve every container, so flushes from all brokers and Postgres queue on the same device.
- The VM's `fdatasync` is about 10 times faster than a cloud volume's. The preflight correction compensates for it.
- Timer wake-ups in the Docker VM lag by up to 10 ms at P99 when CPUs idle. Both scripts measure latency from the send, not from the schedule, and the proxy runs with the diskless load in S1, which keeps the CPUs awake. An idle proxy run reports higher latency than a loaded one.
- `kafka-producer-perf-test` reports client latency that includes queueing in the producer, about 1 to 2 s for diskless topics. Use the broker metrics for diskless produce latency.
- The proxy's parts travel as produce requests from `librdkafka`, not as the coordinator's private RPC, and the SQLite apply runs in a separate container.
