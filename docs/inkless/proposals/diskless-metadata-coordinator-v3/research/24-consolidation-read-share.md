# Consumer read sources on consolidation services

Research for [Routing policy for clients a gateway can't serve](../issues/24-gateway-routing-policy.md), run on 2026-11-19 against Grafana. It measures how much consumer traffic on services with consolidation on comes from the broker-local log, the diskless WAL path, and the remote tier, and how much is catch-up reading.

## Summary

- Consumers on the two consolidation services almost never read the local log. Local-log bytes are 0.035% of `BytesOut` on one service and 0.00014% on the other. The remote tier serves nothing.
- Consumer reads go through the diskless WAL path, and the object cache serves 99.1% and 99.999% of their bytes.
- Catch-up reads are too small to measure on the consolidation services: the cold-path request share is 0% and 0.003%. On five diskless services without consolidation, it runs from 0% to 7.4%.
- Tail reads of a consolidating partition sit above the replica's local log end, because consolidation trails the diskless log end. `fetchMessages` sends such a read straight to the diskless path (`ReplicaManager.scala:2805-2806`). So the local log serves only reads of older data, and those are rare here.

## Sources

- Datasource "Thanos Cold (up to 90 days)", UID `cfhjxlg642kg0f`, grouped by `service_id`. Each JMX meter exports as a `_Count` counter, and every figure is `rate(...[7d])`, a 7-day mean.
- A service counts as consolidation-enabled when `rate(kafka_server_ReplicaManager_ConsolidationFetchBytesInPerSec_Count[7d]) > 0`. The config flag itself wasn't checked.

## Consolidation services, 7-day means

| | `4046330`, `kafka-logs-inkless-eu`, 3 brokers | `6061257`, `sa-inkless-multicloud`, 3 brokers |
| --- | --- | --- |
| `BytesOut` | 19.23 MB/s | 2.39 MB/s |
| `ConsolidationLocalBytesPerSec` | 6.78 KB/s | 3.3 B/s |
| `ConsolidationSupplementBytesPerSec` | 24 B/s | 0 |
| Consumer `CacheHitBytesPerSec` | 76.48 MB/s | 11.47 MB/s |
| Consumer `StorageBytesInPerSec` | 0.70 MB/s | 155 B/s |
| `RemoteFetchBytesPerSec` | 0 | 0 |
| Local-log bytes as a share of `BytesOut` | 0.035% | 0.00014% |
| Object cache hit ratio by bytes | 99.09%, worst 5-minute p95 at least 95.9% | 99.9987% |
| Consumer cold-path to hot-path requests | 0 to 111.6 per second | 0.0026 to 78.7 per second |

## Diskless services without consolidation

| `service_id` | Byte hit ratio | Cold-path share of requests |
| --- | --- | --- |
| 6420964 | 87.6% | 1.99% |
| 6003264 | 98.08% | about 0% |
| 6003265 | 98.41% | 0% |
| 6003249 | 98.05% | 0% |
| 6003091 | 95.0% | 7.4% |

## Caveats

- Two consolidation services, both small and internal or solution-architecture workloads. A workload with heavy catch-up reading or replays would read the local log and the remote tier far more.
- `CacheHitBytesPerSec` and `StorageBytesInPerSec` count whole object extents, about 4 to 5 times the delivered bytes, so they're usable as ratios, not volumes. Shares of `BytesOut` are the better volume estimate.
- The raw `wal-segment-cache` counters include the consolidation fetcher's own lookups, so the consumer hit ratio comes from the byte meters and from subtracting the fetcher's cold fetches.
- `BytesOut` covers every topic on the service.
