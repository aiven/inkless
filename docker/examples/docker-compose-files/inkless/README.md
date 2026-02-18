# Inkless Docker Compose Demo

Docker Compose examples for running Inkless with different storage backends.

See the [Quickstart guide](../../../../docs/inkless/QUICKSTART.md#dockerized-demo) for complete setup instructions.

## Quick reference

| Command                 | Description                                       |
|-------------------------|---------------------------------------------------|
| `make s3-local`         | MinIO S3-compatible storage (default)             |
| `make gcs-local`        | Fake GCS server                                   |
| `make azure-local`      | Azurite Azure-compatible storage                  |
| `make s3-aws`           | Real AWS S3 (requires AWS credentials)            |
| `make managed-replicas` | Multi-AZ cluster with managed replicas (RF=3)     |
| `make destroy`          | Manual cleanup (automatic on Ctrl+C)              |

## Image version

By default, the demo pulls the `edge` image (latest development build). Override with `KAFKA_VERSION`:

```bash
make s3-local KAFKA_VERSION=latest        # Latest stable release
make s3-local KAFKA_VERSION=4.1.0-0.33    # Specific Kafka+Inkless version
make s3-local KAFKA_VERSION=local         # Locally built image (requires: make docker_build)
```

See [Releases](../../../../docs/inkless/RELEASES.md#docker-images) for all available tags.

## Testing Managed Replicas (Multi-AZ)

Test diskless topics with managed replicas (RF = rack_count) using the managed replicas overlay.

### Quick start

```bash
# Start 2-broker cluster with rack assignments (az1, az2)
make managed-replicas KAFKA_VERSION=local

# Or manually:
docker compose -f docker-compose.yml -f docker-compose.s3-local.yml -f docker-compose.managed-replicas.yml up -d
```

### Cluster topology

| Broker   | Node ID | Rack | Role              |
|----------|---------|------|-------------------|
| broker   | 1       | az1  | broker+controller |
| broker2  | 2       | az2  | broker+controller |

### Test procedure

**Step 1: Create a diskless topic**

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create --topic test-managed \
  --config diskless.enable=true \
  --partitions 3
```

**Step 2: Verify RF=2 (one replica per AZ)**

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --describe --topic test-managed
```

Expected output:
```
Topic: test-managed  PartitionCount: 3  ReplicationFactor: 2
  Partition: 0  Leader: 1  Replicas: 1,2  Isr: 1,2
  Partition: 1  Leader: 2  Replicas: 2,1  Isr: 2,1
  Partition: 2  Leader: 1  Replicas: 1,2  Isr: 1,2
```

**Step 3: Produce messages**

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker:19092 \
  --topic test-managed
```

**Step 4: Consume messages**

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic test-managed \
  --from-beginning
```

### Testing AZ-aware routing

Use the `diskless_az=<az>` prefix in client ID to enable AZ-aware routing:

```bash
# Produce with AZ hint (prefers az1 replica)
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker:19092 \
  --topic test-managed \
  --producer-property client.id=diskless_az=az1

# Consume with AZ hint (prefers az2 replica)
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic test-managed \
  --from-beginning \
  --consumer-property client.id=diskless_az=az2
```

### Verifying metrics

Check transformer metrics via Prometheus endpoint:

```bash
# View all AZ routing metrics
curl -s http://localhost:7070/metrics | grep -i "clientaz"

# Key metrics:
# - client-az-hit-rate: Requests where broker found in client AZ
# - client-az-miss-rate: Requests routed to different AZ
# - client-az-unaware-rate: Requests without AZ hint
# - cross-az-routing-total: Cross-AZ routing events
# - fallback-total: Fallbacks to non-replica brokers
# - offline-replicas-routed-around: Routing around offline replicas
```

### Cleanup

```bash
docker compose down
```
