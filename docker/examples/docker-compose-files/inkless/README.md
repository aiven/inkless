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
| `make ts-unification`   | Diskless tiered storage unification               |
| `make destroy`          | Manual cleanup (run after stopping the demo)      |

## Image version

By default, the demo pulls the `edge` image (latest development build). Override with `KAFKA_VERSION`:

```bash
make s3-local KAFKA_VERSION=latest        # Latest stable release
make s3-local KAFKA_VERSION=4.1.0-0.33    # Specific Kafka+Inkless version
make s3-local KAFKA_VERSION=local         # Locally built image (requires: make docker_build)
```

See [Releases](../../../../docs/inkless/RELEASES.md#docker-images) for all available tags.

## Diskless Tiered Storage Unification

Test classic-to-diskless migration and tiered storage unification features. The Inkless image bundles the
[Aiven Tiered Storage plugin](https://github.com/Aiven-Open/tiered-storage-for-apache-kafka) (v1.1.1) —
no local plugin build required.

### Quick start

```bash
# Start 2-broker cluster with classic TS + diskless + migration bridge
make ts-unification

# Or manually:
docker compose -f docker-compose.yml -f docker-compose.s3-local.yml \
               -f docker-compose.ts-unification.yml up -d
```

### What's enabled

| Config                                | Value  | Purpose                                         |
|---------------------------------------|--------|--------------------------------------------------|
| `remote.log.storage.system.enable`    | `true` | Classic tiered storage via Aiven TS plugin       |
| `diskless.allow.from.classic.enable`  | `true` | Allow classic-to-diskless topic migration        |
| `classic.remote.storage.force.enable` | `true` | All classic topics get `remote.storage.enable=true` |
| `log.diskless.enable`                 | `false`| Topics start as classic (not diskless by default)|
| `diskless.managed.rf.enable`          | `true` | Managed replicas with rack-aware placement       |
| `default.replication.factor`          | `2`    | One replica per AZ                               |

### Cluster topology

| Broker   | Node ID | Rack | Role              |
|----------|---------|------|-------------------|
| broker   | 1       | az1  | broker+controller |
| broker2  | 2       | az2  | broker+controller |

### Storage

Both classic tiered storage and diskless (Inkless) share the same MinIO `inkless` bucket:
- **Classic tiered storage**: Aiven TS plugin → `tiered-storage/` prefix
- **Diskless (Inkless)**: Inkless → bucket root

### Test procedure

**Step 1: Create a classic topic (default)**

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --create --topic test-classic \
  --partitions 1 --replication-factor 2
```

Verify it has `remote.storage.enable=true` and `diskless.enable=false`:

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --describe --topic test-classic
```

**Step 2: Produce messages**

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server broker:19092 \
  --topic test-classic
```

**Step 3: Migrate to diskless**

```bash
docker compose exec broker /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server broker:19092 \
  --alter --entity-type topics --entity-name test-classic \
  --add-config diskless.enable=true
```

**Step 4: Verify migration**

```bash
docker compose exec broker /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:19092 \
  --describe --topic test-classic
```

The topic should now show `diskless.enable=true`.

**Step 5: Consume from beginning (spans remote-classic + diskless)**

```bash
docker compose exec broker /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server broker:19092 \
  --topic test-classic \
  --from-beginning
```

### Cleanup

```bash
docker compose down
```
