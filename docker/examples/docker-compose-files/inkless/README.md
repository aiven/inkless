# Inkless demo

Contains examples of how to run Inkless with Postgres as batch coordinator and different object storage back-ends using Docker Compose.

Running a make task will start all services and run the Inkless demo, including topic creation, producer/consumer perf clients, and monitoring with Prometheus and Grafana. Press `Ctrl+C` to stop, then run `make destroy` to clean up all containers and networks.

## Prerequisites

- Docker and Docker Compose v2
- Available ports: 9092 (Kafka), 3000 (Grafana), 9001 (MinIO), 5432 (PostgreSQL)
- ~2GB disk space for images

## Services and credentials

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | http://localhost:3000 | `admin` / `admin` |
| MinIO Console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Kafka Bootstrap | localhost:9092 | - |
| PostgreSQL | localhost:5432 | `admin` / `admin` |

## Run the demo

By default, the demo pulls the `edge` image from GHCR. To use a different image:

```bash
make s3-local KAFKA_VERSION=latest        # Latest stable release
make s3-local KAFKA_VERSION=4.1.0-0.33    # Specific version
make s3-local KAFKA_VERSION=local         # Locally built (requires: make docker_build from repo root)
```

### S3 local (recommended)

This setup uses MinIO as a local S3-compatible object storage and Postgres as a metadata store.

```
make s3-local
```

### GCS local

This setup uses `fake-gcs-server` as a local GCS-compatible object storage and Postgres as a metadata store.

```
make gcs-local
```

### Azure local

This setup uses `azurite` as a local Azure-compatible object storage and Postgres as a metadata store.

```
make azure-local
```

### AWS S3

This example demonstrates how to use Inkless with remote data and metadata stores,
using a Remote Postgres instance for metadata and AWS S3 for data storage.

#### Prerequisites

Set the environment variables:

For AWS S3:

```properties
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_SESSION_TOKEN=
AWS_REGION=
AWS_S3_BUCKET_NAME=
```

Then run

```bash
make s3-aws
```

## Monitoring

### View logs (in a separate terminal)

```bash
docker compose logs -f broker        # Kafka broker logs
docker compose logs -f producer-1    # Producer performance stats
docker compose logs -f consumer      # Consumer performance stats
```

### Grafana dashboards

Access Grafana at http://localhost:3000 and navigate to:
- **Kafka Inkless** - Inkless-specific metrics (batch sizes, object storage operations)
- **Kafka Clients** - Producer and consumer performance metrics
