# Inkless System Tests

System tests for inkless use [ducktape](https://github.com/confluentinc/ducktape), the same distributed testing framework used by Apache Kafka.
Tests run inside Docker containers managed by the `ducker-ak` tool.

Unlike the JUnit integration tests (`./gradlew :storage:inkless:integrationTest`), these tests exercise full multi-broker clusters with real network communication between processes, and external infrastructure (Postgres, MinIO) running as separate containers.

The broker properties template (`tests/kafkatest/services/kafka/templates/kafka.properties`) is preconfigured to connect to Postgres at `postgres:5432` and MinIO at `storage:9000` on the `ducknet` Docker network.

## Prerequisites

- Docker installed and running
- The project compiles successfully (`./gradlew build -x test`)

## Running Inkless System Tests

### 1. Bring up the ducker cluster

```shell
bash tests/docker/ducker-ak up -n 11
```

This creates:

- A Docker network called `ducknet`
- `ducker01` -- the test driver node (runs ducktape itself)
- `ducker02` through `ducker11` -- worker nodes where Kafka brokers, producers, and consumers run

**How many nodes?** The number must be at least 1 + the `@cluster(num_nodes=N)` declared in the test.
The inkless topic migration tests declare up to `@cluster(num_nodes=10)`, so 11 total nodes is the minimum.
The upstream default of 14 covers the entire Kafka test suite; for inkless tests alone, 11 is sufficient.

### 2. Start Postgres on the ducker network

The inkless control plane requires a Postgres instance reachable at hostname `postgres`:

```shell
docker run -d --name postgres --network ducknet --network-alias postgres \
  -e POSTGRES_DB=inkless \
  -e POSTGRES_USER=admin \
  -e POSTGRES_PASSWORD=admin \
  postgres:17.2
```

### 3. Start MinIO on the ducker network

Object storage reachable at hostname `storage`:

```shell
docker run -d --name storage --network ducknet --network-alias storage \
  quay.io/minio/minio server /data --console-address ":9001"
```

### 4. Create the inkless bucket

The brokers expect a bucket named `inkless` to exist:

```shell
docker run --rm --network ducknet --entrypoint /bin/sh quay.io/minio/mc -c '
  until /usr/bin/mc alias set local http://storage:9000 minioadmin minioadmin; do
    echo "waiting for minio"; sleep 2;
  done;
  /usr/bin/mc mb -p local/inkless
'
```

### 5. Run the tests

Run a specific test file:

```shell
bash tests/docker/ducker-ak test tests/kafkatest/tests/inkless/inkless_topic_migration_test.py
```

Run a specific test class:

```shell
bash tests/docker/ducker-ak test tests/kafkatest/tests/inkless/inkless_topic_migration_test.py::InklessTopicMigrationTest
```

Run a specific test method:

```shell
bash tests/docker/ducker-ak test tests/kafkatest/tests/inkless/inkless_topic_migration_test.py::InklessTopicMigrationTest.test_migration_happy_path
```

Pass additional ducktape arguments after `--`:

```shell
bash tests/docker/ducker-ak test tests/kafkatest/tests/inkless/inkless_topic_migration_test.py -- --test-runner-timeout 1800000
```

Run with debug logging:

```shell
bash tests/docker/ducker-ak test tests/kafkatest/tests/inkless/inkless_topic_migration_test.py -- --debug
```

### 6. Teardown

```shell
docker rm -f postgres storage
bash tests/docker/ducker-ak down -f
```

## How It Works

The `ducker-ak` tool orchestrates the test lifecycle:

1. `ducker-ak up` builds a Docker image with Kafka binaries and creates the test cluster on the `ducknet` network
2. `ducker-ak test` compiles `systemTestLibs` (if needed), then runs ducktape on `ducker01` which orchestrates the worker nodes
3. Ducktape connects into worker nodes with SSH to start/stop Kafka brokers, producers, consumers, and asserts on their behavior
4. Postgres and MinIO are standalone containers on the same network, reachable by their hostnames from all ducker nodes

```
ducknet (Docker network)
├── ducker01          (test driver -- runs ducktape)
├── ducker02-11       (workers -- run Kafka brokers, producers, consumers)
├── postgres          (control plane metadata store)
└── storage           (MinIO -- S3-compatible object storage)
```

## Troubleshooting

**"not enough nodes" error**: Increase the `-n` parameter in `ducker-ak up`. The test's `@cluster(num_nodes=N)` annotation declares how many worker nodes it requires (excluding the driver).

**Build failures during `ducker-ak test`**: The tool runs `./gradlew systemTestLibs` automatically. If this fails, fix compilation errors first with `./gradlew build -x test`.

**Postgres connection refused**: Ensure the Postgres container is healthy before running tests:

```shell
docker exec postgres pg_isready --dbname=inkless -U admin
```

**MinIO bucket errors**: Verify the bucket exists:

```shell
docker run --rm --network ducknet --entrypoint /bin/sh quay.io/minio/mc -c '
  /usr/bin/mc alias set local http://storage:9000 minioadmin minioadmin;
  /usr/bin/mc ls local/inkless
'
```

**SSH into a ducker node** for debugging:

```shell
bash tests/docker/ducker-ak ssh ducker02
```

**View test results**: Results are written to `tests/results/` after each run.
