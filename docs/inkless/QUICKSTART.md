# Quickstart

## Prerequisites

Before running Inkless, ensure you have:

- **Docker** and **Docker Compose v2** installed
- **Available ports:** 9092 (Kafka), 3000 (Grafana), 9001 (MinIO), 5432 (PostgreSQL)
- **Disk space:** ~2GB for demo images
- **For building from source:** JDK 17, Gradle (included via wrapper)

## Dockerized demo

Run:

```shell
make demo
# or with podman
make demo DOCKER=podman
```

It will pull the Docker image and start the local demo with two producers, one consumer, the PostgreSQL-backed control plane, and MinIO as the object storage. Press `Ctrl+C` to stop; containers and networks are cleaned up automatically.

### Demo services

| Service          | URL                   | Credentials             |
|------------------|-----------------------|-------------------------|
| Grafana          | http://localhost:3000 | `admin` / `admin`       |
| MinIO Console    | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Kafka Bootstrap  | localhost:9092        | -                       |
| PostgreSQL       | localhost:5432        | `admin` / `admin`       |

### Demo backends

The default backend is `s3-local` (MinIO). You can choose different storage backends:

```shell
make demo DEMO=s3-local     # MinIO S3-compatible storage (default)
make demo DEMO=gcs-local    # Fake GCS server
make demo DEMO=azure-local  # Azurite Azure-compatible storage
make demo DEMO=s3-aws       # Real AWS S3 (requires AWS credentials)
```

#### AWS S3 backend

For `s3-aws`, set the following environment variables before running:

```shell
export AWS_ACCESS_KEY_ID=<your-access-key>
export AWS_SECRET_ACCESS_KEY=<your-secret-key>
export AWS_SESSION_TOKEN=<your-session-token>  # if using temporary credentials
export AWS_REGION=<your-region>
export AWS_S3_BUCKET_NAME=<your-bucket-name>

make demo DEMO=s3-aws
```

### Using a locally built image

By default, the demo pulls the `edge` image from GHCR. If the image is not available or you want to test local changes:

```shell
make docker_build
make demo KAFKA_VERSION=local
```

### Monitoring the demo

The logs will print the producer and consumer stats. To observe the metrics, access Grafana at http://localhost:3000 and navigate to:

- **Kafka Inkless** - Inkless-specific metrics (batch sizes, object storage operations)
- **Kafka Clients** - Producer and consumer performance metrics

**View service logs (in a separate terminal):**
```shell
cd docker/examples/docker-compose-files/inkless
docker compose logs -f broker        # Kafka broker logs (or: podman compose logs -f broker)
docker compose logs -f producer-1    # Producer performance stats
docker compose logs -f consumer      # Consumer performance stats
```


## Using prebuilt binaries

If you prefer to run Inkless without Docker, download prebuilt binaries from [GitHub Releases](https://github.com/aiven/inkless/releases). See [Releases](./RELEASES.md#binary-distributions) for download instructions.

### Configure and run

Sample configurations are provided in `config/inkless/`:

| Config | Control Plane | Storage Backend |
|--------|---------------|-----------------|
| `single-broker-pg-0.properties` | PostgreSQL | S3 (MinIO) |
| `single-broker-pg-0-gcs.properties` | PostgreSQL | GCS |
| `single-broker-pg-0-azure.properties` | PostgreSQL | Azure Blob |

1. **Copy and edit a sample config** for your environment:
   ```shell
   cp config/inkless/single-broker-pg-0.properties config/server.properties
   # Edit config/server.properties with your storage and database settings
   ```

2. **Format storage and start broker:**
   ```shell
   # Format storage directory (first time only)
   bin/kafka-storage.sh format -t $(bin/kafka-storage.sh random-uuid) -c config/server.properties

   # Start the broker
   bin/kafka-server-start.sh config/server.properties
   ```

See [configs.rst](./configs.rst) for the complete configuration reference and [Versioning Strategy](./VERSIONING-STRATEGY.md) for choosing the right version.


## Topic creation

Given that Inkless introduces a new topic configuration `diskless.enable`, you need to use inkless binaries to create topics.

You can either build the binaries yourself, use the Docker image, or download the prebuilt binaries from the [releases page](https://github.com/aiven/inkless/releases).

To build the binaries, run:

```shell
# make clean # to rebuild the binaries
make build_release
```

To run docker image:

```shell
# Use locally built image (after running: make docker_build)
docker run -it --entrypoint bash ghcr.io/aiven/inkless:local

# Or use latest edge build from GHCR
docker run -it --entrypoint bash ghcr.io/aiven/inkless:edge

# Or use latest stable release
docker run -it --entrypoint bash ghcr.io/aiven/inkless:latest
```

Example session:
```shell
4e3b0026634e:/$ cd /opt/kafka/
4e3b0026634e:/opt/kafka$ bin/kafka-topics.sh
```

Be aware that if server default configuration is set to `log.diskless.enable=true`, topics (except internal ones) will be created with `diskless.enable=true` by default.

## Local development: Run Kafka from IDE

To run Kafka from the IDE (e.g. Intellij), you need to set up the project and dependencies correctly:

Adjust the module `:core` libraries to include log4j release libraries:

```diff
diff --git a/build.gradle b/build.gradle
--- a/build.gradle	(revision 9fca1c41bda4f1442df495278d01347eb9927e5d)
+++ b/build.gradle	(date 1741171600475)
@@ -992,7 +992,7 @@
}

dependencies {
-    releaseOnly log4jReleaseLibs
+    implementation log4jReleaseLibs
     // `core` is often used in users' tests, define the following dependencies as `api` for backwards compatibility
     // even though the `core` module doesn't expose any public API
     api project(':clients')
```

Start the Backend services:

Run MinIO in the background:

```shell
make local_minio
```

> [!NOTE]
> If running Postgres as Control-Plane backend:
> ```shell
> make local_pg
> ```

There are 2 Kafka configurations, one to use the in-memory Control-Plane `config/inkless/single-broker-0.properties` and another to use Postgres as Control-Plane `config/inkless/single-broker-pg-0.properties`.

> [!NOTE]
> Before the first time running Kafka, you need to format the log directories:
> 
> ```shell
> make kafka_storage_format
> ```

Then run the `kafka.Kafka` class in the `kafka.core.main` module:

Setup Kafka run configuration on Intellij:

1. Go to `Run` -> `Edit Configurations...`
2. Click on the `+` button and select `Kafka`
3. Set the `Name` to `Kafka`
4. Set the `Main class` to `kafka.Kafka`
5. Set the `Program arguments` to `config/inkless/single-broker-0.properties` (or `config/inkless/single-broker-pg-0.properties`)
6. Set the `Working directory` to the root of the project
7. Set the `Use classpath of module` to `kafka.core.main`
8. Use Java 17
9. Set the JVM arguments to `-Dlog4j2.configurationFile=./config/inkless/log4j2.yaml`

At this point all should be ready to run the Intellij configuration.

Create topic:

```shell
make create_topic ARGS="t1 --partitions 12"
```

Start consuming messages:

```shell
bin/kafka-consumer-perf-test.sh --bootstrap-server 127.0.0.1:9092 \
  --messages 10000000 --from-latest \
  --reporting-interval 5000 \
  --show-detailed-stats \
  --timeout 60000 \
  --topic t1
```

And then, produce messages with:

```shell
bin/kafka-producer-perf-test.sh \
  --record-size 1000 --num-records 10000000 --throughput -1 \
  --producer-props bootstrap.servers=127.0.0.1:9092 batch.size=1000000 linger.ms=100 max.request.size=12000000 \
  --topic t1
```

> [!IMPORTANT]  
> Single produce client is constraint to a single request process at a time.
> To overcome this limitation, a producer can write to multiple partitions and increase the request max size.
> See the [Performance](./PERFORMANCE.md) section for more details.


Check MinIO for remote files on `http://localhost:9001/browse/inkless/` (`minioadmin:minioadmin`)

To clean up the local environment, stop the Kafka process and run:

```shell
make cleanup
```

