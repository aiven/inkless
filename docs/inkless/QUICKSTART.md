# Quickstart

## Dockerized demo

Run:

```shell
make demo
```

It will pull the Docker image, and start the local demo with two producers, one consumer, the PostgreSQL-backed control plane, and Minio as the object storage. Grafana with metrics will be accessible at http://localhost:3000 (login `admin`, password `admin`). Minio will be accessible at http://localhost:9001 (login `minioadmin`, password `minioadmin`). 

For more details, see the [demo docs](./../../docker/examples/docker-compose-files/inkless/README.md).

## Run Kafka from Intellij

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

Backend services:
Start MinIO in the background:

```shell
make local_minio
# docker-compose up -d minio
```

> [!NOTE]
> If running Postgres as Control-Plane backend:
> ```shell
> docker-compose up -d postgres
> ```

There are 2 Kafka configurations, one to use the in-memory Control-Plane `config/inkless/single-broker-0.properties` and another to use Postgres as Control-Plane `config/inkless/single-broker-pg-0.properties`.

> [!NOTE]
> Before the first time running Kafka, you need to format the log directories:
> 
> ```shell
> KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
> bin/kafka-storage.sh format --standalone -t $KAFKA_CLUSTER_ID -c $CONFIG_FILE
> ```

Setup Kafka run configuration on Intellij:

1. Go to `Run` -> `Edit Configurations...`
2. Click on the `+` button and select `Kafka`
3. Set the `Name` to `Kafka`
4. Set the `Main class` to `kafka.Kafka`
5. Set the `Program arguments` to `config/inkless/single-broker-0.properties` (or `config/inkless/single-broker-pg-0.properties`)
6. Set the `Working directory` to the root of the project
7. Set the `Use classpath of module` to `kafka.core.main`
8. Use Java 17
9. Set the JVM arguments to `-Dlog4j.configuration=file:config/inkless/log4j.properties`

At this point all should be ready to run the Intellij configuration.

Create topic:

```shell
make create_topic ARGS=t1
```

Produce some messages:

```shell
bin/kafka-producer-perf-test.sh \
  --record-size 1000 --num-records 1000000 --throughput -1 \
  --producer-props bootstrap.servers=127.0.0.1:9092 batch.size=1048576 linger.ms=100 \
  --topic t1
```

Consume messages:

```shell
bin/kafka-consumer-perf-test.sh --bootstrap-server 127.0.0.1:9092 \
  --messages 1000000 --from-latest \
  --topic t1
```

Check MinIO for remote files on `http://localhost:9000/browse/inkless/` (`minioadmin:minioadmin`)
