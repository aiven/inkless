# Inkless Kafka

## How to run

### Run Kafka from Intellij

Adjust the module `:server` libraries to include Reload4j:

```diff
diff --git a/build.gradle b/build.gradle
--- a/build.gradle	(revision 7efc6203bae3754d96b5aa678e2f38c8db40fd61)
+++ b/build.gradle	(date 1740171633116)
@@ -944,7 +944,8 @@
 
-    compileOnly libs.reload4j
+    implementation libs.reload4j
+    implementation libs.slf4jReload4j
```

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

Backend services: 
Start MinIO in the background:

```shell
docker-compose up -d minio
```

> [!NOTE]
> If running Postgres as Control-Plane backend:
> ```shell
> docker-compose up -d postgres
> ```

At this point all should be ready to run the Intellij configuration.
