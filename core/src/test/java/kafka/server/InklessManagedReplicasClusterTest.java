/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlane;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneConfig;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.storage_backend.s3.S3StorageConfig;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;
import io.aiven.inkless.test_utils.S3TestContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for diskless topics with managed replicas (RF > 1).
 * Uses a 2-broker cluster with rack assignments to verify rack-aware placement.
 */
@Testcontainers
public class InklessManagedReplicasClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(InklessManagedReplicasClusterTest.class);
    private static final String ROTATING_POSTGRES_HOST = "rotating-inkless-pg";
    private static final int ROTATING_POSTGRES_PORT = 5432;

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        RotatingPostgresSocketFactory.configure(
            pgContainer.getHost(),
            pgContainer.getMappedPort(5432),
            Duration.ofMinutes(10)
        );

        // Create 2-broker cluster with rack assignments (matching docker compose setup)
        // Node 0: combined broker+controller, Node 1: broker-only
        Map<Integer, Map<String, String>> perServerProps = Map.of(
            0, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az1"),
            1, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az2")
        );
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(2)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProps)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            // Enable managed replicas with RF=2 (one replica per AZ)
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
            // PG control plane config
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, rotatingPostgresJdbcUrl())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_POOL_TIMEOUT_MS_CONFIG, "1000")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.TCP_CONNECT_TIMEOUT_MS_CONFIG, "1000")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.SOCKET_TIMEOUT_MS_CONFIG, "1000")
            // S3 storage config
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
    }

    @AfterEach
    public void teardown() throws Exception {
        cluster.close();
    }

    @Test
    public void createDisklessTopicWithManagedReplicas() throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.name());

        String topicName = "managed-replicas-topic";
        int numPartitions = 3;
        int numRecords = 100;

        // Create diskless topic with RF=-1 → resolves to default.replication.factor=2
        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic topic = new NewTopic(topicName, numPartitions, (short) -1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
            result.all().get(30, TimeUnit.SECONDS);

            // Verify managed replicas behavior: RF=2 from default.replication.factor
            // Wait for metadata propagation across brokers before describing
            TopicDescription description = waitForTopicDescription(admin, topicName);
            log.info("Topic {} created with {} partitions", topicName, description.partitions().size());
            assertEquals(numPartitions, description.partitions().size());

            // Node 0 is in az1, Node 1 is in az2
            Set<Integer> expectedBrokers = Set.of(0, 1);

            for (TopicPartitionInfo partition : description.partitions()) {
                log.info("Partition {}: leader={}, replicas={}, isr={}",
                    partition.partition(), partition.leader().id(),
                    partition.replicas().stream().map(Node::id).toList(),
                    partition.isr().stream().map(Node::id).toList());

                // With 2 racks, RF should be 2
                assertEquals(2, partition.replicas().size(),
                    "Expected RF=2 for managed replicas with 2 racks");

                // Verify replicas contain brokers from both racks (nodes 0 and 1)
                Set<Integer> replicaIds = partition.replicas().stream()
                    .map(Node::id)
                    .collect(Collectors.toSet());
                assertEquals(expectedBrokers, replicaIds,
                    "Replicas should include one broker from each rack (az1=node0, az2=node1)");

                // Verify all replicas are in ISR initially
                Set<Integer> isrIds = partition.isr().stream()
                    .map(Node::id)
                    .collect(Collectors.toSet());
                assertEquals(replicaIds, isrIds,
                    "All replicas should be in ISR initially");

                // Verify leader is one of the replicas
                assertTrue(replicaIds.contains(partition.leader().id()),
                    "Leader should be one of the replicas");
            }
        }

        // Produce and consume records
        final long now = System.currentTimeMillis();
        produceRecords(clientConfigs, numRecords, i -> {
            byte[] value = ("message-" + i).getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topicName, i % numPartitions, now, null, value);
        });

        List<TopicPartition> partitions = new ArrayList<>();
        for (int i = 0; i < numPartitions; i++) {
            partitions.add(new TopicPartition(topicName, i));
        }
        consumeWithAssign(clientConfigs, partitions, numRecords);
    }

    @Test
    public void produceAndConsumeWithClientAZ() throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.name());
        // Set client AZ via client.id prefix
        clientConfigs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "diskless_az=az1");

        String topicName = "az-aware-topic";
        int numRecords = 50;

        // Create diskless topic with managed replicas
        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic topic = new NewTopic(topicName, 1, (short) -1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
            result.all().get(30, TimeUnit.SECONDS);

            // Verify managed replicas: RF=2 with replicas on both brokers
            TopicDescription description = admin.describeTopics(Collections.singletonList(topicName))
                .allTopicNames().get(30, TimeUnit.SECONDS).get(topicName);
            TopicPartitionInfo partition = description.partitions().get(0);
            assertEquals(2, partition.replicas().size(),
                "Expected RF=2 for managed replicas with 2 racks");
            Set<Integer> replicaIds = partition.replicas().stream()
                .map(Node::id)
                .collect(Collectors.toSet());
            assertEquals(Set.of(0, 1), replicaIds,
                "Replicas should include one broker from each rack");

            // Verify AZ-aware routing: client hints az1, so transformer should select node 0 (az1) as leader
            assertEquals(0, partition.leader().id(),
                "Leader should be the az1 broker (node 0) for a client with diskless_az=az1");
        }

        // Produce and consume records with AZ-aware client
        produceRecords(clientConfigs, numRecords, i -> {
            byte[] value = ("az-message-" + i).getBytes(StandardCharsets.UTF_8);
            return new ProducerRecord<>(topicName, value);
        });

        consumeWithSubscribe(clientConfigs, topicName, numRecords);
    }

    @Test
    public void nonDisklessTopicDoesNotGetManagedReplicas() throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        String topicName = "regular-topic";
        short explicitRF = 1;

        // Create regular (non-diskless) topic with explicit RF=1
        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic topic = new NewTopic(topicName, 2, explicitRF);
            // Explicitly NOT setting diskless.enable=true
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
            result.all().get(30, TimeUnit.SECONDS);

            // Verify the topic was created with the explicit RF=1, not managed replicas RF=2
            // Wait for metadata propagation across brokers before describing
            TopicDescription description = waitForTopicDescription(admin, topicName);
            log.info("Regular topic {} created", topicName);

            for (TopicPartitionInfo partition : description.partitions()) {
                log.info("Partition {}: leader={}, replicas={}",
                    partition.partition(), partition.leader().id(),
                    partition.replicas().stream().map(Node::id).toList());

                // Non-diskless topic should keep the explicit RF=1
                assertEquals(1, partition.replicas().size(),
                    "Non-diskless topic should use explicit RF, not managed replicas");
            }
        }
    }

    @Test
    public void produceRecoversAfterPostgresReplacement() throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.ACKS_CONFIG, "all");
        clientConfigs.put(ProducerConfig.LINGER_MS_CONFIG, "0");
        clientConfigs.put(ProducerConfig.RETRIES_CONFIG, "10");
        clientConfigs.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "250");
        clientConfigs.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");
        clientConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "30000");
        String topicName = "diskless-pg-replacement-topic";
        int partition = 0;

        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic disklessTopic = new NewTopic(topicName, 1, (short) -1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            CreateTopicsResult topics = admin.createTopics(List.of(disklessTopic));
            topics.all().get(10, TimeUnit.SECONDS);
        }

        // Warm up the broker-side Hikari pool so it has connections to the original PostgreSQL process.
        final RecordMetadata beforeReplacement = produceRecord(clientConfigs, topicName, partition, "before-pg-restart");
        assertEquals(topicName, beforeReplacement.topic());
        assertEquals(partition, beforeReplacement.partition());

        // Replace PostgreSQL while preserving its logical database state, like a coordinator failover.
        final int oldPostgresPort = RotatingPostgresSocketFactory.dnsEndpoint().port();
        final String postgresDataDump = dumpPostgresData();
        pgContainer.stop();
        try (BlackholeServer blackholeServer = new BlackholeServer(oldPostgresPort)) {
            assertTrue(blackholeServer.isOpen());
            pgContainer.start();
            restorePostgresData(postgresDataDump);

            // Keep the client-side DNS cache pinned to the old endpoint even after DNS rotates.
            RotatingPostgresSocketFactory.cacheCurrentDns(Duration.ofMinutes(10));
            RotatingPostgresSocketFactory.rotateDns(pgContainer.getHost(), pgContainer.getMappedPort(5432));
            assertEquals(pgContainer.getMappedPort(5432), RotatingPostgresSocketFactory.dnsEndpoint().port());

            // The stale endpoint must fail quickly with a broker-side storage error, not a client timeout.
            final Map<String, Object> noRetryClientConfigs = new HashMap<>(clientConfigs);
            noRetryClientConfigs.put(ProducerConfig.RETRIES_CONFIG, "0");
            assertProduceFailsWithStorageError(noRetryClientConfigs, topicName, partition, "stale-endpoint-attempt", Duration.ofSeconds(5));
            RotatingPostgresSocketFactory.expireCachedDns();

            // Once DNS is refreshed, new produce attempts should reconnect to the replacement PostgreSQL process.
            final RecordMetadata afterReplacement =
                produceRecordEventually(clientConfigs, topicName, partition, "fresh-producer-after-pg-replacement", Duration.ofSeconds(8));
            assertEquals(topicName, afterReplacement.topic());
            assertEquals(partition, afterReplacement.partition());
        }
    }

    private void produceRecords(Map<String, Object> configs, int numRecords,
                                IntFunction<ProducerRecord<byte[], byte[]>> recordFactory) {
        AtomicInteger recordsProduced = new AtomicInteger();
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(configs)) {
            for (int i = 0; i < numRecords; i++) {
                producer.send(recordFactory.apply(i), (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record", exception);
                    } else {
                        recordsProduced.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }
        assertEquals(numRecords, recordsProduced.get());
    }

    private void consumeWithAssign(Map<String, Object> configs, List<TopicPartition> partitions, int numRecords) {
        int recordsConsumed = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs)) {
            consumer.assign(partitions);
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(5));
                recordsConsumed += poll.count();
                if (recordsConsumed >= numRecords) break;
            }
        }
        assertEquals(numRecords, recordsConsumed);
    }

    private void consumeWithSubscribe(Map<String, Object> configs, String topic, int numRecords) {
        Map<String, Object> consumerConfigs = new HashMap<>(configs);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        int recordsConsumed = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(Collections.singletonList(topic));
            for (int i = 0; i < 10; i++) {
                ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(5));
                recordsConsumed += poll.count();
                if (recordsConsumed >= numRecords) break;
            }
        }
        assertEquals(numRecords, recordsConsumed);
    }

    private static String rotatingPostgresJdbcUrl() {
        final String socketFactoryParam =
            "socketFactory=" + InklessManagedReplicasClusterTest.RotatingPostgresSocketFactory.class.getName();
        final String rewrittenUrl = pgContainer.getJdbcUrl()
            .replaceFirst("//[^/]+", "//" + ROTATING_POSTGRES_HOST + ":" + ROTATING_POSTGRES_PORT);
        final String separator = rewrittenUrl.contains("?") ? "&" : "?";
        return rewrittenUrl + separator + socketFactoryParam;
    }

    private static String dumpPostgresData() throws Exception {
        final org.testcontainers.containers.Container.ExecResult result = pgContainer.execInContainer(
            "pg_dump",
            "-U", PostgreSQLTestContainer.USERNAME,
            "--inserts",
            "--column-inserts",
            "--no-owner",
            "--no-privileges",
            postgresDatabaseName()
        );
        if (result.getExitCode() != 0) {
            throw new AssertionError("Failed to dump PostgreSQL data: " + result.getStderr());
        }
        return result.getStdout();
    }

    private static void restorePostgresData(final String postgresDataDump) throws Exception {
        pgContainer.copyFileToContainer(
            org.testcontainers.images.builder.Transferable.of(postgresDataDump.getBytes(StandardCharsets.UTF_8)),
            "/tmp/inkless-data.sql"
        );

        final org.testcontainers.containers.Container.ExecResult result = pgContainer.execInContainer(
            "psql",
            "-v", "ON_ERROR_STOP=1",
            "-U", PostgreSQLTestContainer.USERNAME,
            "-d", postgresDatabaseName(),
            "-f", "/tmp/inkless-data.sql"
        );
        if (result.getExitCode() != 0) {
            throw new AssertionError("Failed to restore PostgreSQL data: " + result.getStderr());
        }
    }

    private static String postgresDatabaseName() {
        final String jdbcUrl = pgContainer.getJdbcUrl();
        final int databaseNameStart = jdbcUrl.indexOf('/', "jdbc:postgresql://".length()) + 1;
        final int queryStart = jdbcUrl.indexOf('?', databaseNameStart);
        if (queryStart == -1) {
            return jdbcUrl.substring(databaseNameStart);
        }
        return jdbcUrl.substring(databaseNameStart, queryStart);
    }

    private static class BlackholeServer implements AutoCloseable {
        private final ServerSocket serverSocket;
        private final List<Socket> sockets = Collections.synchronizedList(new ArrayList<>());
        private final Thread acceptThread;
        private volatile boolean closed = false;

        BlackholeServer(int port) throws IOException {
            serverSocket = new ServerSocket(port);
            acceptThread = new Thread(this::acceptConnections, "postgres-blackhole");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }

        boolean isOpen() {
            return !serverSocket.isClosed();
        }

        private void acceptConnections() {
            while (!closed) {
                try {
                    sockets.add(serverSocket.accept());
                } catch (IOException e) {
                    if (!closed) {
                        log.warn("Postgres blackhole accept loop failed", e);
                    }
                }
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            serverSocket.close();
            synchronized (sockets) {
                for (Socket socket : sockets) {
                    socket.close();
                }
            }
        }
    }

    public static class RotatingPostgresSocketFactory extends javax.net.SocketFactory {
        private static final AtomicReference<Endpoint> DNS_ENDPOINT = new AtomicReference<>();
        private static final AtomicReference<CachedEndpoint> CACHED_ENDPOINT = new AtomicReference<>();
        private static volatile long cacheTtlMs = 0L;

        public static void configure(String host, int port, Duration ttl) {
            final Endpoint endpoint = new Endpoint(host, port);
            DNS_ENDPOINT.set(endpoint);
            CACHED_ENDPOINT.set(new CachedEndpoint(endpoint, System.currentTimeMillis()));
            cacheTtlMs = ttl.toMillis();
        }

        public static void rotateDns(String host, int port) {
            DNS_ENDPOINT.set(new Endpoint(host, port));
        }

        public static void cacheCurrentDns(Duration ttl) {
            final Endpoint endpoint = DNS_ENDPOINT.get();
            if (endpoint == null) {
                throw new IllegalStateException("Rotating Postgres DNS endpoint has not been configured");
            }
            CACHED_ENDPOINT.set(new CachedEndpoint(endpoint, System.currentTimeMillis()));
            cacheTtlMs = ttl.toMillis();
        }

        public static void expireCachedDns() {
            CACHED_ENDPOINT.set(null);
            cacheTtlMs = 0L;
        }

        public static Endpoint dnsEndpoint() {
            return DNS_ENDPOINT.get();
        }

        @Override
        public Socket createSocket() throws IOException {
            return connectSocket(null, 0);
        }

        @Override
        public Socket createSocket(String host, int port) throws IOException {
            return connectSocket(null, 0);
        }

        @Override
        public Socket createSocket(String host, int port, InetAddress localHost, int localPort) throws IOException {
            return connectSocket(localHost, localPort);
        }

        @Override
        public Socket createSocket(InetAddress host, int port) throws IOException {
            return connectSocket(null, 0);
        }

        @Override
        public Socket createSocket(InetAddress address, int port, InetAddress localAddress, int localPort) throws IOException {
            return connectSocket(localAddress, localPort);
        }

        private static Socket connectSocket(InetAddress localAddress, int localPort) throws IOException {
            final Endpoint endpoint = resolveCachedEndpoint();
            final Socket socket = new Socket();
            if (localAddress != null) {
                socket.bind(new InetSocketAddress(localAddress, localPort));
            }
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()));
            return socket;
        }

        private static Endpoint resolveCachedEndpoint() {
            final long now = System.currentTimeMillis();
            final CachedEndpoint cached = CACHED_ENDPOINT.get();
            if (cached != null && now - cached.cachedAtMs() < cacheTtlMs) {
                return cached.endpoint();
            }

            final Endpoint endpoint = DNS_ENDPOINT.get();
            if (endpoint == null) {
                throw new IllegalStateException("Rotating Postgres DNS endpoint has not been configured");
            }
            CACHED_ENDPOINT.set(new CachedEndpoint(endpoint, now));
            return endpoint;
        }
    }

    public record Endpoint(String host, int port) {
    }

    private record CachedEndpoint(Endpoint endpoint, long cachedAtMs) {
    }

    private static RecordMetadata produceRecord(Map<String, Object> clientConfigs,
                                                String topicName,
                                                int partition,
                                                String value) throws Exception {
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(clientConfigs)) {
            final ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topicName, partition, null, value.getBytes(StandardCharsets.UTF_8));
            return producer.send(record).get(30, TimeUnit.SECONDS);
        }
    }

    private static void assertProduceFailsWithStorageError(Map<String, Object> clientConfigs,
                                                           String topicName,
                                                           int partition,
                                                           String value,
                                                           Duration timeout) {
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(clientConfigs)) {
            final ProducerRecord<byte[], byte[]> record =
                new ProducerRecord<>(topicName, partition, null, value.getBytes(StandardCharsets.UTF_8));
            final ExecutionException exception = assertThrows(
                ExecutionException.class,
                () -> producer.send(record).get(timeout.toMillis(), TimeUnit.MILLISECONDS)
            );
            assertInstanceOf(KafkaStorageException.class, exception.getCause());
        }
    }

    private static RecordMetadata produceRecordEventually(Map<String, Object> clientConfigs,
                                                          String topicName,
                                                          int partition,
                                                          String value,
                                                          Duration timeout) throws Exception {
        final long deadlineMs = System.currentTimeMillis() + timeout.toMillis();
        Exception lastException = null;
        while (System.currentTimeMillis() < deadlineMs) {
            try {
                return produceRecord(clientConfigs, topicName, partition, value);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof org.apache.kafka.common.errors.TimeoutException) {
                    throw e;
                }
                lastException = e;
                Thread.sleep(250);
            }
        }
        throw new AssertionError("Timed out waiting for produce to recover after PostgreSQL replacement", lastException);
    }

    // In multi-broker clusters, metadata propagation from the controller to follower brokers is async.
    // describeTopics may hit a broker that hasn't received the update yet, causing UnknownTopicOrPartitionException.
    private TopicDescription waitForTopicDescription(Admin admin, String topicName) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return admin.describeTopics(Collections.singletonList(topicName))
                    .allTopicNames().get(30, TimeUnit.SECONDS).get(topicName);
            } catch (ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)
                    || System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                Thread.sleep(200);
            }
        }
    }
}
