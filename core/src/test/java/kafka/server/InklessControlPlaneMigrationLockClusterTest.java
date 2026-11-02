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
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlaneAvailability;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlane;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneConfig;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.storage_backend.s3.S3StorageConfig;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;
import io.aiven.inkless.test_utils.S3TestContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cluster coverage for a broker that starts while another node holds the schema migration lock.
 * The test takes Flyway's advisory lock from its own session and then restarts one of two brokers.
 * Each test checks what the brokers serve while the test holds the lock, and after the test
 * releases it or the migration times out.
 */
@Testcontainers
public class InklessControlPlaneMigrationLockClusterTest {
    private static final String TOPIC_NAME = "migration-lock-topic";
    private static final String CLASSIC_TOPIC_NAME = "migration-lock-classic-topic";
    /**
     * Flyway's Postgres advisory lock key for the default schema history table. Flyway derives it
     * from a fixed base plus the hash of the quoted table name, so it matches the lock every broker
     * contends for in {@code Migrations.migrate}.
     */
    private static final long FLYWAY_MIGRATION_LOCK_KEY =
        77431708279161L + "\"public\".\"flyway_schema_history\"".hashCode();
    private static final String CONTROL_PLANE_PREFIX = InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX;

    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private KafkaClusterTestKit cluster;
    private CompletableFuture<Void> restart;

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
    }

    @AfterEach
    public void teardown() throws Exception {
        try {
            if (restart != null) {
                // `BrokerServer.shutdown()` does nothing while startup is in progress, so closing the
                // cluster first would stop the controller and delete directories underneath the
                // restarting broker. The test has released the lock by now, so startup can finish.
                restart.exceptionally(e -> null).get(120, TimeUnit.SECONDS);
            }
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void startingBrokerStaysFencedWhileMigrationLockIsHeldAndServesDisklessAfterRelease() throws Exception {
        startCluster(120_000);
        final BrokerServer running = cluster.brokers().get(0);
        final BrokerServer starting = cluster.brokers().get(1);
        final Uuid topicId = createTopics(CLASSIC_TOPIC_NAME, running.config().brokerId());

        starting.shutdown();
        starting.awaitShutdown();

        try (Connection lock = holdMigrationLock()) {
            restart = CompletableFuture.runAsync(starting::startup);
            // RECOVERY means the broker has caught up on metadata and waits for the controller to
            // unfence it. The broker builds the control plane between those two steps.
            TestUtils.waitForCondition(() -> starting.brokerState() == BrokerState.RECOVERY, 60_000,
                "The starting broker must catch up on metadata and wait for the control plane");
            final ControlPlaneAvailability availability = starting.sharedServer().inklessControlPlaneAvailability().get();

            // Flyway retries the lock once a second, so this spans several attempts.
            Thread.sleep(5_000);
            assertFalse(restart.isDone(), "Startup must wait for the migration lock");
            assertEquals(BrokerState.RECOVERY, starting.brokerState());
            assertTrue(isFenced(starting.config().brokerId()), "The starting broker must stay fenced");
            assertFalse(availability.isAvailable());

            final String runningBootstrap = "localhost:" + InklessDisklessRequestClient.brokerPort(running);
            produceAndConsumeClassic(runningBootstrap, CLASSIC_TOPIC_NAME);
        }

        restart.get(60, TimeUnit.SECONDS);
        cluster.waitForReadyBrokers();
        assertFalse(isFenced(starting.config().brokerId()));
        assertTrue(starting.sharedServer().inklessControlPlaneAvailability().get().isAvailable());
        assertEquals(Errors.NONE, InklessDisklessRequestClient.produce(InklessDisklessRequestClient.brokerPort(starting), topicId, 0));
    }

    @Test
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void startingBrokerServesClassicAndGatesDisklessWhenMigrationOutlastsItsTimeout() throws Exception {
        final long migrationTimeoutMs = 5_000;
        startCluster(migrationTimeoutMs);
        final BrokerServer starting = cluster.brokers().get(1);
        // The classic topic lives only on the restarted broker, so classic traffic reaches it.
        final Uuid topicId = createTopics(CLASSIC_TOPIC_NAME, starting.config().brokerId());

        starting.shutdown();
        starting.awaitShutdown();

        try (Connection lock = holdMigrationLock()) {
            // The build gives up after `migration.timeout.ms`, so startup finishes without a delegate.
            restart = CompletableFuture.runAsync(starting::startup);
            restart.get(120, TimeUnit.SECONDS);
            cluster.waitForReadyBrokers();
            assertFalse(isFenced(starting.config().brokerId()));

            final ControlPlaneAvailability availability = starting.sharedServer().inklessControlPlaneAvailability().get();
            assertEquals(ControlPlaneAvailability.State.UNKNOWN, availability.state());

            produceAndConsumeClassic(cluster.bootstrapServers(), CLASSIC_TOPIC_NAME);

            final int port = InklessDisklessRequestClient.brokerPort(starting);
            assertEquals(Errors.KAFKA_STORAGE_ERROR, InklessDisklessRequestClient.produce(port, topicId, 0));
            // The reconciler retries in the background, and each retry times out on the same lock.
            Thread.sleep(2 * migrationTimeoutMs);
            assertFalse(availability.isAvailable());
            assertEquals(Errors.KAFKA_STORAGE_ERROR, InklessDisklessRequestClient.produce(port, topicId, 0));
        }
    }

    private void startCluster(final long migrationTimeoutMs) throws Exception {
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(false)
            .setNumBrokerNodes(2)
            .setNumControllerNodes(1)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            .setConfigProp(CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.MIGRATION_TIMEOUT_MS_CONFIG, String.valueOf(migrationTimeoutMs))
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

    /** Creates the diskless topic, and the classic topic with its only replica on {@code classicBrokerId}. */
    private Uuid createTopics(final String classicTopic, final int classicBrokerId) throws Exception {
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()))) {
            admin.createTopics(List.of(
                new NewTopic(TOPIC_NAME, 1, (short) 1).configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true")),
                new NewTopic(classicTopic, Map.of(0, List.of(classicBrokerId))))).all().get(30, TimeUnit.SECONDS);
            return admin.describeTopics(List.of(TOPIC_NAME)).allTopicNames().get(30, TimeUnit.SECONDS)
                .get(TOPIC_NAME).topicId();
        }
    }

    /** Takes the lock on a session of its own. Closing the returned connection releases it. */
    private static Connection holdMigrationLock() throws SQLException {
        final Connection connection = DriverManager.getConnection(
            pgContainer.getJdbcUrl(), PostgreSQLTestContainer.USERNAME, PostgreSQLTestContainer.PASSWORD);
        try (Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(" + FLYWAY_MIGRATION_LOCK_KEY + ")");
        } catch (final SQLException e) {
            connection.close();
            throw e;
        }
        return connection;
    }

    private boolean isFenced(final int brokerId) throws Exception {
        final String bootstrap = "localhost:" + InklessDisklessRequestClient.brokerPort(cluster.brokers().get(0));
        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrap))) {
            return admin.describeCluster(new DescribeClusterOptions().includeFencedBrokers(true))
                .nodes().get(30, TimeUnit.SECONDS).stream()
                .filter(node -> node.id() == brokerId)
                .anyMatch(Node::isFenced);
        }
    }

    private static void produceAndConsumeClassic(final String bootstrap, final String topic) throws Exception {
        final TopicPartition partition = new TopicPartition(topic, 0);
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            producer.send(new ProducerRecord<>(topic, 0, null, "value")).get(30, TimeUnit.SECONDS);
        }
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()))) {
            consumer.assign(List.of(partition));
            consumer.seekToBeginning(List.of(partition));
            final AtomicInteger consumed = new AtomicInteger();
            TestUtils.waitForCondition(() -> consumed.addAndGet(consumer.poll(Duration.ofMillis(500)).count()) > 0,
                30_000, "Classic consume must return the produced record from " + partition);
        }
    }
}
