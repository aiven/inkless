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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;
import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@Testcontainers
public class InklessConsolidatedDisklessTopicsTest {

    private static final Logger log = LoggerFactory.getLogger(InklessConsolidatedDisklessTopicsTest.class);

    private static final String RSM_CONFIG_PREFIX = RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;

    private static final String RSM_OBJECT_KEY_PREFIX = "tsu-int-test/";

    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private KafkaClusterTestKit cluster;
    private String topicName = "consolidated-diskless-topic";
    private int numPartitions = 2;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

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
            // Allow enabling diskless on an already existing (classic) topic, i.e. classic -> diskless migration
            .setConfigProp(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, "true")
            // Enable diskless topic consolidation (diskless.enable + remote.storage.enable on new topics)
            .setConfigProp(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true")
            // Run consolidated diskless WAL pruning frequently enough for the test wait windows
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONSOLIDATION_CLEANUP_INTERVAL_MS_CONFIG, 5000)
            // PG control plane config
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            // S3 storage config
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            // Tiered storage configs
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "5000")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP, "4")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, TopicBasedRemoteLogMetadataManager.class.getName())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "EXTERNAL")
            .setConfigProp(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
                + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, "1")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "io.aiven.kafka.tieredstorage.RemoteStorageManager")
            .setConfigProp(RSM_CONFIG_PREFIX + "chunk.size", 1048576)
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.class", "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.path",
                Files.createTempDirectory("ts-fetch-chunk-cache").toAbsolutePath().toString())
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.size", "10737418")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.prefetch.max.size", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.retention.ms", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "custom.metadata.fields.include", "REMOTE_SIZE")
            .setConfigProp(RSM_CONFIG_PREFIX + "key.prefix", "tsu-int-test/")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.bucket.name", s3Container.getBucketName())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.region", s3Container.getRegion())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.endpoint.url", s3Container.getEndpoint())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.path.style.access.enabled", "true")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.aws.access.key.id", s3Container.getAccessKey())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.aws.secret.access.key", s3Container.getSecretKey())
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
    public void testNewConsolidatedDisklessTopics() throws Exception {
        Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        Map<String, String> topicConfigs = Map.of(
            DISKLESS_ENABLE_CONFIG, "true",
            REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
            SEGMENT_BYTES_CONFIG, "1048576"
        );

        final Uuid topicUuid;
        try (Admin admin = AdminClient.create(commonConfigs)) {
            final NewTopic topic = new NewTopic(topicName, numPartitions, (short) -1).configs(topicConfigs);
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
            result.all().get(30, TimeUnit.SECONDS);

            final TopicDescription[] descriptionHolder = new TopicDescription[1];
            TestUtils.waitForCondition(() -> {
                try {
                    descriptionHolder[0] = admin.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get(30, TimeUnit.SECONDS).get(topicName);
                    return descriptionHolder[0].partitions().size() == numPartitions;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        return false;
                    }
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }, 60_000, () -> "Topic should become visible with " + numPartitions + " partitions");
            TopicDescription description = descriptionHolder[0];
            assertEquals(numPartitions, description.partitions().size());
            topicUuid = description.topicId();

            Set<Integer> expectedBrokers = Set.of(0, 1);
            for (TopicPartitionInfo partition : description.partitions()) {
                assertEquals(2, partition.replicas().size(),
                    "Expected RF=2 for managed replicas with consolidation enabled");
                Set<Integer> replicaIds = partition.replicas().stream().map(Node::id).collect(Collectors.toSet());
                assertEquals(expectedBrokers, replicaIds);
            }
        }

        int recordsToSendAndReceive = 50;

        produceRecords(commonConfigs, recordsToSendAndReceive, i -> {
            String value = TestUtils.randomString(104_857);
            return new ProducerRecord<>(topicName, i % numPartitions, null, value);
        });

        try (S3Client s3 = s3Container.getS3Client()) {
            final String bucket = s3Container.getBucketName();
            TestUtils.waitForCondition(() -> countObjectsWithPrefix(s3, bucket, RSM_OBJECT_KEY_PREFIX) > 0,
                120_000,
                () -> "Expected at least one tiered object in the Minio bucket after produce");
        }

        ControlPlaneDisklessSnapshot beforeConsumeSnapshot = readControlPlaneDisklessSnapshot(topicUuid);
        log.info("Control plane snapshot before first consume: {}", beforeConsumeSnapshot);

        assertBrokerEarliestOffsetsEqual(commonConfigs, 0L,
            "before first consume (expect full 0..N-1 readable with auto.offset.reset=earliest)");

        consumeAndVerify(commonConfigs, recordsToSendAndReceive);

        waitUntilTieredDisklessDataPrunedFromControlPlane(topicUuid, beforeConsumeSnapshot);

        consumeAndVerify(commonConfigs, recordsToSendAndReceive);
    }

    @Test
    public void testClassicToConsolidatedDisklessTopicSwitch() throws Exception {
        Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        // Records produced into the topic as a classic topic, before any migration.
        final int preMigrationRecords = 10;
        // Records produced concurrently while the topic is being consolidated.
        final int duringConsolidationRecords = 50;
        final int totalRecords = preMigrationRecords + duringConsolidationRecords;
        // Large values to force segment rolls so the consolidated WAL is actually tiered to remote storage.
        final IntFunction<ProducerRecord<String, String>> recordFactory = i ->
            new ProducerRecord<>(topicName, i % numPartitions, null, TestUtils.randomString(104_857));

        final Uuid topicUuid;
        try (Admin admin = AdminClient.create(commonConfigs)) {
            // Phase 1: create a CLASSIC topic (diskless.enable=false, remote.storage.enable=false).
            topicUuid = createClassicTopic(admin);
            assertEquals("false", TopicMetadataProbe.readValue(admin, topicName, DISKLESS_ENABLE_CONFIG),
                "Topic should start as a classic (non-diskless) topic");
            assertEquals("false", TopicMetadataProbe.readValue(admin, topicName, REMOTE_LOG_STORAGE_ENABLE_CONFIG),
                "Classic topic should not have remote storage enabled");

            // Produce a baseline of records into the classic topic. These offsets form the classic
            // prefix that must remain readable after the topic is migrated and consolidated.
            produceRecords(commonConfigs, preMigrationRecords, recordFactory);

            // Phase 2: migrate the classic topic to diskless while a producer keeps sending records.
            // With the consolidation flag enabled at the broker level, the switch auto-enables remote
            // storage atomically (like topic creation), so the topic becomes a consolidated diskless
            // topic in a single step. The producer runs on a background thread so its sends overlap
            // the switch and the background consolidation (WAL -> tiered storage) that follows.
            final AtomicReference<Throwable> producerFailure = new AtomicReference<>(null);
            final Thread producerThread = new Thread(() -> {
                try {
                    produceRecords(commonConfigs, duringConsolidationRecords, recordFactory);
                } catch (Throwable t) {
                    producerFailure.set(t);
                }
            }, "consolidation-producer");
            producerThread.setDaemon(true);

            producerThread.start();
            try {
                log.info("Migrating classic topic {} to diskless (remote storage auto-enabled atomically)", topicName);
                incrementalAlterTopicConfigs(admin, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
                TopicMetadataProbe.awaitValue(admin, topicName, DISKLESS_ENABLE_CONFIG, "true");
                TopicMetadataProbe.awaitValue(admin, topicName, REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
            } finally {
                producerThread.join(TimeUnit.SECONDS.toMillis(120));
                // If the producer is still running (e.g. a send blocked), interrupt it so it unwinds
                // instead of lingering. produceRecords propagates the interrupt and closes the producer.
                if (producerThread.isAlive()) {
                    producerThread.interrupt();
                    producerThread.join(TimeUnit.SECONDS.toMillis(10));
                }
            }
            assertFalse(producerThread.isAlive(), "Producer thread did not finish in time");
            if (producerFailure.get() != null) {
                throw new RuntimeException("Producer failed while the topic was being consolidated", producerFailure.get());
            }

            assertEquals("true", TopicMetadataProbe.readValue(admin, topicName, DISKLESS_ENABLE_CONFIG),
                "Topic should be diskless after the switch");
            assertEquals("true", TopicMetadataProbe.readValue(admin, topicName, REMOTE_LOG_STORAGE_ENABLE_CONFIG),
                "Switch should auto-enable remote storage atomically (consolidated diskless topic)");
        }

        // Consolidation copies the diskless WAL data into Kafka tiered storage; wait until at least one
        // object lands in the bucket as evidence that consolidation made progress.
        try (S3Client s3 = s3Container.getS3Client()) {
            final String bucket = s3Container.getBucketName();
            TestUtils.waitForCondition(() -> countObjectsWithPrefix(s3, bucket, RSM_OBJECT_KEY_PREFIX) > 0,
                120_000,
                () -> "Expected at least one tiered object in the Minio bucket after consolidation");
        }

        ControlPlaneDisklessSnapshot beforeConsumeSnapshot = readControlPlaneDisklessSnapshot(topicUuid);
        log.info("Control plane snapshot before first consume: {}", beforeConsumeSnapshot);

        // Everything (classic prefix + consolidated diskless records) must be readable from the beginning.
        assertBrokerEarliestOffsetsEqual(commonConfigs, 0L,
            "after consolidation (expect full 0..N-1 readable with auto.offset.reset=earliest)");

        consumeAndVerify(commonConfigs, totalRecords);

        // Once the consolidated WAL has been tiered and pruned, the same data must still be fully readable.
        waitUntilTieredDisklessDataPrunedFromControlPlane(topicUuid, beforeConsumeSnapshot);

        consumeAndVerify(commonConfigs, totalRecords);
    }

    private Uuid createClassicTopic(Admin admin) throws Exception {
        // Create the topic as a classic topic by relying on the broker defaults (diskless.enable=false,
        // remote.storage.enable=false). Setting both keys explicitly at creation time — even to false —
        // trips the diskless/remote-storage mutual-exclusion validation, so we deliberately omit them.
        final Map<String, String> classicConfigs = Map.of(
            CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
            SEGMENT_BYTES_CONFIG, "1048576"
        );
        final NewTopic topic = new NewTopic(topicName, numPartitions, (short) -1).configs(classicConfigs);
        admin.createTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);

        final TopicDescription[] descriptionHolder = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                descriptionHolder[0] = admin.describeTopics(Collections.singletonList(topicName))
                    .allTopicNames().get(30, TimeUnit.SECONDS).get(topicName);
                return descriptionHolder[0].partitions().size() == numPartitions;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                }
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }, 60_000, () -> "Classic topic should become visible with " + numPartitions + " partitions");
        return descriptionHolder[0].topicId();
    }

    private void incrementalAlterTopicConfigs(Admin admin, Map<String, String> configs)
        throws ExecutionException, InterruptedException, TimeoutException {
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        final List<AlterConfigOp> ops = configs.entrySet().stream()
            .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
            .collect(Collectors.toList());
        admin.incrementalAlterConfigs(Map.of(resource, ops)).all().get(30, TimeUnit.SECONDS);
    }


    /**
     * Queries {@link OffsetSpec#earliest()} via the admin client (same basis as consumer
     * {@code auto.offset.reset=earliest}) and asserts every partition matches {@code expectedEarliest}.
     *
     * <p>The cross-tier earliest offset is published asynchronously: once the WAL is tiered and
     * pruned, the leader's {@code RemoteLogManager} callback enqueues the remote log start offset and
     * {@code CrossTierLogStartReporter} flushes it to the control plane on its next interval. Wait for
     * that publication to converge before the hard assertion so the check does not race it.
     */
    private void assertBrokerEarliestOffsetsEqual(Map<String, Object> commonConfigs,
                                                  long expectedEarliest,
                                                  String context) throws Exception {
        // Let query exceptions propagate: transient errors during convergence (e.g. LEADER_NOT_AVAILABLE)
        // are retried by waitForCondition, and a persistent one is surfaced as the timeout's cause instead
        // of being swallowed. lastSeen carries the offsets into the message for the stuck-value case.
        final AtomicReference<Map<TopicPartition, Long>> lastSeen = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            Map<TopicPartition, Long> earliest = queryBrokerEarliestOffsets(commonConfigs);
            lastSeen.set(earliest);
            return earliest.values().stream().allMatch(offset -> Long.valueOf(expectedEarliest).equals(offset));
        }, 60_000, () -> "ListOffsets earliest for all partitions should converge to " + expectedEarliest
            + " " + context + "; last observed: " + lastSeen.get());

        Map<TopicPartition, Long> earliest = queryBrokerEarliestOffsets(commonConfigs);
        log.info("Broker ListOffsets earliest per partition {}: {}", context, earliest);
        for (int p = 0; p < numPartitions; p++) {
            TopicPartition tp = new TopicPartition(topicName, p);
            assertEquals(expectedEarliest, earliest.get(tp),
                "ListOffsets earliest for " + tp + " should be " + expectedEarliest + " " + context
                    + "; otherwise the consumer cannot read all partitions from the beginning.");
        }
    }

    private Map<TopicPartition, Long> queryBrokerEarliestOffsets(Map<String, Object> commonConfigs)
        throws ExecutionException, InterruptedException, TimeoutException {
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (int p = 0; p < numPartitions; p++) {
            request.put(new TopicPartition(topicName, p), OffsetSpec.earliest());
        }
        try (Admin admin = AdminClient.create(commonConfigs)) {
            Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> infos =
                admin.listOffsets(request).all().get(30, TimeUnit.SECONDS);
            Map<TopicPartition, Long> out = new HashMap<>();
            for (int p = 0; p < numPartitions; p++) {
                TopicPartition tp = new TopicPartition(topicName, p);
                out.put(tp, infos.get(tp).offset());
            }
            return out;
        }
    }

    private record ControlPlaneDisklessSnapshot(long batchRowCount, long minLogStartOffset) { }

    private boolean hasPrunedDisklessData(final ControlPlaneDisklessSnapshot snapshot) {
        return snapshot.batchRowCount() == 0 || snapshot.minLogStartOffset() > 0;
    }

    private static UUID toJavaUuid(Uuid topicId) {
        return new UUID(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
    }

    private ControlPlaneDisklessSnapshot readControlPlaneDisklessSnapshot(Uuid kafkaTopicId) throws SQLException {
        UUID id = toJavaUuid(kafkaTopicId);
        try (
            Connection connection = DriverManager.getConnection(
                pgContainer.getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD)
        ) {
            long batches;
            try (PreparedStatement ps = connection.prepareStatement(
                "SELECT COUNT(*) FROM batches WHERE topic_id = ?")) {
                ps.setObject(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    batches = rs.getLong(1);
                }
            }
            long minLogStart;
            try (PreparedStatement ps = connection.prepareStatement(
                "SELECT MIN(log_start_offset) FROM logs WHERE topic_id = ?")) {
                ps.setObject(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next() || rs.getObject(1) == null) {
                        minLogStart = 0L;
                    } else {
                        minLogStart = rs.getLong(1);
                    }
                }
            }
            return new ControlPlaneDisklessSnapshot(batches, minLogStart);
        }
    }

    /**
     * Waits until consolidated-diskless pruning removed WAL batches that are already represented in remote storage
     * (fewer rows in {@code batches}, or {@code logs.log_start_offset} advanced).
     */
    private void waitUntilTieredDisklessDataPrunedFromControlPlane(Uuid kafkaTopicId,
                                                                   ControlPlaneDisklessSnapshot baseline)
        throws InterruptedException {
        if (hasPrunedDisklessData(baseline)) {
            log.info("Diskless data was already pruned before waiting: baseline={}", baseline);
            return;
        }
        TestUtils.waitForCondition(() -> {
            try {
                ControlPlaneDisklessSnapshot current = readControlPlaneDisklessSnapshot(kafkaTopicId);
                log.info("Waiting for diskless prune in control plane: current={}, baseline={}", current, baseline);
                return hasPrunedDisklessData(current);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 120_000, () -> "Expected diskless WAL batches tiered to remote to be pruned from control plane "
            + "(batch count should become 0 or min(log_start_offset) should become positive)");
    }

    private void produceRecords(Map<String, Object> commonConfigs, int numRecords,
                                        IntFunction<ProducerRecord<String, String>> recordFactory) {
        var producerConfigs = new HashMap<>(commonConfigs);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        long totalSize = 0;
        try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            for (int i = 0; i < numRecords; i++) {
                var record = recordFactory.apply(i);
                var metadata = producer.send(record).get();
                totalSize += metadata.serializedValueSize();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        log.info("Produced {} records in total size {}", numRecords, totalSize);
    }

    private void consumeAndVerify(Map<String, Object> commonConfigs, int expectedTotalRecords) throws InterruptedException {
        var consumerConfigs = new HashMap<>(commonConfigs);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id-" + UUID.randomUUID());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(consumerConfigs)) {
            consumer.subscribe(Collections.singletonList(topicName));
            var offsetsByPartition = new HashMap<TopicPartition, List<Long>>();
            var consumedCount = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                var records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(r -> {
                    var tp = new TopicPartition(r.topic(), r.partition());
                    offsetsByPartition.computeIfAbsent(tp, __ -> new ArrayList<>()).add(r.offset());
                });
                consumedCount.addAndGet(records.count());
                return consumedCount.get() >= expectedTotalRecords;
            }, 60_000, "Not all records have been consumed");
            // Monotonicity + no gaps per partition
            offsetsByPartition.forEach((tp, offsets) -> {
                for (int i = 1; i < offsets.size(); i++) {
                    long prev = offsets.get(i - 1);
                    long cur = offsets.get(i);
                    assertEquals(prev + 1, cur, "Offset gap or reordering for " + tp);
                }
            });
        }
    }

    private static int countObjectsWithPrefix(S3Client s3, String bucket, String prefix) {
        int total = 0;
        String continuationToken = null;
        ListObjectsV2Response page;
        do {
            ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
            if (continuationToken != null) {
                b.continuationToken(continuationToken);
            }
            page = s3.listObjectsV2(b.build());
            total += page.contents().size();
            continuationToken = page.isTruncated() ? page.nextContinuationToken() : null;
        } while (continuationToken != null);
        return total;
    }
}
