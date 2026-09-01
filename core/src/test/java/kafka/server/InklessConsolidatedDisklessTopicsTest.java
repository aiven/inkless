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
import org.apache.kafka.clients.admin.DeleteRecordsResult;
import org.apache.kafka.clients.admin.DeletedRecords;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
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
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.Errors;
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
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import static org.apache.kafka.common.config.TopicConfig.MAX_MESSAGE_BYTES_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    @TempDir
    private Path tempCacheDir;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        // Create 2-broker cluster with rack assignments (matching docker compose setup)
        // Node 0: combined broker+controller, Node 1: broker-only.
        Map<Integer, Map<String, String>> perServerProps = new HashMap<>();
        // Per-broker cache dir: DiskChunkCache wipes its path on startup, so a shared dir races.
        for (int brokerId = 0; brokerId < 2; brokerId++) {
            perServerProps.put(brokerId, Map.of(
                ServerConfigs.BROKER_RACK_CONFIG, "az" + (brokerId + 1),
                RSM_CONFIG_PREFIX + "fetch.chunk.cache.path",
                Files.createDirectories(tempCacheDir.resolve("ts-fetch-chunk-cache-" + brokerId)).toAbsolutePath().toString()));
        }
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
            // fetch.chunk.cache.path is set per broker in perServerProps (must be broker-exclusive).
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
        final IntFunction<ProducerRecord<String, String>> recordFactory = largeValueRecordFactory();

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
                switchTopicToConsolidatedDiskless(admin);
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
        waitForAtLeastOneTieredObject("after consolidation");

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

    /**
     * KC-234: an idle switched (classic→diskless) topic must tier its classic tail.
     *
     * <p>Switching seals the partition at offset {@code S} and force-rolls the active segment
     * so the boundary {@code [base_active, S)} becomes a closed tiering candidate. This test
     * verifies that even with no post-switch appends, {@code latest-tiered} reaches the seal.
     */
    @Test
    public void testIdleSwitchedTopicTiersClassicTail() throws Exception {
        // Single partition so seal == preSwitchRecords (no per-partition seal math).
        numPartitions = 1;
        topicName = "idle-switched-classic-tail";

        final Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        // 30 × ~104 KB ≈ 3 MiB → ~3-4 segments at segment.bytes=1 MiB; the last is the boundary
        // [base_active, seal) that KC-234 says never tiers.
        final int preSwitchRecords = 30;
        final long seal = preSwitchRecords;

        try (Admin admin = AdminClient.create(commonConfigs)) {
            createClassicTopic(admin);
            produceRecords(commonConfigs, preSwitchRecords, largeValueRecordFactory());
            switchTopicToConsolidatedDiskless(admin);
            // The idle case: no post-switch appends. The fix in seal() force-rolls the boundary
            // segment so it becomes a closed tiering candidate even without further appends.
        }

        // Evidence that RLM is running: the closed classic segments tier normally.
        waitForAtLeastOneTieredObject("after the switch (closed classic segments)");

        // The boundary segment must also tier for the classic tail to be durable in remote.
        // latest-tiered == highestOffsetInRemoteStorage, which is the last record offset in the
        // last tiered segment. When the boundary [base_active, seal) is tiered, this equals
        // seal - 1 (the last record before the LEO).
        final TopicPartition tp = new TopicPartition(topicName, 0);
        final long lastTieredOffset = seal - 1;
        final AtomicLong lastSeenTiered = new AtomicLong(-1L);
        try (Admin admin = AdminClient.create(commonConfigs)) {
            TestUtils.waitForCondition(() -> {
                long latestTiered = admin.listOffsets(Map.of(tp, OffsetSpec.latestTiered()))
                    .all().get(10, TimeUnit.SECONDS).get(tp).offset();
                lastSeenTiered.set(latestTiered);
                return latestTiered >= lastTieredOffset;
            }, 60_000, () -> "KC-234: latest-tiered (" + lastSeenTiered.get()
                + ") never reached the last record offset (" + lastTieredOffset
                + "); the idle switched topic's classic tail [base_active, " + seal
                + ") was not tiered");
        }
    }

    /**
     * Cross-tier {@code DeleteRecords} on a switched consolidated topic must advance the earliest to
     * exactly the requested boundary per partition, never over-reclaim the surviving classic prefix down
     * to the seal, and honor the forward-only control-plane advance.
     *
     * <p>This exercises paths the single-partition ducktape system tests do not: two partitions across two
     * AZs with managed replicas, a different delete boundary per partition, and repeated/idempotent deletes.
     * It also asserts the cross-tier earliest bootstraps to {@code 0} in the control plane (never NULL),
     * the invariant the reclaim floor and become-leader report depend on.
     */
    @Test
    public void testCrossTierDeleteRecordsOnConsolidatedTopic() throws Exception {
        topicName = "cross-tier-delete-records";
        // Even split so each partition's seal and offsets are deterministic: 40 records over 2 partitions
        // means offsets [0, 20) per partition, so the switch seals each partition at 20.
        final int totalRecords = 40;
        final long sealPerPartition = totalRecords / numPartitions;

        final Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        final Uuid topicUuid;
        try (Admin admin = AdminClient.create(commonConfigs)) {
            topicUuid = createClassicTopic(admin);
            // Produce the whole classic prefix before switching so the per-partition seal is exactly
            // sealPerPartition; large values roll segments so the prefix tiers to remote.
            produceRecords(commonConfigs, totalRecords, largeValueRecordFactory());
            switchTopicToConsolidatedDiskless(admin);
        }

        // The classic prefix tiers to remote (the switch force-rolls the boundary segment).
        waitForAtLeastOneTieredObject("after the switch");

        // Everything is readable from 0, and the cross-tier earliest bootstrapped to 0 (not NULL, not the
        // seal): the value DeleteRecords advances forward from.
        assertBrokerEarliestOffsetsEqual(commonConfigs, 0L, "after the switch");
        assertRemoteLogStartOffsetBootstrapped(topicUuid, 0L);

        final TopicPartition p0 = new TopicPartition(topicName, 0);
        final TopicPartition p1 = new TopicPartition(topicName, 1);

        // 1) Per-partition boundaries, both below the seal and inside the remote classic prefix. The low
        // watermark is the stored cross-tier earliest, advanced synchronously by the control plane.
        try (Admin admin = AdminClient.create(commonConfigs)) {
            final Map<TopicPartition, Long> lw = deleteRecordsBefore(admin, Map.of(p0, 3L, p1, 5L));
            assertEquals(3L, lw.get(p0).longValue(), "DeleteRecords low watermark for " + p0);
            assertEquals(5L, lw.get(p1).longValue(), "DeleteRecords low watermark for " + p1);
        }
        // Earliest settles at exactly each boundary, not the seal: the reclaim floor / reported earliest is
        // the delete boundary, so the surviving classic prefix [boundary, seal) is preserved.
        assertBrokerEarliestOffsets(commonConfigs, Map.of(p0, 3L, p1, 5L), "after per-partition DeleteRecords");

        // 2) Monotonic advance: a higher boundary moves the earliest up on both partitions.
        try (Admin admin = AdminClient.create(commonConfigs)) {
            final Map<TopicPartition, Long> lw = deleteRecordsBefore(admin, Map.of(p0, 7L, p1, 7L));
            assertEquals(7L, lw.get(p0).longValue(), "monotonic advance low watermark for " + p0);
            assertEquals(7L, lw.get(p1).longValue(), "monotonic advance low watermark for " + p1);
        }

        // 3) Forward-only: a boundary below the current start is a no-op that returns the current start and
        // never moves the earliest backwards.
        try (Admin admin = AdminClient.create(commonConfigs)) {
            final Map<TopicPartition, Long> lw = deleteRecordsBefore(admin, Map.of(p0, 2L, p1, 2L));
            assertEquals(7L, lw.get(p0).longValue(), "below-start DeleteRecords must not move " + p0 + " back");
            assertEquals(7L, lw.get(p1).longValue(), "below-start DeleteRecords must not move " + p1 + " back");
        }
        assertBrokerEarliestOffsets(commonConfigs, Map.of(p0, 7L, p1, 7L),
            "after monotonic advance and below-start no-op");

        // No creep: give the RLM a couple of reclaim cycles and confirm the earliest stays at the delete
        // boundary rather than drifting up to the seal (the over-reclaim symptom this branch fixes).
        Thread.sleep(TimeUnit.SECONDS.toMillis(15));
        assertBrokerEarliestOffsets(commonConfigs, Map.of(p0, 7L, p1, 7L), "after waiting out RLM reclaim cycles");

        // The survivors [7, seal) on each partition are still readable and contiguous across the cut.
        final int survivorsPerPartition = (int) (sealPerPartition - 7);
        consumeAndVerify(commonConfigs, survivorsPerPartition * numPartitions);
    }

    /**
     * Diskless never held {@code [0, classicToDisklessStartOffset)}, so a consumer asking for an offset
     * below the seal must be answered from the classic log or the remote tier. A reply that starts at the
     * seal instead has silently dropped the range the consumer asked for.
     *
     * <p>The read is served by a replica that lost its local data and rebuilt it, which is the reachable
     * way a replica ends up below the seal: DR, a replaced log directory, or a reassignment target. The
     * switch itself cannot produce that state, since it is refused while the partition is
     * under-replicated.
     *
     * <p>Recovery of a small prefix outruns any attempt to route a consumer at the replica mid-catch-up,
     * so this does not observe the high watermark sitting below the seal. The consumer-frontier floors
     * that cover that window are unit-tested in {@code ReplicaManagerInklessTest}.
     */
    @Test
    public void testWipedReplicaServesBelowSealReadAtRequestedOffset() throws Exception {
        numPartitions = 1;
        topicName = "below-seal-catching-up-replica";
        final TopicPartition tp = new TopicPartition(topicName, 0);
        final int prefixRecords = 60;
        final long seal = prefixRecords;

        final Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        try (Admin admin = AdminClient.create(commonConfigs)) {
            createClassicTopic(admin);
            produceRecords(commonConfigs, prefixRecords, largeValueRecordFactory());
            switchTopicToConsolidatedDiskless(admin);

            // describeTopics reports the transformer-selected leader for a diskless topic, so take
            // leadership from the KRaft image.
            final var kraftPartition = cluster.brokers().get(0).metadataCache()
                .currentImage().topics().getTopic(topicName).partitions().get(0);
            final int targetBrokerId = kraftPartition.leader == 0 ? 1 : 0;
            log.info("KRaft leader is {}, wiping follower {}", kraftPartition.leader, targetBrokerId);

            cluster.brokers().get(targetBrokerId).shutdown();
            cluster.brokers().get(targetBrokerId).awaitShutdown();
            wipeLocalPartitionData(targetBrokerId, tp);
            cluster.brokers().get(targetBrokerId).startup();

            // client.rack pins the read to the wiped replica: the transformer advertises the alive
            // in-AZ replica as leader. auto.offset.reset=none keeps a broker-side out-of-range a test
            // failure instead of a silent seek elsewhere.
            final long belowSealOffset = seal - 5;
            final Map<String, Object> pinnedConfigs = new HashMap<>(commonConfigs);
            pinnedConfigs.put(ConsumerConfig.CLIENT_RACK_CONFIG, "az" + (targetBrokerId + 1));
            pinnedConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            pinnedConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            pinnedConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "below-seal-group-" + UUID.randomUUID());
            pinnedConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");

            log.info("Local log state on the recovering replica {} before the read: {}",
                targetBrokerId, localLogState(targetBrokerId, tp));

            try (var consumer = new KafkaConsumer<String, String>(pinnedConfigs)) {
                consumer.assign(Collections.singletonList(tp));
                consumer.seek(tp, belowSealOffset);
                final AtomicLong firstOffset = new AtomicLong(-1L);
                TestUtils.waitForCondition(() -> {
                    final var records = consumer.poll(Duration.ofMillis(500));
                    if (records.isEmpty()) {
                        return false;
                    }
                    firstOffset.set(records.iterator().next().offset());
                    return true;
                }, 120_000, () -> "A below-seal read was never served from the catching-up replica; state="
                    + localLogState(targetBrokerId, tp));
                assertEquals(belowSealOffset, firstOffset.get(),
                    "The below-seal read must resume at the requested offset, not skip to the seal; state="
                        + localLogState(targetBrokerId, tp));
            }
        }
    }

    /** Simulates the loss of a replica's local data: DR, a replaced log directory, or a fresh target. */
    private void wipeLocalPartitionData(int brokerId, TopicPartition tp) throws Exception {
        final String entryPrefix = tp.topic() + " " + tp.partition() + " ";
        for (String logDir : cluster.brokers().get(brokerId).config().logDirs()) {
            final Path partitionDir = Path.of(logDir, tp.topic() + "-" + tp.partition());
            if (Files.isDirectory(partitionDir)) {
                try (var paths = Files.walk(partitionDir)) {
                    paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
                log.info("Deleted local partition data at {}", partitionDir);
            }
            final Path checkpoint = Path.of(logDir, "replication-offset-checkpoint");
            if (Files.exists(checkpoint)) {
                final List<String> lines = Files.readAllLines(checkpoint);
                final List<String> kept = new ArrayList<>();
                int entries = 0;
                for (int i = 0; i < lines.size(); i++) {
                    if (i < 2) {
                        kept.add(lines.get(i));
                    } else if (!lines.get(i).startsWith(entryPrefix)) {
                        kept.add(lines.get(i));
                        entries++;
                    }
                }
                if (kept.size() > 1) {
                    kept.set(1, Integer.toString(entries));
                }
                Files.write(checkpoint, kept);
            }
        }
    }

    private record LocalLogState(long logStartOffset, long localLogStartOffset,
                                 long highWatermark, long logEndOffset) { }

    private LocalLogState localLogState(int brokerId, TopicPartition tp) {
        final var log = cluster.brokers().get(brokerId).logManager().getLog(tp, false);
        if (log.isEmpty()) {
            return null;
        }
        final var unifiedLog = log.get();
        return new LocalLogState(unifiedLog.logStartOffset(), unifiedLog.localLogStartOffset(),
            unifiedLog.highWatermark(), unifiedLog.logEndOffset());
    }

    /**
     * A consumer that negotiates Fetch below v11 must be able to read a consolidated diskless partition
     * from a replica that is not the KRaft leader.
     *
     * <p>The broker attaches client metadata to a fetch only from v11 onward, and without it
     * {@code FetchParams.fetchOnlyLeader} is true, so the local read is refused on a non-leader replica.
     * AZ-aware metadata advertises exactly such a replica as the leader, so before this branch a
     * pre-v11 consumer got NOT_LEADER_OR_FOLLOWER, refreshed metadata, was pointed back at the same
     * replica, and never progressed - while reporting no error on either side.
     *
     * <p>No Kafka client can reproduce this any more: every current client negotiates v11 or later. The
     * request is built by hand for that reason.
     */
    @Test
    public void testPreKip392ConsumerReadsConsolidatedPartitionFromReplica() throws Exception {
        numPartitions = 1;
        topicName = "pre-kip-392-consolidated-read";
        final TopicPartition tp = new TopicPartition(topicName, 0);
        final int recordCount = 20;

        final Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        try (Admin admin = AdminClient.create(commonConfigs)) {
            final NewTopic topic = new NewTopic(topicName, numPartitions, (short) -1).configs(Map.of(
                DISKLESS_ENABLE_CONFIG, "true",
                REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
                SEGMENT_BYTES_CONFIG, "1048576"));
            admin.createTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
        }
        produceRecords(commonConfigs, recordCount, largeValueRecordFactory());

        // Consolidation has to have copied the range into the local log and committed it, or the fetch
        // routes to Inkless and never reaches the leader-only check.
        final AtomicReference<LocalLogState> stateRef = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            for (int brokerId : new int[] {0, 1}) {
                final LocalLogState state = localLogState(brokerId, tp);
                if (state == null || state.highWatermark <= 0) {
                    return false;
                }
            }
            return true;
        }, 120_000, () -> "Both replicas must consolidate and commit the range before the fetch; broker 0="
            + localLogState(0, tp) + " broker 1=" + localLogState(1, tp));

        final var kraftPartition = cluster.brokers().get(0).metadataCache()
            .currentImage().topics().getTopic(topicName).partitions().get(0);
        final int targetBrokerId = kraftPartition.leader == 0 ? 1 : 0;
        stateRef.set(localLogState(targetBrokerId, tp));
        log.info("KRaft leader is {}, fetching at Fetch v10 from replica {} with state {}",
            kraftPartition.leader, targetBrokerId, stateRef.get());

        // An offset the replica holds locally and has committed.
        final long fetchOffset = stateRef.get().localLogStartOffset;
        final PreKip392FetchClient.Reply reply = PreKip392FetchClient.fetch(
            cluster.brokers().get(targetBrokerId), topicName, 0, fetchOffset, (short) 10);

        assertEquals(Errors.NONE, reply.error(),
            "A pre-v11 consumer must be allowed to read a consolidated partition from a non-leader "
                + "replica; state=" + localLogState(targetBrokerId, tp));
        assertTrue(reply.recordCount() > 0, "The reply must carry records, got " + reply);
        assertEquals(fetchOffset, reply.firstOffset(),
            "The reply must start at the requested offset, not skip ahead");
    }

    /**
     * A raised {@code max.message.bytes} must take effect on a consolidated diskless topic that has already
     * been produced to.
     *
     * <p>The diskless append path validates against a per-broker cached {@code LogConfig} populated on the
     * first produce. A consolidated topic always has a local log on its replicas, so a config-change refresh
     * gated on "no local log" never reaches it and produces above the previously cached limit keep failing
     * with {@code MESSAGE_TOO_LARGE}.
     */
    @Test
    public void testRaisedMaxMessageBytesIsHonouredOnConsolidatedDisklessTopic() throws Exception {
        topicName = "raised-max-message-bytes";
        numPartitions = 1;
        final int initialMaxMessageBytes = 262_144;
        final int raisedMaxMessageBytes = 6_291_456;
        // Over the initial limit, under the raised one.
        final int largeValueSize = 1_048_576;

        final Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        try (Admin admin = AdminClient.create(commonConfigs)) {
            final Map<String, String> topicConfigs = Map.of(
                DISKLESS_ENABLE_CONFIG, "true",
                REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
                SEGMENT_BYTES_CONFIG, "1048576",
                MAX_MESSAGE_BYTES_CONFIG, Integer.toString(initialMaxMessageBytes)
            );
            final NewTopic topic = new NewTopic(topicName, numPartitions, (short) -1).configs(topicConfigs);
            admin.createTopics(Collections.singletonList(topic)).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitValue(admin, topicName, MAX_MESSAGE_BYTES_CONFIG,
                Integer.toString(initialMaxMessageBytes));

            // The first produce is what populates the leader's cached LogConfig.
            produceRecords(commonConfigs, 1,
                i -> new ProducerRecord<>(topicName, 0, null, TestUtils.randomString(1024)));

            assertFalse(tryProduceValueOfSize(commonConfigs, largeValueSize),
                "A record above the initial max.message.bytes must be rejected before the config is raised");

            incrementalAlterTopicConfigs(admin,
                Map.of(MAX_MESSAGE_BYTES_CONFIG, Integer.toString(raisedMaxMessageBytes)));
            TopicMetadataProbe.awaitValue(admin, topicName, MAX_MESSAGE_BYTES_CONFIG,
                Integer.toString(raisedMaxMessageBytes));

            // Polled rather than asserted once: brokers apply the published config change asynchronously.
            TestUtils.waitForCondition(() -> tryProduceValueOfSize(commonConfigs, largeValueSize),
                60_000,
                () -> "A " + largeValueSize + " byte record must be accepted after max.message.bytes was raised to "
                    + raisedMaxMessageBytes + "; the diskless append path is still validating against the stale "
                    + "cached limit (" + initialMaxMessageBytes + ")");
        }
    }

    /**
     * Sends one record with a value of {@code valueSize} bytes and reports whether the broker accepted it.
     * {@code false} means it was rejected as too large; any other failure propagates.
     */
    private boolean tryProduceValueOfSize(Map<String, Object> commonConfigs, int valueSize) throws Exception {
        final Map<String, Object> producerConfigs = new HashMap<>(commonConfigs);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Keep the client-side limit out of the way; the broker-side limit is what is under test.
        producerConfigs.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 8 * 1024 * 1024);
        try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            producer.send(new ProducerRecord<>(topicName, 0, null, TestUtils.randomString(valueSize)))
                .get(30, TimeUnit.SECONDS);
            return true;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RecordTooLargeException) {
                return false;
            }
            throw e;
        }
    }

    /**
     * A partition-count increase applied while a classic topic is switching to diskless must not lose the
     * still-switching partition's pre-switch records (KC-387).
     *
     * <p>This attempts the interleaving through the real controller and admin path by growing the partition
     * count straight after the switch is submitted. Deterministic coverage of the conflicting writes lives
     * in {@code AbstractControlPlaneTest.initAfterPartitionCreateMustSetLatestToSeal}; this test also verifies
     * that the added partition reaches the control plane.
     */
    @Test
    public void testPartitionIncreaseDuringSwitchKeepsClassicPrefix() throws Exception {
        numPartitions = 1;
        final int preSwitchRecords = 30;
        final long seal = preSwitchRecords;
        topicName = "switch-increase-keep-prefix";

        final Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        final Uuid topicId;
        try (Admin admin = AdminClient.create(commonConfigs)) {
            topicId = createClassicTopic(admin);
            produceRecords(commonConfigs, preSwitchRecords, largeValueRecordFactory());

            // The alter future returns once the controller has committed diskless=true, which is well before
            // the leader's initDisklessLog reaches the control plane.
            incrementalAlterTopicConfigs(admin, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
            admin.createPartitions(Map.of(topicName, NewPartitions.increaseTo(2)))
                .all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitValue(admin, topicName, DISKLESS_ENABLE_CONFIG, "true");
        }

        final long highWatermark = awaitPartitionHighWatermark(toJavaUuid(topicId), 0, seal, 60_000);
        assertEquals(seal, highWatermark,
            "partition 0 diskless high_watermark must equal the seal " + seal + " after the switch; a value "
                + "of 0 means the partition-count increase re-created the row and the seal was dropped, so the "
                + "classic prefix is truncated away and lost");

        final long newPartitionHighWatermark =
            awaitPartitionHighWatermark(toJavaUuid(topicId), 1, 0, 60_000);
        assertEquals(0, newPartitionHighWatermark,
            "new partition 1 must have a control-plane row after the partition-count increase");

        // End-to-end: with the seal intact the whole classic prefix is still readable from offset 0.
        consumeAndVerify(commonConfigs, preSwitchRecords);
    }

    /**
     * A record factory that produces ~104 KB values (large enough to roll segments at
     * {@code segment.bytes=1 MiB} after ~10 records) with round-robin partitioning.
     */
    private IntFunction<ProducerRecord<String, String>> largeValueRecordFactory() {
        return i -> new ProducerRecord<>(topicName, i % numPartitions, null, TestUtils.randomString(104_857));
    }

    /**
     * Switch a classic topic to consolidating diskless via a single {@code diskless.enable=true}
     * alter. With the broker-level consolidation flag on, this auto-enables remote storage
     * atomically; wait for both config values to converge.
     */
    private void switchTopicToConsolidatedDiskless(Admin admin)
        throws ExecutionException, InterruptedException, TimeoutException {
        log.info("Migrating classic topic {} to diskless (remote storage auto-enabled atomically)", topicName);
        incrementalAlterTopicConfigs(admin, Map.of(DISKLESS_ENABLE_CONFIG, "true"));
        TopicMetadataProbe.awaitValue(admin, topicName, DISKLESS_ENABLE_CONFIG, "true");
        TopicMetadataProbe.awaitValue(admin, topicName, REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
    }

    /**
     * Wait until at least one segment has been tiered to the MinIO bucket.
     */
    private void waitForAtLeastOneTieredObject(String contextForMessage) throws InterruptedException {
        try (S3Client s3 = s3Container.getS3Client()) {
            final String bucket = s3Container.getBucketName();
            TestUtils.waitForCondition(() -> countObjectsWithPrefix(s3, bucket, RSM_OBJECT_KEY_PREFIX) > 0,
                120_000,
                () -> "Expected at least one tiered object in the Minio bucket " + contextForMessage);
        }
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

    /**
     * Issues {@code DeleteRecords} before the given per-partition offsets and returns the resulting low
     * watermark per partition (for a consolidating topic this is the stored cross-tier earliest, which the
     * control plane advances synchronously and forward-only).
     */
    private Map<TopicPartition, Long> deleteRecordsBefore(Admin admin, Map<TopicPartition, Long> beforeOffsets)
        throws ExecutionException, InterruptedException, TimeoutException {
        final Map<TopicPartition, RecordsToDelete> request = new HashMap<>();
        beforeOffsets.forEach((tp, offset) -> request.put(tp, RecordsToDelete.beforeOffset(offset)));
        final DeleteRecordsResult result = admin.deleteRecords(request);
        final Map<TopicPartition, Long> lowWatermarks = new HashMap<>();
        for (final TopicPartition tp : request.keySet()) {
            final DeletedRecords deleted = result.lowWatermarks().get(tp).get(30, TimeUnit.SECONDS);
            lowWatermarks.put(tp, deleted.lowWatermark());
        }
        return lowWatermarks;
    }

    /**
     * Like {@link #assertBrokerEarliestOffsetsEqual}, but asserts a distinct expected earliest per
     * partition (waits for convergence, then hard-asserts each partition).
     */
    private void assertBrokerEarliestOffsets(Map<String, Object> commonConfigs,
                                             Map<TopicPartition, Long> expected,
                                             String context) throws Exception {
        final AtomicReference<Map<TopicPartition, Long>> lastSeen = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            Map<TopicPartition, Long> earliest = queryBrokerEarliestOffsets(commonConfigs);
            lastSeen.set(earliest);
            return expected.entrySet().stream().allMatch(e -> e.getValue().equals(earliest.get(e.getKey())));
        }, 90_000, () -> "ListOffsets earliest should converge to " + expected + " " + context
            + "; last observed: " + lastSeen.get());

        Map<TopicPartition, Long> earliest = queryBrokerEarliestOffsets(commonConfigs);
        log.info("Broker ListOffsets earliest per partition {}: {}", context, earliest);
        expected.forEach((tp, exp) -> assertEquals(exp, earliest.get(tp),
            "ListOffsets earliest for " + tp + " should be " + exp + " " + context));
    }

    /**
     * Waits until {@code logs.remote_log_start_offset} for every partition is bootstrapped to
     * {@code expected} (never NULL). A NULL here would make {@code ListOffsets(EARLIEST)} COALESCE to the
     * pruned WAL frontier and hide the still-live remote prefix, the regression the become-leader report
     * exists to prevent.
     */
    private void assertRemoteLogStartOffsetBootstrapped(Uuid kafkaTopicId, long expected) throws InterruptedException {
        final UUID id = toJavaUuid(kafkaTopicId);
        final AtomicReference<Map<Integer, Long>> lastSeen = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            try {
                Map<Integer, Long> perPartition = readRemoteLogStartOffsets(id);
                lastSeen.set(perPartition);
                return perPartition.size() == numPartitions
                    && perPartition.values().stream().allMatch(v -> v != null && v == expected);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 120_000, () -> "remote_log_start_offset should be bootstrapped to " + expected + " for all "
            + numPartitions + " partitions (never NULL, never the seal); last observed: " + lastSeen.get());
    }

    /**
     * Polls the {@code logs} row for {@code partition} until its {@code high_watermark} reaches
     * {@code expected} or {@code deadlineMs} elapses, then returns the last value seen. Once initDisklessLog
     * records the seal this converges to it; if the defect drops the seal, the value stays at 0 and the
     * deadline is hit, so the caller can assert on the returned value.
     */
    private long awaitPartitionHighWatermark(UUID topicId, int partition, long expected, long deadlineMs)
        throws InterruptedException {
        final long end = System.currentTimeMillis() + deadlineMs;
        long lastSeen = -1L;
        while (System.currentTimeMillis() < end) {
            final Long hw = readPartitionHighWatermark(topicId, partition);
            if (hw != null) {
                lastSeen = hw;
                if (hw == expected) {
                    return hw;
                }
            }
            Thread.sleep(500);
        }
        return lastSeen;
    }

    private Long readPartitionHighWatermark(UUID topicId, int partition) {
        try (
            Connection connection = DriverManager.getConnection(
                pgContainer.getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD);
            PreparedStatement ps = connection.prepareStatement(
                "SELECT high_watermark FROM logs WHERE topic_id = ? AND partition = ?")
        ) {
            ps.setObject(1, topicId);
            ps.setInt(2, partition);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return null;
                }
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private Map<Integer, Long> readRemoteLogStartOffsets(UUID topicId) throws SQLException {
        Map<Integer, Long> out = new HashMap<>();
        try (
            Connection connection = DriverManager.getConnection(
                pgContainer.getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD);
            PreparedStatement ps = connection.prepareStatement(
                "SELECT partition, remote_log_start_offset FROM logs WHERE topic_id = ?")
        ) {
            ps.setObject(1, topicId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    int partition = rs.getInt(1);
                    long value = rs.getLong(2);
                    out.put(partition, rs.wasNull() ? null : value);
                }
            }
        }
        return out;
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
