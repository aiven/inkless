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
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.QuotaConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
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
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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
import scala.Option;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;
import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class InklessConsolidatedDisklessReassignmentTest {

    private static final Logger log = LoggerFactory.getLogger(InklessConsolidatedDisklessReassignmentTest.class);

    private static final int NUM_BROKERS = 3;
    private static final short REPLICATION_FACTOR = 2;

    private static final int RECORD_SIZE_BYTES = 104_857; // ~100 KB
    private static final String SEGMENT_SIZE = "2097152"; // 2 MB is small to ensure multiple segment rolls, and large enough for consolidation batches
    private static final long CONSOLIDATION_TIMEOUT_MS = 120_000;
    private static final long DEFAULT_TIMEOUT_MS = 60_000;
    private static final long ADMIN_CALL_TIMEOUT_SECONDS = 30;

    private static final String TOPIC_NAME = "consolidation-reassignment-test";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC_NAME, 0);

    private static final Map<String, String> TOPIC_CONFIGS = Map.of(
        DISKLESS_ENABLE_CONFIG, "true",
        REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
        CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
        SEGMENT_BYTES_CONFIG, SEGMENT_SIZE
    );

    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    @TempDir
    private Path tempCacheDir;

    private KafkaClusterTestKit cluster;
    private Admin admin;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        cluster = createCluster();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        admin = AdminClient.create(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()));
    }

    @AfterEach
    public void teardown() throws Exception {
        try {
            if (admin != null) {
                admin.close();
            }
        } finally {
            if (cluster != null) {
                cluster.close();
            }
        }
    }

    @Test
    public void testReassignmentRestartsConsolidationOnNewBroker() throws Exception {
        // Produce enough records to create multiple segments (~5MB across 2MB segments)
        // so that re-consolidation from object storage takes measurable time.
        int recordCount = 50;
        List<Integer> originalReplicas = createTopicAndAwaitConsolidation(recordCount);

        // Record max LEO across original brokers before reassignment
        long originalLeaderLeo = originalReplicas.stream()
            .mapToLong(id -> getLogEndOffset(id).orElse(0))
            .max().orElse(0);
        assertTrue(originalLeaderLeo > 0, "At least one original replica should have LEO > 0 after consolidation");

        int newBrokerId = findUnusedBrokerId(originalReplicas);
        List<Integer> newReplicas = List.of(originalReplicas.get(0), newBrokerId);
        log.info("Reassigning {} from {} to {}", TOPIC_PARTITION, originalReplicas, newReplicas);

        // Throttle replication to 1 byte/sec to ensure the new broker doesn't catch up
        // before we can assert on the LEO, preventing flakiness.
        setReplicationThrottle(1);

        AtomicLong leoAtReassignment = new AtomicLong(-1);
        reassignAndAwait(newReplicas, () -> leoAtReassignment.set(getLogEndOffset(newBrokerId).orElse(0)));

        // After reassignment, the new broker should NOT have the full consolidated data yet.
        // If inter-broker replication transferred consolidated data, the new broker would have
        // LEO == originalLeaderLeo immediately. Instead, consolidation restarts from object
        // storage, so the new broker should have significantly less data than the original leader.
        long newBrokerLeoAfterReassignment = leoAtReassignment.get();
        assertTrue(newBrokerLeoAfterReassignment < originalLeaderLeo,
            "New broker should not have full consolidated data immediately after reassignment " +
            "(consolidation restarts from object storage). New broker LEO: " + newBrokerLeoAfterReassignment +
            ", original leader LEO: " + originalLeaderLeo);

        // Lift the throttle so consolidation can proceed normally.
        removeReplicationThrottle();

        awaitConsolidationProgress(newBrokerId);
        verifyAllRecordsConsumable(recordCount);
    }

    @Test
    public void testReassignmentCleansUpOldBrokerLog() throws Exception {
        List<Integer> originalReplicas = createTopicAndAwaitConsolidation(30);

        int removedBrokerId = originalReplicas.get(1);
        int keptBrokerId = originalReplicas.get(0);
        int newBrokerId = findUnusedBrokerId(originalReplicas);

        reassignAndAwait(List.of(keptBrokerId, newBrokerId));

        TestUtils.waitForCondition(
            () -> !hasLog(removedBrokerId),
            DEFAULT_TIMEOUT_MS,
            () -> "Removed broker " + removedBrokerId + " should clean up its local log for " + TOPIC_PARTITION);

        log.info("Removed broker {} cleaned up local log for {}", removedBrokerId, TOPIC_PARTITION);
    }

    private KafkaClusterTestKit createCluster() throws Exception {
        Map<Integer, Map<String, String>> perServerProps = new HashMap<>();
        for (int i = 0; i < NUM_BROKERS; i++) {
            perServerProps.put(i, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az" + (i + 1)));
        }
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(NUM_BROKERS)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProps)
            .build();

        KafkaClusterTestKit.Builder builder = new KafkaClusterTestKit.Builder(nodes);
        configureDiskless(builder);
        configureControlPlane(builder);
        configureObjectStorage(builder);
        configureTieredStorage(builder);
        return builder.build();
    }

    private void configureDiskless(KafkaClusterTestKit.Builder builder) {
        builder
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, String.valueOf(REPLICATION_FACTOR))
            .setConfigProp(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true");
    }

    private void configureControlPlane(KafkaClusterTestKit.Builder builder) {
        String prefix = InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX;
        builder
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(prefix + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(prefix + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(prefix + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD);
    }

    private void configureObjectStorage(KafkaClusterTestKit.Builder builder) {
        String prefix = InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX;
        builder
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(prefix + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(prefix + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(prefix + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(prefix + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(prefix + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(prefix + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey());
    }

    private void configureTieredStorage(KafkaClusterTestKit.Builder builder) throws IOException {
        Path fetchChunkCachePath = Files.createDirectories(tempCacheDir.resolve("ts-fetch-chunk-cache"));
        String rsmPrefix = RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
        builder
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "5000")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP, "4")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, TopicBasedRemoteLogMetadataManager.class.getName())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "EXTERNAL")
            .setConfigProp(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
                + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, "1")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "io.aiven.kafka.tieredstorage.RemoteStorageManager")
            .setConfigProp(rsmPrefix + "chunk.size", 1048576)
            .setConfigProp(rsmPrefix + "fetch.chunk.cache.class", "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
            .setConfigProp(rsmPrefix + "fetch.chunk.cache.path", fetchChunkCachePath.toAbsolutePath().toString())
            .setConfigProp(rsmPrefix + "fetch.chunk.cache.size", "10737418")
            .setConfigProp(rsmPrefix + "fetch.chunk.cache.prefetch.max.size", "16777216")
            .setConfigProp(rsmPrefix + "fetch.chunk.cache.retention.ms", "16777216")
            .setConfigProp(rsmPrefix + "custom.metadata.fields.include", "REMOTE_SIZE")
            .setConfigProp(rsmPrefix + "key.prefix", "tsu-int-test/")
            .setConfigProp(rsmPrefix + "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .setConfigProp(rsmPrefix + "storage.s3.bucket.name", s3Container.getBucketName())
            .setConfigProp(rsmPrefix + "storage.s3.region", s3Container.getRegion())
            .setConfigProp(rsmPrefix + "storage.s3.endpoint.url", s3Container.getEndpoint())
            .setConfigProp(rsmPrefix + "storage.s3.path.style.access.enabled", "true")
            .setConfigProp(rsmPrefix + "storage.aws.access.key.id", s3Container.getAccessKey())
            .setConfigProp(rsmPrefix + "storage.aws.secret.access.key", s3Container.getSecretKey());
    }

    private List<Integer> createTopicAndAwaitConsolidation(int recordCount) throws Exception {
        NewTopic topic = new NewTopic(TOPIC_NAME, 1, REPLICATION_FACTOR).configs(TOPIC_CONFIGS);
        admin.createTopics(List.of(topic)).all().get(ADMIN_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        List<Integer> replicas = awaitTopicAndGetReplicas();
        log.info("Original replicas for {}: {}", TOPIC_PARTITION, replicas);

        produceRecords(recordCount);

        TestUtils.waitForCondition(() -> {
            for (int brokerId : replicas) {
                OptionalLong leo = getLogEndOffset(brokerId);
                if (leo.isPresent() && leo.getAsLong() > 0) {
                    log.info("Broker {} has local LEO {} for {}", brokerId, leo.getAsLong(), TOPIC_PARTITION);
                    return true;
                }
            }
            return false;
        }, CONSOLIDATION_TIMEOUT_MS, () -> "Consolidation should make progress on original brokers");

        return replicas;
    }

    private List<Integer> awaitTopicAndGetReplicas() throws Exception {
        final AtomicReference<TopicDescription> holder = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription topicDescription = admin.describeTopics(List.of(TOPIC_NAME))
                    .allTopicNames().get(ADMIN_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(TOPIC_NAME);
                holder.set(topicDescription);
                return topicDescription.partitions().size() == 1;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) return false;
                throw new RuntimeException(e);
            }
        }, DEFAULT_TIMEOUT_MS, () -> "Topic " + TOPIC_NAME + " should become visible");

        return holder.get().partitions().get(0).replicas().stream()
            .map(Node::id).collect(Collectors.toList());
    }

    private void reassignAndAwait(List<Integer> targetReplicas) throws Exception {
        reassignAndAwait(targetReplicas, () -> { });
    }

    private void reassignAndAwait(List<Integer> targetReplicas, Runnable onComplete) throws Exception {
        admin.alterPartitionReassignments(Map.of(
            TOPIC_PARTITION, Optional.of(new NewPartitionReassignment(targetReplicas))
        )).all().get(ADMIN_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        TestUtils.waitForCondition(() -> {
            try {
                TopicDescription desc = admin.describeTopics(List.of(TOPIC_NAME))
                    .allTopicNames().get(ADMIN_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS).get(TOPIC_NAME);
                List<Integer> currentReplicas = desc.partitions().get(0).replicas().stream()
                    .map(Node::id).collect(Collectors.toList());
                if (new HashSet<>(currentReplicas).equals(new HashSet<>(targetReplicas))) {
                    onComplete.run();
                    return true;
                }
                return false;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) return false;
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }, DEFAULT_TIMEOUT_MS, () -> "Reassignment to " + targetReplicas + " should complete");

        log.info("Reassignment completed. New replicas: {}", targetReplicas);
    }

    private void awaitConsolidationProgress(int brokerId) throws Exception {
        TestUtils.waitForCondition(() -> {
            OptionalLong leo = getLogEndOffset(brokerId);
            return leo.isPresent() && leo.getAsLong() > 0;
        }, CONSOLIDATION_TIMEOUT_MS, () -> "Broker " + brokerId + " should start consolidation");

        log.info("Broker {} LEO after reassignment: {}", brokerId, getLogEndOffset(brokerId).orElse(0));
    }

    private int findUnusedBrokerId(List<Integer> usedIds) {
        for (int id = 0; id < NUM_BROKERS; id++) {
            if (!usedIds.contains(id)) return id;
        }
        throw new IllegalStateException("All broker IDs 0-" + (NUM_BROKERS - 1) + " are in use");
    }

    private void setReplicationThrottle(long bytesPerSecond) throws Exception {
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        for (int id = 0; id < NUM_BROKERS; id++) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id));
            Collection<AlterConfigOp> ops = List.of(
                new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG,
                    String.valueOf(bytesPerSecond)), AlterConfigOp.OpType.SET),
                new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG,
                    String.valueOf(bytesPerSecond)), AlterConfigOp.OpType.SET)
            );
            configs.put(brokerResource, ops);
        }
        admin.incrementalAlterConfigs(configs).all().get(ADMIN_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void removeReplicationThrottle() throws Exception {
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        for (int id = 0; id < NUM_BROKERS; id++) {
            ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(id));
            Collection<AlterConfigOp> ops = List.of(
                new AlterConfigOp(new ConfigEntry(QuotaConfig.LEADER_REPLICATION_THROTTLED_RATE_CONFIG, null),
                    AlterConfigOp.OpType.DELETE),
                new AlterConfigOp(new ConfigEntry(QuotaConfig.FOLLOWER_REPLICATION_THROTTLED_RATE_CONFIG, null),
                    AlterConfigOp.OpType.DELETE)
            );
            configs.put(brokerResource, ops);
        }
        admin.incrementalAlterConfigs(configs).all().get(ADMIN_CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private OptionalLong getLogEndOffset(int brokerId) {
        Option<UnifiedLog> logOpt = cluster.brokers().get(brokerId).logManager().getLog(TOPIC_PARTITION, false);
        return logOpt.isDefined()
            ? OptionalLong.of(logOpt.get().logEndOffset())
            : OptionalLong.empty();
    }

    private boolean hasLog(int brokerId) {
        return cluster.brokers().get(brokerId).logManager().getLog(TOPIC_PARTITION, false).isDefined();
    }

    private void produceRecords(int numRecords) {
        Map<String, Object> producerConfigs = Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (var producer = new KafkaProducer<String, String>(producerConfigs)) {
            for (int i = 0; i < numRecords; i++) {
                String value = TestUtils.randomString(RECORD_SIZE_BYTES);
                producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, value)).get();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        log.info("Produced {} records (~{}KB each)", numRecords, RECORD_SIZE_BYTES / 1024);
    }

    private void verifyAllRecordsConsumable(int expectedRecordCount) throws InterruptedException {
        Map<String, Object> consumerConfigs = Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(consumerConfigs)) {
            consumer.subscribe(List.of(TOPIC_NAME));
            var consumedCount = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                var records = consumer.poll(Duration.ofMillis(1000));
                consumedCount.addAndGet(records.count());
                return consumedCount.get() >= expectedRecordCount;
            }, DEFAULT_TIMEOUT_MS,
                () -> "Expected " + expectedRecordCount + " records but got " + consumedCount.get());
        }
    }
}
