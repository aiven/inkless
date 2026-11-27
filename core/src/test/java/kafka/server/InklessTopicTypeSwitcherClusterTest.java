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

import kafka.server.metadata.InklessMetadataView;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.AlterDisklessSwitchOptions;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.admin.TransactionDescription;
import org.apache.kafka.clients.admin.TransactionState;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.raft.KRaftConfigs;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class InklessTopicTypeSwitcherClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(InklessTopicTypeSwitcherClusterTest.class);
    private static final String RSM_CONFIG_PREFIX = RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
    private static final int NUM_PARTITIONS = 3;
    private static final int MAX_PRODUCE_RETRIES = 6;
    private static final int PRODUCE_RETRY_BACKOFF_MS = 250;
    private static final Duration PRODUCE_SEND_TIMEOUT = Duration.ofSeconds(5);

    private KafkaClusterTestKit cluster;

    @TempDir
    private Path tempCacheDir;

    private enum AlterConfigsMode {
        INCREMENTAL,
        LEGACY
    }

    /**
     * Tags a test that keeps a switched partition in phase 1. For such a test, the setup configures the
     * tiered storage plugin with a bucket that doesn't exist. Segment uploads fail, and the remote copy
     * task retries them, until the test creates the bucket.
     */
    private static final String BLOCKS_REMOTE_UPLOADS = "blocks-remote-uploads";

    /**
     * Tags a test that stops a broker like a crash. For such a test, the setup turns off the controlled
     * shutdown and sets the broker session timeout to 30 seconds. The stopped broker then stays in the ISR
     * until its session expires.
     */
    private static final String NO_CONTROLLED_SHUTDOWN = "no-controlled-shutdown";

    private String tieredStorageBucket;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        log.warn("[stage=setup] Initializing containers and 3-broker cluster");
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        // Phase 2 of the classic-to-diskless switch deletes the local classic prefix and serves those
        // offsets from remote storage. A no-op remote storage manager reports the copy as complete and
        // doesn't keep the bytes. The prefix is then unreadable. Tiered storage serves the prefix after
        // the switch.
        // `DiskChunkCache` wipes its path on startup. Each broker gets its own cache directory.
        final Map<Integer, Map<String, String>> perServerProps = new HashMap<>();
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            perServerProps.put(brokerId, Map.of(
                ServerConfigs.BROKER_RACK_CONFIG, "az" + (brokerId + 1),
                RSM_CONFIG_PREFIX + "fetch.chunk.cache.path",
                Files.createDirectories(tempCacheDir.resolve("ts-fetch-chunk-cache-" + brokerId)).toAbsolutePath().toString()
            ));
        }

        tieredStorageBucket = testInfo.getTags().contains(BLOCKS_REMOTE_UPLOADS)
            ? "tiered-" + UUID.randomUUID().toString().substring(0, 8)
            : s3Container.getBucketName();

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProps)
            .build();

        final KafkaClusterTestKit.Builder clusterBuilder = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "5000")
            .setConfigProp(ServerLogConfigs.LOG_CLEANUP_INTERVAL_MS_CONFIG, "500")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP, "4")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, TopicBasedRemoteLogMetadataManager.class.getName())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "EXTERNAL")
            .setConfigProp(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
                + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, "1")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "io.aiven.kafka.tieredstorage.RemoteStorageManager")
            .setConfigProp(RSM_CONFIG_PREFIX + "chunk.size", 1048576)
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.class", "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.size", "10737418")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.prefetch.max.size", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.retention.ms", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "custom.metadata.fields.include", "REMOTE_SIZE")
            .setConfigProp(RSM_CONFIG_PREFIX + "key.prefix", "switch-int-test/")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.bucket.name", tieredStorageBucket)
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.region", s3Container.getRegion())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.endpoint.url", s3Container.getEndpoint())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.path.style.access.enabled", "true")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.aws.access.key.id", s3Container.getAccessKey())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.aws.secret.access.key", s3Container.getSecretKey())
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "3")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey());
        if (testInfo.getTags().contains(NO_CONTROLLED_SHUTDOWN)) {
            clusterBuilder
                .setConfigProp(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, "false")
                .setConfigProp(KRaftConfigs.BROKER_SESSION_TIMEOUT_MS_CONFIG, "30000");
        }
        cluster = clusterBuilder.build();

        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
        log.warn("[stage=setup] Cluster is ready. bootstrapServers={}", cluster.bootstrapServers());
    }

    @AfterEach
    public void teardown() throws Exception {
        log.warn("[stage=teardown] Closing cluster");
        cluster.close();
    }

    @ParameterizedTest(name = "alterConfigsMode={0}")
    @EnumSource(AlterConfigsMode.class)
    public void switchClassicTopicToDiskless(final AlterConfigsMode alterConfigsMode) throws Exception {
        final String topicSuffix = UUID.randomUUID().toString().substring(0, 8);
        final String disklessTopic = "diskless-" + topicSuffix;
        final String classicTopic = "classic-" + topicSuffix;
        final String classicToDisklessTopic = "classic-to-diskless-" + topicSuffix;
        final List<String> topics = List.of(disklessTopic, classicTopic, classicToDisklessTopic);
        final Map<String, Integer> producedCounts = new HashMap<>();
        producedCounts.put(disklessTopic, 0);
        producedCounts.put(classicTopic, 0);
        producedCounts.put(classicToDisklessTopic, 0);

        final Map<String, Object> adminConfigs = baseClientConfigs();
        final Map<String, Object> producerConfigs = producerConfigs();
        log.warn("[stage=test-start] alterConfigsMode={}, topics: diskless={}, classic={}, switching={}", alterConfigsMode, disklessTopic, classicTopic, classicToDisklessTopic);

        try (Admin admin = AdminClient.create(adminConfigs)) {
            final NewTopic diskless = new NewTopic(disklessTopic, NUM_PARTITIONS, (short) -1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final NewTopic classic = new NewTopic(classicTopic, NUM_PARTITIONS, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
            final NewTopic classicToDiskless = new NewTopic(classicToDisklessTopic, NUM_PARTITIONS, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));

            final CreateTopicsResult createTopics = admin.createTopics(List.of(diskless, classic, classicToDiskless));
            createTopics.all().get(30, TimeUnit.SECONDS);
            log.warn("[stage=topics-created] Created all topics");

            TopicMetadataProbe.awaitValue(admin, disklessTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
            TopicMetadataProbe.awaitValue(admin, classicTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "false");
            TopicMetadataProbe.awaitValue(admin, classicToDisklessTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "false");

            final AtomicBoolean keepProducing = new AtomicBoolean(true);
            final AtomicReference<Throwable> producerFailure = new AtomicReference<>(null);
            final CountDownLatch producerStarted = new CountDownLatch(1);

            final Thread producerThread = new Thread(() -> {
                try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs)) {
                    log.warn("[stage=pre-switch-produce] Producing baseline records to all topics");
                    produceRounds(producer, topics, producedCounts, 12, true);
                    producerStarted.countDown();

                    log.warn("[stage=during-switch-produce] Continuing produces during switch");
                    while (keepProducing.get()) {
                        produceRounds(producer, topics, producedCounts, 1, false);
                    }
                } catch (Throwable t) {
                    producerFailure.set(t);
                    producerStarted.countDown();
                }
            }, "switch-producer-thread");

            producerThread.start();
            assertTrue(producerStarted.await(30, TimeUnit.SECONDS), "Producer thread did not finish baseline production in time");

            try {
                log.warn("[stage=switch-start] Enabling diskless for topic={} via {}", classicToDisklessTopic, alterConfigsMode);
                alterTopicConfig(admin, classicToDisklessTopic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"), alterConfigsMode);

                log.warn("[stage=await-switch] Waiting for diskless.enable=true on topic={}", classicToDisklessTopic);
                TopicMetadataProbe.awaitValue(admin, classicToDisklessTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
            } finally {
                keepProducing.set(false);
                producerThread.join(TimeUnit.SECONDS.toMillis(60));
            }

            assertTrue(!producerThread.isAlive(), "Producer thread did not stop in time");
            if (producerFailure.get() != null) {
                throw new RuntimeException("Producer thread failed", producerFailure.get());
            }

            try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs)) {
                log.warn("[stage=post-switch-produce] Producing records after switch");
                produceRounds(producer, topics, producedCounts, 1, true);
            }

            TopicMetadataProbe.awaitValue(admin, disklessTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
            TopicMetadataProbe.awaitValue(admin, classicTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "false");
            TopicMetadataProbe.awaitValue(admin, classicToDisklessTopic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
            log.warn("[stage=post-switch-validated] Topic configurations and placement checks passed");
        }

        // `diskless.enable` commits before the prefix upload. Without this wait, the consumer can read
        // the whole prefix from local logs in phase 1 of the classic-to-diskless switch.
        log.warn("[stage=await-prefix-deletion] Waiting for every broker to delete the local classic prefix of topic={}", classicToDisklessTopic);
        waitForLocalClassicPrefixDeletion(classicToDisklessTopic);

        log.warn("[stage=consume-verify] Produced counts before consume verification={}", producedCounts);
        consumeAndAssertTopicCounts(baseConsumerConfigs(), topics, producedCounts);
    }

    // The consumer tails the log. Its fetches wait at the log end, and diskless storage answers
    // them. A second consumer in the same group validates the committed position with
    // `OffsetsForLeaderEpoch` before its first fetch, and resets if the answer is below the position.
    @Test
    public void testConsumerResumesFromCommittedOffsetPastSeal() throws Exception {
        final String topic = "resume-past-seal-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);
        final int classicRecords = 20;
        final int disklessRecords = 30;
        final int recordsAfterCommit = 5;

        final Map<String, Object> consumerConfigs = baseConsumerConfigs();
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        try (Admin admin = AdminClient.create(baseClientConfigs());
             Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false")))).all().get(30, TimeUnit.SECONDS);

            for (int i = 0; i < classicRecords; i++) {
                assertTrue(sendWithRetry(producer, new ProducerRecord<>(topic, 0, null, ("classic-" + i).getBytes(StandardCharsets.UTF_8))));
            }

            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            TopicMetadataProbe.awaitValue(admin, topic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");

            final long committedOffset;
            try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
                consumer.assign(List.of(tp));
                final long deadline = System.currentTimeMillis() + Duration.ofSeconds(120).toMillis();
                while (consumer.position(tp) < classicRecords && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(200));
                }
                for (int i = 0; i < disklessRecords; i++) {
                    producer.send(new ProducerRecord<>(topic, 0, null, ("diskless-" + i).getBytes(StandardCharsets.UTF_8)));
                    while (consumer.position(tp) < classicRecords + i + 1 && System.currentTimeMillis() < deadline) {
                        consumer.poll(Duration.ofMillis(200));
                    }
                }
                consumer.commitSync();
                final var committed = consumer.committed(Set.of(tp)).get(tp);
                log.warn("[stage=committed] committed={}", committed);
                committedOffset = committed.offset();
                assertTrue(committed.leaderEpoch().orElse(-1) > 0,
                    "A position past the seal carries a diskless leader epoch, but the commit has " + committed.leaderEpoch());
            }
            assertEquals(classicRecords + disklessRecords, committedOffset);

            for (int i = 0; i < recordsAfterCommit; i++) {
                assertTrue(sendWithRetry(producer, new ProducerRecord<>(topic, 0, null, ("after-commit-" + i).getBytes(StandardCharsets.UTF_8))));
            }

            try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
                consumer.assign(List.of(tp));
                final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
                ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
                while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                    records = consumer.poll(Duration.ofSeconds(1));
                }
                assertTrue(!records.isEmpty(), "The resumed consumer read no records");
                assertEquals(committedOffset, records.iterator().next().offset(),
                    "The resumed consumer didn't start at its committed offset");
            }
        }
    }

    // Local retention deletes the consolidated segments that the leader uploaded, so the local log of the leader
    // starts past the seal. A replica that a reassignment adds starts below the seal, and it rebuilds its log from
    // the consolidated segment in remote storage that ends before the local log start of the leader. On an idle
    // leader, local retention also rolls the last segment, and the broker uploads and deletes it. The local log of
    // the leader is then empty.
    @ParameterizedTest(name = "idleLeader={0}")
    @ValueSource(booleans = {false, true})
    public void testAddedReplicaRebuildsWhenLeaderLocalLogStartsPastSeal(final boolean idleLeader) throws Exception {
        final String topic = "switch-added-replica-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);
        final int classicRecords = 300;
        final int disklessRecords = 2500;
        final byte[] value = new byte[1500];

        try (Admin admin = AdminClient.create(baseClientConfigs());
             Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs())) {
            // Consolidation fetches one batch at a time unless a segment is larger than the largest batch.
            admin.createTopics(List.of(new NewTopic(topic, Map.of(0, List.of(0, 1))).configs(Map.of(
                TopicConfig.DISKLESS_ENABLE_CONFIG, "false",
                TopicConfig.SEGMENT_BYTES_CONFIG, "1048576",
                TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "262144")))).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, topic);
            for (int i = 0; i < classicRecords; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null, value));
            }
            producer.flush();
            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final long seal = waitForSealOffset(admin, topic, 0, offset -> offset >= 0);
            assertEquals(classicRecords, seal);
            waitForClassicPrefixInRemoteStorage(admin, topic);
            for (int i = 0; i < disklessRecords; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null, value));
            }
            producer.flush();
            final long endOffset = seal + disklessRecords;

            final int leaderId = cluster.brokers().get(0).replicaManager().getPartitionOrException(tp).isLeader() ? 0 : 1;
            final UnifiedLog leaderLog = cluster.brokers().get(leaderId).replicaManager().getLog(tp).get();
            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "1000"));
            final long expectedLocalLogStart = idleLeader ? endOffset : seal + 1;
            TestUtils.waitForCondition(() -> leaderLog.localLogStartOffset() >= expectedLocalLogStart, 240_000L,
                () -> "Local retention didn't move the local log start of the leader to " + expectedLocalLogStart
                    + ": it's " + leaderLog.localLogStartOffset() + ", and the log end offset is "
                    + leaderLog.logEndOffset());

            admin.alterPartitionReassignments(Map.of(tp, Optional.of(new NewPartitionReassignment(List.of(0, 1, 2)))))
                .all().get(30, TimeUnit.SECONDS);
            final ReplicaManager addedReplica = cluster.brokers().get(2).replicaManager();
            // The classic fetcher rebuilds the log, and it evicts the partition when it hands off to consolidation.
            TestUtils.waitForCondition(
                () -> addedReplica.getLog(tp).isDefined() && addedReplica.getLog(tp).get().logEndOffset() >= seal
                    && addedReplica.replicaFetcherManager().getFetcher(tp).isEmpty(),
                60_000L,
                () -> "The added replica didn't rebuild its log up to the seal " + seal + " and hand off to "
                    + "consolidation: its log end offset is "
                    + (addedReplica.getLog(tp).isDefined() ? addedReplica.getLog(tp).get().logEndOffset() : -1)
                    + ", and the classic fetcher " + (addedReplica.replicaFetcherManager().getFetcher(tp).isEmpty()
                        ? "doesn't hold" : "still holds") + " the partition");

            // Broker 2 is in az3. The metadata routing sends a consumer in that zone to the added replica.
            final Map<String, Object> consumerConfigs = baseConsumerConfigs();
            consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, "added-replica,diskless_az=az3");
            try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
                consumer.assign(List.of(tp));
                consumer.seek(tp, 0L);
                long nextOffset = 0;
                final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
                while (nextOffset < endOffset && System.currentTimeMillis() < deadline) {
                    for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofMillis(500))) {
                        assertEquals(nextOffset, record.offset());
                        nextOffset++;
                    }
                }
                assertEquals(endOffset, nextOffset, "The consumer in az3 didn't read every record");
            }
        }
    }

    private Map<String, Object> baseClientConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return configs;
    }

    private Map<String, Object> producerConfigs() {
        final Map<String, Object> configs = baseClientConfigs();
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configs.put(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        configs.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        return configs;
    }

    private Map<String, Object> baseConsumerConfigs() {
        final Map<String, Object> configs = baseClientConfigs();
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "it-group-" + UUID.randomUUID());
        return configs;
    }

    private void produceRounds(final Producer<byte[], byte[]> producer,
                               final List<String> topics,
                               final Map<String, Integer> producedCounts,
                               final int rounds,
                               final boolean failOnExhaustedRetries) throws Exception {
        log.warn("[stage=produce-rounds] rounds={}, failOnExhaustedRetries={}, topics={}",
            rounds, failOnExhaustedRetries, topics);
        for (int i = 0; i < rounds; i++) {
            for (String topic : topics) {
                final int partition = i % NUM_PARTITIONS;
                final byte[] value = (topic + "-value-" + i).getBytes(StandardCharsets.UTF_8);
                final boolean sent = sendWithRetry(producer, new ProducerRecord<>(topic, partition, null, value));
                if (!sent && failOnExhaustedRetries) {
                    throw new RuntimeException("Exhausted producer retries for topic " + topic);
                }
                if (sent) {
                    producedCounts.compute(topic, (k, v) -> v == null ? 1 : v + 1);
                }
            }
        }
        producer.flush();
        log.warn("[stage=produce-rounds] completed rounds={}, currentProducedCounts={}", rounds, producedCounts);
    }

    private boolean sendWithRetry(final Producer<byte[], byte[]> producer,
                                  final ProducerRecord<byte[], byte[]> record) throws Exception {
        Exception latest = null;
        for (int attempt = 1; attempt <= MAX_PRODUCE_RETRIES; attempt++) {
            try {
                producer.send(record).get(PRODUCE_SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                return true;
            } catch (Exception e) {
                latest = e;
                log.warn("Producer send failed for topic={}, partition={}, attempt={}/{}",
                    record.topic(), record.partition(), attempt, MAX_PRODUCE_RETRIES, e);
                Thread.sleep(PRODUCE_RETRY_BACKOFF_MS);
            }
        }
        log.warn("Exhausted producer retries for topic={}, partition={}", record.topic(), record.partition(), latest);
        return false;
    }

    private void consumeAndAssertTopicCounts(final Map<String, Object> consumerConfigs,
                                             final List<String> topics,
                                             final Map<String, Integer> expectedCounts) {
        final Map<String, Integer> consumedCounts = new HashMap<>();
        topics.forEach(topic -> consumedCounts.put(topic, 0));
        log.warn("[stage=consume-start] topics={}, expectedCounts={}", topics, expectedCounts);

        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(topics);
            final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();

            while (System.currentTimeMillis() < deadline) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    consumedCounts.compute(record.topic(), (k, v) -> v == null ? 1 : v + 1);
                }

                if (hasReachedExpected(consumedCounts, expectedCounts, topics)) {
                    break;
                }
            }
        }
        log.warn("[stage=consume-end] consumedCounts={}", consumedCounts);

        for (String topic : topics) {
            final int expected = expectedCounts.getOrDefault(topic, 0);
            final int actual = consumedCounts.getOrDefault(topic, 0);
            if (expected > 0) {
                assertTrue(actual >= expected,
                    "Expected to consume at least " + expected + " record(s) from topic "
                        + topic + " after producing to it, but consumed " + actual);
            }
        }
    }

    private boolean hasReachedExpected(final Map<String, Integer> consumedCounts,
                                       final Map<String, Integer> expectedCounts,
                                       final List<String> topics) {
        for (String topic : topics) {
            if (consumedCounts.getOrDefault(topic, 0) < expectedCounts.getOrDefault(topic, 0)) {
                return false;
            }
        }
        return true;
    }

    private void alterTopicConfig(final Admin admin,
                                  final String topic,
                                  final Map<String, String> newConfigs,
                                  final AlterConfigsMode alterConfigsMode) throws Exception {
        switch (alterConfigsMode) {
            case INCREMENTAL:
                alterTopicConfigWithIncrementalAlterConfigs(admin, topic, newConfigs);
                break;
            case LEGACY:
                alterTopicConfigWithLegacyAlterConfigs(topic, newConfigs);
                break;
            default:
                throw new IllegalArgumentException("Unsupported alter configs mode " + alterConfigsMode);
        }
    }

    private void alterTopicConfigWithIncrementalAlterConfigs(final Admin admin,
                                                            final String topic,
                                                            final Map<String, String> newConfigs) throws Exception {
        final ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final Collection<AlterConfigOp> operations = newConfigs.entrySet().stream()
            .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
            .toList();
        admin.incrementalAlterConfigs(Map.of(topicResource, operations)).all().get(20, TimeUnit.SECONDS);
    }

    // The metadata routing sends a client to an alive ISR member while the switch is pending. A consumer validates
    // a position that has a leader epoch with OffsetsForLeaderEpoch before it fetches. An in-sync follower must
    // answer that request.
    @Test
    @Tag(NO_CONTROLLED_SHUTDOWN)
    public void testConsumerRoutedToInSyncFollowerReadsWhileSwitchIsPending() throws Exception {
        final String topic = "switch-pending-follower-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);
        final long position = 5;

        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false")))).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, topic);
            produceRecords(topic, 20);

            final var partition = admin.describeTopics(List.of(topic)).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(topic).partitions().get(0);
            final int leader = partition.leader().id();
            final List<Integer> followers = partition.replicas().stream()
                .map(node -> node.id()).filter(id -> id != leader).sorted().toList();
            // Broker 0 also runs the controller. The test stops the follower with the higher ID, which is never broker 0.
            final int follower = followers.get(0);
            final int stoppedFollower = followers.get(1);
            final int classicLeaderEpoch =
                cluster.brokers().get(leader).replicaManager().onlinePartition(tp).get().getLeaderEpoch();

            // The stopped follower stays in the ISR and never fetches the next record. The leader's high watermark
            // then stays below its log end offset, and the leader doesn't propose the seal.
            cluster.brokers().get(stoppedFollower).shutdown();
            final Map<String, Object> acksOneConfigs = baseClientConfigs();
            acksOneConfigs.put(ProducerConfig.ACKS_CONFIG, "1");
            acksOneConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false");
            try (Producer<byte[], byte[]> producer = new KafkaProducer<>(acksOneConfigs)) {
                producer.send(new ProducerRecord<>(topic, 0, null, "unreplicated".getBytes(StandardCharsets.UTF_8)))
                    .get(30, TimeUnit.SECONDS);
            }

            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final InklessMetadataView followerView = cluster.brokers().get(follower).replicaManager().inklessMetadataView();
            final long pendingDeadline = System.currentTimeMillis() + Duration.ofSeconds(10).toMillis();
            while (followerView.getClassicToDisklessStartOffset(tp) != PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING
                    && System.currentTimeMillis() < pendingDeadline) {
                Thread.sleep(10);
            }
            assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                followerView.getClassicToDisklessStartOffset(tp), "The follower didn't apply the pending switch");

            // Broker N is in zone az(N+1). A client in the follower's zone gets the follower as the leader.
            final String followerZone = "az" + (follower + 1);
            final Map<String, Object> zoneAdminConfigs = baseClientConfigs();
            zoneAdminConfigs.put(CommonClientConfigs.CLIENT_ID_CONFIG, "pending-follower-admin,diskless_az=" + followerZone);
            try (Admin zoneAdmin = AdminClient.create(zoneAdminConfigs)) {
                assertEquals(follower, zoneAdmin.describeTopics(List.of(topic)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topic).partitions().get(0).leader().id());
            }

            final Map<String, Object> consumerConfigs = baseConsumerConfigs();
            consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
            consumerConfigs.put(ConsumerConfig.CLIENT_ID_CONFIG, "pending-follower-consumer,diskless_az=" + followerZone);
            try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
                consumer.assign(List.of(tp));
                // A consumer that resumes from a committed offset has the leader epoch of that offset.
                consumer.seek(tp, new OffsetAndMetadata(position, Optional.of(classicLeaderEpoch), ""));
                final long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
                ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
                while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                    records = consumer.poll(Duration.ofMillis(250));
                }
                assertFalse(records.isEmpty(), "The consumer read no record from the in-sync follower");
                assertEquals(position, records.iterator().next().offset());
            }
            assertEquals(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                followerView.getClassicToDisklessStartOffset(tp), "The switch didn't stay pending during the read");
        }
    }

    @Test
    public void testSwitchRejectedWhenPartitionIsOfflineOrUnderReplicated() throws Exception {
        final String topic = "switch-unhealthy-" + UUID.randomUUID().toString().substring(0, 8);

        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            // Create a classic topic with RF=3
            admin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 3)
                    .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"))
            )).all().get(30, TimeUnit.SECONDS);

            // Shut down a broker to make the partition under-replicated
            final int brokerToShutdown = 2;
            cluster.brokers().get(brokerToShutdown).shutdown();
            waitForIsrShrink(admin, topic, 0, 3);

            // Attempt to switch to diskless (should fail with INVALID_CONFIG due to under-replication)
            final ExecutionException ex = assertThrows(ExecutionException.class, () ->
                alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true")));
            assertInstanceOf(InvalidConfigurationException.class, ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("under-replicated"),
                "Expected 'under-replicated' in: " + ex.getCause().getMessage());

            // Restart the broker and wait for ISR to recover
            cluster.brokers().get(brokerToShutdown).startup();
            waitForIsrRecovery(admin, topic, 0, 3);

            // Now the switch should succeed
            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            TopicMetadataProbe.awaitValue(admin, topic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
        }
    }

    @Test
    public void testSwitchRejectedWhenUncleanLeaderElectionEnabledAtClusterLevel() throws Exception {
        final String topic = "switch-cluster-unclean-" + UUID.randomUUID().toString().substring(0, 8);

        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            // Enable unclean leader election at cluster level first
            final ConfigResource clusterResource = new ConfigResource(ConfigResource.Type.BROKER, "");
            admin.incrementalAlterConfigs(Map.of(clusterResource, List.of(
                    new AlterConfigOp(new ConfigEntry(
                            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"),
                            AlterConfigOp.OpType.SET)
            ))).all().get(20, TimeUnit.SECONDS);

            // Create a classic topic
            admin.createTopics(List.of(
                    new NewTopic(topic, 1, (short) 3)
                            .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"))
            )).all().get(30, TimeUnit.SECONDS);

            // Attempt to switch to diskless — should fail because unclean leader election
            // is enabled (via cluster-level config)
            final ExecutionException ex = assertThrows(ExecutionException.class, () ->
                    alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(
                            TopicConfig.DISKLESS_ENABLE_CONFIG, "true")));
            assertInstanceOf(InvalidConfigurationException.class, ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("unclean leader election"),
                    "Expected 'unclean leader election' in error, got: " + ex.getCause().getMessage());

            // Same check via the legacy AlterConfigs path
            final Errors legacyError = alterTopicConfigWithLegacyAlterConfigsExpectingError(
                    topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            assertEquals(Errors.INVALID_CONFIG, legacyError,
                    "Legacy path should reject switch when unclean leader election is enabled");
        }
    }

    @Test
    public void testUncleanLeaderElectionRejectedWhileSwitchPending() throws Exception {
        final String topic = "switch-unclean-" + UUID.randomUUID().toString().substring(0, 8);

        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            // Create a classic topic and switch it to diskless
            admin.createTopics(List.of(
                    new NewTopic(topic, 1, (short) 3)
                            .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"))
            )).all().get(30, TimeUnit.SECONDS);

            // Enable diskless to mark partition as switch pending
            alterTopicConfigWithIncrementalAlterConfigs(
                    admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));

            // Should fail trying to enable unclean leader election
            final ExecutionException ex = assertThrows(ExecutionException.class, () ->
                    alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(
                            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true")));
            assertInstanceOf(InvalidConfigurationException.class, ex.getCause());
            assertTrue(ex.getCause().getMessage().contains("classic prefix is not in remote storage yet"),
                    "Expected 'classic prefix is not in remote storage yet' in: " + ex.getCause().getMessage());

            // Same check via the legacy AlterConfigs path
            final Errors legacyError = alterTopicConfigWithLegacyAlterConfigsExpectingError(
                    topic, Map.of(
                            TopicConfig.DISKLESS_ENABLE_CONFIG, "true",
                            TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "true"));
            assertEquals(Errors.INVALID_CONFIG, legacyError,
                    "Legacy path should reject enabling unclean leader election during pending switch");
        }
    }

    @Test
    public void testAlterDisklessSwitchRejectsReArmAndNonLowerSealOnCommittedSwitch() throws Exception {
        final String topic = "switch-alter-committed-" + UUID.randomUUID().toString().substring(0, 8);

        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            admin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 3)
                    .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"))
            )).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, topic);

            alterTopicConfigWithIncrementalAlterConfigs(
                admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final long sealOffset = waitForSealOffset(admin, topic, 0, offset -> offset >= 0);

            // A committed seal can't be re-armed, aborted, or raised: the controller accepts only a lower seal.
            for (long requested : List.of(PartitionRegistration.CLASSIC_TO_DISKLESS_SWITCH_PENDING,
                    PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET, sealOffset, sealOffset + 1)) {
                final ExecutionException ex = assertThrows(ExecutionException.class, () ->
                    admin.alterDisklessSwitch(topic, 0, requested).all().get(20, TimeUnit.SECONDS));
                assertInstanceOf(InvalidRequestException.class, ex.getCause());
                assertEquals(sealOffset, readSealOffset(admin, topic, 0));
            }
        }
    }

    @Test
    @Tag(BLOCKS_REMOTE_UPLOADS)
    public void testElectedReplicaOutsideTheIsrRecoversAPhaseOnePartition() throws Exception {
        final String topic = "switch-elect-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);
        final int lostLeader = 1;
        final int electedReplica = 2;
        final int classicRecords = 20;
        final int disklessRecords = 10;

        // The bucket of the tiered storage plugin doesn't exist yet. Uploads fail, and the classic prefix
        // stays on the local disks.
        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            // Broker 0 isn't a replica and stays up for the test's clients.
            admin.createTopics(List.of(
                new NewTopic(topic, Map.of(0, List.of(lostLeader, electedReplica)))
                    .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"))
            )).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, topic);
            produceRecords(topic, classicRecords);

            alterTopicConfigWithIncrementalAlterConfigs(
                admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final long sealOffset = waitForSealOffset(admin, topic, 0, offset -> offset >= 0);
            assertEquals(classicRecords, sealOffset);

            // The follower leaves the ISR with the whole classic prefix. The leader is then the only ISR
            // member, and its shutdown stands for the loss of its disk.
            cluster.brokers().get(electedReplica).shutdown();
            waitForIsrShrink(admin, topic, 0, 2);
            assertEquals(List.of(lostLeader), waitForLeader(admin, topic, lostLeader));
            assertFalse(readClassicPrefixInRemoteStorage(admin, topic, 0),
                "The partition must still be in phase 1 when its last ISR member goes away");
            cluster.brokers().get(lostLeader).shutdown();
            cluster.brokers().get(electedReplica).startup();

            // The replica is back, but it's outside the ISR, and phase 1 blocks unclean leader election.
            assertFalse(waitForLeader(admin, topic, -1).contains(electedReplica));
            assertFalse(readClassicPrefixInRemoteStorage(admin, topic, 0));

            admin.alterDisklessSwitch(topic, 0, PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET,
                new AlterDisklessSwitchOptions().leaderId(electedReplica)).all().get(20, TimeUnit.SECONDS);
            assertEquals(List.of(electedReplica), waitForLeader(admin, topic, electedReplica));
            assertEquals(sealOffset, readSealOffset(admin, topic, 0));

            // The elected replica's log ends at the seal, so it serves the whole classic prefix.
            assertEquals(classicRecords, countCommittedRecordsUpTo(tp, sealOffset));

            // The partition also takes diskless records again.
            produceRecords(topic, disklessRecords);
            assertEquals(classicRecords + disklessRecords,
                countCommittedRecordsUpTo(tp, sealOffset + disklessRecords));

            // The old leader follows the elected replica and rejoins the ISR. It's the preferred replica,
            // so the controller can move leadership back to it afterward.
            cluster.brokers().get(lostLeader).startup();
            waitForIsrRecovery(admin, topic, 0, 2);

            // With the bucket in place, the leader uploads the classic prefix, and phase 2 starts.
            try (var s3Client = s3Container.getS3Client()) {
                s3Client.createBucket(request -> request.bucket(tieredStorageBucket));
            }
            waitForClassicPrefixInRemoteStorage(admin, topic);
        }
    }

    // A consumer validates a position and its leader epoch with OffsetsForLeaderEpoch. For a classic epoch, the end
    // offset in the response is at or below the seal, so ListOffsets must return a diskless epoch with a diskless offset.
    @Test
    @Tag(BLOCKS_REMOTE_UPLOADS)
    public void testConsumerKeepsAPositionFromListOffsetsOnASwitchedPartition() throws Exception {
        final String topic = "switch-list-offsets-epoch-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);
        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false")))).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, topic);
            produceRecords(topic, 20);
            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            assertEquals(20L, waitForSealOffset(admin, topic, 0, offset -> offset >= 0));
            produceRecords(topic, 30);

            // Phase 1.
            assertConsumerKeepsTheLatestPosition(admin, tp, 50L);

            try (var s3Client = s3Container.getS3Client()) {
                s3Client.createBucket(request -> request.bucket(tieredStorageBucket));
            }
            waitForClassicPrefixInRemoteStorage(admin, topic);

            // Phase 2 set a new diskless leader epoch.
            assertConsumerKeepsTheLatestPosition(admin, tp, 55L);
        }
    }

    private void assertConsumerKeepsTheLatestPosition(final Admin admin,
                                                      final TopicPartition tp,
                                                      final long expectedLatest) throws Exception {
        final ListOffsetsResult.ListOffsetsResultInfo latest = admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
            .partitionResult(tp).get(30, TimeUnit.SECONDS);
        assertEquals(expectedLatest, latest.offset());
        assertTrue(latest.leaderEpoch().isPresent(), "ListOffsets returned no leader epoch for a diskless offset");

        final Map<String, Object> consumerConfigs = baseConsumerConfigs();
        consumerConfigs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.assign(List.of(tp));
            // librdkafka keeps the epoch of a ListOffsets reset. A Java client does the same with this seek.
            consumer.seek(tp, new OffsetAndMetadata(latest.offset(), latest.leaderEpoch(), ""));
            produceRecords(tp.topic(), 5);
            final long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records = consumer.poll(Duration.ofMillis(500));
            }
            assertFalse(records.isEmpty(), "The consumer read no records");
            assertEquals(latest.offset(), records.iterator().next().offset(),
                "The consumer didn't start at the offset and leader epoch from ListOffsets");
        }
    }

    // DeleteRecords moves the log start of the leader only. In phase 1 the followers don't fetch, so they would keep
    // serving the deleted records. The broker rejects the request until phase 2.
    @Test
    @Tag(BLOCKS_REMOTE_UPLOADS)
    public void testDeleteRecordsIsRejectedUntilPhaseTwo() throws Exception {
        final String topic = "switch-delete-records-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);
        final int classicRecords = 20;
        final long deleteBefore = 10;

        try (Admin admin = AdminClient.create(baseClientConfigs())) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false")))).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, topic);
            produceRecords(topic, classicRecords);
            alterTopicConfigWithIncrementalAlterConfigs(admin, topic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            assertEquals(classicRecords, waitForSealOffset(admin, topic, 0, offset -> offset >= 0));
            waitForIsrRecovery(admin, topic, 0, 3);
            assertFalse(readClassicPrefixInRemoteStorage(admin, topic, 0));

            final ExecutionException rejected = assertThrows(ExecutionException.class, () ->
                admin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(deleteBefore)))
                    .lowWatermarks().get(tp).get(30, TimeUnit.SECONDS));
            assertInstanceOf(InvalidRequestException.class, rejected.getCause());
            for (Map.Entry<Integer, BrokerServer> broker : cluster.brokers().entrySet()) {
                assertEquals(0L, broker.getValue().replicaManager().getLog(tp).get().logStartOffset(),
                    "Broker " + broker.getKey() + " moved its log start offset");
            }
            // Brokers 0, 1, and 2 are in az1, az2, and az3. Each consumer reads from the replica in its zone.
            for (int zone = 1; zone <= 3; zone++) {
                assertEquals(0L, firstOffsetReadFromTheStart(tp, "az" + zone), "The consumer in az" + zone);
            }

            // With the bucket in place, the leader uploads the classic prefix, and phase 2 starts.
            try (var s3Client = s3Container.getS3Client()) {
                s3Client.createBucket(request -> request.bucket(tieredStorageBucket));
            }
            // `Admin` reads the phase from one broker. The leader rejects the request until its own
            // metadata shows phase 2, and the client doesn't retry `INVALID_REQUEST`.
            waitForPhaseTwoOnEveryBroker(tp);
            assertEquals(deleteBefore, admin.deleteRecords(Map.of(tp, RecordsToDelete.beforeOffset(deleteBefore)))
                .lowWatermarks().get(tp).get(30, TimeUnit.SECONDS).lowWatermark());
        }
    }

    private void waitForPhaseTwoOnEveryBroker(final TopicPartition tp) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (cluster.brokers().values().stream().allMatch(broker ->
                    broker.replicaManager().inklessMetadataView().isClassicPrefixInRemoteStorage(tp))) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Not every broker applied phase 2 of " + tp + " within timeout");
    }

    private long firstOffsetReadFromTheStart(final TopicPartition tp, final String zone) {
        final Map<String, Object> configs = baseConsumerConfigs();
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        configs.put(ConsumerConfig.CLIENT_ID_CONFIG, "delete-records,diskless_az=" + zone);
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs)) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, 0L);
            final long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
            while (System.currentTimeMillis() < deadline) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next().offset();
                }
            }
            throw new AssertionError("The consumer in " + zone + " read no record from " + tp);
        }
    }

    private void waitForClassicPrefixInRemoteStorage(final Admin admin, final String topic) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (readClassicPrefixInRemoteStorage(admin, topic, 0)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Partition " + topic + "-0 did not enter phase 2 within timeout");
    }

    private void produceRecords(final String topic, final int count) throws Exception {
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs())) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null,
                    ("record-" + i).getBytes(StandardCharsets.UTF_8))).get(30, TimeUnit.SECONDS);
            }
        }
    }

    /**
     * Waits until partition 0 of the topic has the leader, and returns its ISR at that time. A leader of
     * `-1` means no leader.
     */
    private List<Integer> waitForLeader(final Admin admin,
                                        final String topic,
                                        final int expectedLeader) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        int leader = -1;
        while (System.currentTimeMillis() < deadline) {
            final var partition = admin.describeTopics(List.of(topic)).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(topic).partitions().get(0);
            leader = partition.leader() == null ? -1 : partition.leader().id();
            if (leader == expectedLeader) {
                return partition.isr().stream().map(node -> node.id()).sorted().toList();
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Partition " + topic + "-0 did not reach leader=" + expectedLeader
            + " within timeout; last observed leader=" + leader);
    }

    private boolean readClassicPrefixInRemoteStorage(final Admin admin,
                                                     final String topic,
                                                     final int partition) throws Exception {
        return admin.describeTopicPartitions(List.of(topic)).rawResponse().get(10, TimeUnit.SECONDS)
            .topics().find(topic).partitions().stream()
            .filter(p -> p.partitionIndex() == partition)
            .findFirst()
            .map(p -> InitDisklessLogFields.decodeClassicPrefixInRemoteStorage(p.unknownTaggedFields()))
            .orElseThrow();
    }

    @Test
    public void testTransactionOpenAtSwitchIsAbortedAndCompletes() throws Exception {
        final String suffix = UUID.randomUUID().toString().substring(0, 8);
        final String switchedTopic = "switch-open-txn-" + suffix;
        final String classicTopic = "switch-open-txn-classic-" + suffix;
        final String transactionalId = "switch-open-txn-id-" + suffix;
        final TopicPartition switchedTp = new TopicPartition(switchedTopic, 0);
        final int recordsInOpenTransaction = 5;

        final Map<String, Object> transactionalProducerConfigs = producerConfigs();
        transactionalProducerConfigs.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);

        try (Admin admin = AdminClient.create(baseClientConfigs());
             Producer<byte[], byte[]> producer = new KafkaProducer<>(transactionalProducerConfigs)) {
            admin.createTopics(List.of(
                new NewTopic(switchedTopic, 1, (short) 3).configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false")),
                new NewTopic(classicTopic, 1, (short) 3).configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"))
            )).all().get(30, TimeUnit.SECONDS);
            TopicMetadataProbe.awaitVisible(admin, switchedTopic);
            TopicMetadataProbe.awaitVisible(admin, classicTopic);

            // The transaction writes to the partition while the topic is classic, and the producer doesn't end it.
            producer.initTransactions();
            producer.beginTransaction();
            for (int i = 0; i < recordsInOpenTransaction; i++) {
                producer.send(new ProducerRecord<>(switchedTopic, 0, null,
                    ("open-txn-" + i).getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
            }

            alterTopicConfigWithIncrementalAlterConfigs(
                admin, switchedTopic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final long seal = waitForSealOffset(admin, switchedTopic, 0, offset -> offset >= 0);
            log.warn("[stage=sealed] topic={} seal offset={}", switchedTopic, seal);

            // The classic prefix is the records of the open transaction and one `ABORT` marker.
            final long markerOffset = recordsInOpenTransaction;
            assertEquals(markerOffset + 1, seal);

            // The leader writes the marker in the leader epoch of the switch, and the followers replicate it.
            // A replica that wrote its own marker has it in another epoch.
            final Map<Integer, Integer> markerEpochByBroker = new HashMap<>();
            final Map<Integer, Integer> lastRecordEpochByBroker = new HashMap<>();
            for (final Map.Entry<Integer, BrokerServer> broker : cluster.brokers().entrySet()) {
                final UnifiedLog localLog = broker.getValue().replicaManager().getLog(switchedTp).get();
                markerEpochByBroker.put(broker.getKey(),
                    localLog.leaderEpochCache().epochForOffset(markerOffset).orElse(-1));
                lastRecordEpochByBroker.put(broker.getKey(),
                    localLog.leaderEpochCache().epochForOffset(markerOffset - 1).orElse(-1));
            }
            assertEquals(1, Set.copyOf(markerEpochByBroker.values()).size(),
                "Replicas disagree on the leader epoch of the ABORT marker: " + markerEpochByBroker);
            assertEquals(1, Set.copyOf(lastRecordEpochByBroker.values()).size(),
                "Replicas disagree on the leader epoch of the last record: " + lastRecordEpochByBroker);
            final int markerEpoch = markerEpochByBroker.values().iterator().next();
            final int lastRecordEpoch = lastRecordEpochByBroker.values().iterator().next();
            assertTrue(markerEpoch > lastRecordEpoch,
                "The ABORT marker epoch " + markerEpoch + " must be past the pre-switch leader epoch " + lastRecordEpoch);

            // The coordinator completes the transaction, although the partition doesn't take its marker.
            producer.commitTransaction();
            awaitTransactionState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);

            // The `transactional.id` is usable again.
            producer.beginTransaction();
            producer.send(new ProducerRecord<>(classicTopic, 0, null,
                "after-switch".getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
            producer.commitTransaction();
            awaitTransactionState(admin, transactionalId, TransactionState.COMPLETE_COMMIT);

            // The switch aborted the records on the switched partition.
            assertEquals(0, countCommittedRecordsUpTo(switchedTp, seal));
            assertEquals(1, countCommittedRecordsUpTo(new TopicPartition(classicTopic, 0), 2));
        }
    }

    private void awaitTransactionState(final Admin admin,
                                       final String transactionalId,
                                       final TransactionState expectedState) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        TransactionDescription last = null;
        while (System.currentTimeMillis() < deadline) {
            last = admin.describeTransactions(List.of(transactionalId))
                .description(transactionalId).get(10, TimeUnit.SECONDS);
            if (last.state() == expectedState && last.topicPartitions().isEmpty()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Transaction " + transactionalId + " did not reach " + expectedState
            + " with no pending partitions within timeout; last observed=" + last);
    }

    /**
     * Returns the number of records that a `read_committed` consumer gets from the partition
     * before its position reaches `endOffset`.
     */
    private int countCommittedRecordsUpTo(final TopicPartition tp, final long endOffset) {
        final Map<String, Object> configs = baseConsumerConfigs();
        configs.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        int count = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs)) {
            consumer.assign(List.of(tp));
            consumer.seekToBeginning(List.of(tp));
            final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (consumer.position(tp) < endOffset) {
                if (System.currentTimeMillis() > deadline) {
                    throw new AssertionError("Consumer position of " + tp + " stopped at " + consumer.position(tp)
                        + " before " + endOffset);
                }
                count += consumer.poll(Duration.ofMillis(500)).count();
            }
        }
        return count;
    }

    private long waitForSealOffset(final Admin admin,
                                   final String topic,
                                   final int partition,
                                   final java.util.function.LongPredicate condition) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        long last = PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET;
        while (System.currentTimeMillis() < deadline) {
            last = readSealOffset(admin, topic, partition);
            if (condition.test(last)) {
                return last;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Seal offset for " + topic + "-" + partition
            + " did not reach the expected state within timeout; last observed=" + last);
    }

    private void waitForLocalClassicPrefixDeletion(final String topic) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (localClassicPrefixDeletedOnAllBrokers(topic)) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Not every broker deleted the local classic prefix of " + topic + " within timeout");
    }

    private boolean localClassicPrefixDeletedOnAllBrokers(final String topic) {
        for (BrokerServer broker : cluster.brokers().values()) {
            final InklessMetadataView metadataView = broker.replicaManager().inklessMetadataView();
            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
                final TopicPartition tp = new TopicPartition(topic, partition);
                final long seal = metadataView.getClassicToDisklessStartOffset(tp);
                if (seal < 0 || !metadataView.isClassicPrefixInRemoteStorage(tp)) {
                    return false;
                }
                final scala.Option<UnifiedLog> log = broker.replicaManager().getLog(tp);
                if (log.isEmpty() || log.get().localLogStartOffset() < seal) {
                    return false;
                }
            }
        }
        return true;
    }

    private long readSealOffset(final Admin admin, final String topic, final int partition) throws Exception {
        final DescribeTopicPartitionsResponseData responseData =
            admin.describeTopicPartitions(List.of(topic)).rawResponse().get(10, TimeUnit.SECONDS);
        final DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic topicData =
            responseData.topics().find(topic);
        if (topicData == null) {
            // Topic metadata may not have propagated to this broker yet; treat as not-yet-switched.
            return PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET;
        }
        return topicData.partitions().stream()
            .filter(p -> p.partitionIndex() == partition)
            .findFirst()
            .map(p -> InitDisklessLogFields.decodeClassicToDisklessStartOffset(p.unknownTaggedFields()))
            .orElse(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET);
    }

    private void alterTopicConfigWithLegacyAlterConfigs(final String topic,
                                                       final Map<String, String> newConfigs) throws Exception {
        final ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final Collection<AlterConfigsRequest.ConfigEntry> configEntries = newConfigs.entrySet().stream()
            .map(entry -> new AlterConfigsRequest.ConfigEntry(entry.getKey(), entry.getValue()))
            .toList();
        final AlterConfigsRequest.Config config = new AlterConfigsRequest.Config(configEntries);
        final AlterConfigsRequest request = new AlterConfigsRequest.Builder(Map.of(topicResource, config), false)
            .build(ApiKeys.ALTER_CONFIGS.latestVersion());
        final AlterConfigsResponse response = IntegrationTestUtils.connectAndReceive(request, firstBrokerPort());
        final var apiError = response.errors().get(topicResource);
        assertNotNull(apiError, "Legacy AlterConfigs response did not contain topic resource " + topicResource);
        assertEquals(Errors.NONE, apiError.error(), "Legacy AlterConfigs failed: " + apiError.message());
    }

    private Errors alterTopicConfigWithLegacyAlterConfigsExpectingError(final String topic,
                                                                        final Map<String, String> newConfigs) throws Exception {
        final ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final Collection<AlterConfigsRequest.ConfigEntry> configEntries = newConfigs.entrySet().stream()
            .map(entry -> new AlterConfigsRequest.ConfigEntry(entry.getKey(), entry.getValue()))
            .toList();
        final AlterConfigsRequest.Config config = new AlterConfigsRequest.Config(configEntries);
        final AlterConfigsRequest request = new AlterConfigsRequest.Builder(Map.of(topicResource, config), false)
            .build(ApiKeys.ALTER_CONFIGS.latestVersion());
        final AlterConfigsResponse response = IntegrationTestUtils.connectAndReceive(request, firstBrokerPort());
        final var apiError = response.errors().get(topicResource);
        assertTrue(apiError != null, "Legacy AlterConfigs response did not contain topic resource " + topicResource);
        return apiError.error();
    }

    private int firstBrokerPort() {
        final String firstBootstrapServer = cluster.bootstrapServers().split(",")[0];
        return Integer.parseInt(firstBootstrapServer.substring(firstBootstrapServer.lastIndexOf(':') + 1));
    }

    private void waitForIsrShrink(final Admin admin, final String topic,
                                   final int partition, final int maxIsrSize) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
        while (System.currentTimeMillis() < deadline) {
            final var description = admin.describeTopics(List.of(topic)).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(topic);
            final int isrSize = description.partitions().get(partition).isr().size();
            if (isrSize < maxIsrSize) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("ISR for " + topic + "-" + partition +
            " did not shrink below " + maxIsrSize + " within timeout");
    }

    private void waitForIsrRecovery(final Admin admin, final String topic,
                                    final int partition, final int expectedIsrSize) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (System.currentTimeMillis() < deadline) {
            final var description = admin.describeTopics(List.of(topic)).allTopicNames()
                .get(10, TimeUnit.SECONDS).get(topic);
            final int isrSize = description.partitions().get(partition).isr().size();
            if (isrSize >= expectedIsrSize) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("ISR for " + topic + "-" + partition +
            " did not recover to " + expectedIsrSize + " within timeout");
    }
}
