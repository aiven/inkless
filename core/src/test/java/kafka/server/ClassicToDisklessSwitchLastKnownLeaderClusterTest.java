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

import kafka.cluster.Partition;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.message.DescribeTopicPartitionsResponseData;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.image.TopicImage;
import org.apache.kafka.metadata.InitDisklessLogFields;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.server.HostedPartition;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.LogSegmentData;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogSegmentMetadata;
import org.apache.kafka.server.log.remote.storage.RemoteStorageException;
import org.apache.kafka.server.log.remote.storage.RemoteStorageManager;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;
import org.apache.kafka.storage.internals.log.UnifiedLog;

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
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlane;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneConfig;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.storage_backend.s3.S3StorageConfig;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;
import io.aiven.inkless.test_utils.S3TestContainer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Drives a partition in phase 1 of the classic-to-diskless switch into a last-known-leader election (KIP-966)
 * whose leader lost the tail of the classic prefix, then recovers it with a downward re-seal.
 */
@Testcontainers
public class ClassicToDisklessSwitchLastKnownLeaderClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(ClassicToDisklessSwitchLastKnownLeaderClusterTest.class);
    private static final String RSM_CONFIG_PREFIX = RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
    private static final int CLASSIC_RECORDS = 20;
    private static final long TRUNCATED_LEO = 15;
    private static final int DISKLESS_RECORDS = 5;

    /**
     * Fails every segment copy while the gate is closed. The leader rolls the active segment at the seal, so
     * without the gate the prefix upload finishes within one task interval and the partition leaves phase 1.
     */
    public static class GatedRemoteStorageManager implements RemoteStorageManager {
        static final AtomicBoolean COPY_ALLOWED = new AtomicBoolean(true);
        private final RemoteStorageManager delegate;

        public GatedRemoteStorageManager() throws ReflectiveOperationException {
            delegate = (RemoteStorageManager) Class.forName("io.aiven.kafka.tieredstorage.RemoteStorageManager")
                .getDeclaredConstructor().newInstance();
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            delegate.configure(configs);
        }

        @Override
        public Optional<RemoteLogSegmentMetadata.CustomMetadata> copyLogSegmentData(
            final RemoteLogSegmentMetadata metadata, final LogSegmentData data) throws RemoteStorageException {
            if (!COPY_ALLOWED.get()) {
                throw new RemoteStorageException("Segment copy is gated by the test");
            }
            return delegate.copyLogSegmentData(metadata, data);
        }

        @Override
        public InputStream fetchLogSegment(final RemoteLogSegmentMetadata metadata, final int startPosition)
            throws RemoteStorageException {
            return delegate.fetchLogSegment(metadata, startPosition);
        }

        @Override
        public InputStream fetchLogSegment(final RemoteLogSegmentMetadata metadata, final int startPosition,
                                           final int endPosition) throws RemoteStorageException {
            return delegate.fetchLogSegment(metadata, startPosition, endPosition);
        }

        @Override
        public InputStream fetchIndex(final RemoteLogSegmentMetadata metadata, final IndexType indexType)
            throws RemoteStorageException {
            return delegate.fetchIndex(metadata, indexType);
        }

        @Override
        public void deleteLogSegmentData(final RemoteLogSegmentMetadata metadata) throws RemoteStorageException {
            delegate.deleteLogSegmentData(metadata);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private KafkaClusterTestKit cluster;

    @TempDir
    private Path tempCacheDir;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        GatedRemoteStorageManager.COPY_ALLOWED.set(true);
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        final Map<Integer, Map<String, String>> perServerProps = new HashMap<>();
        for (int brokerId = 0; brokerId < 3; brokerId++) {
            perServerProps.put(brokerId, Map.of(
                ServerConfigs.BROKER_RACK_CONFIG, "az" + (brokerId + 1),
                RSM_CONFIG_PREFIX + "fetch.chunk.cache.path",
                Files.createDirectories(tempCacheDir.resolve("ts-fetch-chunk-cache-" + brokerId)).toAbsolutePath().toString()
            ));
        }

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProps)
            .build();

        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "2000")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP, "4")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, TopicBasedRemoteLogMetadataManager.class.getName())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "EXTERNAL")
            .setConfigProp(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
                + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, "1")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, GatedRemoteStorageManager.class.getName())
            .setConfigProp(RSM_CONFIG_PREFIX + "chunk.size", 1048576)
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.class", "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.size", "10737418")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.prefetch.max.size", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.retention.ms", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "custom.metadata.fields.include", "REMOTE_SIZE")
            .setConfigProp(RSM_CONFIG_PREFIX + "key.prefix", "switch-elr-test/")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.bucket.name", s3Container.getBucketName())
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
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            .build();

        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
    }

    @AfterEach
    public void teardown() throws Exception {
        GatedRemoteStorageManager.COPY_ALLOWED.set(true);
        cluster.close();
    }

    @Test
    public void testReSealRecoversLastKnownLeaderBelowSeal() throws Exception {
        final String topic = "switch-elr-" + UUID.randomUUID().toString().substring(0, 8);
        final TopicPartition tp = new TopicPartition(topic, 0);

        try (Admin admin = AdminClient.create(clientConfigs())) {
            final int leader = startPhaseOne(admin, topic);
            driveIntoLastKnownLeaderBelowSeal(admin, topic, leader);
            reSealAndRecover(admin, topic, leader);
        }
        assertEquals(expectedRecords(), consumeFromStart(tp));
    }

    /** Returns the leader of the partition once the switch is in phase 1 with diskless records past the seal. */
    private int startPhaseOne(final Admin admin, final String topic) throws Exception {
        GatedRemoteStorageManager.COPY_ALLOWED.set(false);
        admin.createTopics(List.of(new NewTopic(topic, 1, (short) 3).configs(Map.of(
                TopicConfig.DISKLESS_ENABLE_CONFIG, "false",
                TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2"))))
            .all().get(30, TimeUnit.SECONDS);
        TopicMetadataProbe.awaitVisible(admin, topic);
        waitForPartition(admin, topic, p -> p.isr().size() == 3, "a full ISR");

        produce(topic, "classic-", CLASSIC_RECORDS);
        setTopicConfig(admin, topic, TopicConfig.DISKLESS_ENABLE_CONFIG, "true");
        waitForSealOffset(admin, topic, CLASSIC_RECORDS);
        produce(topic, "diskless-", DISKLESS_RECORDS);

        final PartitionRegistration phaseOne = registration(0, topic);
        final int leader = phaseOne.leader;
        log.warn("[stage=phase1] {}", phaseOne);
        return leader;
    }

    private void driveIntoLastKnownLeaderBelowSeal(final Admin admin, final String topic, final int leaderHint) throws Exception {
        final TopicPartition tp = new TopicPartition(topic, 0);
        final PartitionRegistration before = registration(leaderHint, topic);
        final int leader = before.leader;
        assertEquals(leaderHint, leader, "the leader moved before the failure: " + before);
        final List<Integer> followers = Arrays.stream(before.replicas).boxed().filter(id -> id != leader).toList();
        final int firstDown = followers.get(0);
        final int secondDown = followers.get(1);

        cluster.brokers().get(firstDown).shutdown();
        log.warn("[stage=first-down] {}", waitForRegistration(leader, topic,
            r -> r.isr.length == 2, "an ISR of 2 after the first follower stops"));
        cluster.brokers().get(secondDown).shutdown();
        log.warn("[stage=second-down] {}", waitForRegistration(leader, topic,
            r -> r.leader == leader && r.isr.length == 1 && Arrays.stream(r.elr).anyMatch(id -> id == secondDown),
            "an ISR of the leader only, with the second follower in the ELR"));

        // The leader loses the tail of the classic prefix, as after a power loss.
        final UnifiedLog leaderLog = cluster.brokers().get(leader).replicaManager().getLog(tp).get();
        leaderLog.truncateTo(TRUNCATED_LEO);
        assertEquals(TRUNCATED_LEO, leaderLog.logEndOffset());
        cluster.brokers().get(leader).shutdown();

        // Without the marker, the controller sees an unclean shutdown and removes the replica from the ELR.
        deleteCleanShutdownFiles(leader);
        deleteCleanShutdownFiles(secondDown);
        cluster.brokers().get(secondDown).startup();
        cluster.brokers().get(firstDown).startup();
        log.warn("[stage=followers-back] {}", waitForRegistration(secondDown, topic,
            r -> Arrays.equals(r.elr, new int[] {leader}), "the unclean restart to remove the follower from the ELR"));
        cluster.brokers().get(leader).startup();

        final PartitionRegistration elected = waitForRegistration(secondDown, topic,
            r -> r.leader == leader, "the last known leader to be elected");
        log.warn("[stage=case-b] {}", elected);
        assertArrayEquals(new int[] {leader}, elected.isr);
        waitForCondition(() -> isOffline(leader, tp), "the leader to mark the partition offline");
    }

    private PartitionRegistration registration(final int brokerId, final String topic) {
        final TopicImage image = cluster.brokers().get(brokerId).metadataCache().currentImage().topics().getTopic(topic);
        return image == null ? null : image.partitions().get(0);
    }

    private PartitionRegistration waitForRegistration(final int brokerId, final String topic,
                                                      final Predicate<PartitionRegistration> condition,
                                                      final String description) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        PartitionRegistration last = null;
        while (System.currentTimeMillis() < deadline) {
            last = registration(brokerId, topic);
            if (last != null && condition.test(last)) {
                return last;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for " + description + "; last observed=" + last);
    }

    private void reSealAndRecover(final Admin admin, final String topic, final int leader) throws Exception {
        final TopicPartition tp = new TopicPartition(topic, 0);

        // `kafka-topic-switch.sh seal --offset` checks the offset against these two values before it sends the request.
        final long earliest = admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).all().get(20, TimeUnit.SECONDS).get(tp).offset();
        final long latest = admin.listOffsets(Map.of(tp, OffsetSpec.latest())).all().get(20, TimeUnit.SECONDS).get(tp).offset();
        log.warn("[stage=cli-validation] listOffsets earliest={} latest={}", earliest, latest);

        admin.alterDisklessSwitch(topic, 0, TRUNCATED_LEO).all().get(20, TimeUnit.SECONDS);
        waitForSealOffset(admin, topic, TRUNCATED_LEO);
        Thread.sleep(2000);
        log.warn("[stage=resealed] offline on the leader before the restart={}", isOffline(leader, tp));

        cluster.brokers().get(leader).shutdown();
        cluster.brokers().get(leader).startup();
        final TopicPartitionInfo restarted = waitForPartition(admin, topic,
            p -> p.leader() != null && p.leader().id() == leader, "the leader to return after the restart");
        log.warn("[stage=restarted] leader={} isr={} elr={}", restarted.leader(), restarted.isr(), restarted.elr());
        waitForCondition(() -> isOnlineLeader(leader, tp), "the partition to be online on the leader");

        admin.repairDisklessLog(topic, 0, leader).all().get(20, TimeUnit.SECONDS);
        GatedRemoteStorageManager.COPY_ALLOWED.set(true);

        waitForPartition(admin, topic, p -> p.isr().size() == 3, "the ISR to recover");
        waitForCondition(() -> cluster.brokers().values().stream().allMatch(b ->
            b.replicaManager().inklessMetadataView().isClassicPrefixInRemoteStorage(tp)), "phase 2 to start");
        for (BrokerServer broker : cluster.brokers().values()) {
            log.warn("[stage=phase2] broker={} log={}", broker.config().nodeId(),
                broker.replicaManager().getLog(tp).map(l -> "lso=" + l.logStartOffset() + " leo=" + l.logEndOffset()));
        }
    }

    private List<String> consumeFromStart(final TopicPartition tp) {
        final Map<String, Object> configs = clientConfigs();
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "elr-group-" + UUID.randomUUID());
        final List<String> consumed = new ArrayList<>();
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs)) {
            consumer.assign(List.of(tp));
            consumer.seek(tp, 0);
            final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (System.currentTimeMillis() < deadline && consumed.size() < expectedRecords().size()) {
                for (ConsumerRecord<byte[], byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
                    consumed.add(record.offset() + "=" + new String(record.value(), StandardCharsets.UTF_8));
                }
            }
        }
        log.warn("[stage=consumed] {}", consumed);
        return consumed;
    }

    private static List<String> expectedRecords() {
        final List<String> expected = new ArrayList<>();
        for (int i = 0; i < TRUNCATED_LEO; i++) {
            expected.add(i + "=classic-" + i);
        }
        for (int i = 0; i < DISKLESS_RECORDS; i++) {
            expected.add((CLASSIC_RECORDS + i) + "=diskless-" + i);
        }
        return expected;
    }

    private boolean isOnlineLeader(final int brokerId, final TopicPartition tp) {
        return cluster.brokers().get(brokerId).replicaManager().getPartition(tp) instanceof HostedPartition.Online<?> online
            && ((Partition) online.partition()).isLeader();
    }

    private boolean isOffline(final int brokerId, final TopicPartition tp) {
        return cluster.brokers().get(brokerId).replicaManager().getPartition(tp) instanceof HostedPartition.Offline;
    }

    private void produce(final String topic, final String prefix, final int count) throws Exception {
        final Map<String, Object> configs = clientConfigs();
        configs.put(ProducerConfig.ACKS_CONFIG, "all");
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(configs)) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, 0, null,
                    (prefix + i).getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
            }
        }
    }

    private Map<String, Object> clientConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return configs;
    }

    private void setTopicConfig(final Admin admin, final String topic, final String key, final String value) throws Exception {
        final ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        admin.incrementalAlterConfigs(Map.of(resource,
                List.of(new AlterConfigOp(new ConfigEntry(key, value), AlterConfigOp.OpType.SET))))
            .all().get(20, TimeUnit.SECONDS);
    }

    private void deleteCleanShutdownFiles(final int brokerId) throws Exception {
        for (String logDir : cluster.brokers().get(brokerId).config().logDirs()) {
            Files.deleteIfExists(Path.of(logDir, CleanShutdownFileHandler.CLEAN_SHUTDOWN_FILE_NAME));
        }
    }

    private TopicPartitionInfo describePartition(final Admin admin, final String topic) throws Exception {
        return admin.describeTopics(List.of(topic)).allTopicNames().get(10, TimeUnit.SECONDS)
            .get(topic).partitions().get(0);
    }

    private TopicPartitionInfo waitForPartition(final Admin admin, final String topic,
                                                final Predicate<TopicPartitionInfo> condition,
                                                final String description) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        TopicPartitionInfo last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                last = describePartition(admin, topic);
                if (condition.test(last)) {
                    return last;
                }
            } catch (ExecutionException e) {
                log.warn("describeTopics failed while waiting for {}: {}", description, e.toString());
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Timed out waiting for " + description + "; last observed=" + last);
    }

    private void waitForCondition(final BooleanSupplier condition, final String description) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(500);
        }
        throw new AssertionError("Timed out waiting for " + description);
    }

    private void waitForSealOffset(final Admin admin, final String topic, final long expected) throws Exception {
        waitForCondition(() -> {
            try {
                return readSealOffset(admin, topic) == expected;
            } catch (Exception e) {
                return false;
            }
        }, "seal offset " + expected);
    }

    private long readSealOffset(final Admin admin, final String topic) throws Exception {
        final DescribeTopicPartitionsResponseData responseData =
            admin.describeTopicPartitions(List.of(topic)).rawResponse().get(10, TimeUnit.SECONDS);
        final DescribeTopicPartitionsResponseData.DescribeTopicPartitionsResponseTopic topicData =
            responseData.topics().find(topic);
        if (topicData == null) {
            return PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET;
        }
        return topicData.partitions().stream()
            .filter(p -> p.partitionIndex() == 0)
            .findFirst()
            .map(p -> InitDisklessLogFields.decodeClassicToDisklessStartOffset(p.unknownTaggedFields()))
            .orElse(PartitionRegistration.NO_CLASSIC_TO_DISKLESS_START_OFFSET);
    }
}
