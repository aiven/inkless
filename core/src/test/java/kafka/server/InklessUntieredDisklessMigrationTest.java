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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNode;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.metadata.PartitionRegistration;
import org.apache.kafka.metadata.storage.Formatter;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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

import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Migrates an untiered diskless topic to consolidation. The topic starts on a cluster with
 * consolidation off. A second cluster reuses the directories of the first with consolidation on,
 * and an alter of {@code remote.storage.enable} starts consolidation for the topic.
 */
@Testcontainers
public class InklessUntieredDisklessMigrationTest {

    private static final int NUM_BROKERS = 3;
    private static final short REPLICATION_FACTOR = 2;
    private static final String TOPIC_NAME = "untiered-diskless-migration";
    private static final TopicPartition TOPIC_PARTITION = new TopicPartition(TOPIC_NAME, 0);
    private static final int RECORD_COUNT = 10;
    private static final long TIMEOUT_MS = 60_000;
    private static final long LOCAL_RETENTION_MS = 600_000;
    private static final long LOCAL_RETENTION_BYTES = 104_857_600;

    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    @TempDir
    private Path baseDirectory;
    @TempDir
    private Path cacheDirectory;

    /**
     * @param walStartOffset the WAL start before the migration. A positive value means retention or
     *                       DeleteRecords pruned the WAL while no remote segment exists.
     */
    @ParameterizedTest
    @ValueSource(ints = {0, 5})
    public void testAlterStartsConsolidationOfUntieredDisklessTopic(final int walStartOffset, final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        final String clusterId = UUID.randomUUID().toString();

        // Untiered diskless topic on a cluster with consolidation off.
        try (KafkaClusterTestKit first = createCluster(clusterId, false)) {
            first.format();
            first.startup();
            first.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, first.bootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(TOPIC_NAME, 1, REPLICATION_FACTOR)
                    .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true", TopicConfig.SEGMENT_BYTES_CONFIG, "1048576"))))
                    .all().get(30, TimeUnit.SECONDS);
            }
            produce(first, TOPIC_NAME, 1, RECORD_COUNT);
            if (walStartOffset > 0) {
                try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, first.bootstrapServers()))) {
                    admin.deleteRecords(Map.of(TOPIC_PARTITION, RecordsToDelete.beforeOffset(walStartOffset)))
                        .all().get(30, TimeUnit.SECONDS);
                }
            }
        }

        // Restart with consolidation on. The alter of the topic starts consolidation.
        try (KafkaClusterTestKit second = createCluster(clusterId, true)) {
            // No format(): the directories contain the metadata log and `meta.properties` of the first cluster.
            second.startup();
            second.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                alterTopic(admin, TOPIC_NAME, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
            }
            TestUtils.waitForCondition(
                () -> maxLogEndOffset(second, TOPIC_PARTITION).orElse(0) >= RECORD_COUNT,
                TIMEOUT_MS,
                () -> "Consolidation should reach the end of the WAL for " + TOPIC_PARTITION
                    + ", max local LEO: " + maxLogEndOffset(second, TOPIC_PARTITION));
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                assertEquals(walStartOffset, admin.listOffsets(Map.of(TOPIC_PARTITION, OffsetSpec.earliest()))
                    .partitionResult(TOPIC_PARTITION).get(30, TimeUnit.SECONDS).offset());
            }
            final UnifiedLog localLog = longestLocalLog(second, TOPIC_PARTITION).orElseThrow();
            assertEquals(walStartOffset, localLog.logStartOffset());
            assertEquals(LOCAL_RETENTION_MS, localLog.config().localRetentionMs());
            assertEquals(LOCAL_RETENTION_BYTES, localLog.config().localRetentionBytes());
            consume(second, TOPIC_NAME, RECORD_COUNT - walStartOffset);

            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                // About 3 MiB of new records roll the 1 MiB segments, so the remote log manager tiers the records written before the alter.
                produce(second, TOPIC_NAME, 1, 30, 100_000);
                TestUtils.waitForCondition(
                    () -> admin.listOffsets(Map.of(TOPIC_PARTITION, OffsetSpec.latestTiered()))
                        .partitionResult(TOPIC_PARTITION).get(30, TimeUnit.SECONDS).offset() >= RECORD_COUNT - 1,
                    TIMEOUT_MS * 2,
                    "The records written before the alter should reach remote storage");

                // The controller rejects a revert of the alter while consolidation is on.
                // Upstream validation accepts this revert only with `remote.log.delete.on.disable=true`.
                assertRemoteStorageRequired(() -> alterTopic(admin, TOPIC_NAME, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false",
                    TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true")));
                assertRemoteStorageRequired(() -> admin.incrementalAlterConfigs(Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME),
                    List.of(new AlterConfigOp(new ConfigEntry(REMOTE_LOG_STORAGE_ENABLE_CONFIG, ""), AlterConfigOp.OpType.DELETE))))
                    .all().get(30, TimeUnit.SECONDS));
            }
        }
    }

    /**
     * An untiered diskless topic keeps working as a diskless topic when the cluster turns on consolidation,
     * next to a topic that migrates.
     */
    @Test
    public void testUntieredTopicKeepsWorkingWhenConsolidationIsEnabled(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        final String clusterId = UUID.randomUUID().toString();
        final int partitions = 3;
        final String untiered = "untiered-stays";
        final String migrating = "untiered-migrating";
        final String shortRetention = "untiered-short-retention";
        final TopicPartition migratingPartition = new TopicPartition(migrating, 0);

        try (KafkaClusterTestKit first = createCluster(clusterId, false)) {
            first.format();
            first.startup();
            first.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, first.bootstrapServers()))) {
                admin.createTopics(List.of(
                    new NewTopic(untiered, partitions, REPLICATION_FACTOR).configs(Map.of(DISKLESS_ENABLE_CONFIG, "true")),
                    new NewTopic(migrating, 1, REPLICATION_FACTOR).configs(Map.of(DISKLESS_ENABLE_CONFIG, "true")),
                    new NewTopic(shortRetention, 1, REPLICATION_FACTOR).configs(Map.of(DISKLESS_ENABLE_CONFIG, "true",
                        TopicConfig.RETENTION_MS_CONFIG, String.valueOf(LOCAL_RETENTION_MS / 2)))
                )).all().get(30, TimeUnit.SECONDS);
            }
            produce(first, untiered, partitions, 30);
            produce(first, migrating, 1, RECORD_COUNT);
            consume(first, untiered, 30);
        }

        try (KafkaClusterTestKit second = createCluster(clusterId, true)) {
            second.startup();
            second.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                // Data written before the restart is readable, and new writes work.
                consume(second, untiered, 30);
                produce(second, untiered, partitions, 20);
                consume(second, untiered, 50);

                // ListOffsets answers from the control plane.
                long latestSum = 0;
                for (int p = 0; p < partitions; p++) {
                    final TopicPartition tp = new TopicPartition(untiered, p);
                    assertEquals(0L, admin.listOffsets(Map.of(tp, OffsetSpec.earliest()))
                        .partitionResult(tp).get(30, TimeUnit.SECONDS).offset());
                    latestSum += admin.listOffsets(Map.of(tp, OffsetSpec.latest()))
                        .partitionResult(tp).get(30, TimeUnit.SECONDS).offset();
                }
                assertEquals(50L, latestSum);

                // DeleteRecords advances the start.
                final TopicPartition first0 = new TopicPartition(untiered, 0);
                admin.deleteRecords(Map.of(first0, RecordsToDelete.beforeOffset(1))).all().get(30, TimeUnit.SECONDS);
                assertEquals(1L, admin.listOffsets(Map.of(first0, OffsetSpec.earliest()))
                    .partitionResult(first0).get(30, TimeUnit.SECONDS).offset());

                // A routine alter still passes validation.
                alterTopic(admin, untiered, Map.of(TopicConfig.RETENTION_MS_CONFIG, "86400000"));

                // The controller creates a new diskless topic with remote storage on and rejects an explicit false.
                admin.createTopics(List.of(new NewTopic("born-consolidating", 1, REPLICATION_FACTOR)
                    .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true")))).all().get(30, TimeUnit.SECONDS);
                final var bornConfigs = admin.describeConfigs(List.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, "born-consolidating")))
                    .all().get(30, TimeUnit.SECONDS).values().iterator().next();
                assertEquals("true", bornConfigs.get(REMOTE_LOG_STORAGE_ENABLE_CONFIG).value());
                assertRemoteStorageRequired(() ->
                    admin.createTopics(List.of(new NewTopic("born-untiered", 1, REPLICATION_FACTOR)
                        .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true", REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"))))
                        .all().get(30, TimeUnit.SECONDS));

                // The controller validates a new diskless topic with remote storage on, so a create that inherits
                // a broker local retention larger than the topic retention fails at create time.
                final ExecutionException bornTooShort = assertThrows(ExecutionException.class, () ->
                    admin.createTopics(List.of(new NewTopic("born-short-retention", 1, REPLICATION_FACTOR)
                        .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true",
                            TopicConfig.RETENTION_MS_CONFIG, String.valueOf(LOCAL_RETENTION_MS / 2)))))
                        .all().get(30, TimeUnit.SECONDS));
                assertInstanceOf(InvalidConfigurationException.class, bornTooShort.getCause());
                assertTrue(bornTooShort.getCause().getMessage().contains(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG),
                    bornTooShort.getCause().getMessage());
                admin.createTopics(List.of(new NewTopic("born-short-retention", 1, REPLICATION_FACTOR)
                    .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true",
                        TopicConfig.RETENTION_MS_CONFIG, String.valueOf(LOCAL_RETENTION_MS / 2),
                        TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, String.valueOf(LOCAL_RETENTION_MS / 4)))))
                    .all().get(30, TimeUnit.SECONDS);
                alterTopic(admin, "born-short-retention", Map.of(TopicConfig.MAX_MESSAGE_BYTES_CONFIG, "2097152"));

                // The broker local retention is larger than the topic retention, so only an alter with a topic-level bound passes.
                final ExecutionException tooShort = assertThrows(ExecutionException.class, () ->
                    alterTopic(admin, shortRetention, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")));
                assertInstanceOf(InvalidConfigurationException.class, tooShort.getCause());
                assertTrue(tooShort.getCause().getMessage().contains(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG),
                    tooShort.getCause().getMessage());
                alterTopic(admin, shortRetention, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                    TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, String.valueOf(LOCAL_RETENTION_MS / 4)));

                // The untiered topic has no local log and doesn't consolidate.
                for (int p = 0; p < partitions; p++) {
                    assertEquals(OptionalLong.empty(), maxLogEndOffset(second, new TopicPartition(untiered, p)));
                }

                // Migrating one topic doesn't change the other.
                alterTopic(admin, migrating, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
                TestUtils.waitForCondition(
                    () -> maxLogEndOffset(second, migratingPartition).orElse(0) >= RECORD_COUNT,
                    TIMEOUT_MS,
                    () -> "The altered topic should consolidate: " + maxLogEndOffset(second, migratingPartition));
                for (int p = 0; p < partitions; p++) {
                    assertEquals(OptionalLong.empty(), maxLogEndOffset(second, new TopicPartition(untiered, p)));
                }
                produce(second, untiered, partitions, 5);
                consume(second, untiered, 54);
                consume(second, migrating, RECORD_COUNT);
            }
        }
    }

    /**
     * An untiered topic created with replication factor 1 on a cluster with managed replicas off.
     * The cluster after the restart has managed replicas and consolidation on. The replication
     * factor goes from 1 to 3 with a partition reassignment, either before or after the alter that
     * starts consolidation.
     *
     * @param increaseBeforeAlter whether the reassignment runs while the topic is still untiered
     */
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testReplicationFactorIncreaseOfUntieredTopic(final boolean increaseBeforeAlter, final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        final String clusterId = UUID.randomUUID().toString();
        final String topic = "untiered-rf-increase";
        final TopicPartition partition = new TopicPartition(topic, 0);
        final int initialRecords = 20;

        try (KafkaClusterTestKit first = createCluster(clusterId, false, false)) {
            first.format();
            first.startup();
            first.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, first.bootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)
                    .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true")))).all().get(30, TimeUnit.SECONDS);
            }
            produce(first, topic, 1, initialRecords);
        }

        try (KafkaClusterTestKit second = createCluster(clusterId, true, true)) {
            second.startup();
            second.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                assertEquals(1, replicasOf(second, topic).size());
                if (increaseBeforeAlter) {
                    increaseReplicationFactor(second, admin, partition, NUM_BROKERS);
                    consume(second, topic, initialRecords);
                    assertEquals(OptionalLong.empty(), maxLogEndOffset(second, partition));
                }

                alterTopic(admin, topic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));

                if (!increaseBeforeAlter) {
                    awaitLocalLogEndOffsets(second, partition, replicasOf(second, topic), initialRecords);
                    increaseReplicationFactor(second, admin, partition, NUM_BROKERS);
                }

                final Set<Integer> replicas = replicasOf(second, topic);
                assertEquals(NUM_BROKERS, replicas.size());
                awaitLocalLogEndOffsets(second, partition, replicas, initialRecords);

                produce(second, topic, 1, 5);
                awaitLocalLogEndOffsets(second, partition, replicas, initialRecords + 5);
                consume(second, topic, initialRecords + 5);
            }
        }
    }

    /**
     * Metadata routing and reads for clients in each availability zone and for a client without one.
     * Both topics start untiered with replication factor 1 on a cluster with managed replicas off.
     * After the restart, one topic stays untiered and routes to a broker in the client zone. The other
     * gets replication factor 2 and then remote storage, as the migration procedure says, and routes
     * to a replica, in the client zone when one exists there.
     */
    @Test
    public void testClientRoutingOfUntieredAndConsolidatedTopics(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        final String clusterId = UUID.randomUUID().toString();
        final String untiered = "routing-untiered";
        final String consolidated = "routing-consolidated";
        final int records = 20;

        try (KafkaClusterTestKit first = createCluster(clusterId, false, false)) {
            first.format();
            first.startup();
            first.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, first.bootstrapServers()))) {
                admin.createTopics(List.of(
                    new NewTopic(untiered, 1, (short) 1).configs(Map.of(DISKLESS_ENABLE_CONFIG, "true")),
                    new NewTopic(consolidated, 1, (short) 1).configs(Map.of(DISKLESS_ENABLE_CONFIG, "true"))
                )).all().get(30, TimeUnit.SECONDS);
            }
            produce(first, untiered, 1, records);
            produce(first, consolidated, 1, records);
        }

        try (KafkaClusterTestKit second = createCluster(clusterId, true, true)) {
            second.startup();
            second.waitForReadyBrokers();
            final TopicPartition consolidatedPartition = new TopicPartition(consolidated, 0);
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                increaseReplicationFactor(second, admin, consolidatedPartition, 2);
                alterTopic(admin, consolidated, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
            }
            final Set<Integer> replicas = replicasOf(second, consolidated);
            awaitLocalLogEndOffsets(second, consolidatedPartition, replicas, records);

            final List<String> violations = new ArrayList<>();
            for (final String topic : List.of(untiered, consolidated)) {
                for (final String az : Arrays.asList("az1", "az2", "az3", null)) {
                    final RoutingResult result = routingOf(second, topic, az, records);
                    final String leaderAz = "az" + (result.leader() + 1);
                    final String cell = topic + " clientAz=" + az
                        + " leader=" + result.leader() + " consumed=" + result.consumed();
                    if (result.leader() < 0) {
                        violations.add(cell + ": no leader");
                    }
                    if (result.consumed() != records) {
                        violations.add(cell + ": read " + result.consumed() + " of " + records);
                    }
                    if (az != null && topic.equals(untiered) && !az.equals(leaderAz)) {
                        violations.add(cell + ": untiered topic should route to the client zone");
                    }
                    if (topic.equals(consolidated)) {
                        if (!replicas.contains(result.leader())) {
                            violations.add(cell + ": leader is not an assigned replica " + replicas);
                        } else if (az != null && replicas.stream().anyMatch(id -> az.equals("az" + (id + 1))) && !az.equals(leaderAz)) {
                            violations.add(cell + ": a replica exists in the client zone");
                        }
                    }
                }
            }
            assertTrue(violations.isEmpty(), () -> String.join("\n", violations));
        }
    }

    private record RoutingResult(int leader, int consumed) { }

    private static RoutingResult routingOf(KafkaClusterTestKit cluster, String topic, String clientAz, int expected) {
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            CommonClientConfigs.CLIENT_ID_CONFIG, clientAz == null ? "routing-client" : "routing-client,diskless_az=" + clientAz,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_ID_CONFIG, "routing-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            final Node leaderNode = consumer.partitionsFor(topic).get(0).leader();
            final int leader = leaderNode == null ? -1 : leaderNode.id();
            consumer.subscribe(List.of(topic));
            int consumed = 0;
            final long deadline = System.currentTimeMillis() + TIMEOUT_MS;
            while (consumed < expected && System.currentTimeMillis() < deadline) {
                consumed += consumer.poll(Duration.ofMillis(500)).count();
            }
            return new RoutingResult(leader, consumed);
        }
    }

    /**
     * Replaces the brokers during the upgrade instead of restarting them on the same disks. The new
     * brokers start with consolidation on. The old brokers keep the old configuration until the
     * untiered topic moves to the new brokers, and then they stop. The alter runs after that.
     */
    @Test
    public void testUntieredTopicMigratesAfterBrokerReplacement(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        final String clusterId = UUID.randomUUID().toString();
        final String topic = "untiered-broker-replacement";
        final TopicPartition partition = new TopicPartition(topic, 0);
        final int initialRecords = 20;
        final List<Integer> oldBrokers = List.of(0, 1, 2);
        final List<Integer> newBrokers = List.of(3, 4, 5);

        try (KafkaClusterTestKit first = createCluster(clusterId, false, true, false, NUM_BROKERS, Map.of())) {
            first.format();
            first.startup();
            first.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, first.bootstrapServers()))) {
                admin.createTopics(List.of(new NewTopic(topic, 1, REPLICATION_FACTOR)
                    .configs(Map.of(DISKLESS_ENABLE_CONFIG, "true")))).all().get(30, TimeUnit.SECONDS);
            }
            produce(first, topic, 1, initialRecords);
        }

        final Map<String, String> oldConfig = Map.of(
            ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "false",
            RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "false");
        final Map<Integer, Map<String, String>> overrides = oldBrokers.stream()
            .collect(Collectors.toMap(id -> id, id -> oldConfig));
        try (KafkaClusterTestKit second = createCluster(clusterId, true, true, false, 2 * NUM_BROKERS, overrides)) {
            // No format() for the old brokers and the controller: their directories contain the metadata of the first cluster.
            formatBrokers(second, newBrokers);
            second.startup();
            second.waitForReadyBrokers();
            try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, second.bootstrapServers()))) {
                // The first group read creates __consumer_offsets, and the new brokers create the remote log metadata topic.
                // The move must include both.
                TestUtils.waitForCondition(
                    () -> second.brokers().get(Collections.max(second.brokers().keySet())).metadataCache().currentImage()
                        .topics().getTopic(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME) != null,
                    TIMEOUT_MS,
                    "The remote log metadata topic should exist before the move");
                consume(second, topic, initialRecords);
                moveAllPartitions(second, admin, NUM_BROKERS);
                final Set<Integer> newReplicas = replicasOf(second, topic);
                assertTrue(newBrokers.containsAll(newReplicas), () -> "Replicas should be new brokers: " + newReplicas);
                consume(second, topic, initialRecords);
                for (final int id : oldBrokers) {
                    second.brokers().get(id).shutdown();
                    second.brokers().get(id).awaitShutdown();
                }
                assertEquals(OptionalLong.empty(), maxLogEndOffset(second, partition, newBrokers));

                alterTopic(admin, topic, Map.of(REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"));
                awaitLocalLogEndOffsets(second, partition, newReplicas, initialRecords);
                consume(second, topic, initialRecords);

                produce(second, topic, 1, 5);
                awaitLocalLogEndOffsets(second, partition, newReplicas, initialRecords + 5);
                consume(second, topic, initialRecords + 5);
            }
        }
    }

    /**
     * Moves every partition, internal topics included, from each broker {@code id < offset} to broker
     * {@code id + offset}, which is in the same availability zone.
     */
    private static void moveAllPartitions(KafkaClusterTestKit cluster, Admin admin, int offset) throws Exception {
        final Map<TopicPartition, Optional<NewPartitionReassignment>> moves = new HashMap<>();
        final var image = cluster.brokers().get(Collections.max(cluster.brokers().keySet())).metadataCache().currentImage();
        image.topics().topicsByName().forEach((name, topicImage) -> topicImage.partitions().forEach((index, registration) ->
            moves.put(new TopicPartition(name, index), Optional.of(new NewPartitionReassignment(
                Arrays.stream(registration.replicas).map(id -> id < offset ? id + offset : id).boxed().toList())))));
        admin.alterPartitionReassignments(moves).all().get(30, TimeUnit.SECONDS);
        TestUtils.waitForCondition(
            () -> admin.listPartitionReassignments().reassignments().get(30, TimeUnit.SECONDS).isEmpty()
                && moves.entrySet().stream().allMatch(e ->
                    Arrays.stream(kraftPartition(cluster, e.getKey()).replicas).boxed().toList()
                        .equals(e.getValue().orElseThrow().targetReplicas())),
            TIMEOUT_MS,
            () -> "All " + moves.size() + " partitions should move to the new brokers");
    }

    /** Formats brokers that join with empty directories. {@link KafkaClusterTestKit#format()} formats every node. */
    private static void formatBrokers(KafkaClusterTestKit cluster, List<Integer> brokerIds) throws Exception {
        for (final int id : brokerIds) {
            final TestKitNode node = cluster.nodes().brokerNodes().get(id);
            final Set<String> directories = new HashSet<>(node.logDataDirectories());
            directories.add(node.metadataDirectory());
            new Formatter()
                .setNodeId(id)
                .setClusterId(cluster.nodes().clusterId())
                .setDirectories(directories)
                .setReleaseVersion(cluster.nodes().bootstrapMetadata().metadataVersion())
                .setUnstableFeatureVersionsEnabled(true)
                .setControllerListenerName(cluster.nodes().controllerListenerName().value())
                .setMetadataLogDirectory(node.metadataDirectory())
                .run();
        }
    }

    /** Reads the assignment from the KRaft image, because describeTopics reports a transformer-selected replica at replication factor 1. */
    private static Set<Integer> replicasOf(KafkaClusterTestKit cluster, String topic) {
        return Arrays.stream(kraftPartition(cluster, new TopicPartition(topic, 0)).replicas).boxed().collect(Collectors.toSet());
    }

    /** Reads from the broker with the highest id, which is the last to stop in a broker replacement. */
    private static PartitionRegistration kraftPartition(KafkaClusterTestKit cluster, TopicPartition partition) {
        return cluster.brokers().get(Collections.max(cluster.brokers().keySet())).metadataCache().currentImage().topics()
            .getTopic(partition.topic()).partitions().get(partition.partition());
    }

    /** Appends brokers after the current replicas, so the current leader is first. */
    private static void increaseReplicationFactor(KafkaClusterTestKit cluster, Admin admin, TopicPartition partition, int targetReplicationFactor) throws Exception {
        final List<Integer> target = new ArrayList<>(Arrays.stream(kraftPartition(cluster, partition).replicas).boxed().toList());
        for (int id = 0; id < NUM_BROKERS && target.size() < targetReplicationFactor; id++) {
            if (!target.contains(id)) {
                target.add(id);
            }
        }
        reassign(cluster, admin, partition, target);
    }

    private static void reassign(KafkaClusterTestKit cluster, Admin admin, TopicPartition partition, List<Integer> target) throws Exception {
        admin.alterPartitionReassignments(Map.of(partition, Optional.of(new NewPartitionReassignment(target))))
            .all().get(30, TimeUnit.SECONDS);
        TestUtils.waitForCondition(
            () -> replicasOf(cluster, partition.topic()).equals(Set.copyOf(target))
                && admin.listPartitionReassignments().reassignments().get(30, TimeUnit.SECONDS).isEmpty(),
            TIMEOUT_MS,
            () -> "Reassignment of " + partition + " to " + target + " should complete");
    }

    private static void assertRemoteStorageRequired(Executable request) {
        final ExecutionException rejected = assertThrows(ExecutionException.class, request);
        assertInstanceOf(InvalidConfigurationException.class, rejected.getCause());
        assertTrue(rejected.getCause().getMessage().contains("Diskless topics must have remote storage enabled"),
            rejected.getCause().getMessage());
    }

    private static void alterTopic(Admin admin, String topic, Map<String, String> configs) throws Exception {
        admin.incrementalAlterConfigs(Map.of(new ConfigResource(ConfigResource.Type.TOPIC, topic), configs.entrySet().stream()
            .map(e -> new AlterConfigOp(new ConfigEntry(e.getKey(), e.getValue()), AlterConfigOp.OpType.SET))
            .toList())).all().get(30, TimeUnit.SECONDS);
    }

    private static void awaitLocalLogEndOffsets(KafkaClusterTestKit cluster, TopicPartition partition, Set<Integer> brokerIds, long expected) throws Exception {
        TestUtils.waitForCondition(
            () -> brokerIds.stream().allMatch(id -> {
                final var log = cluster.brokers().get(id).logManager().getLog(partition, false);
                return log.isDefined() && ((UnifiedLog) log.get()).logEndOffset() >= expected;
            }),
            TIMEOUT_MS * 2,
            () -> "Replicas " + brokerIds + " of " + partition + " should reach the end offset " + expected);
    }

    private static Optional<UnifiedLog> longestLocalLog(KafkaClusterTestKit cluster, TopicPartition partition) {
        return cluster.brokers().values().stream()
            .map(broker -> broker.logManager().getLog(partition, false))
            .filter(Option::isDefined)
            .map(log -> (UnifiedLog) log.get())
            .max(Comparator.comparingLong(UnifiedLog::logEndOffset));
    }

    private static OptionalLong maxLogEndOffset(KafkaClusterTestKit cluster, TopicPartition partition) {
        return maxLogEndOffset(cluster, partition, List.copyOf(cluster.brokers().keySet()));
    }

    private static OptionalLong maxLogEndOffset(KafkaClusterTestKit cluster, TopicPartition partition, List<Integer> brokerIds) {
        return brokerIds.stream()
            .map(id -> cluster.brokers().get(id).logManager().getLog(partition, false))
            .filter(Option::isDefined)
            .mapToLong(log -> ((UnifiedLog) log.get()).logEndOffset())
            .max();
    }

    private KafkaClusterTestKit createCluster(String clusterId, boolean consolidation) throws Exception {
        return createCluster(clusterId, consolidation, true);
    }

    private KafkaClusterTestKit createCluster(String clusterId, boolean consolidation, boolean managedReplicas) throws Exception {
        return createCluster(clusterId, consolidation, managedReplicas, true, NUM_BROKERS, Map.of());
    }

    /**
     * @param brokerOverrides per-broker configs that take precedence over the cluster-wide ones
     */
    private KafkaClusterTestKit createCluster(String clusterId, boolean consolidation, boolean managedReplicas,
                                              boolean combined, int numBrokers,
                                              Map<Integer, Map<String, String>> brokerOverrides) throws Exception {
        final String rsmPrefix = RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;
        final Map<Integer, Map<String, String>> perServerProps = new HashMap<>();
        for (int i = 0; i < numBrokers; i++) {
            final Path cache = Files.createDirectories(cacheDirectory.resolve("ts-fetch-chunk-cache-" + i));
            final Map<String, String> props = new HashMap<>(Map.of(
                ServerConfigs.BROKER_RACK_CONFIG, "az" + (i % NUM_BROKERS + 1),
                rsmPrefix + "fetch.chunk.cache.path", cache.toAbsolutePath().toString()));
            props.putAll(brokerOverrides.getOrDefault(i, Map.of()));
            perServerProps.put(i, props);
        }
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(combined)
            .setNumBrokerNodes(numBrokers)
            .setNumControllerNodes(1)
            .setClusterId(clusterId)
            .setBaseDirectory(baseDirectory)
            .setPerServerProperties(perServerProps)
            .build();

        final String controlPlane = InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX;
        final String storage = InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX;
        return new KafkaClusterTestKit.Builder(nodes)
            .setDeleteOnClose(false)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, String.valueOf(REPLICATION_FACTOR))
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, String.valueOf(managedReplicas))
            .setConfigProp(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, String.valueOf(consolidation))
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(controlPlane + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG, pgContainer.getJdbcUrl())
            .setConfigProp(controlPlane + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(controlPlane + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(storage + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(storage + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(storage + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(storage + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(storage + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(storage + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            .setConfigProp(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, String.valueOf(LOCAL_RETENTION_MS))
            .setConfigProp(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, String.valueOf(LOCAL_RETENTION_BYTES))
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, String.valueOf(consolidation))
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "5000")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP, "4")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, TopicBasedRemoteLogMetadataManager.class.getName())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "EXTERNAL")
            .setConfigProp(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
                + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, "1")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "io.aiven.kafka.tieredstorage.RemoteStorageManager")
            .setConfigProp(rsmPrefix + "chunk.size", 1048576)
            .setConfigProp(rsmPrefix + "fetch.chunk.cache.class", "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
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
            .setConfigProp(rsmPrefix + "storage.aws.secret.access.key", s3Container.getSecretKey())
            .build();
    }

    private static void produce(KafkaClusterTestKit cluster, String topic, int partitions, int count) throws Exception {
        produce(cluster, topic, partitions, count, 0);
    }

    private static void produce(KafkaClusterTestKit cluster, String topic, int partitions, int count, int valueBytes) throws Exception {
        final String padding = "x".repeat(valueBytes);
        try (var producer = new KafkaProducer<String, String>(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()))) {
            for (int i = 0; i < count; i++) {
                producer.send(new ProducerRecord<>(topic, i % partitions, null, "value-" + i + padding)).get();
            }
        }
    }

    private static void consume(KafkaClusterTestKit cluster, String topic, int expected) throws Exception {
        try (var consumer = new KafkaConsumer<String, String>(Map.of(
            CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_ID_CONFIG, "group-" + UUID.randomUUID(),
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"))) {
            consumer.subscribe(List.of(topic));
            final var consumed = new AtomicLong();
            TestUtils.waitForCondition(() -> {
                consumed.addAndGet(consumer.poll(Duration.ofMillis(1000)).count());
                return consumed.get() >= expected;
            }, TIMEOUT_MS, () -> "Expected " + expected + " records but got " + consumed.get());
            consumed.addAndGet(consumer.poll(Duration.ofMillis(1000)).count());
            assertEquals(expected, consumed.get());
        }
    }
}
