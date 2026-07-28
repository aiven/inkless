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
import org.apache.kafka.clients.admin.NewPartitionReassignment;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
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
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Migration test for enabling managed replicas on an existing (legacy) diskless cluster.
 *
 * <p>Scenario:
 * <ol>
 *   <li>Start a cluster with {@code diskless.managed.rf.enable=false}, create a diskless topic
 *       (legacy RF=1), and produce/consume to it.</li>
 *   <li>Restart the whole cluster with {@code diskless.managed.rf.enable=true} while preserving
 *       both the KRaft metadata (broker/controller log dirs are reused) and the diskless data
 *       (the PostgreSQL control plane and S3 bucket are untouched).</li>
 *   <li>Increase the topic's replication factor from 1 to 3 via a partition reassignment, then
 *       verify the pre-restart data is still readable and produce/consume continues to work.</li>
 * </ol>
 *
 * <p>The restart is implemented by closing the first {@link KafkaClusterTestKit} without deleting
 * its directories ({@code setDeleteOnClose(false)}) and building a second cluster over the same
 * {@code baseDirectory}/{@code clusterId} without re-formatting — the same technique used by
 * {@code KRaftClusterTest#testOldBootstrapMetadataFile}.
 */
@Testcontainers
public class InklessManagedReplicasRfMigrationClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(InklessManagedReplicasRfMigrationClusterTest.class);

    private static final String TOPIC = "rf-migration-topic";
    private static final int NUM_PARTITIONS = 3;
    private static final int PRE_RESTART_RECORDS = 60;
    private static final int POST_INCREASE_RECORDS = 30;
    private static final short TARGET_RF = 3;
    private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(30);

    @TempDir
    private Path baseDirectory;

    private String clusterId;
    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);
        // Reused across the restart so the second cluster recovers the first cluster's on-disk state.
        clusterId = Uuid.randomUuid().toString();
    }

    @AfterEach
    public void teardown() throws Exception {
        if (cluster != null) {
            cluster.close();
            cluster = null;
        }
    }

    @Test
    public void increaseReplicationFactorAfterEnablingManagedReplicas() throws Exception {
        // Phase 1: legacy cluster (managed replicas disabled), create diskless topic + produce/consume.
        cluster = buildCluster(false);
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();

        final String bootstrapServers = cluster.bootstrapServers();
        try (Admin admin = AdminClient.create(adminConfigs(bootstrapServers))) {
            final NewTopic topic = new NewTopic(TOPIC, NUM_PARTITIONS, (short) 1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            admin.createTopics(List.of(topic)).all().get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

            // Legacy diskless topics are pinned to RF=1.
            final TopicDescription description = waitForTopicDescription(admin, TOPIC);
            assertEquals(NUM_PARTITIONS, description.partitions().size());
            for (final TopicPartitionInfo partition : description.partitions()) {
                assertEquals(1, partition.replicas().size(),
                    "Legacy diskless topic should be created with RF=1");
            }
        }

        produceRecords(bootstrapServers, PRE_RESTART_RECORDS, "before-restart");
        assertEquals(PRE_RESTART_RECORDS, consumeAll(bootstrapServers, PRE_RESTART_RECORDS),
            "All pre-restart records should be consumable on the legacy cluster");

        // Restart: close without wiping the directories so the metadata log survives.
        cluster.close();
        cluster = null;

        // Phase 2: restart the cluster with managed replicas enabled, reusing the same state.
        cluster = buildCluster(true);
        // No format() — the second cluster recovers the KRaft metadata written by the first one.
        cluster.startup();
        cluster.waitForReadyBrokers();

        final String bootstrapServersAfterRestart = cluster.bootstrapServers();

        // The pre-restart data must still be present after the restart.
        assertEquals(PRE_RESTART_RECORDS, consumeAll(bootstrapServersAfterRestart, PRE_RESTART_RECORDS),
            "Pre-restart records should survive the restart (data lives in the control plane + object storage)");

        try (Admin admin = AdminClient.create(adminConfigs(bootstrapServersAfterRestart))) {
            // The topic is still there and still RF=1 (managed replicas do not retrofit existing topics).
            final TopicDescription beforeIncrease = waitForTopicDescription(admin, TOPIC);
            for (final TopicPartitionInfo partition : beforeIncrease.partitions()) {
                assertEquals(1, partition.replicas().size(),
                    "Existing diskless topic keeps RF=1 until explicitly reassigned");
            }

            // Increase the replication factor from 1 to 3.
            increaseReplicationFactor(admin, TOPIC, beforeIncrease);

            final TopicDescription afterIncrease = waitForReplicationFactor(admin, TOPIC, TARGET_RF);
            for (final TopicPartitionInfo partition : afterIncrease.partitions()) {
                final Set<Integer> replicaIds = partition.replicas().stream()
                    .map(Node::id).collect(Collectors.toSet());
                assertEquals(Set.of(0, 1, 2), replicaIds,
                    "Partition " + partition.partition() + " should be replicated across all 3 brokers");
                // Diskless ISR is liveness-gated; all brokers are alive so ISR == replicas.
                final Set<Integer> isrIds = partition.isr().stream()
                    .map(Node::id).collect(Collectors.toSet());
                assertEquals(replicaIds, isrIds,
                    "All replicas should be in ISR (diskless brokers are in-sync via object storage)");
                assertNotNull(partition.leader(), "Partition should have a leader after RF increase");
                assertTrue(replicaIds.contains(partition.leader().id()),
                    "Leader should be one of the replicas");
            }
        }

        // Old data is still readable at the new RF, and produce/consume keeps working.
        produceRecords(bootstrapServersAfterRestart, POST_INCREASE_RECORDS, "after-increase");
        final int expectedTotal = PRE_RESTART_RECORDS + POST_INCREASE_RECORDS;
        assertEquals(expectedTotal, consumeAll(bootstrapServersAfterRestart, expectedTotal),
            "Both pre-restart and post-increase records should be consumable at RF=3");
    }

    private KafkaClusterTestKit buildCluster(final boolean managedReplicasEnabled) throws Exception {
        // Node 0 is a combined broker+controller; nodes 1 and 2 are broker-only. One broker per rack.
        final Map<Integer, Map<String, String>> perServerProps = Map.of(
            0, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az1"),
            1, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az2"),
            2, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az3")
        );
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setClusterId(clusterId)
            .setBaseDirectory(baseDirectory)
            .setCombined(true)
            .setNumBrokerNodes(3)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProps)
            .build();

        return new KafkaClusterTestKit.Builder(nodes)
            // Preserve directories on close so the second cluster can recover the metadata log.
            .setDeleteOnClose(false)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, String.valueOf(managedReplicasEnabled))
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, String.valueOf(TARGET_RF))
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
            .build();
    }

    private void increaseReplicationFactor(final Admin admin,
                                           final String topic,
                                           final TopicDescription current) throws Exception {
        final List<Integer> targetReplicas = List.of(0, 1, 2);
        final Map<TopicPartition, Optional<NewPartitionReassignment>> reassignments = new HashMap<>();
        for (final TopicPartitionInfo partition : current.partitions()) {
            reassignments.put(new TopicPartition(topic, partition.partition()),
                Optional.of(new NewPartitionReassignment(targetReplicas)));
        }
        // AlterPartitionReassignmentsOptions.allowReplicationFactorChange() defaults to true.
        admin.alterPartitionReassignments(reassignments).all().get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
        log.info("Requested RF increase to {} for {} partitions of {}", TARGET_RF, current.partitions().size(), topic);
    }

    private Map<String, Object> adminConfigs(final String bootstrapServers) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        return configs;
    }

    private void produceRecords(final String bootstrapServers, final int numRecords, final String valuePrefix) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.ACKS_CONFIG, "all");

        final AtomicInteger produced = new AtomicInteger();
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(configs)) {
            for (int i = 0; i < numRecords; i++) {
                final byte[] value = (valuePrefix + "-" + i).getBytes(StandardCharsets.UTF_8);
                final ProducerRecord<byte[], byte[]> record =
                    new ProducerRecord<>(TOPIC, i % NUM_PARTITIONS, null, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record", exception);
                    } else {
                        produced.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }
        assertEquals(numRecords, produced.get(), "All records should be produced");
    }

    private int consumeAll(final String bootstrapServers, final int expectedRecords) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "rf-migration-group-" + UUID.randomUUID());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        int consumed = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(configs)) {
            consumer.subscribe(List.of(TOPIC));
            final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
            while (System.currentTimeMillis() < deadline && consumed < expectedRecords) {
                final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
                consumed += records.count();
            }
        }
        return consumed;
    }

    // In multi-broker clusters, metadata propagation from the controller to follower brokers is async.
    private TopicDescription waitForTopicDescription(final Admin admin, final String topic) throws Exception {
        final long deadline = System.currentTimeMillis() + ADMIN_TIMEOUT.toMillis();
        while (true) {
            try {
                return admin.describeTopics(List.of(topic))
                    .allTopicNames().get(ADMIN_TIMEOUT.toSeconds(), TimeUnit.SECONDS).get(topic);
            } catch (final ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException)
                    || System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                Thread.sleep(200);
            }
        }
    }

    private TopicDescription waitForReplicationFactor(final Admin admin,
                                                      final String topic,
                                                      final short expectedRf) throws Exception {
        final long deadline = System.currentTimeMillis() + ADMIN_TIMEOUT.toMillis();
        TopicDescription description = null;
        while (System.currentTimeMillis() < deadline) {
            description = waitForTopicDescription(admin, topic);
            final boolean allAtTargetRf = description.partitions().stream()
                .allMatch(p -> p.replicas().size() == expectedRf && p.isr().size() == expectedRf);
            if (allAtTargetRf) {
                return description;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Topic " + topic + " did not reach RF=" + expectedRf
            + " within timeout; last description=" + description);
    }
}
