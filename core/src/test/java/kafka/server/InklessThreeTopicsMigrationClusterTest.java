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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
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

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class InklessThreeTopicsMigrationClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(InklessThreeTopicsMigrationClusterTest.class);
    private static final int NUM_PARTITIONS = 3;
    private static final int MAX_PRODUCE_RETRIES = 6;
    private static final int PRODUCE_RETRY_BACKOFF_MS = 250;
    private static final Duration PRODUCE_SEND_TIMEOUT = Duration.ofSeconds(5);

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        log.warn("[stage=setup] Initializing containers and 3-broker cluster");
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        final Map<Integer, Map<String, String>> perServerProps = Map.of(
            0, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az1"),
            1, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az2"),
            2, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az3")
        );

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
            .setConfigProp(ServerConfigs.DISKLESS_ALLOW_FROM_CLASSIC_ENABLE_CONFIG, "true")
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
        log.warn("[stage=setup] Cluster is ready. bootstrapServers={}", cluster.bootstrapServers());
    }

    @AfterEach
    public void teardown() throws Exception {
        log.warn("[stage=teardown] Closing cluster");
        cluster.close();
    }

    @Test
    public void endToEndFlowFromSealingToControlPlaneInitWithThreeTopics() throws Exception {
        final String topicSuffix = UUID.randomUUID().toString().substring(0, 8);
        final String disklessTopic = "it-diskless-" + topicSuffix;
        final String classicTopic = "it-classic-" + topicSuffix;
        final String migratingTopic = "it-classic-to-diskless-" + topicSuffix;
        final List<String> topics = List.of(disklessTopic, classicTopic, migratingTopic);
        final Map<String, Integer> producedCounts = new HashMap<>();
        producedCounts.put(disklessTopic, 0);
        producedCounts.put(classicTopic, 0);
        producedCounts.put(migratingTopic, 0);

        final Map<String, Object> adminConfigs = baseClientConfigs();
        final Map<String, Object> producerConfigs = producerConfigs();
        log.warn("[stage=test-start] topics: diskless={}, classic={}, migrating={}",
            disklessTopic, classicTopic, migratingTopic);

        // Given: a 3-broker cluster and 3 topics with 3 partitions / 3 replicas each.
        try (Admin admin = AdminClient.create(adminConfigs)) {
            final NewTopic diskless = new NewTopic(disklessTopic, NUM_PARTITIONS, (short) -1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final NewTopic classic = new NewTopic(classicTopic, NUM_PARTITIONS, (short) 3)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
            // Use leader-only placement for the migration topic to avoid follower fetches on diskless partitions.
            final NewTopic classicToDiskless = new NewTopic(migratingTopic, leaderOnlyAssignment())
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));

            final CreateTopicsResult createTopics = admin.createTopics(List.of(diskless, classic, classicToDiskless));
            createTopics.all().get(30, TimeUnit.SECONDS);
            log.warn("[stage=topics-created] Created all topics");

            assertTopicSpread(admin, disklessTopic);
            assertTopicSpread(admin, classicTopic);
            assertLeaderOnlySpread(admin, migratingTopic);
            assertEquals("true", getTopicConfig(admin, disklessTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("false", getTopicConfig(admin, classicTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("false", getTopicConfig(admin, migratingTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));

            // Given: all topics are writable before migration.
            try (Producer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs)) {
                log.warn("[stage=pre-migration-produce] Producing baseline records to all topics");
                produceRounds(producer, topics, producedCounts, 12, true);

                // When: migration is started only for the 3rd topic.
                log.warn("[stage=migration-start] Enabling diskless for topic={}", migratingTopic);
                alterTopicConfig(admin, migratingTopic, Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));

                // When: producers keep writing to all 3 topics during migration (with retries on errors).
                log.warn("[stage=during-migration-produce] Continuing produces during migration");
                produceRounds(producer, topics, producedCounts, 12, false);
            }

            log.warn("[stage=await-migration] Waiting for diskless.enable=true on topic={}", migratingTopic);
            waitForTopicDisklessValue(admin, migratingTopic, "true");

            // Then: only the 3rd topic migrated, and topic placement remains 3x3 across the cluster.
            assertEquals("true", getTopicConfig(admin, disklessTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("false", getTopicConfig(admin, classicTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("true", getTopicConfig(admin, migratingTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertTopicSpread(admin, disklessTopic);
            assertTopicSpread(admin, classicTopic);
            assertLeaderOnlySpread(admin, migratingTopic);
            log.warn("[stage=post-migration-validated] Topic configurations and placement checks passed");
        }

        // Then: all records produced across all 3 topics are consumable.
        log.warn("[stage=consume-verify] Produced counts before consume verification={}", producedCounts);
        consumeAndAssertTopicCounts(baseConsumerConfigs(), topics, producedCounts);
        log.warn("[stage=test-complete] Test finished successfully");
    }

    private Map<Integer, List<Integer>> leaderOnlyAssignment() {
        return Map.of(
            0, List.of(0),
            1, List.of(1),
            2, List.of(2)
        );
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
                assertTrue(actual > 0,
                    "Expected to consume at least one record from topic "
                        + topic + " after producing to it");
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

    private void waitForTopicDisklessValue(final Admin admin,
                                           final String topic,
                                           final String expectedValue) throws Exception {
        final long deadline = System.currentTimeMillis() + Duration.ofSeconds(60).toMillis();
        while (System.currentTimeMillis() < deadline) {
            final String disklessValue = getTopicConfig(admin, topic).get(TopicConfig.DISKLESS_ENABLE_CONFIG);
            if (expectedValue.equals(disklessValue)) {
                return;
            }
            Thread.sleep(500);
        }
        assertEquals(expectedValue, getTopicConfig(admin, topic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
    }

    private void alterTopicConfig(final Admin admin,
                                  final String topic,
                                  final Map<String, String> newConfigs) throws Exception {
        final ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        final Collection<AlterConfigOp> operations = newConfigs.entrySet().stream()
            .map(entry -> new AlterConfigOp(new ConfigEntry(entry.getKey(), entry.getValue()), AlterConfigOp.OpType.SET))
            .toList();
        admin.incrementalAlterConfigs(Map.of(topicResource, operations)).all().get(20, TimeUnit.SECONDS);
    }

    private Map<String, String> getTopicConfig(final Admin admin, final String topic) throws Exception {
        int maxRetries = 5;
        long retryDelayMs = 1000;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                final ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
                final var configsResult = admin.describeConfigs(Collections.singletonList(topicResource));
                final var allConfigs = configsResult.all().get(10, TimeUnit.SECONDS);
                final Map<String, String> topicConfigs = new HashMap<>();
                allConfigs.get(topicResource).entries().forEach(entry -> topicConfigs.put(entry.name(), entry.value()));
                return topicConfigs;
            } catch (ExecutionException e) {
                if (attempt == maxRetries) {
                    throw e;
                }
                Thread.sleep(retryDelayMs);
            }
        }
        throw new IllegalStateException("Exited retry loop unexpectedly.");
    }

    private void assertTopicSpread(final Admin admin, final String topic) throws Exception {
        final TopicDescription description = admin.describeTopics(Collections.singletonList(topic))
            .allTopicNames()
            .get(20, TimeUnit.SECONDS)
            .get(topic);
        assertEquals(NUM_PARTITIONS, description.partitions().size(),
            "Topic should have exactly 3 partitions: " + topic);

        final Set<Integer> expectedReplicaSet = Set.of(0, 1, 2);
        description.partitions().forEach(partition -> {
            final Set<Integer> replicaIds = partition.replicas().stream().map(Node::id).collect(java.util.stream.Collectors.toSet());
            assertEquals(3, partition.replicas().size(), "Each partition must have 3 replicas");
            assertEquals(expectedReplicaSet, replicaIds, "Replicas must be spread over brokers 0,1,2");
            assertTrue(replicaIds.contains(partition.leader().id()), "Leader must be one of replicas");
        });
    }

    private void assertLeaderOnlySpread(final Admin admin, final String topic) throws Exception {
        final TopicDescription description = admin.describeTopics(Collections.singletonList(topic))
            .allTopicNames()
            .get(20, TimeUnit.SECONDS)
            .get(topic);
        assertEquals(NUM_PARTITIONS, description.partitions().size(),
            "Topic should have exactly 3 partitions: " + topic);

        description.partitions().forEach(partition ->
            assertEquals(1, partition.replicas().size(),
                "Migrating topic partitions should have one replica (leader-only)"));
    }
}
