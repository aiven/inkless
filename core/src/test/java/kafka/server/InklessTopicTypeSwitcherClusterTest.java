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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AlterConfigsRequest;
import org.apache.kafka.common.requests.AlterConfigsResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.IntegrationTestUtils;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Testcontainers
public class InklessTopicTypeSwitcherClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(InklessTopicTypeSwitcherClusterTest.class);
    private static final int NUM_PARTITIONS = 3;
    private static final int MAX_PRODUCE_RETRIES = 6;
    private static final int PRODUCE_RETRY_BACKOFF_MS = 250;
    private static final Duration PRODUCE_SEND_TIMEOUT = Duration.ofSeconds(5);

    private KafkaClusterTestKit cluster;

    private enum AlterConfigsMode {
        INCREMENTAL,
        LEGACY
    }

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
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager")
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

            assertEquals("true", getTopicConfig(admin, disklessTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("false", getTopicConfig(admin, classicTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("false", getTopicConfig(admin, classicToDisklessTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));

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
                waitForTopicDisklessValue(admin, classicToDisklessTopic, "true");
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

            assertEquals("true", getTopicConfig(admin, disklessTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("false", getTopicConfig(admin, classicTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            assertEquals("true", getTopicConfig(admin, classicToDisklessTopic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
            log.warn("[stage=post-switch-validated] Topic configurations and placement checks passed");
        }

        log.warn("[stage=consume-verify] Produced counts before consume verification={}", producedCounts);
        consumeAndAssertTopicCounts(baseConsumerConfigs(), topics, producedCounts);
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
            assertEquals("true", getTopicConfig(admin, topic).get(TopicConfig.DISKLESS_ENABLE_CONFIG));
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
            assertTrue(ex.getCause().getMessage().contains("pending classic-to-diskless switch"),
                    "Expected 'pending classic-to-diskless switch' in: " + ex.getCause().getMessage());
        }
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
        assertTrue(apiError != null, "Legacy AlterConfigs response did not contain topic resource " + topicResource);
        assertEquals(Errors.NONE, apiError.error(), "Legacy AlterConfigs failed: " + apiError.message());
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
}
