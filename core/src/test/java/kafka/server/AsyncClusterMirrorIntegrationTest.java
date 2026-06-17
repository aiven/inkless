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

import kafka.utils.TestUtils;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ClusterMirrorDescription;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateClusterMirrorOptions;
import org.apache.kafka.clients.admin.DeleteClusterMirrorOptions;
import org.apache.kafka.clients.admin.DescribeClusterMirrorsOptions;
import org.apache.kafka.clients.admin.ListConfigResourcesOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StopMirrorTopicsOptions;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordKey;
import org.apache.kafka.coordinator.mirror.ClusterMirrorRecordSerde;
import org.apache.kafka.coordinator.mirror.generated.LastMirrorEpochsKey;
import org.apache.kafka.coordinator.mirror.generated.MirrorPartitionStateKey;
import org.apache.kafka.server.config.ClusterMirrorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.config.ServerLogConfigs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.kafka.common.internals.Topic.MIRROR_STATE_TOPIC_NAME;
import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for async Cluster Mirroring between two independent KRaft clusters.
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
public class AsyncClusterMirrorIntegrationTest {
    private static final String TOPIC_NAME = "my-topic-async";
    private static final long METADATA_REFRESH_INTERVAL_MS = 5_000L;
    private static final String MIRROR_NAME = "my-mirror";

    private KafkaClusterTestKit srcCluster;
    private KafkaClusterTestKit dstCluster;
    private Admin srcAdmin;
    private Admin dstAdmin;

    private String singleSourceBootstrapServer;

    @BeforeEach
    void beforeEach() throws Exception {
        // Source cluster: 2 brokers, 1 controller
        srcCluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(2)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
                .build();
        srcCluster.format();
        srcCluster.startup();
        srcCluster.waitForReadyBrokers();
        singleSourceBootstrapServer = srcCluster.bootstrapServers().split(",")[0];

        // Destination cluster: 2 brokers, 1 controller
        // Use a short metadata refresh interval so mirror topics are created quickly
        dstCluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(2)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ClusterMirrorConfig.MIRROR_NUM_REPLICA_FETCHERS_CONFIG, "2")
                .setConfigProp(ClusterMirrorConfig.MIRROR_METADATA_REFRESH_INTERVAL_MS_CONFIG,
                        String.valueOf(METADATA_REFRESH_INTERVAL_MS))
                .setConfigProp(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false")
                .setConfigProp(ClusterMirrorConfig.MIRROR_STATE_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
                .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
                .setConfigProp(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
                .build();
        dstCluster.format();
        dstCluster.startup();
        dstCluster.waitForReadyBrokers();

        srcAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, srcCluster.bootstrapServers()
        ));
        dstAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, dstCluster.bootstrapServers()
        ));
    }

    @AfterEach
    void afterEach() {
        closeQuietly(srcAdmin);
        closeQuietly(dstAdmin);
        closeQuietly(srcCluster);
        closeQuietly(dstCluster);
    }

    /**
     * Tests that ASYNC replication works.
     * This is a simpler smoke test for the mirror setup.
     */
    @Test
    void testAsyncMirrorReplication() throws Exception {
        // Create topic and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(TOPIC_NAME, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, TOPIC_NAME, 0, 50);

        // Create mirror and add topic
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics(MIRROR_NAME, Set.of(TOPIC_NAME), new StartMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(TOPIC_NAME);

        // Produce more data while in ASYNC mode
        produceRecords(srcCluster, TOPIC_NAME, 50, 50);

        // Verify all 100 records arrive at destination
        List<ConsumerRecord<String, String>> records = consumeRecords(
                dstCluster, TOPIC_NAME, 100, null);
        assertEquals(100, records.size(),
                "Destination should have all 100 records");
    }

    /**
     * Test that stopping a mirror topic and then draining the destination partition
     * does not cause consumer group offsets to be overwritten by stale source offsets.
     */
    @Test
    void testStoppedMirrorDoesNotOverwriteOffsets() throws Exception {
        String topic = "drain-test";
        String groupId = "drain-group";
        int recordCount = 50;

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, recordCount);

        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics(MIRROR_NAME, Set.of(topic), new StartMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(topic);

        // Stop the mirror topic
        dstAdmin.stopMirrorTopics(MIRROR_NAME, Set.of(topic), new StopMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorState(topic, "STOPPED");

        // Consume all records from the destination with a stable consumer group
        consumeRecords(dstCluster, topic, recordCount, groupId);

        // Poll across several metadata refresh cycles to verify the offset stays stable
        // Log end is recordCount + 1 due to PID-reset control record written on mirror stop
        TopicPartition tp = new TopicPartition(topic, 0);
        assertStableOffset(dstAdmin, groupId, tp, recordCount + 1);
    }

    /**
     * Tests whether the fetcher works when topics are pre-created.
     */
    @Test
    void testMirrorWithPreCreatedTopic() throws Exception {
        // Create topic on source
        srcAdmin.createTopics(List.of(
                new NewTopic(TOPIC_NAME, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        // Get source topic description (TopicId)
        var topicDesc = describeTopics(srcAdmin, List.of(TOPIC_NAME));
        String sourceTopicId = topicDesc.get(TOPIC_NAME).topicId().toString();

        // Produce data to source
        produceRecords(srcCluster, TOPIC_NAME, 0, 50);

        // Pre-create topic on destination with source's TopicId (like ClusterMirrorCommand does)
        dstAdmin.createTopics(List.of(
                new NewTopic(TOPIC_NAME, Optional.of(1), Optional.empty(), Optional.of(sourceTopicId))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics(MIRROR_NAME, Set.of(TOPIC_NAME), new StartMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(TOPIC_NAME);

        // Wait for async replication
        consumeRecords(dstCluster, TOPIC_NAME, 50);
    }

    /**
     * Tests that mirror.topics.include regex patterns trigger auto-discovery and replication,
     * and that appending a new pattern discovers additional topics.
     * No explicit startMirrorTopics calls, discoverTopicsByPattern handles it.
     */
    @Test
    void testMirrorTopicsIncludeRegexMerge() throws Exception {
        String ordersTopic = "orders-us";
        String eventsTopic = "events-click";

        // Create source topics and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(ordersTopic, 1, (short) 1),
                new NewTopic(eventsTopic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, ordersTopic, 0, 30);
        produceRecords(srcCluster, eventsTopic, 0, 20);

        // Create mirror with mirror.topics.include=orders-.* — auto-discovery picks up orders-us
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(".*");

        // Auto-discovery should find orders-us and start replication
        consumeRecords(dstCluster, ordersTopic, 30);

        // Append second pattern to mirror.topics.include
        appendToTopicsInclude(MIRROR_NAME, "events-.*");

        // Auto-discovery should now find events-click and start replication
        consumeRecords(dstCluster, eventsTopic, 20);
    }

    /**
     * Tests that mirror.topics.exclude prevents specific topics from
     * being mirrored even when they match mirror.topics.include.
     */
    @Test
    void testMirrorTopicsExclude() throws Exception {
        String includedTopic = "orders-us";
        String excludedTopic = "orders-internal";

        // Create source topics and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(includedTopic, 1, (short) 1),
                new NewTopic(excludedTopic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, includedTopic, 0, 30);
        produceRecords(srcCluster, excludedTopic, 0, 20);

        // Create mirror: include orders-.* but exclude orders-internal
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "orders-.*",
                ClusterMirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG, "orders-internal"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(includedTopic);

        // Only orders-us should be discovered, orders-internal is excluded
        consumeRecords(dstCluster, includedTopic, 30);

        // Verify orders-internal was NOT mirrored
        assertStableRecordCount(dstCluster, excludedTopic, 0,
                "Excluded topic should have no mirrored records");
    }

    /**
     * Test that the default mirror.topics.exclude=__.*  prevents internal topics
     * from being mirrored even when mirror.topics.include=.* matches everything.
     */
    @Test
    void testMirrorTopicsExcludeInternalTopics() throws Exception {
        String userTopic = "user-events";
        String internalTopic = "__test-internal";

        // Create source topics and produce data
        srcAdmin.createTopics(List.of(
                new NewTopic(userTopic, 1, (short) 1),
                new NewTopic(internalTopic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, userTopic, 0, 25);
        produceRecords(srcCluster, internalTopic, 0, 10);

        // Create mirror with include=.* and no explicit exclude — default __.*  applies
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, ".*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(userTopic);

        // user-events should be discovered and replicated
        consumeRecords(dstCluster, userTopic, 25);

        // __test-internal should NOT be mirrored due to default exclude
        assertStableRecordCount(dstCluster, internalTopic, 0,
                "Internal topic starting with __ should not be mirrored by default");
    }

    /**
     * Tests that a literal topic name in mirror.topics.include works for auto-discovery.
     */
    @Test
    void testMirrorTopicsIncludeLiteral() throws Exception {
        String topic = "payments";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 40);

        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "payments"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(topic);

        consumeRecords(dstCluster, topic, 40);
    }

    /**
     * Tests that a mix of literal and regex patterns in mirror.topics.include works.
     */
    @Test
    void testMirrorTopicsIncludeLiteralAndRegex() throws Exception {
        String literalTopic = "payments";
        String regexMatchedTopic = "orders-eu";
        String unmatchedTopic = "logs-app";

        srcAdmin.createTopics(List.of(
                new NewTopic(literalTopic, 1, (short) 1),
                new NewTopic(regexMatchedTopic, 1, (short) 1),
                new NewTopic(unmatchedTopic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, literalTopic, 0, 20);
        produceRecords(srcCluster, regexMatchedTopic, 0, 15);
        produceRecords(srcCluster, unmatchedTopic, 0, 10);

        // Include literal "payments" and regex "orders-.*"
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "payments,orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(literalTopic, regexMatchedTopic);

        consumeRecords(dstCluster, literalTopic, 20);
        consumeRecords(dstCluster, regexMatchedTopic, 15);

        // logs-app should NOT be mirrored
        assertStableRecordCount(dstCluster, unmatchedTopic, 0,
                "Unmatched topic should not be mirrored");
    }

    /**
     * Tests that a stopped topic is not re-discovered by discoverTopicsByPattern,
     * because the STOPPED desired state keeps it in getConfiguredTopics.
     */
    @Test
    void testStopMirrorTopicPreventsRediscovery() throws Exception {
        String topicA = "orders-us";
        String topicB = "orders-eu";

        srcAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 1),
                new NewTopic(topicB, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, topicA, 0, 20);
        produceRecords(srcCluster, topicB, 0, 20);

        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(topicA, topicB);

        consumeRecords(dstCluster, topicA, 20);
        consumeRecords(dstCluster, topicB, 20);

        // Stop orders-eu (sets mirror.name to test-mirror.stopped)
        dstAdmin.stopMirrorTopics(MIRROR_NAME, Set.of(topicB), new StopMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorState(topicB, "STOPPED");

        // Produce more data to both topics (only orders-us should receive new records)
        produceRecords(srcCluster, topicA, 20, 20);
        produceRecords(srcCluster, topicB, 20, 20);

        consumeRecords(dstCluster, topicA, 40);

        // Verify orders-eu did NOT receive new data (still at 20, not 40)
        assertStableRecordCount(dstCluster, topicB, 20,
                "Stopped topic should not have received new mirrored records");
    }

    /**
     * Tests that startMirrorTopics with includePatterns in options
     * persists patterns to mirror.topics.include via KafkaAdminClient.
     */
    @Test
    void testStartMirrorTopicsWithInclude() throws Exception {
        String topic = "orders-us";

        srcAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 1)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(srcCluster, topic, 0, 30);

        // Create mirror with NO initial mirror.topics.include
        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        // Start mirroring with includePatterns — should persist "orders-.*" to config
        dstAdmin.startMirrorTopics(MIRROR_NAME, Set.of(topic),
                new StartMirrorTopicsOptions().includePatterns(List.of("orders-.*")))
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(topic);

        consumeRecords(dstCluster, topic, 30);

        // Verify the pattern was persisted to mirror.topics.include
        ConfigResource mirrorResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, MIRROR_NAME);
        var configResult = dstAdmin.describeConfigs(List.of(mirrorResource)).all().get(30, TimeUnit.SECONDS);
        var mirrorConfigEntries = configResult.get(mirrorResource);
        ConfigEntry includeEntry = mirrorConfigEntries.get(ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG);
        assertTrue(includeEntry != null && includeEntry.value().contains("orders-.*"),
                "mirror.topics.include should contain 'orders-.*' after startMirrorTopics with includePatterns");
    }

    /**
     * Tests that a mirror can be deleted after all its topics are stopped, and that
     * the mirror no longer appears in listClusterMirrors after deletion.
     * And the internal topic __mirror_state contains the tombstone records in the last batch.
     */
    @Test
    void testDeleteClusterMirror() throws Exception {
        String topic = "delete-mirror-topic";

        var result = srcAdmin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)));
        result.all().get(30, TimeUnit.SECONDS);

        Uuid topicId = result.topicId(topic).get();

        dstAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);
        dstAdmin.startMirrorTopics(MIRROR_NAME, Set.of(topic), new StartMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorLagZero(topic);

        var listConfigResult = dstAdmin.listConfigResources(Set.of(ConfigResource.Type.CLUSTER_MIRROR),
                new ListConfigResourcesOptions()).all().get(30, TimeUnit.SECONDS);
        assertEquals(1, listConfigResult.size());

        // Verify mirror is listed before deletion
        var listingsBefore = dstAdmin.listClusterMirrors().all().get(30, TimeUnit.SECONDS);
        assertTrue(listingsBefore.stream().anyMatch(l -> MIRROR_NAME.equals(l.mirrorName())),
                "Mirror should be listed before deletion");

        // Stop all topics (required precondition for deletion)
        dstAdmin.stopMirrorTopics(MIRROR_NAME, Set.of(topic), new StopMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirrorState(topic, "STOPPED");

        dstAdmin.deleteClusterMirror(MIRROR_NAME, new DeleteClusterMirrorOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForListMirrorEmpty();

        // Verify the mirror is no longer listed after deletion
        listConfigResult = dstAdmin.listConfigResources(Set.of(ConfigResource.Type.CLUSTER_MIRROR),
                new ListConfigResourcesOptions()).all().get(30, TimeUnit.SECONDS);
        assertTrue(listConfigResult.isEmpty());

        // Verify that the __mirror_state topic contains the tombstone records
        // for both `MirrorPartitionStateKey` and `LastMirrorEpochsKey` types
        // 1. Get the partition index hosting the metadata for the mirrored topic partition
        int partId = dstCluster.brokers().get(0).clusterMirrorCoordinator()
                .getCoordinatorPartitionByKey(new ClusterMirrorRecordKey(MIRROR_NAME, topicId, 0));
        // 2. Get the partition leader
        int leaderMirrorStatePartition = dstCluster.brokers().get(0).metadataCache()
                .getLeaderAndIsr(MIRROR_STATE_TOPIC_NAME, partId).get().leader();
        // 3. Get the last batch of the partition
        var lastBatch = dstCluster.brokers().get(leaderMirrorStatePartition).replicaManager()
                .getLog(new TopicPartition(MIRROR_STATE_TOPIC_NAME, partId)).get().activeSegment().log().lastBatch().get();
        // 4. Deserialize the records in the last batch
        List<CoordinatorRecord> records = new ArrayList<>();
        ClusterMirrorRecordSerde serde = new ClusterMirrorRecordSerde();
        lastBatch.forEach(record -> records.add(serde.deserialize(record.key(), record.value())));
        // 5. Make sure the last batch contains the 2 tombstone records
        assertEquals(2, records.size());
        assertTrue(records.get(0).key().apiKey() == new MirrorPartitionStateKey().apiKey() && records.get(0).value() == null);
        assertTrue(records.get(1).key().apiKey() == new LastMirrorEpochsKey().apiKey() && records.get(1).value() == null);
    }

    private void produceRecords(KafkaClusterTestKit cluster, String topic,
                                int startIndex, int count) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = startIndex; i < startIndex + count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i));
            }
            producer.flush();
        }
    }

    private List<ConsumerRecord<String, String>> consumeRecords(
            KafkaClusterTestKit cluster, String topic, int expectedRecords) throws Exception {
        return consumeRecords(cluster, topic, expectedRecords, null);
    }

    private List<ConsumerRecord<String, String>> consumeRecords(
            KafkaClusterTestKit cluster, String topic, int expectedRecords,
            String groupId) throws Exception {
        boolean commitOffsets = groupId != null;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, commitOffsets ? groupId : "test-" + System.currentTimeMillis());
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 30_000;
        while (allRecords.size() < expectedRecords && System.currentTimeMillis() < deadline) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                consumer.subscribe(List.of(topic));
                allRecords.clear();
                long pollDeadline = System.currentTimeMillis() + 5_000;
                while (allRecords.size() < expectedRecords && System.currentTimeMillis() < pollDeadline) {
                    ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                    batch.forEach(allRecords::add);
                }
                if (commitOffsets && allRecords.size() >= expectedRecords) {
                    consumer.commitSync();
                }
            } catch (Exception e) {
                if (commitOffsets && allRecords.size() >= expectedRecords) {
                    throw e;
                }
            }
            if (allRecords.size() < expectedRecords) {
                TimeUnit.MILLISECONDS.sleep(1_000);
            }
        }
        assertEquals(expectedRecords, allRecords.size(),
                "Expected to consume " + expectedRecords + " records from " + topic);
        return allRecords;
    }

    private Map<String, TopicDescription> describeTopics(
            Admin admin, List<String> topics) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return admin.describeTopics(topics).allTopicNames().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
    }

    private boolean allPartitionsSatisfy(
            String topicPattern, Predicate<ClusterMirrorDescription.LeaderState> condition) throws Exception {
        var result = dstAdmin.describeClusterMirrors(
                List.of(MIRROR_NAME), new DescribeClusterMirrorsOptions());
        var descriptions = result.allDescriptions().get(5, TimeUnit.SECONDS);
        ClusterMirrorDescription desc = descriptions.get(MIRROR_NAME);
        if (desc == null) return false;
        var pattern = java.util.regex.Pattern.compile(topicPattern);
        var matched = desc.topics().entrySet().stream()
                .filter(e -> pattern.matcher(e.getKey()).matches())
                .toList();
        return !matched.isEmpty()
                && matched.stream().allMatch(e -> e.getValue().stream().allMatch(condition));
    }

    private void waitForMirrorState(String topicPattern, String state) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            if (allPartitionsSatisfy(topicPattern, s -> state.equals(s.state()))) return;
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
        throw new AssertionError("Mirror partitions for " + topicPattern + " did not reach state " + state + " within timeout");
    }

    private void waitForListMirrorEmpty() {
        TestUtils.waitUntilTrue(() -> {
            try {
                var listMirror = dstAdmin.listClusterMirrors().all().get(30, TimeUnit.SECONDS);
                return listMirror.isEmpty();
            } catch (Exception e) {
                return false;
            }
        }, () -> "Cluster Mirror is not deleted successfully", DEFAULT_MAX_WAIT_MS, 100);
    }

    private void waitForMirrorLagZero(String... topicPatterns) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            boolean allReady = true;
            for (String pattern : topicPatterns) {
                if (!allPartitionsSatisfy(pattern, s -> s.lag() == 0 && "MIRRORING".equals(s.state()))) {
                    allReady = false;
                    break;
                }
            }
            if (allReady) return;
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
        throw new AssertionError("Mirror lag did not reach zero for " + List.of(topicPatterns) + " within timeout");
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void appendToTopicsInclude(String mirrorName, String pattern) throws Exception {
        ConfigResource mirrorResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName);
        var configResult = dstAdmin.describeConfigs(List.of(mirrorResource)).all().get(30, TimeUnit.SECONDS);
        var mirrorConfigEntries = configResult.get(mirrorResource);

        String existingValue = "";
        ConfigEntry existingEntry = mirrorConfigEntries.get(ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG);
        if (existingEntry != null && existingEntry.value() != null && !existingEntry.value().isEmpty()) {
            existingValue = existingEntry.value();
        }
        String newValue = existingValue.isEmpty() ? pattern : existingValue + "," + pattern;

        dstAdmin.incrementalAlterConfigs(Map.of(mirrorResource, List.of(
                new AlterConfigOp(
                        new ConfigEntry(ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, newValue),
                        AlterConfigOp.OpType.SET)
        ))).all().get(30, TimeUnit.SECONDS);
    }

    private void assertStableOffset(Admin admin, String groupId,
                                    TopicPartition tp, long expectedOffset) throws Exception {
        long deadline = System.currentTimeMillis() + 3 * METADATA_REFRESH_INTERVAL_MS;
        while (System.currentTimeMillis() < deadline) {
            long current = getCommittedOffset(admin, groupId, tp);
            assertEquals(expectedOffset, current,
                    "Consumer group offset must not be overwritten after mirror stop");
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }

    private long getCommittedOffset(Admin admin, String groupId, TopicPartition tp) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (System.currentTimeMillis() < deadline) {
            try {
                var offsets = admin.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
                OffsetAndMetadata oam = offsets.get(tp);
                if (oam != null) return oam.offset();
            } catch (Exception e) {
                // group may not be visible yet, retry
            }
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
        throw new AssertionError("No committed offset for " + tp + " in group " + groupId + " within timeout");
    }

    private void assertStableRecordCount(KafkaClusterTestKit cluster, String topic,
                                         int expectedCount, String message) throws Exception {
        long deadline = System.currentTimeMillis() + 3 * METADATA_REFRESH_INTERVAL_MS;
        while (System.currentTimeMillis() < deadline) {
            var records = consumeRecords(cluster, topic, expectedCount);
            assertEquals(expectedCount, records.size(), message);
            TimeUnit.MILLISECONDS.sleep(1_000);
        }
    }
}
