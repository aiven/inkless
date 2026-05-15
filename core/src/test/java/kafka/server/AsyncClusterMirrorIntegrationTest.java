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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ClusterMirrorListing;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.CreateClusterMirrorOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.StartMirrorTopicsOptions;
import org.apache.kafka.clients.admin.StopMirrorTopicsOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration test for async cluster mirroring between two independent KRaft clusters.
 * Tests the full lifecycle:
 * 1. Create source and destination KRaft clusters
 * 2. Create a topic on the source cluster and produce data
 * 3. Create a mirror link on the destination cluster pointing to the source
 * 4. Add topics to the mirror (ASYNC replication)
 * 5. Wait for the destination to catch up
 * 6. Verify all data is present on the destination
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
public class AsyncClusterMirrorIntegrationTest {

    private static final String TOPIC = "test-async-topic";
    private static final String MIRROR_NAME = "test-mirror";

    private KafkaClusterTestKit sourceCluster;
    private KafkaClusterTestKit destCluster;
    private Admin sourceAdmin;
    private Admin destAdmin;

    private String singleSourceBootstrapServer;

    @BeforeEach
    void setUp() throws Exception {
        // Source cluster: 2 brokers, 1 controller
        sourceCluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(2)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
                .build();
        sourceCluster.format();
        sourceCluster.startup();
        sourceCluster.waitForReadyBrokers();
        singleSourceBootstrapServer = sourceCluster.bootstrapServers().split(",")[0];

        // Destination cluster: 2 brokers, 1 controller
        // Use a short metadata refresh interval so mirror topics are created quickly
        destCluster = new KafkaClusterTestKit.Builder(
                new TestKitNodes.Builder()
                        .setNumBrokerNodes(2)
                        .setNumControllerNodes(1)
                        .build())
                .setConfigProp(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
                .setConfigProp(ClusterMirrorConfig.METADATA_REFRESH_INTERVAL_MS_CONFIG, "5000")
                .setConfigProp(ServerLogConfigs.AUTO_CREATE_TOPICS_ENABLE_CONFIG, "false")
                .build();
        destCluster.format();
        destCluster.startup();
        destCluster.waitForReadyBrokers();

        sourceAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, sourceCluster.bootstrapServers()
        ));
        destAdmin = Admin.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, destCluster.bootstrapServers()
        ));
    }

    @AfterEach
    void tearDown() {
        closeQuietly(sourceAdmin);
        closeQuietly(destAdmin);
        closeQuietly(sourceCluster);
        closeQuietly(destCluster);
    }

    private void produceRecords(KafkaClusterTestKit cluster, String topic,
                                int startIndex, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = startIndex; i < startIndex + count; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }
    }

    private List<ConsumerRecord<String, String>> consumeFromCluster(
            KafkaClusterTestKit cluster, String topic, int expectedRecords) throws Exception {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        long deadline = System.currentTimeMillis() + 90_000;
        while (allRecords.size() < expectedRecords && System.currentTimeMillis() < deadline) {
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
                var partitions = consumer.partitionsFor(topic).stream()
                        .map(info -> new org.apache.kafka.common.TopicPartition(topic, info.partition()))
                        .toList();
                consumer.assign(partitions);
                consumer.seekToBeginning(partitions);
                allRecords.clear();
                long pollDeadline = System.currentTimeMillis() + 5_000;
                while (allRecords.size() < expectedRecords && System.currentTimeMillis() < pollDeadline) {
                    ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                    batch.forEach(allRecords::add);
                }
            } catch (Exception e) {
                // topic may not exist yet, retry
            }
            if (allRecords.size() < expectedRecords) {
                Thread.sleep(1000);
            }
        }
        assertEquals(expectedRecords, allRecords.size(),
                "Destination topic " + topic + " did not catch up within timeout. Expected " +
                        expectedRecords + " records but got " + allRecords.size());
        return allRecords;
    }

    private void waitForMirror(int numberOfTopics) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        boolean mirrorFound = false;
        List<ClusterMirrorListing> mirrorListings = new ArrayList<>();
        String sourceClusterId = sourceAdmin.describeCluster().clusterId().get();
        ClusterMirrorListing expectedMirror = new ClusterMirrorListing(
                MIRROR_NAME, singleSourceBootstrapServer, sourceClusterId, numberOfTopics
        );
        while (System.currentTimeMillis() < deadline) {
            try {
                mirrorListings = destAdmin.listClusterMirrors().all().get(5, TimeUnit.SECONDS).stream().toList();
                if (mirrorListings.contains(expectedMirror)) {
                    mirrorFound = true;
                    break;
                }
                Thread.sleep(2000);
            } catch (Exception e) {
                Thread.sleep(2000);
            }
        }
        assertTrue(mirrorFound,
                "mirrorFound " + MIRROR_NAME + " was not created on destination cluster within timeout. Expected mirror =" +
                        expectedMirror + " not found in " + mirrorListings);

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

    private Map<String, org.apache.kafka.clients.admin.TopicDescription> describeTopicsWithRetry(
            Admin admin, List<String> topics) throws Exception {
        long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                return admin.describeTopics(topics).allTopicNames().get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                if (System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                Thread.sleep(500);
            }
        }
    }

    /**
     * Test that mirrors the ClusterMirrorCommand flow: pre-create topics on the destination
     * with the source's TopicId before adding them to the mirror.
     * This isolates whether the fetcher works when topics are pre-created.
     */
    @Test
    void testAsyncMirrorWithPreCreatedTopic() throws Exception {
        // Create topic on source
        sourceAdmin.createTopics(List.of(
                new NewTopic(TOPIC, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        // Get source topic description (TopicId)
        var topicDesc = describeTopicsWithRetry(sourceAdmin, List.of(TOPIC));
        String sourceTopicId = topicDesc.get(TOPIC).topicId().toString();

        // Produce data to source
        produceRecords(sourceCluster, TOPIC, 0, 50);

        // Pre-create topic on destination with source's TopicId (like ClusterMirrorCommand does)
        destAdmin.createTopics(List.of(
                new NewTopic(TOPIC, Optional.of(1), Optional.empty(), Optional.of(sourceTopicId))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror link
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(0);

        // Add topic to mirror
        destAdmin.startMirrorTopics(MIRROR_NAME, Set.of(TOPIC), new StartMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirror(1);

        // Wait for async replication
        consumeFromCluster(destCluster, TOPIC, 50);
    }

    /**
     * Test that ASYNC replication works.
     * This is a simpler smoke test for the mirror link setup.
     */
    @Test
    void testAsyncMirrorReplication() throws Exception {
        // Create topic and produce data
        sourceAdmin.createTopics(List.of(
                new NewTopic(TOPIC, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, TOPIC, 0, 50);

        // Create mirror and add topic
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(0);

        destAdmin.startMirrorTopics(MIRROR_NAME, Set.of(TOPIC), new StartMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);

        // Now verify the mirror listing shows the topic
        waitForMirror(1);

        // Produce more data while in ASYNC mode
        produceRecords(sourceCluster, TOPIC, 50, 50);

        // Verify all 100 records arrive at destination
        List<ConsumerRecord<String, String>> records = consumeFromCluster(
                destCluster, TOPIC, 100);
        assertEquals(100, records.size(),
                "Destination should have all 100 records via async replication");
    }

    /**
     * Test that mirror.topics.include regex patterns trigger auto-discovery and replication,
     * and that appending a new pattern discovers additional topics.
     * No explicit startMirrorTopics calls — discoverTopicsByPattern handles it.
     */
    @Test
    void testMirrorTopicsIncludeRegexMerge() throws Exception {
        String ordersTopic = "orders-us";
        String eventsTopic = "events-click";

        // Create source topics and produce data
        sourceAdmin.createTopics(List.of(
                new NewTopic(ordersTopic, 1, (short) 2),
                new NewTopic(eventsTopic, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, ordersTopic, 0, 30);
        produceRecords(sourceCluster, eventsTopic, 0, 20);

        // Get source topic descriptions for pre-creation on destination
        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(ordersTopic, eventsTopic));

        // Pre-create topics on destination with source TopicIds
        destAdmin.createTopics(List.of(
                new NewTopic(ordersTopic,
                        Optional.of(sourceDescs.get(ordersTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(ordersTopic).topicId().toString())),
                new NewTopic(eventsTopic,
                        Optional.of(sourceDescs.get(eventsTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(eventsTopic).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror with mirror.topics.include=orders-.* — auto-discovery picks up orders-us
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        // Auto-discovery should find orders-us and start replication
        waitForMirror(1);
        consumeFromCluster(destCluster, ordersTopic, 30);

        // Append second pattern to mirror.topics.include
        appendToTopicsInclude(MIRROR_NAME, "events-.*");

        // Auto-discovery should now find events-click and start replication
        waitForMirror(2);
        consumeFromCluster(destCluster, eventsTopic, 20);
    }

    private void appendToTopicsInclude(String mirrorName, String pattern) throws Exception {
        ConfigResource mirrorResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, mirrorName);
        var configResult = destAdmin.describeConfigs(List.of(mirrorResource)).all().get(30, TimeUnit.SECONDS);
        var mirrorConfigEntries = configResult.get(mirrorResource);

        String existingValue = "";
        ConfigEntry existingEntry = mirrorConfigEntries.get(ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG);
        if (existingEntry != null && existingEntry.value() != null && !existingEntry.value().isEmpty()) {
            existingValue = existingEntry.value();
        }
        String newValue = existingValue.isEmpty() ? pattern : existingValue + "," + pattern;

        destAdmin.incrementalAlterConfigs(Map.of(mirrorResource, List.of(
                new AlterConfigOp(
                        new ConfigEntry(ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, newValue),
                        AlterConfigOp.OpType.SET)
        ))).all().get(30, TimeUnit.SECONDS);
    }

    /**
     * Test that mirror.topics.exclude prevents specific topics from being mirrored
     * even when they match mirror.topics.include.
     */
    @Test
    void testMirrorTopicsExclude() throws Exception {
        String includedTopic = "orders-us";
        String excludedTopic = "orders-internal";

        // Create source topics and produce data
        sourceAdmin.createTopics(List.of(
                new NewTopic(includedTopic, 1, (short) 2),
                new NewTopic(excludedTopic, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, includedTopic, 0, 30);
        produceRecords(sourceCluster, excludedTopic, 0, 20);

        // Pre-create both topics on destination
        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(includedTopic, excludedTopic));
        destAdmin.createTopics(List.of(
                new NewTopic(includedTopic,
                        Optional.of(sourceDescs.get(includedTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(includedTopic).topicId().toString())),
                new NewTopic(excludedTopic,
                        Optional.of(sourceDescs.get(excludedTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(excludedTopic).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror: include orders-.* but exclude orders-internal
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "orders-.*",
                ClusterMirrorConfig.MIRROR_TOPICS_EXCLUDE_CONFIG, "orders-internal"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        // Only orders-us should be discovered — orders-internal is excluded
        waitForMirror(1);
        consumeFromCluster(destCluster, includedTopic, 30);

        // Verify orders-internal was NOT mirrored by waiting and checking it has no records
        Thread.sleep(15_000);
        List<ConsumerRecord<String, String>> excludedRecords = tryConsumeFromCluster(
                destCluster, excludedTopic, Duration.ofSeconds(5));
        assertEquals(0, excludedRecords.size(),
                "Excluded topic should have no mirrored records");
    }

    private List<ConsumerRecord<String, String>> tryConsumeFromCluster(
            KafkaClusterTestKit cluster, String topic, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new org.apache.kafka.common.TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(allRecords::add);
            }
        }
        return allRecords;
    }

    /**
     * Test that the default mirror.topics.exclude=__.*  prevents internal topics
     * from being mirrored even when mirror.topics.include=.* matches everything.
     */
    @Test
    void testMirrorTopicsExcludeDefaultInternalTopics() throws Exception {
        String userTopic = "user-events";
        String internalTopic = "__test-internal";

        // Create source topics and produce data
        sourceAdmin.createTopics(List.of(
                new NewTopic(userTopic, 1, (short) 2),
                new NewTopic(internalTopic, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, userTopic, 0, 25);
        produceRecords(sourceCluster, internalTopic, 0, 10);

        // Pre-create both on destination
        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(userTopic, internalTopic));
        destAdmin.createTopics(List.of(
                new NewTopic(userTopic,
                        Optional.of(sourceDescs.get(userTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(userTopic).topicId().toString())),
                new NewTopic(internalTopic,
                        Optional.of(sourceDescs.get(internalTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(internalTopic).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror with include=.* and no explicit exclude — default __.*  applies
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, ".*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        // user-events should be discovered and replicated
        waitForMirror(1);
        consumeFromCluster(destCluster, userTopic, 25);

        // __test-internal should NOT be mirrored due to default exclude
        Thread.sleep(15_000);
        List<ConsumerRecord<String, String>> internalRecords = tryConsumeFromCluster(
                destCluster, internalTopic, Duration.ofSeconds(5));
        assertEquals(0, internalRecords.size(),
                "Internal topic starting with __ should not be mirrored by default");
    }

    /**
     * Test that a literal topic name in mirror.topics.include works for auto-discovery.
     */
    @Test
    void testMirrorTopicsIncludeLiteral() throws Exception {
        String topic = "payments";

        sourceAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, topic, 0, 40);

        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(topic));
        destAdmin.createTopics(List.of(
                new NewTopic(topic,
                        Optional.of(sourceDescs.get(topic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(topic).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "payments"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(1);
        consumeFromCluster(destCluster, topic, 40);
    }

    /**
     * Test that a mix of literal and regex patterns in mirror.topics.include works.
     */
    @Test
    void testMirrorTopicsIncludeMixedLiteralAndRegex() throws Exception {
        String literalTopic = "payments";
        String regexMatchedTopic = "orders-eu";
        String unmatchedTopic = "logs-app";

        sourceAdmin.createTopics(List.of(
                new NewTopic(literalTopic, 1, (short) 2),
                new NewTopic(regexMatchedTopic, 1, (short) 2),
                new NewTopic(unmatchedTopic, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, literalTopic, 0, 20);
        produceRecords(sourceCluster, regexMatchedTopic, 0, 15);
        produceRecords(sourceCluster, unmatchedTopic, 0, 10);

        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(literalTopic, regexMatchedTopic, unmatchedTopic));
        destAdmin.createTopics(List.of(
                new NewTopic(literalTopic,
                        Optional.of(sourceDescs.get(literalTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(literalTopic).topicId().toString())),
                new NewTopic(regexMatchedTopic,
                        Optional.of(sourceDescs.get(regexMatchedTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(regexMatchedTopic).topicId().toString())),
                new NewTopic(unmatchedTopic,
                        Optional.of(sourceDescs.get(unmatchedTopic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(unmatchedTopic).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        // Include literal "payments" and regex "orders-.*"
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "payments,orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(2);

        consumeFromCluster(destCluster, literalTopic, 20);
        consumeFromCluster(destCluster, regexMatchedTopic, 15);

        // logs-app should NOT be mirrored
        Thread.sleep(15_000);
        List<ConsumerRecord<String, String>> unmatchedRecords = tryConsumeFromCluster(
                destCluster, unmatchedTopic, Duration.ofSeconds(5));
        assertEquals(0, unmatchedRecords.size(),
                "Unmatched topic should not be mirrored");
    }

    /**
     * Test that a stopped topic is not re-discovered by discoverTopicsByPattern,
     * because mirror.name with STOPPED_TOPIC_SUFFIX keeps it in getConfiguredTopics.
     */
    @Test
    void testStopMirrorTopicPreventsRediscovery() throws Exception {
        String topicA = "orders-us";
        String topicB = "orders-eu";

        sourceAdmin.createTopics(List.of(
                new NewTopic(topicA, 1, (short) 2),
                new NewTopic(topicB, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, topicA, 0, 20);
        produceRecords(sourceCluster, topicB, 0, 20);

        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(topicA, topicB));
        destAdmin.createTopics(List.of(
                new NewTopic(topicA,
                        Optional.of(sourceDescs.get(topicA).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(topicA).topicId().toString())),
                new NewTopic(topicB,
                        Optional.of(sourceDescs.get(topicB).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(topicB).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer,
                ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG, "orders-.*"
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(2);

        consumeFromCluster(destCluster, topicA, 20);
        consumeFromCluster(destCluster, topicB, 20);

        // Stop orders-eu — sets mirror.name to test-mirror.stopped
        destAdmin.stopMirrorTopics(MIRROR_NAME, Set.of(topicB), new StopMirrorTopicsOptions())
                .all().get(30, TimeUnit.SECONDS);

        waitForMirror(1);

        // Produce more data to both topics — only orders-us should receive new records
        produceRecords(sourceCluster, topicA, 20, 20);
        produceRecords(sourceCluster, topicB, 20, 20);

        consumeFromCluster(destCluster, topicA, 40);

        // Verify orders-eu did NOT receive new data (still at 20, not 40)
        Thread.sleep(15_000);
        List<ConsumerRecord<String, String>> stoppedRecords = tryConsumeFromCluster(
                destCluster, topicB, Duration.ofSeconds(5));
        assertEquals(20, stoppedRecords.size(),
                "Stopped topic should not have received new mirrored records");
    }

    /**
     * Test that startMirrorTopics with includePatterns in options
     * persists patterns to mirror.topics.include via KafkaAdminClient.
     */
    @Test
    void testStartMirrorTopicsWithIncludePatterns() throws Exception {
        String topic = "orders-us";

        sourceAdmin.createTopics(List.of(
                new NewTopic(topic, 1, (short) 2)
        )).all().get(30, TimeUnit.SECONDS);

        produceRecords(sourceCluster, topic, 0, 30);

        var sourceDescs = describeTopicsWithRetry(sourceAdmin, List.of(topic));
        destAdmin.createTopics(List.of(
                new NewTopic(topic,
                        Optional.of(sourceDescs.get(topic).partitions().size()),
                        Optional.empty(),
                        Optional.of(sourceDescs.get(topic).topicId().toString()))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror with NO initial mirror.topics.include
        destAdmin.createClusterMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateClusterMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(0);

        // Start mirroring with includePatterns — should persist "orders-.*" to config
        destAdmin.startMirrorTopics(MIRROR_NAME, Set.of(topic),
                new StartMirrorTopicsOptions().includePatterns(List.of("orders-.*")))
                .all().get(30, TimeUnit.SECONDS);

        waitForMirror(1);
        consumeFromCluster(destCluster, topic, 30);

        // Verify the pattern was persisted to mirror.topics.include
        ConfigResource mirrorResource = new ConfigResource(ConfigResource.Type.CLUSTER_MIRROR, MIRROR_NAME);
        var configResult = destAdmin.describeConfigs(List.of(mirrorResource)).all().get(30, TimeUnit.SECONDS);
        var mirrorConfigEntries = configResult.get(mirrorResource);
        ConfigEntry includeEntry = mirrorConfigEntries.get(ClusterMirrorConfig.MIRROR_TOPICS_INCLUDE_CONFIG);
        assertTrue(includeEntry != null && includeEntry.value().contains("orders-.*"),
                "mirror.topics.include should contain 'orders-.*' after startMirrorTopics with includePatterns");
    }
}