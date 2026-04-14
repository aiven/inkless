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
import org.apache.kafka.clients.admin.AddTopicsToMirrorOptions;
import org.apache.kafka.clients.admin.CreateMirrorOptions;
import org.apache.kafka.clients.admin.MirrorListing;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.server.config.MirrorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Properties;
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
public class AsyncMirrorIntegrationTest {

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
                .setConfigProp(MirrorConfig.METADATA_REFRESH_INTERVAL_MS_CONFIG, "5000")
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

    private void produceRecords(KafkaClusterTestKit cluster,
                                int startIndex, int count) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = startIndex; i < startIndex + count; i++) {
                producer.send(new ProducerRecord<>(AsyncMirrorIntegrationTest.TOPIC, "key-" + i, "value-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }
    }

    private List<ConsumerRecord<String, String>> consumeFromCluster(
            KafkaClusterTestKit cluster,
            int expectedRecords, Duration timeout) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.currentTimeMillis());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<ConsumerRecord<String, String>> allRecords = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            var partitions = consumer.partitionsFor(AsyncMirrorIntegrationTest.TOPIC).stream()
                    .map(info -> new org.apache.kafka.common.TopicPartition(AsyncMirrorIntegrationTest.TOPIC, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (allRecords.size() < expectedRecords && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(allRecords::add);
            }
        }
        return allRecords;
    }

    private void waitForMirror(int numberOfTopics) throws Exception {
        long deadline = System.currentTimeMillis() + 90_000;
        boolean mirrorFound = false;
        List<MirrorListing> mirrorListings = new ArrayList<>();
        String sourceClusterId = sourceAdmin.describeCluster().clusterId().get();
        MirrorListing expectedMirror = new MirrorListing(
                MIRROR_NAME, singleSourceBootstrapServer, sourceClusterId, numberOfTopics
        );
        while (System.currentTimeMillis() < deadline) {
            try {
                mirrorListings = destAdmin.listMirrors().all().get(5, TimeUnit.SECONDS).stream().toList();
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
    private void waitForRecordsOnDestination(int expectedRecords) throws Exception {
        // Wait up to 90s to allow for: mirror coordinator startup + metadata refresh
        // (topic auto-creation on destination) + replication catch-up
        long deadline = System.currentTimeMillis() + 90_000;

        // First, wait for the topic to exist on the destination
        boolean topicFound = false;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (destAdmin.listTopics().names().get(5, TimeUnit.SECONDS).contains(TOPIC)) {
                    topicFound = true;
                    break;
                }
                Thread.sleep(2000);
            } catch (Exception e) {
                Thread.sleep(2000);
            }
        }
        assertTrue(topicFound,
                "Topic " + TOPIC + " was not auto-created on destination cluster within timeout");

        // Then wait for records to be replicated
        while (System.currentTimeMillis() < deadline) {
            List<ConsumerRecord<String, String>> records = consumeFromCluster(
                    destCluster, expectedRecords, Duration.ofSeconds(5));
            if (records.size() >= expectedRecords) {
                return;
            }
            Thread.sleep(1000);
        }
        // Final attempt with assertion
        List<ConsumerRecord<String, String>> records = consumeFromCluster(
                destCluster, expectedRecords, Duration.ofSeconds(10));
        assertEquals(expectedRecords, records.size(),
                "Destination did not catch up within timeout. Expected " +
                        expectedRecords + " records but got " + records.size());
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

    /**
     * Test that mirrors the MirrorCommand flow: pre-create topics on the destination
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
        var topicDesc = sourceAdmin.describeTopics(List.of(TOPIC)).allTopicNames().get(30, TimeUnit.SECONDS);
        String sourceTopicId = topicDesc.get(TOPIC).topicId().toString();

        // Produce data to source
        produceRecords(sourceCluster, 0, 50);

        // Pre-create topic on destination with source's TopicId (like MirrorCommand does)
        destAdmin.createTopics(List.of(
                new NewTopic(TOPIC, Optional.of(1), Optional.empty(), Optional.of(sourceTopicId))
        )).all().get(30, TimeUnit.SECONDS);

        // Create mirror link
        destAdmin.createMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(0);

        // Add topic to mirror
        destAdmin.addTopicsToMirror(MIRROR_NAME, Set.of(TOPIC), new AddTopicsToMirrorOptions())
                .all().get(30, TimeUnit.SECONDS);
        waitForMirror(1);

        // Wait for async replication
        List<ConsumerRecord<String, String>> records = consumeFromCluster(
                destCluster, 50, Duration.ofSeconds(60));
        assertEquals(50, records.size(),
                "Destination should have all 50 records via async replication with pre-created topic");
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

        produceRecords(sourceCluster, 0, 50);

        // Create mirror and add topic
        destAdmin.createMirror(MIRROR_NAME, Map.of(
                "bootstrap.servers", singleSourceBootstrapServer
        ), new CreateMirrorOptions()).all().get(30, TimeUnit.SECONDS);

        waitForMirror(0);

        destAdmin.addTopicsToMirror(MIRROR_NAME, Set.of(TOPIC), new AddTopicsToMirrorOptions())
                .all().get(30, TimeUnit.SECONDS);

        // Wait for the topic to be auto-created on destination by the mirror coordinator
        waitForRecordsOnDestination(50);

        // Now verify the mirror listing shows the topic
        waitForMirror(1);

        // Produce more data while in ASYNC mode
        produceRecords(sourceCluster, 50, 50);

        // Verify all 100 records arrive at destination
        List<ConsumerRecord<String, String>> records = consumeFromCluster(
                destCluster, 100, Duration.ofSeconds(60));
        assertEquals(100, records.size(),
                "Destination should have all 100 records via async replication");
    }

}
