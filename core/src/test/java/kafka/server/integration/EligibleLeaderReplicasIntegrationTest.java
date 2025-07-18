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
package kafka.server.integration;
import kafka.integration.KafkaServerTestHarness;
import kafka.server.KafkaBroker;
import kafka.server.KafkaConfig;
import kafka.utils.Logging;
import kafka.utils.TestUtils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.FeatureUpdate;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.admin.UpdateFeaturesOptions;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.server.common.EligibleLeaderReplicasVersion;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.storage.internals.checkpoint.CleanShutdownFileHandler;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.mutable.HashMap;

import static org.apache.kafka.test.TestUtils.DEFAULT_MAX_WAIT_MS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EligibleLeaderReplicasIntegrationTest extends KafkaServerTestHarness implements Logging {
    private String bootstrapServer;
    private String testTopicName;
    private Admin adminClient;

    @Override
    public MetadataVersion metadataVersion() {
        return MetadataVersion.IBP_4_0_IV1;
    }

    @Override
    public Seq<KafkaConfig> generateConfigs() {
        List<Properties> brokerConfigs = new ArrayList<>();
        brokerConfigs.addAll(scala.collection.JavaConverters.seqAsJavaList(TestUtils.createBrokerConfigs(
            5, // The tests require 4 brokers to host the partition. However, we need the 5th broker to handle the admin client requests.
            true,
            true,
            scala.Option.<SecurityProtocol>empty(),
            scala.Option.<File>empty(),
            scala.Option.<Properties>empty(),
            true,
            false,
            false,
            false,
            new HashMap<>(),
            1,
            false,
            1,
            (short) 4,
            0,
            false,
            scala.Option.empty()
        )));
        List<KafkaConfig> configs = new ArrayList<>();
        for (Properties props : brokerConfigs) {
            configs.add(KafkaConfig.fromProps(props));
        }
        return JavaConverters.asScalaBuffer(configs).toSeq();
    }

    @BeforeEach
    @Override
    public void setUp(TestInfo info) {
        super.setUp(info);
        // create adminClient
        Properties props = new Properties();
        bootstrapServer = bootstrapServers(listenerName());
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        adminClient = Admin.create(props);
        adminClient.updateFeatures(
            Map.of(EligibleLeaderReplicasVersion.FEATURE_NAME,
                new FeatureUpdate(EligibleLeaderReplicasVersion.ELRV_1.featureLevel(), FeatureUpdate.UpgradeType.UPGRADE)),
            new UpdateFeaturesOptions()
        );
        testTopicName = String.format("%s-%s", info.getTestMethod().get().getName(), "ELR-test");
    }

    @AfterEach
    public void close() throws Exception {
        if (adminClient != null) adminClient.close();
    }

    @Test
    public void testHighWatermarkShouldNotAdvanceIfUnderMinIsr() throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();
        Producer producer = null;
        Consumer consumer = null;
        try {
            TopicDescription testTopicDescription = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            Properties producerProps = new Properties();
            producerProps.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            // Use Ack=1 for the producer.
            producerProps.put(ProducerConfig.ACKS_CONFIG, "1");
            producer = new KafkaProducer(producerProps);

            Properties consumerProps = new Properties();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
            consumerProps.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "10");
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumer = new KafkaConsumer<>(consumerProps);
            consumer.subscribe(Set.of(testTopicName));

            producer.send(new ProducerRecord<>(testTopicName, "0", "0")).get();
            waitUntilOneMessageIsConsumed(consumer);

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 2 && elrSize == 1;
            });

            // Now the partition is under min ISR. HWM should not advance.
            producer.send(new ProducerRecord<>(testTopicName, "1", "1")).get();
            Thread.sleep(100);
            assertEquals(0, consumer.poll(Duration.ofSeconds(1L)).count());

            // Restore the min ISR and the previous log should be visible.
            startBroker(initialReplicas.get(1).id());
            startBroker(initialReplicas.get(0).id());
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 4 && elrSize == 0;
            });

            waitUntilOneMessageIsConsumed(consumer);
        } finally {
            restartDeadBrokers(false);
            if (consumer != null) consumer.close();
            if (producer != null) producer.close();
        }
    }

    void waitUntilOneMessageIsConsumed(Consumer consumer) {
        TestUtils.waitUntilTrue(
            () -> {
                try {
                    ConsumerRecords record = consumer.poll(Duration.ofMillis(100L));
                    return record.count() >= 1;
                } catch (Exception e) {
                    return false;
                }
            },
            () -> "fail to consume messages",
            DEFAULT_MAX_WAIT_MS, 100L
        );
    }

    @Test
    public void testElrMemberCanBeElected() throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();

        try {
            TopicDescription testTopicDescription = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());
            killBroker(initialReplicas.get(2).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 1 && elrSize == 2;
            });

            killBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 3;
            });

            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(1, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            int expectLastKnownLeader = initialReplicas.get(3).id();
            assertEquals(expectLastKnownLeader, topicPartitionInfo.lastKnownElr().get(0).id(), topicPartitionInfo.toString());

            // At this point, all the replicas are failed and the last know leader is No.3 and 3 members in the ELR.
            // Restart one broker of the ELR and it should be the leader.

            int expectLeader = topicPartitionInfo.elr().stream()
                .filter(node -> node.id() != expectLastKnownLeader).toList().get(0).id();

            startBroker(expectLeader);
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 1 && elrSize == 2;
            });

            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            assertEquals(expectLeader, topicPartitionInfo.leader().id(), topicPartitionInfo.toString());

            // Start another 2 brokers and the ELR fields should be cleaned.
            topicPartitionInfo.replicas().stream().filter(node -> node.id() != expectLeader).limit(2)
                .forEach(node -> startBroker(node.id()));

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 3 && elrSize == 0;
            });

            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertEquals(0, topicPartitionInfo.lastKnownElr().size(), topicPartitionInfo.toString());
            assertEquals(expectLeader, topicPartitionInfo.leader().id(), topicPartitionInfo.toString());
        } finally {
            restartDeadBrokers(false);
        }
    }

    @Test
    public void testElrMemberShouldBeKickOutWhenUncleanShutdown() throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();

        try {
            TopicDescription testTopicDescription = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());
            killBroker(initialReplicas.get(2).id());
            killBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 3;
            });
            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);

            int brokerToBeUncleanShutdown = topicPartitionInfo.elr().get(0).id();
            KafkaBroker broker = brokers().find(b -> {
                return b.config().brokerId() == brokerToBeUncleanShutdown;
            }).get();
            Seq<File> dirs = broker.logManager().liveLogDirs();
            assertEquals(1, dirs.size());
            CleanShutdownFileHandler handler = new CleanShutdownFileHandler(dirs.apply(0).toString());
            assertTrue(handler.exists());
            assertDoesNotThrow(() -> handler.delete());

            // After remove the clean shutdown file, the broker should report unclean shutdown during restart.
            startBroker(brokerToBeUncleanShutdown);
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 2;
            });
            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertNull(topicPartitionInfo.leader());
            assertEquals(1, topicPartitionInfo.lastKnownElr().size());
        } finally {
            restartDeadBrokers(false);
        }
    }

    /*
        This test is only valid for KIP-966 part 1. When the unclean recovery is implemented, it should be removed.
     */
    @Test
    public void testLastKnownLeaderShouldBeElectedIfEmptyElr() throws ExecutionException, InterruptedException {
        adminClient.createTopics(
            List.of(new NewTopic(testTopicName, 1, (short) 4))).all().get();
        TestUtils.waitForPartitionMetadata(brokers(), testTopicName, 0, 1000);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, testTopicName);
        Collection<AlterConfigOp> ops = new ArrayList<>();
        ops.add(new AlterConfigOp(new ConfigEntry(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "3"), AlterConfigOp.OpType.SET));
        Map<ConfigResource, Collection<AlterConfigOp>> configOps = Map.of(configResource, ops);
        // alter configs on target cluster
        adminClient.incrementalAlterConfigs(configOps).all().get();

        try {
            TopicDescription testTopicDescription = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName);
            TopicPartitionInfo topicPartitionInfo = testTopicDescription.partitions().get(0);
            List<Node> initialReplicas = topicPartitionInfo.replicas();
            assertEquals(4, topicPartitionInfo.isr().size());
            assertEquals(0, topicPartitionInfo.elr().size());
            assertEquals(0, topicPartitionInfo.lastKnownElr().size());

            killBroker(initialReplicas.get(0).id());
            killBroker(initialReplicas.get(1).id());
            killBroker(initialReplicas.get(2).id());
            killBroker(initialReplicas.get(3).id());

            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 3;
            });
            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            int lastKnownLeader = topicPartitionInfo.lastKnownElr().get(0).id();

            Set<Integer> initialReplicaSet = initialReplicas.stream().map(node -> node.id()).collect(Collectors.toSet());
            brokers().foreach(broker -> {
                if (initialReplicaSet.contains(broker.config().brokerId())) {
                    Seq<File> dirs = broker.logManager().liveLogDirs();
                    assertEquals(1, dirs.size());
                    CleanShutdownFileHandler handler = new CleanShutdownFileHandler(dirs.apply(0).toString());
                    assertDoesNotThrow(() -> handler.delete());
                }
                return true;
            });


            // After remove the clean shutdown file, the broker should report unclean shutdown during restart.
            topicPartitionInfo.replicas().forEach(replica -> {
                if (replica.id() != lastKnownLeader) startBroker(replica.id());
            });
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize == 0 && elrSize == 1;
            });
            topicPartitionInfo = adminClient.describeTopics(List.of(testTopicName))
                .allTopicNames().get().get(testTopicName).partitions().get(0);
            assertNull(topicPartitionInfo.leader());
            assertEquals(1, topicPartitionInfo.lastKnownElr().size());

            // Now if the last known leader goes through unclean shutdown, it will still be elected.
            startBroker(lastKnownLeader);
            waitForIsrAndElr((isrSize, elrSize) -> {
                return isrSize > 0 && elrSize == 0;
            });

            TestUtils.waitUntilTrue(
                () -> {
                    try {
                        TopicPartitionInfo partition = adminClient.describeTopics(List.of(testTopicName))
                            .allTopicNames().get().get(testTopicName).partitions().get(0);
                        if (partition.leader() == null) return false;
                        return partition.lastKnownElr().isEmpty() && partition.elr().isEmpty() && partition.leader().id() == lastKnownLeader;
                    } catch (Exception e) {
                        return false;
                    }
                },
                () -> String.format("Partition metadata for %s is not correct", testTopicName),
                DEFAULT_MAX_WAIT_MS, 100L
            );
        } finally {
            restartDeadBrokers(false);
        }
    }

    void waitForIsrAndElr(BiFunction<Integer, Integer, Boolean> isIsrAndElrSizeSatisfied) {
        TestUtils.waitUntilTrue(
            () -> {
                try {
                    TopicDescription topicDescription = adminClient.describeTopics(List.of(testTopicName))
                        .allTopicNames().get().get(testTopicName);
                    TopicPartitionInfo partition = topicDescription.partitions().get(0);
                    return isIsrAndElrSizeSatisfied.apply(partition.isr().size(), partition.elr().size());
                } catch (Exception e) {
                    return false;
                }
            },
            () -> String.format("Partition metadata for %s is not propagated", testTopicName),
            DEFAULT_MAX_WAIT_MS, 100L);
    }
}
