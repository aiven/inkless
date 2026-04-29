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
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;
import org.apache.kafka.server.log.remote.storage.RemoteLogManagerConfig;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntFunction;
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
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;

import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.CLEANUP_POLICY_DELETE;
import static org.apache.kafka.common.config.TopicConfig.DISKLESS_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG;
import static org.apache.kafka.common.config.TopicConfig.SEGMENT_BYTES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class InklessConsolidatedDisklessTopicsTest {

    private static final Logger log = LoggerFactory.getLogger(InklessConsolidatedDisklessTopicsTest.class);

    private static final String RSM_CONFIG_PREFIX = RemoteLogManagerConfig.DEFAULT_REMOTE_STORAGE_MANAGER_CONFIG_PREFIX;

    private static final String RSM_OBJECT_KEY_PREFIX = "tsu-int-test/";

    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private KafkaClusterTestKit cluster;
    private String topicName = "consolidated-diskless-topic";
    private int numPartitions = 2;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        // Create 2-broker cluster with rack assignments (matching docker compose setup)
        // Node 0: combined broker+controller, Node 1: broker-only
        Map<Integer, Map<String, String>> perServerProps = Map.of(
            0, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az1"),
            1, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az2")
        );
        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(2)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProps)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            // Enable managed replicas with RF=2 (one replica per AZ)
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
            // Enable diskless topic consolidation (diskless.enable + remote.storage.enable on new topics)
            .setConfigProp(ServerConfigs.DISKLESS_REMOTE_STORAGE_CONSOLIDATION_ENABLE_CONFIG, "true")
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
            // Tiered storage configs
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_TASK_INTERVAL_MS_PROP, "5000")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_MANAGER_FOLLOWER_THREAD_POOL_SIZE_PROP, "4")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, TopicBasedRemoteLogMetadataManager.class.getName())
            .setConfigProp(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP, "EXTERNAL")
            .setConfigProp(RemoteLogManagerConfig.DEFAULT_REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX
                + TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_REPLICATION_FACTOR_PROP, "1")
            .setConfigProp(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, "io.aiven.kafka.tieredstorage.RemoteStorageManager")
            .setConfigProp(RSM_CONFIG_PREFIX + "chunk.size", 1048576)
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.class", "io.aiven.kafka.tieredstorage.fetch.cache.DiskChunkCache")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.path",
                Files.createTempDirectory("ts-fetch-chunk-cache").toAbsolutePath().toString())
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.size", "10737418")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.prefetch.max.size", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "fetch.chunk.cache.retention.ms", "16777216")
            .setConfigProp(RSM_CONFIG_PREFIX + "custom.metadata.fields.include", "REMOTE_SIZE")
            .setConfigProp(RSM_CONFIG_PREFIX + "key.prefix", "tsu-int-test/")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.backend.class", "io.aiven.kafka.tieredstorage.storage.s3.S3Storage")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.bucket.name", s3Container.getBucketName())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.region", s3Container.getRegion())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.endpoint.url", s3Container.getEndpoint())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.s3.path.style.access.enabled", "true")
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.aws.access.key.id", s3Container.getAccessKey())
            .setConfigProp(RSM_CONFIG_PREFIX + "storage.aws.secret.access.key", s3Container.getSecretKey())
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
    }

    @AfterEach
    public void teardown() throws Exception {
        cluster.close();
    }

    @Test
    public void testNewConsolidatedDisklessTopics() throws Exception {
        Map<String, Object> commonConfigs = new HashMap<>();
        commonConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        Map<String, String> topicConfigs = Map.of(
            DISKLESS_ENABLE_CONFIG, "true",
            REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
            CLEANUP_POLICY_CONFIG, CLEANUP_POLICY_DELETE,
            SEGMENT_BYTES_CONFIG, "1048576"
        );

        try (Admin admin = AdminClient.create(commonConfigs)) {
            final NewTopic topic = new NewTopic(topicName, numPartitions, (short) -1).configs(topicConfigs);
            CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
            result.all().get(30, TimeUnit.SECONDS);

            final TopicDescription[] descriptionHolder = new TopicDescription[1];
            TestUtils.waitForCondition(() -> {
                try {
                    descriptionHolder[0] = admin.describeTopics(Collections.singletonList(topicName))
                        .allTopicNames().get(30, TimeUnit.SECONDS).get(topicName);
                    return descriptionHolder[0].partitions().size() == numPartitions;
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                        return false;
                    }
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }, 60_000, () -> "Topic should become visible with " + numPartitions + " partitions");
            TopicDescription description = descriptionHolder[0];
            assertEquals(numPartitions, description.partitions().size());

            Set<Integer> expectedBrokers = Set.of(0, 1);
            for (TopicPartitionInfo partition : description.partitions()) {
                assertEquals(2, partition.replicas().size(),
                    "Expected RF=2 for managed replicas with consolidation enabled");
                Set<Integer> replicaIds = partition.replicas().stream().map(Node::id).collect(Collectors.toSet());
                assertEquals(expectedBrokers, replicaIds);
            }
        }

        int recordsToSendAndReceive = 50;

        produceRecords(commonConfigs, recordsToSendAndReceive, i -> {
            String value = TestUtils.randomString(104_857);
            return new ProducerRecord<>(topicName, i % numPartitions, null, value);
        });

        try (S3Client s3 = s3Container.getS3Client()) {
            final String bucket = s3Container.getBucketName();
            TestUtils.waitForCondition(() -> countObjectsWithPrefix(s3, bucket, RSM_OBJECT_KEY_PREFIX) > 0,
                120_000,
                () -> "Expected at least one tiered object in the Minio bucket after produce");
        }

        consumeAndVerify(commonConfigs, recordsToSendAndReceive);
    }

    private void produceRecords(Map<String, Object> commonConfigs, int numRecords,
                                        IntFunction<ProducerRecord<String, String>> recordFactory) {
        var producerConfigs = new HashMap<>(commonConfigs);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        long totalSize = 0;
        try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            for (int i = 0; i < numRecords; i++) {
                var record = recordFactory.apply(i);
                var metadata = producer.send(record).get();
                totalSize += metadata.serializedValueSize();
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        log.info("Produced {} records in total size {}", numRecords, totalSize);
    }

    private void consumeAndVerify(Map<String, Object> commonConfigs, int expectedTotalRecords) throws InterruptedException {
        var consumerConfigs = new HashMap<>(commonConfigs);
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-id-" + UUID.randomUUID());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (var consumer = new KafkaConsumer<String, String>(consumerConfigs)) {
            consumer.subscribe(Collections.singletonList(topicName));
            var offsetsByPartition = new HashMap<TopicPartition, List<Long>>();
            var consumedCount = new AtomicLong(0);
            TestUtils.waitForCondition(() -> {
                var records = consumer.poll(Duration.ofMillis(1000));
                records.forEach(r -> {
                    var tp = new TopicPartition(r.topic(), r.partition());
                    offsetsByPartition.computeIfAbsent(tp, __ -> new ArrayList<>()).add(r.offset());
                });
                consumedCount.addAndGet(records.count());
                return consumedCount.get() >= expectedTotalRecords;
            }, 60_000, "Not all records have been consumed");
            // Monotonicity + no gaps per partition
            offsetsByPartition.forEach((tp, offsets) -> {
                for (int i = 1; i < offsets.size(); i++) {
                    long prev = offsets.get(i - 1);
                    long cur = offsets.get(i);
                    assertEquals(prev + 1, cur, "Offset gap or reordering for " + tp);
                }
            });
        }
    }

    private static int countObjectsWithPrefix(S3Client s3, String bucket, String prefix) {
        int total = 0;
        String continuationToken = null;
        ListObjectsV2Response page;
        do {
            ListObjectsV2Request.Builder b = ListObjectsV2Request.builder().bucket(bucket).prefix(prefix);
            if (continuationToken != null) {
                b.continuationToken(continuationToken);
            }
            page = s3.listObjectsV2(b.build());
            total += page.contents().size();
            continuationToken = page.isTruncated() ? page.nextContinuationToken() : null;
        } while (continuationToken != null);
        return total;
    }
}
