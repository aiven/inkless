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
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the ListOffsets answers for a born-diskless topic before consolidation as seen by clients,
 * on each broker. The answers describe the partition and do not depend on the replica that
 * receives the request.
 */
@Testcontainers
public class InklessDisklessListOffsetsTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final String TOPIC = "diskless-list-offsets";
    private static final TopicPartition TP = new TopicPartition(TOPIC, 0);

    // Producer batches A and B carry CreateTime timestamps. Batch B has its max timestamp on its
    // second record, so record-level and batch-level answers differ.
    private static final long[] BATCH_A_TIMESTAMPS = {1_000L, 1_001L, 1_002L, 1_003L, 1_004L};
    private static final long[] BATCH_B_TIMESTAMPS = {2_000L, 2_009L, 2_002L, 2_003L, 2_004L};
    private static final long RECORD_COUNT = BATCH_A_TIMESTAMPS.length + BATCH_B_TIMESTAMPS.length;

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(2)
            .setNumControllerNodes(1)
            .setPerServerProperties(Map.of(
                0, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az1"),
                1, Map.of(ServerConfigs.BROKER_RACK_CONFIG, "az2")
            ))
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "2")
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

        try (Admin admin = Admin.create(clientConfigs("diskless_az=az1"))) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) -1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"))))
                .all().get(30, TimeUnit.SECONDS);
        }
        produceBatch(BATCH_A_TIMESTAMPS);
        produceBatch(BATCH_B_TIMESTAMPS);
    }

    @AfterEach
    public void teardown() throws Exception {
        cluster.close();
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"diskless_az=az1", "diskless_az=az2"})
    public void answersEveryTimestampTypeFromTheControlPlane(final String clientId) throws Exception {
        try (Admin admin = Admin.create(clientConfigs(clientId))) {
            assertServingBroker(admin, clientId);

            assertAnswer(admin, OffsetSpec.earliest(), -1L, 0L);
            assertAnswer(admin, OffsetSpec.latest(), -1L, RECORD_COUNT);
            // The control plane log start offset. There is no local log.
            assertAnswer(admin, OffsetSpec.earliestLocal(), -1L, 0L);
            // Classic Kafka answers -1 for the leader epoch. The control plane answers 0.
            assertAnswer(admin, OffsetSpec.latestTiered(), -1L, -1L);
            // The answer is batch-level. The offset is 9, the last offset of the batch with the max
            // timestamp. The record with that timestamp is at offset 6.
            assertAnswer(admin, OffsetSpec.maxTimestamp(), 2_009L, 9L);
            // The answer is batch-level. It returns the base offset of the first batch whose max timestamp
            // is at or after the target, with that batch max timestamp. Classic Kafka answers (1_002, 2).
            assertAnswer(admin, OffsetSpec.forTimestamp(1_002L), 1_004L, 0L);
            assertAnswer(admin, OffsetSpec.forTimestamp(1_005L), 2_009L, 5L);
            assertAnswer(admin, OffsetSpec.forTimestamp(3_000L), -1L, -1L);

            // Classic Kafka answers (-1, -1) when remote storage is off. The control plane rejects
            // the timestamp as an unsupported special timestamp.
            final ExecutionException e = assertThrows(ExecutionException.class,
                () -> listOffsets(admin, OffsetSpec.earliestPendingUpload()));
            assertInstanceOf(UnknownServerException.class, e.getCause());
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {"diskless_az=az1", "diskless_az=az2"})
    public void fetchesTheLastRecordAtLatestMinusOne(final String clientId) {
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs(clientId, null, "none"))) {
            consumer.assign(List.of(TP));
            consumer.seek(TP, RECORD_COUNT - 1);
            final List<ConsumerRecord<byte[], byte[]>> records = pollUntil(consumer, 1);
            assertEquals(RECORD_COUNT - 1, records.get(0).offset());
            assertEquals(BATCH_B_TIMESTAMPS[BATCH_B_TIMESTAMPS.length - 1], records.get(0).timestamp());
        }
    }

    @Test
    public void committedPositionPassesLeaderEpochValidationAfterRestart() throws Exception {
        final String groupId = "diskless-list-offsets-group";
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs("diskless_az=az1", groupId, "earliest"))) {
            consumer.assign(List.of(TP));
            final List<ConsumerRecord<byte[], byte[]>> records = pollUntil(consumer, (int) RECORD_COUNT);
            final ConsumerRecord<byte[], byte[]> last = records.get(records.size() - 1);
            assertEquals(Optional.of(0), last.leaderEpoch());
            consumer.commitSync(Map.of(TP, new OffsetAndMetadata(last.offset() + 1, last.leaderEpoch(), "")));
        }

        produceBatch(new long[] {3_000L, 3_001L});

        final long validationsBefore = offsetForLeaderEpochRequests();
        // With reset policy none, a failed leader epoch validation throws LogTruncationException.
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs("diskless_az=az2", groupId, "none"))) {
            consumer.assign(List.of(TP));
            final List<ConsumerRecord<byte[], byte[]>> records = pollUntil(consumer, 2);
            assertEquals(RECORD_COUNT, records.get(0).offset());
            assertEquals(RECORD_COUNT + 1, records.get(1).offset());
        }
        assertTrue(offsetForLeaderEpochRequests() > validationsBefore, "OffsetsForLeaderEpoch requests");
    }

    // Brokers share one JVM. The default registry sums the counter over all brokers.
    private static long offsetForLeaderEpochRequests() {
        long total = 0;
        for (final Map.Entry<MetricName, Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            final MetricName name = entry.getKey();
            if ("RequestMetrics".equals(name.getType()) && "RequestsPerSec".equals(name.getName())
                && name.getMBeanName().contains("request=OffsetForLeaderEpoch")) {
                total += ((Meter) entry.getValue()).count();
            }
        }
        return total;
    }

    private void assertAnswer(final Admin admin, final OffsetSpec spec, final long timestamp, final long offset) throws Exception {
        final ListOffsetsResultInfo info = listOffsets(admin, spec);
        final String what = spec.getClass().getSimpleName();
        assertEquals(timestamp, info.timestamp(), what + " timestamp");
        assertEquals(offset, info.offset(), what + " offset");
        assertEquals(Optional.of(0), info.leaderEpoch(), what + " leader epoch");
    }

    private static ListOffsetsResultInfo listOffsets(final Admin admin, final OffsetSpec spec) throws Exception {
        return admin.listOffsets(Map.of(TP, spec)).partitionResult(TP).get(30, TimeUnit.SECONDS);
    }

    // The metadata transformer sends each client to the replica in its AZ; the az2 client must
    // reach node 1, or the two parameterized runs test the same broker.
    private static void assertServingBroker(final Admin admin, final String clientId) throws Exception {
        final int expected = clientId.endsWith("az1") ? 0 : 1;
        final long deadline = System.currentTimeMillis() + 30_000;
        while (true) {
            try {
                final TopicDescription description = admin.describeTopics(List.of(TOPIC))
                    .allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC);
                assertEquals(expected, description.partitions().get(0).leader().id(), "serving broker for " + clientId);
                return;
            } catch (final ExecutionException e) {
                if (!(e.getCause() instanceof UnknownTopicOrPartitionException) || System.currentTimeMillis() >= deadline) {
                    throw e;
                }
                Thread.sleep(200);
            }
        }
    }

    // A long linger and an explicit flush put every record of the call into one producer batch.
    private void produceBatch(final long[] timestamps) throws Exception {
        final Map<String, Object> configs = clientConfigs("diskless_az=az1");
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        configs.put(ProducerConfig.LINGER_MS_CONFIG, "60000");
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, "1000000");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(configs)) {
            final List<Future<?>> futures = new ArrayList<>();
            for (final long timestamp : timestamps) {
                futures.add(producer.send(new ProducerRecord<>(TOPIC, 0, timestamp, null, new byte[] {1})));
            }
            producer.flush();
            for (final Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        }
    }

    private Map<String, Object> consumerConfigs(final String clientId, final String groupId, final String autoOffsetReset) {
        final Map<String, Object> configs = clientConfigs(clientId);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        if (groupId != null) {
            configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        }
        return configs;
    }

    private Map<String, Object> clientConfigs(final String clientId) {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(CommonClientConfigs.CLIENT_ID_CONFIG, clientId);
        return configs;
    }

    private static List<ConsumerRecord<byte[], byte[]>> pollUntil(final KafkaConsumer<byte[], byte[]> consumer, final int count) {
        final List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        final long deadline = System.currentTimeMillis() + 30_000;
        while (records.size() < count && System.currentTimeMillis() < deadline) {
            consumer.poll(Duration.ofMillis(500)).forEach(records::add);
        }
        assertEquals(count, records.size(), "records polled");
        return records;
    }
}
