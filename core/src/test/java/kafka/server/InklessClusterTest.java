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
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

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
public class InklessClusterTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(InklessClusterTest.class);

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(2)
            .setNumControllerNodes(1)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
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
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            // Decrease cache block bytes to test cache split due to alignment
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONSUME_CACHE_BLOCK_BYTES_CONFIG, 16 * 1024)
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();
    }

    @AfterEach
    public void teardown() throws Exception {
        cluster.close();
    }

    public static Stream<Arguments> params() {
        return Stream.of(
            Arguments.of(
                true, TimestampType.CREATE_TIME, TopicConfig.DISKLESS_ENABLE_CONFIG
            ),
            Arguments.of(
                false, TimestampType.LOG_APPEND_TIME, TopicConfig.INKLESS_ENABLE_CONFIG
            )
        );
    }

    @ParameterizedTest
    @MethodSource("params")
    public void createDisklessTopic(final boolean idempotenceEnable, final TimestampType timestampType, final String disklessTopicConfig) throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(idempotenceEnable));
        clientConfigs.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
        clientConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, "100000");
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // by default is latest and nothing would get consumed.
        clientConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.name());
        clientConfigs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5000000");
        String topicName = "diskless-topic";
        int numRecords = 500;

        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic topic = new NewTopic(topicName, 1, (short) 1)
                .configs(Map.of(
                    disklessTopicConfig, "true",
                    TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, timestampType.name
                ));
            CreateTopicsResult topics = admin.createTopics(Collections.singletonList(topic));
            topics.all().get(10, TimeUnit.SECONDS);
        }

        AtomicInteger recordsProduced = new AtomicInteger();
        final long now = System.currentTimeMillis();
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(clientConfigs)) {
            for (int i = 0; i < numRecords; i++) {
                byte[] value = new byte[10000];
                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, 0, now, null, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record", exception);
                    } else {
                        log.info("Committed value at offset {} at {}", metadata.offset(), now);
                        recordsProduced.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }

        assertEquals(numRecords, recordsProduced.get());
        consumeWithManualAssignment(timestampType, clientConfigs, topicName, now, numRecords);
        consumeWithSubscription(timestampType, clientConfigs, topicName, now, numRecords);
    }

    @Test
    public void produceToDisklessAndClassic() throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
        clientConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, "100000");
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.name());
        clientConfigs.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "10000000");
        String disklessTopicName = "diskless-test-topic";
        String classicTopicName = "classic-test-topic";
        int numRecords = 10;

        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic disklessTopic = new NewTopic(disklessTopicName, Map.of(0, List.of(0)))
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
            final NewTopic classicTopic = new NewTopic(classicTopicName, Map.of(0, List.of(0)))
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"));
            CreateTopicsResult topics = admin.createTopics(List.of(disklessTopic, classicTopic));
            topics.all().get(10, TimeUnit.SECONDS);
        }

        AtomicInteger recordsProduced = new AtomicInteger();
        final long now = System.currentTimeMillis();
        Callback produceCb = (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send record", exception);
            } else {
                log.info("Committed value for topic {} at offset {} at {}", metadata.topic(), metadata.offset(), now);
                recordsProduced.incrementAndGet();
            }
        };
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(clientConfigs)) {
            for (int i = 0; i < numRecords; i++) {
                byte[] value = new byte[10000];
                final ProducerRecord<byte[], byte[]> disklessRecord = new ProducerRecord<>(disklessTopicName, 0, now, null, value);
                producer.send(disklessRecord, produceCb);
                final ProducerRecord<byte[], byte[]> classicRecord = new ProducerRecord<>(classicTopicName, 0, now, null, value);
                producer.send(classicRecord, produceCb);
            }
            producer.flush();
        }

        assertEquals(numRecords * 2, recordsProduced.get());

        int recordsConsumed = 0;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(clientConfigs)) {
            consumer.assign(List.of(new TopicPartition(classicTopicName, 0), new TopicPartition(disklessTopicName, 0)));
            for (int i = 0; i < 3; i++) {
                ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(1));
                recordsConsumed += poll.count();
            }
        }

        assertEquals(recordsProduced.get(), recordsConsumed);
    }

    private static void consumeWithManualAssignment(TimestampType timestampType, Map<String, Object> clientConfigs, String topicName, long now, int numRecords) {
        int recordsConsumed;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(clientConfigs)) {
            consumer.assign(Collections.singletonList(new TopicPartition(topicName, 0)));
            recordsConsumed = poll(consumer, timestampType, now);
        }
        assertEquals(numRecords, recordsConsumed);
    }

    private static void consumeWithSubscription(TimestampType timestampType, Map<String, Object> clientConfigs, String topicName, long now, int numRecords) {
        final Map<String, Object> consumerConfigs = new HashMap<>(clientConfigs);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, java.util.UUID.randomUUID().toString());
        int recordsConsumed;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(Collections.singletonList(topicName));
            recordsConsumed = poll(consumer, timestampType, now);
        }
        assertEquals(numRecords, recordsConsumed);
    }

    private static int poll(Consumer<byte[], byte[]> consumer, TimestampType timestampType, long now) {
        int recordsConsumed = 0;
        ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(30));
        for (ConsumerRecord<byte[], byte[]> record : poll) {
            log.info("Received record {} at {}", recordsConsumed, record.timestamp());
            switch (timestampType) {
                case CREATE_TIME -> assertEquals(now, record.timestamp());
                case LOG_APPEND_TIME -> assertTrue(record.timestamp() > now);
            }
            recordsConsumed++;
        }
        return recordsConsumed;
    }
}
