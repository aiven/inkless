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
import org.apache.kafka.clients.consumer.internals.AutoOffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.record.TimestampType;
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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
public class ClassicToDisklessMigrationTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final Logger log = LoggerFactory.getLogger(ClassicToDisklessMigrationTest.class);

    private static final int LEADER_BROKER_ID = 0;
    private static final int FOLLOWER_BROKER_ID = 1;

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        // Configure broker.rack for each broker to enable fetch from follower
        Map<Integer, Map<String, String>> perServerProperties = Map.of(
            LEADER_BROKER_ID, Map.of(ServerConfigs.BROKER_RACK_CONFIG, String.valueOf(LEADER_BROKER_ID)),
            FOLLOWER_BROKER_ID, Map.of(ServerConfigs.BROKER_RACK_CONFIG, String.valueOf(FOLLOWER_BROKER_ID))
        );

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(2)
            .setNumControllerNodes(1)
            .setPerServerProperties(perServerProperties)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            // Enable fetch from follower
            .setConfigProp(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, "org.apache.kafka.common.replica.RackAwareReplicaSelector")
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

    @Test
    public void migrateClassicToDisklessTopic() throws Exception {
        Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        clientConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        clientConfigs.put(ProducerConfig.LINGER_MS_CONFIG, "1000");
        clientConfigs.put(ProducerConfig.BATCH_SIZE_CONFIG, "100000");
        clientConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        clientConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        clientConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        // by default is latest and nothing would get consumed.
        clientConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AutoOffsetResetStrategy.EARLIEST.name());
        clientConfigs.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "5000000");
        String topicName = "classic-to-diskless-topic";
        int numRecordsBeforeMigration = 250;
        int numRecordsAfterMigration = 250;
        int totalRecords = numRecordsBeforeMigration + numRecordsAfterMigration;

        // Step 1: Create topic with diskless.enable=false (classic mode) and replication factor 2
        try (Admin admin = AdminClient.create(clientConfigs)) {
            final NewTopic topic = new NewTopic(topicName, 1, (short) 2)
                .configs(Map.of(
                    TopicConfig.DISKLESS_ENABLE_CONFIG, "false",
                    TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name
                ));
            CreateTopicsResult topics = admin.createTopics(Collections.singletonList(topic));
            topics.all().get(10, TimeUnit.SECONDS);
        }

        // Step 2: Produce messages to classic topic
        final long now = System.currentTimeMillis();
        int nextOffset = produceRecords(clientConfigs, topicName, now, 0, numRecordsBeforeMigration);

        // Step 3: Consume messages from classic topic to verify it works
        consumeWithSubscription(TimestampType.CREATE_TIME, clientConfigs, topicName, now, numRecordsBeforeMigration);

        // Step 4: Migrate topic to diskless mode by changing config
        try (Admin admin = AdminClient.create(clientConfigs)) {
            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            AlterConfigOp alterConfigOp = new AlterConfigOp(
                new ConfigEntry(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"),
                AlterConfigOp.OpType.SET
            );
            admin.incrementalAlterConfigs(Map.of(topicResource, Collections.singletonList(alterConfigOp)))
                .all().get(10, TimeUnit.SECONDS);
        }

        // Step 5: Produce more messages after migration
        produceRecords(clientConfigs, topicName, now, nextOffset, numRecordsAfterMigration);

        // Step 6: Consume from the beginning to verify all messages are available
        consumeWithSubscription(TimestampType.CREATE_TIME, clientConfigs, topicName, now, totalRecords);
    }

    private static int produceRecords(Map<String, Object> clientConfigs, String topicName, long timestamp, int startOffset, int numRecords) {
        AtomicInteger recordsProduced = new AtomicInteger();
        try (Producer<byte[], byte[]> producer = new KafkaProducer<>(clientConfigs)) {
            for (int i = 0; i < numRecords; i++) {
                int offset = startOffset + i;
                byte[] value = String.valueOf(offset).getBytes();
                final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topicName, 0, timestamp, null, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send record", exception);
                    } else {
                        log.info("Committed value at offset {} at {}", metadata.offset(), timestamp);
                        recordsProduced.incrementAndGet();
                    }
                });
            }
            producer.flush();
        }
        assertEquals(numRecords, recordsProduced.get());
        return startOffset + numRecords;
    }

    private static void consumeWithSubscription(TimestampType timestampType, Map<String, Object> clientConfigs, String topicName, long now, int numRecords) {
        final Map<String, Object> consumerConfigs = new HashMap<>(clientConfigs);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, java.util.UUID.randomUUID().toString());
        // Set client.rack to match the follower broker's rack to enable fetch from follower
        consumerConfigs.put(ConsumerConfig.CLIENT_RACK_CONFIG, String.valueOf(FOLLOWER_BROKER_ID));
        int recordsConsumed;
        try (Consumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.subscribe(Collections.singletonList(topicName));
            recordsConsumed = poll(consumer, timestampType, now, numRecords);
        }
        assertEquals(numRecords, recordsConsumed);
    }

    private static int poll(Consumer<byte[], byte[]> consumer, TimestampType timestampType, long now, int expectedRecords) {
        int recordsConsumed = 0;
        long deadline = System.currentTimeMillis() + 30_000;
        while (recordsConsumed < expectedRecords && System.currentTimeMillis() < deadline) {
            ConsumerRecords<byte[], byte[]> poll = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<byte[], byte[]> record : poll) {
                log.info("Received record {} at {}", recordsConsumed, record.timestamp());
                switch (timestampType) {
                    case CREATE_TIME -> assertEquals(now, record.timestamp());
                    case LOG_APPEND_TIME -> assertTrue(record.timestamp() > now);
                }
                recordsConsumed++;
            }
        }
        return recordsConsumed;
    }
}
