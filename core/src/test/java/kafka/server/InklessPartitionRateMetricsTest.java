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
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ReplicationConfigs;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Checks that client traffic to a classic topic and a diskless topic shows up in the partition rate
 * metrics for bytes in and bytes out, under the matching topic type.
 */
@Testcontainers
public class InklessPartitionRateMetricsTest {
    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private static final String CLASSIC_TOPIC = "partition-rate-classic";
    private static final String DISKLESS_TOPIC = "partition-rate-diskless";
    private static final String PREFIX = "kafka.server:type=ReplicaManager,name=";

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(true)
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(ServerConfigs.DISKLESS_MANAGED_REPLICAS_ENABLE_CONFIG, "true")
            .setConfigProp(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, "1")
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
            // The test takes the snapshot itself, so the scheduled one must not run during the test.
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.PARTITION_RATE_WINDOW_MS_CONFIG, String.valueOf(Duration.ofHours(1).toMillis()))
            .build();
        cluster.format();
        cluster.startup();
        cluster.waitForReadyBrokers();

        try (Admin admin = Admin.create(clientConfigs())) {
            admin.createTopics(List.of(
                new NewTopic(CLASSIC_TOPIC, 1, (short) 1),
                new NewTopic(DISKLESS_TOPIC, 1, (short) 1).configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"))
            )).all().get(30, TimeUnit.SECONDS);
        }
    }

    @AfterEach
    public void teardown() throws Exception {
        cluster.close();
    }

    @Test
    public void clientTrafficAppearsUnderDirectionAndTopicType() throws Exception {
        final ReplicaManager replicaManager = cluster.brokers().get(0).replicaManager();

        produceAndConsumeBothTopics();
        replicaManager.partitionRateTracker().snapshot();

        for (final String direction : List.of("In", "Out")) {
            final String diskless = "topicType=diskless";
            assertEquals(1L, gauge("ActivePartitionsBytes" + direction, diskless).longValue(), direction);
            assertTrue(gauge("PartitionBytes" + direction + "RateSum", diskless).doubleValue() > 0.0, direction);
            assertEquals(1L, gauge("PartitionsByBytes" + direction + "Rate", diskless + ",le=\"+Inf\"").longValue(), direction);
        }
        final String classic = "topicType=classic";
        assertEquals(1L, gauge("ActivePartitionsBytesIn", classic).longValue());
        assertTrue(gauge("PartitionBytesInRateSum", classic).doubleValue() > 0.0);
        assertEquals(1L, gauge("ActivePartitionsBytesOut", classic + ",role=leader").longValue());
        assertEquals(0L, gauge("ActivePartitionsBytesOut", classic + ",role=follower").longValue());
        assertTrue(gauge("PartitionBytesOutRateSum", classic).doubleValue() > 0.0);

        // The hosted count excludes internal topics such as __consumer_offsets.
        assertEquals(1, gauge("HostedPartitions", "topicType=classic").intValue());
        assertEquals(1, gauge("HostedPartitions", "topicType=diskless").intValue());
    }

    private void produceAndConsumeBothTopics() throws Exception {
        final Map<String, Object> producerConfigs = clientConfigs();
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerConfigs)) {
            for (final String topic : List.of(CLASSIC_TOPIC, DISKLESS_TOPIC)) {
                producer.send(new ProducerRecord<>(topic, 0, null, null, new byte[1000])).get(30, TimeUnit.SECONDS);
            }
        }
        final Map<String, Object> consumerConfigs = clientConfigs();
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerConfigs)) {
            consumer.assign(List.of(new TopicPartition(CLASSIC_TOPIC, 0), new TopicPartition(DISKLESS_TOPIC, 0)));
            final long deadline = System.currentTimeMillis() + 30_000;
            final Set<String> seen = new HashSet<>();
            while (seen.size() < 2 && System.currentTimeMillis() < deadline) {
                consumer.poll(Duration.ofMillis(500)).forEach(r -> seen.add(r.topic()));
            }
            assertEquals(2, seen.size(), "topics read");
        }
    }

    private static Number gauge(final String name, final String tags) {
        final String mbeanName = PREFIX + name + "," + tags;
        for (final Map.Entry<MetricName, Metric> entry : KafkaYammerMetrics.defaultRegistry().allMetrics().entrySet()) {
            if (mbeanName.equals(entry.getKey().getMBeanName())) {
                final Object value = ((Gauge<?>) entry.getValue()).value();
                assertNotNull(value);
                return (Number) value;
            }
        }
        throw new AssertionError("Gauge " + mbeanName + " is not registered");
    }

    private Map<String, Object> clientConfigs() {
        final Map<String, Object> configs = new HashMap<>();
        configs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        return configs;
    }
}
