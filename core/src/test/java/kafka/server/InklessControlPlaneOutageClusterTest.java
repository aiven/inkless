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
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.KafkaStorageException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.KafkaClusterTestKit;
import org.apache.kafka.common.test.TestKitNodes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import io.aiven.inkless.config.InklessConfig;
import io.aiven.inkless.control_plane.ControlPlaneAvailability;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Cluster coverage for taking the control plane out of service at runtime. Emptying the connection
 * string at runtime makes a diskless produce fail with {@code KAFKA_STORAGE_ERROR}
 * without uploading an object. Restoring the connection string brings produce back without a
 * restart.
 */
@Testcontainers
public class InklessControlPlaneOutageClusterTest {
    private static final String OBJECT_KEY_PREFIX = "control-plane-outage-wal";
    private static final String TOPIC_NAME = "control-plane-outage-topic";
    private static final String CONNECTION_STRING_KEY = InklessConfig.PREFIX
        + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.CONNECTION_STRING_CONFIG;

    @Container
    protected static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();
    @Container
    protected static MinioContainer s3Container = S3TestContainer.minio();

    private KafkaClusterTestKit cluster;

    @BeforeEach
    public void setup(final TestInfo testInfo) throws Exception {
        s3Container.createBucket(testInfo);
        pgContainer.createDatabase(testInfo);

        final TestKitNodes nodes = new TestKitNodes.Builder()
            .setCombined(false)
            .setNumBrokerNodes(1)
            .setNumControllerNodes(1)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_CLASS_CONFIG, PostgresControlPlane.class.getName())
            .setConfigProp(CONNECTION_STRING_KEY, pgContainer.getJdbcUrl())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.USERNAME_CONFIG, PostgreSQLTestContainer.USERNAME)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX + PostgresControlPlaneConfig.PASSWORD_CONFIG, PostgreSQLTestContainer.PASSWORD)
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_BACKEND_CLASS_CONFIG, S3Storage.class.getName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_BUCKET_NAME_CONFIG, s3Container.getBucketName())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_REGION_CONFIG, s3Container.getRegion())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_ENDPOINT_URL_CONFIG, s3Container.getEndpoint())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.S3_PATH_STYLE_ENABLED_CONFIG, "true")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_ACCESS_KEY_ID_CONFIG, s3Container.getAccessKey())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.STORAGE_PREFIX + S3StorageConfig.AWS_SECRET_ACCESS_KEY_CONFIG, s3Container.getSecretKey())
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.OBJECT_KEY_PREFIX_CONFIG, OBJECT_KEY_PREFIX)
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
    @Timeout(value = 300, unit = TimeUnit.SECONDS)
    void produceFailsWhileConnectionStringIsEmptyAndRecoversWhenRestored() throws Exception {
        final BrokerServer broker = cluster.brokers().values().iterator().next();
        // The server accepts the connection string only on the default broker resource.
        final ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "");
        final ControlPlaneAvailability availability = broker.sharedServer().inklessControlPlaneAvailability().get();

        try (Admin admin = Admin.create(Map.of(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()));
             S3Client s3 = s3Container.getS3Client()) {
            admin.createTopics(List.of(new NewTopic(TOPIC_NAME, 1, (short) 1)
                .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true")))).all().get(30, TimeUnit.SECONDS);
            TestUtils.waitForCondition(availability::isAvailable, 30_000, "Control plane must be available after startup");

            // Produce succeeds while the control plane is configured.
            produceOnce();
            final long objectsAfterFirstProduce = countObjects(s3);
            assertEquals(1, objectsAfterFirstProduce, "The first produce must upload one WAL object");

            // Emptying the connection string takes the control plane out of service.
            setConnectionString(admin, brokerResource, "");
            TestUtils.waitForCondition(() -> !availability.isAvailable(), 30_000,
                "Control plane must go out of service after the connection string is emptied");

            final ExecutionException failure = assertThrows(ExecutionException.class, this::produceOnce);
            assertInstanceOf(KafkaStorageException.class, failure.getCause());
            assertEquals(objectsAfterFirstProduce, countObjects(s3),
                "A rejected produce must not leave an object behind");

            // Restoring the connection string brings produce back without a restart.
            setConnectionString(admin, brokerResource, pgContainer.getJdbcUrl());
            TestUtils.waitForCondition(() -> {
                try {
                    produceOnce();
                    return true;
                } catch (final ExecutionException e) {
                    return false;
                }
            }, 60_000, "Produce must succeed again after the connection string is restored");
            assertEquals(objectsAfterFirstProduce + 1, countObjects(s3));
        }
    }

    private void setConnectionString(final Admin admin, final ConfigResource resource, final String value) throws Exception {
        final Map<ConfigResource, Collection<AlterConfigOp>> ops = Map.of(resource, List.of(
            new AlterConfigOp(new ConfigEntry(CONNECTION_STRING_KEY, value), AlterConfigOp.OpType.SET)));
        admin.incrementalAlterConfigs(ops).all().get(30, TimeUnit.SECONDS);
    }

    /**
     * Sends one record with a producer that doesn't retry, because {@code KAFKA_STORAGE_ERROR} is
     * retriable and a retrying producer would hide it until the delivery timeout.
     */
    private void produceOnce() throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {
        final Map<String, Object> configs = Map.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "false",
            ProducerConfig.RETRIES_CONFIG, "0",
            ProducerConfig.ACKS_CONFIG, "1",
            ProducerConfig.LINGER_MS_CONFIG, "0");
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(configs)) {
            producer.send(new ProducerRecord<>(TOPIC_NAME, 0, null, "value")).get(30, TimeUnit.SECONDS);
        }
    }

    private long countObjects(final S3Client s3) {
        return s3.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(s3Container.getBucketName())
                .prefix(OBJECT_KEY_PREFIX)
                .build())
            .contents().stream().count();
    }
}
