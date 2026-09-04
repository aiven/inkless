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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Cluster coverage for async diskless {@code DELETE_TOPICS} (KC-349).
 *
 * <p>The Admin response returns after the controller stamps {@code logs.deleted_at}. Brokers then
 * drain batches with {@code TopicPurger} across capped cycles, and {@code FileCleaner} removes the
 * objects after {@code file.cleaner.retention.period.ms}.
 */
@Testcontainers
public class InklessDisklessTopicDeleteTest {

    private static final Logger log = LoggerFactory.getLogger(InklessDisklessTopicDeleteTest.class);

    private static final String OBJECT_KEY_PREFIX = "async-delete-wal";
    private static final String TOPIC_NAME = "async-delete-topic";
    private static final int NUM_BROKERS = 2;
    private static final int NUM_PARTITIONS = 2;
    // One record per commit so TopicPurger has more batch rows than one parallel cycle can drain.
    private static final int NUM_RECORDS = 12;
    private static final int RECORD_VALUE_SIZE = 2048;
    private static final int PRODUCE_BUFFER_MAX_BYTES = 1024;
    private static final int MAX_BATCHES_PER_CYCLE = 1;
    private static final int MIN_BATCHES_FOR_MULTI_CYCLE = MAX_BATCHES_PER_CYCLE * NUM_BROKERS + 1;

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
            .setCombined(true)
            .setNumBrokerNodes(NUM_BROKERS)
            .setNumControllerNodes(1)
            .build();
        cluster = new KafkaClusterTestKit.Builder(nodes)
            .setConfigProp(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
            .setConfigProp(ServerConfigs.DISKLESS_STORAGE_SYSTEM_ENABLE_CONFIG, "true")
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
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.OBJECT_KEY_PREFIX_CONFIG, OBJECT_KEY_PREFIX)
            // Each produce request is larger than the buffer, so the writer commits one batch per send.
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.PRODUCE_COMMIT_INTERVAL_MS_CONFIG, Integer.toString(Integer.MAX_VALUE))
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.PRODUCE_BUFFER_MAX_BYTES_CONFIG, Integer.toString(PRODUCE_BUFFER_MAX_BYTES))
            // Drain and object delete must finish inside the wait windows below. The batch-coordinate
            // cache TTL must stay at most half of file.cleaner.retention.period.ms. Cap the purger so
            // the test can observe leftover batches between cycles.
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.TOPIC_PURGER_INTERVAL_MS_CONFIG, "1000")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.TOPIC_PURGER_MAX_BATCHES_PER_CYCLE_CONFIG, Integer.toString(MAX_BATCHES_PER_CYCLE))
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.FILE_CLEANER_INTERVAL_MS_CONFIG, "1000")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.FILE_CLEANER_RETENTION_PERIOD_MS_CONFIG, "2000")
            .setConfigProp(InklessConfig.PREFIX + InklessConfig.CONSUME_BATCH_COORDINATE_CACHE_TTL_MS_CONFIG, "500")
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
    public void testDeleteTopicsReturnsBeforeBackgroundPurge() throws Exception {
        final Map<String, Object> clientConfigs = new HashMap<>();
        clientConfigs.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());

        final Uuid topicId;
        try (Admin admin = AdminClient.create(clientConfigs)) {
            topicId = createDisklessTopic(admin);
            produceRecords(clientConfigs);

            TestUtils.waitForCondition(() -> {
                ControlPlaneSnapshot snapshot = readControlPlaneSnapshot(topicId);
                return snapshot.batchCount() >= MIN_BATCHES_FOR_MULTI_CYCLE && snapshot.fileCount() > 0;
            }, 60_000, () -> "Expected at least " + MIN_BATCHES_FOR_MULTI_CYCLE
                + " committed batches and a file before delete; last seen: "
                + readControlPlaneSnapshot(topicId));

            try (S3Client s3 = s3Container.getS3Client()) {
                TestUtils.waitForCondition(() -> countObjectsWithPrefix(s3, s3Container.getBucketName(), OBJECT_KEY_PREFIX) > 0,
                    60_000,
                    () -> "Expected at least one WAL object in the Minio bucket after produce");
            }

            final ControlPlaneSnapshot beforeDelete = readControlPlaneSnapshot(topicId);
            log.info("Control plane snapshot before delete: {}", beforeDelete);
            assertTrue(beforeDelete.batchCount() >= MIN_BATCHES_FOR_MULTI_CYCLE,
                "Produce must leave more batches than one parallel purger cycle can drain, got "
                    + beforeDelete.batchCount());
            assertEquals(NUM_PARTITIONS, beforeDelete.logCount(), "Every partition must have a log row");
            assertEquals(0, beforeDelete.deletedLogCount(), "Live logs must not be stamped deleted");

            admin.deleteTopics(Collections.singletonList(TOPIC_NAME)).all().get(30, TimeUnit.SECONDS);

            final ControlPlaneSnapshot afterDelete = readControlPlaneSnapshot(topicId);
            log.info("Control plane snapshot immediately after delete: {}", afterDelete);
            if (afterDelete.logCount() > 0) {
                assertEquals(afterDelete.logCount(), afterDelete.deletedLogCount(),
                    "DELETE_TOPICS must stamp logs.deleted_at rather than drop the rows");
            }
            assertTrue(afterDelete.batchCount() >= MIN_BATCHES_FOR_MULTI_CYCLE,
                "DELETE_TOPICS must return before TopicPurger drains the batches, got "
                    + afterDelete.batchCount());

            waitUntilTopicGone(admin);
            waitUntilPurgedAcrossMultipleCycles(topicId, afterDelete.batchCount());
        }

        try (S3Client s3 = s3Container.getS3Client()) {
            final String bucket = s3Container.getBucketName();
            TestUtils.waitForCondition(() -> {
                ControlPlaneSnapshot snapshot = readControlPlaneSnapshot(topicId);
                return snapshot.fileCount() == 0
                    && countObjectsWithPrefix(s3, bucket, OBJECT_KEY_PREFIX) == 0;
            }, 90_000, () -> "FileCleaner must remove WAL objects after retention; files="
                + readControlPlaneSnapshot(topicId).fileCount()
                + " objects=" + countObjectsWithPrefix(s3, bucket, OBJECT_KEY_PREFIX));
        }
    }

    private Uuid createDisklessTopic(Admin admin) throws Exception {
        final NewTopic topic = new NewTopic(TOPIC_NAME, NUM_PARTITIONS, (short) 1)
            .configs(Map.of(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"));
        CreateTopicsResult result = admin.createTopics(Collections.singletonList(topic));
        result.all().get(30, TimeUnit.SECONDS);

        final TopicDescription[] descriptionHolder = new TopicDescription[1];
        TestUtils.waitForCondition(() -> {
            try {
                descriptionHolder[0] = admin.describeTopics(Collections.singletonList(TOPIC_NAME))
                    .allTopicNames().get(30, TimeUnit.SECONDS).get(TOPIC_NAME);
                return descriptionHolder[0].partitions().size() == NUM_PARTITIONS;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                    return false;
                }
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }, 60_000, () -> "Topic should become visible with " + NUM_PARTITIONS + " partitions");
        return descriptionHolder[0].topicId();
    }

    private void produceRecords(Map<String, Object> clientConfigs) {
        final Map<String, Object> producerConfigs = new HashMap<>(clientConfigs);
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        producerConfigs.put(ProducerConfig.LINGER_MS_CONFIG, "50");

        try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
            for (int i = 0; i < NUM_RECORDS; i++) {
                producer.send(new ProducerRecord<>(TOPIC_NAME, i % NUM_PARTITIONS, null, TestUtils.randomString(RECORD_VALUE_SIZE)))
                    .get(30, TimeUnit.SECONDS);
            }
        } catch (ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private void waitUntilTopicGone(Admin admin) throws InterruptedException {
        TestUtils.waitForCondition(() -> {
            try {
                admin.describeTopics(Collections.singletonList(TOPIC_NAME))
                    .allTopicNames().get(10, TimeUnit.SECONDS);
                return false;
            } catch (ExecutionException e) {
                return e.getCause() instanceof UnknownTopicOrPartitionException;
            } catch (TimeoutException e) {
                return false;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }, 60_000, () -> "Topic must disappear from Kafka metadata after DELETE_TOPICS");
    }

    /**
     * Polls until the control-plane rows are gone and requires a leftover batch count in between.
     * With {@code topic.purger.max.batches.per.cycle} capped, one cycle cannot finish the drain.
     */
    private void waitUntilPurgedAcrossMultipleCycles(Uuid topicId, long batchesAtDelete) throws InterruptedException {
        final List<Long> observedBatchCounts = new ArrayList<>();
        observedBatchCounts.add(batchesAtDelete);
        final AtomicReference<ControlPlaneSnapshot> last = new AtomicReference<>();
        boolean sawPartialDrain = false;
        final long deadline = System.currentTimeMillis() + 90_000;

        while (System.currentTimeMillis() < deadline) {
            ControlPlaneSnapshot snapshot = readControlPlaneSnapshot(topicId);
            last.set(snapshot);
            long batches = snapshot.batchCount();
            if (observedBatchCounts.get(observedBatchCounts.size() - 1) != batches) {
                observedBatchCounts.add(batches);
                log.info("TopicPurger progress: {}", snapshot);
            }
            if (batches > 0 && batches < batchesAtDelete) {
                sawPartialDrain = true;
            }
            if (snapshot.isFullyPurged()) {
                break;
            }
            Thread.sleep(100);
        }

        assertTrue(last.get() != null && last.get().isFullyPurged(),
            "TopicPurger must drop logs, batches, and producer_state; last seen: " + last.get()
                + "; batch counts: " + observedBatchCounts);
        assertTrue(sawPartialDrain,
            "TopicPurger must drain batches across multiple cycles (max "
                + MAX_BATCHES_PER_CYCLE + " per cycle); observed batch counts: " + observedBatchCounts);
    }

    private record ControlPlaneSnapshot(
        int logCount,
        int deletedLogCount,
        long batchCount,
        long producerStateCount,
        long fileCount
    ) {
        boolean isFullyPurged() {
            return logCount == 0 && batchCount == 0 && producerStateCount == 0;
        }
    }

    private ControlPlaneSnapshot readControlPlaneSnapshot(Uuid kafkaTopicId) {
        UUID id = toJavaUuid(kafkaTopicId);
        try (
            Connection connection = DriverManager.getConnection(
                pgContainer.getJdbcUrl(),
                PostgreSQLTestContainer.USERNAME,
                PostgreSQLTestContainer.PASSWORD)
        ) {
            int logCount = 0;
            int deletedLogCount = 0;
            try (PreparedStatement ps = connection.prepareStatement(
                "SELECT COUNT(*), COUNT(deleted_at) FROM logs WHERE topic_id = ?")) {
                ps.setObject(1, id);
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    logCount = rs.getInt(1);
                    deletedLogCount = rs.getInt(2);
                }
            }
            long batches = countByTopicId(connection, "SELECT COUNT(*) FROM batches WHERE topic_id = ?", id);
            long producerState = countByTopicId(connection, "SELECT COUNT(*) FROM producer_state WHERE topic_id = ?", id);
            long files;
            try (PreparedStatement ps = connection.prepareStatement(
                "SELECT COUNT(*) FROM files WHERE object_key LIKE ?")) {
                ps.setString(1, OBJECT_KEY_PREFIX + "%");
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    files = rs.getLong(1);
                }
            }
            return new ControlPlaneSnapshot(logCount, deletedLogCount, batches, producerState, files);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static long countByTopicId(Connection connection, String sql, UUID topicId) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setObject(1, topicId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static UUID toJavaUuid(Uuid topicId) {
        return new UUID(topicId.getMostSignificantBits(), topicId.getLeastSignificantBits());
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
