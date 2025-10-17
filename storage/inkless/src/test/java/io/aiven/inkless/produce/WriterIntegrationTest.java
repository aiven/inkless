/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.server.common.RequestLocal;
import org.apache.kafka.storage.internals.log.LogConfig;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.cache.FixedBlockAlignment;
import io.aiven.inkless.cache.KeyAlignmentStrategy;
import io.aiven.inkless.cache.NullCache;
import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.MinioContainer;
import io.aiven.inkless.test_utils.S3TestContainer;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Tag("integration")
class WriterIntegrationTest {
    @Container
    static final MinioContainer S3_CONTAINER = S3TestContainer.minio();

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);
    static final KeyAlignmentStrategy KEY_ALIGNMENT_STRATEGY = new FixedBlockAlignment(Integer.MAX_VALUE);
    static final ObjectCache OBJECT_CACHE = new NullCache();

    static final Map<String, LogConfig> TOPIC_CONFIGS = Map.of(
        TOPIC_0, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.CREATE_TIME.name)),
        TOPIC_1, logConfig(Map.of(TopicConfig.MESSAGE_TIMESTAMP_TYPE_CONFIG, TimestampType.LOG_APPEND_TIME.name))
    );
    static final RequestLocal REQUEST_LOCAL = RequestLocal.noCaching();

    static LogConfig logConfig(Map<String, ?> config) {
        return new LogConfig(config);
    }

    static final String BUCKET_NAME = "test-bucket";
    S3Storage storage;

    WriterTestUtils.RecordCreator recordCreator;

    @BeforeAll
    static void setupS3() {
        S3_CONTAINER.createBucket(BUCKET_NAME);
    }

    @BeforeEach
    void setup() {
        storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", S3_CONTAINER.getRegion(),
            "s3.endpoint.url", S3_CONTAINER.getEndpoint(),
            "aws.access.key.id", S3_CONTAINER.getAccessKey(),
            "aws.secret.access.key", S3_CONTAINER.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        storage.configure(configs);

        recordCreator = new WriterTestUtils.RecordCreator();
    }

    @AfterEach
    void teardown() throws Exception {
        storage.close();
    }

    @Test
    void test() throws ExecutionException, InterruptedException, TimeoutException, IOException {
        final Time time = new MockTime();
        final InMemoryControlPlane controlPlane = new InMemoryControlPlane(time);
        controlPlane.configure(Map.of());

        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, T0P0.topic(), 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, T1P0.topic(), 1)
        );
        controlPlane.createTopicAndPartitions(createTopicAndPartitionsRequests);

        try (
            final Writer writer = new Writer(
                time, 11, ObjectKey.creator("", false), storage,
                KEY_ALIGNMENT_STRATEGY, OBJECT_CACHE,
                controlPlane, Duration.ofMillis(10),
                10 * 1024,
                1,
                Duration.ofMillis(10),
                8,
                new BrokerTopicStats()
            )
        ) {
            time.sleep(100);

            final var writeFuture1 = writer.write(Map.of(
                T0P0, recordCreator.create(T0P0.topicPartition(), 101),
                T0P1, recordCreator.create(T0P1.topicPartition(), 102),
                T1P0, recordCreator.create(T1P0.topicPartition(), 103)
            ), TOPIC_CONFIGS, REQUEST_LOCAL);
            final var writeFuture2 = writer.write(Map.of(
                T0P0, recordCreator.create(T0P0.topicPartition(), 11),
                T0P1, recordCreator.create(T0P1.topicPartition(), 12),
                T1P0, recordCreator.create(T1P0.topicPartition(), 13)
            ), TOPIC_CONFIGS, REQUEST_LOCAL);
            final var ts1 = time.milliseconds();
            final var result1 = writeFuture1.get(10, TimeUnit.SECONDS);
            final var result2 = writeFuture2.get(10, TimeUnit.SECONDS);

            time.sleep(50);

            final var writeFuture3 = writer.write(Map.of(
                T1P0, recordCreator.create(T1P0.topicPartition(), 1)
            ), TOPIC_CONFIGS, REQUEST_LOCAL);
            final var ts2 = time.milliseconds();
            final var result3 = writeFuture3.get(10, TimeUnit.SECONDS);

            assertThat(result1).isEqualTo(Map.of(
                T0P0, new PartitionResponse(Errors.NONE, 0, -1, 0),
                T0P1, new PartitionResponse(Errors.NONE, 0, -1, 0),
                T1P0, new PartitionResponse(Errors.NONE, 0, ts1, 0)
            ));

            assertThat(result2).isEqualTo(Map.of(
                T0P0, new PartitionResponse(Errors.NONE, 101, -1, 0),
                T0P1, new PartitionResponse(Errors.NONE, 102, -1, 0),
                T1P0, new PartitionResponse(Errors.NONE, 103, ts1, 0)
            ));

            assertThat(result3).isEqualTo(Map.of(
                T1P0, new PartitionResponse(Errors.NONE, 103 + 13, ts2, 0)
            ));
        }
    }
}
