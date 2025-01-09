// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.log.metrics.BrokerTopicStats;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.storage_backend.s3.S3Storage;
import io.aiven.inkless.test_utils.S3TestContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
@Tag("integration")
class WriterIntegrationTest {
    @Container
    static final LocalStackContainer LOCALSTACK = S3TestContainer.container();

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);
    static final TopicIdPartition T0P1 = new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0);
    static final TopicIdPartition T1P0 = new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1);

    static final Map<String, TimestampType> TIMESTAMP_TYPES = Map.of(
        TOPIC_0, TimestampType.CREATE_TIME,
        TOPIC_1, TimestampType.LOG_APPEND_TIME
    );

    static final String BUCKET_NAME = "test-bucket";
    S3Storage storage;

    WriterTestUtils.RecordCreator recordCreator;

    @BeforeAll
    static void setupS3() {
        final var clientBuilder = S3Client.builder();
        clientBuilder.region(Region.of(LOCALSTACK.getRegion()))
            .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(
                        LOCALSTACK.getAccessKey(),
                        LOCALSTACK.getSecretKey()
                    )
                )
            );
        try (final S3Client s3Client = clientBuilder.build()) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        }
    }

    @BeforeEach
    void setup() {
        storage = new S3Storage();
        final Map<String, Object> configs = Map.of(
            "s3.bucket.name", BUCKET_NAME,
            "s3.region", LOCALSTACK.getRegion(),
            "s3.endpoint.url", LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString(),
            "aws.access.key.id", LOCALSTACK.getAccessKey(),
            "aws.secret.access.key", LOCALSTACK.getSecretKey(),
            "s3.path.style.access.enabled", true
        );
        storage.configure(configs);

        recordCreator = new WriterTestUtils.RecordCreator();
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
                time, 11, PlainObjectKey.creator(""), storage, controlPlane, Duration.ofMillis(10),
                10 * 1024,
                1,
                Duration.ofMillis(10),
                new BrokerTopicStats()
            )
        ) {
            time.sleep(100);

            final var writeFuture1 = writer.write(Map.of(
                T0P0, recordCreator.create(T0P0.topicPartition(), 101),
                T0P1, recordCreator.create(T0P1.topicPartition(), 102),
                T1P0, recordCreator.create(T1P0.topicPartition(), 103)
            ), TIMESTAMP_TYPES);
            final var writeFuture2 = writer.write(Map.of(
                T0P0, recordCreator.create(T0P0.topicPartition(), 11),
                T0P1, recordCreator.create(T0P1.topicPartition(), 12),
                T1P0, recordCreator.create(T1P0.topicPartition(), 13)
            ), TIMESTAMP_TYPES);
            final var ts1 = time.milliseconds();
            final var result1 = writeFuture1.get(10, TimeUnit.SECONDS);
            final var result2 = writeFuture2.get(10, TimeUnit.SECONDS);

            time.sleep(50);

            final var writeFuture3 = writer.write(Map.of(
                T1P0, recordCreator.create(T1P0.topicPartition(), 1)
            ), TIMESTAMP_TYPES);
            final var ts2 = time.milliseconds();
            final var result3 = writeFuture3.get(10, TimeUnit.SECONDS);

            assertThat(result1).isEqualTo(Map.of(
                T0P0.topicPartition(), new PartitionResponse(Errors.NONE, 0, ts1, 0),
                T0P1.topicPartition(), new PartitionResponse(Errors.NONE, 0, ts1, 0),
                T1P0.topicPartition(), new PartitionResponse(Errors.NONE, 0, ts1, 0)
            ));

            assertThat(result2).isEqualTo(Map.of(
                T0P0.topicPartition(), new PartitionResponse(Errors.NONE, 101, ts1, 0),
                T0P1.topicPartition(), new PartitionResponse(Errors.NONE, 102, ts1, 0),
                T1P0.topicPartition(), new PartitionResponse(Errors.NONE, 103, ts1, 0)
            ));

            assertThat(result3).isEqualTo(Map.of(
                T1P0.topicPartition(), new PartitionResponse(Errors.NONE, 103 + 13, ts2, 0)
            ));
        }
    }
}
