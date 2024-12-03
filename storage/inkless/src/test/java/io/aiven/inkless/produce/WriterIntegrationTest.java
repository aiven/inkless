// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.storage.internals.log.LogConfig;
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
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import io.aiven.inkless.common.PlainObjectKey;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.control_plane.TopicMetadataChangesSubscriber;
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

    static final Uuid TOPIC_ID_0 = new Uuid(0, 1);
    static final Uuid TOPIC_ID_1 = new Uuid(0, 2);
    static final TopicPartition T0P0 = new TopicPartition("topic0", 0);
    static final TopicPartition T0P1 = new TopicPartition("topic0", 1);
    static final TopicPartition T1P0 = new TopicPartition("topic1", 0);

    static final String BUCKET_NAME = "test-bucket";

    static final MetadataView METADATA_VIEW = new MetadataView() {
        @Override
        public Set<TopicPartition> getTopicPartitions(final String topicName) {
            return Set.of(T0P0, T0P1, T1P0);
        }

        @Override
        public Uuid getTopicId(final String topicName) {
            if (topicName.equals(T0P0.topic())) {
                return TOPIC_ID_0;
            } else if (topicName.equals(T1P0.topic())) {
                return TOPIC_ID_1;
            } else {
                return null;
            }
        }

        @Override
        public boolean isInklessTopic(final String topicName) {
            return true;
        }

        @Override
        public LogConfig getTopicConfig(final String topicName) {
            return LogConfig.fromProps(Map.of(), new Properties());
        }

        @Override
        public void subscribeToTopicMetadataChanges(final TopicMetadataChangesSubscriber subscriber) {
            // We don't create/delete topics/partitions, so this is no-op.
        }
    };

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
        final InMemoryControlPlane controlPlane = new InMemoryControlPlane(time, METADATA_VIEW);

        final var delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(T0P0.topic()).setTopicId(TOPIC_ID_0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_0).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_0).setPartitionId(1));
        delta.replay(new TopicRecord().setName(T1P0.topic()).setTopicId(TOPIC_ID_1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_1).setPartitionId(0));
        controlPlane.onTopicMetadataChanges(delta.topicsDelta());

        try (
            final Writer writer = new Writer(
                time, PlainObjectKey.creator(""), storage, controlPlane, Duration.ofMillis(10),
                10 * 1024,
                1,
                Duration.ofMillis(10),
                new BrokerTopicStats()
            )
        ) {
            time.sleep(100);

            final var writeFuture1 = writer.write(Map.of(
                T0P0, recordCreator.create(T0P0, 101),
                T0P1, recordCreator.create(T0P1, 102),
                T1P0, recordCreator.create(T1P0, 103)
            ));
            final var writeFuture2 = writer.write(Map.of(
                T0P0, recordCreator.create(T0P0, 11),
                T0P1, recordCreator.create(T0P1, 12),
                T1P0, recordCreator.create(T1P0, 13)
            ));
            final var ts1 = time.milliseconds();
            final var result1 = writeFuture1.get(10, TimeUnit.SECONDS);
            final var result2 = writeFuture2.get(10, TimeUnit.SECONDS);

            time.sleep(50);

            final var writeFuture3 = writer.write(Map.of(
                T1P0, recordCreator.create(T1P0, 1)
            ));
            final var ts2 = time.milliseconds();
            final var result3 = writeFuture3.get(10, TimeUnit.SECONDS);

            assertThat(result1).isEqualTo(Map.of(
                T0P0, new PartitionResponse(Errors.NONE, 0, ts1, 0),
                T0P1, new PartitionResponse(Errors.NONE, 0, ts1, 0),
                T1P0, new PartitionResponse(Errors.NONE, 0, ts1, 0)
            ));

            assertThat(result2).isEqualTo(Map.of(
                T0P0, new PartitionResponse(Errors.NONE, 101, ts1, 0),
                T0P1, new PartitionResponse(Errors.NONE, 102, ts1, 0),
                T1P0, new PartitionResponse(Errors.NONE, 103, ts1, 0)
            ));

            assertThat(result3).isEqualTo(Map.of(
                T1P0, new PartitionResponse(Errors.NONE, 103 + 13, ts2, 0)
            ));
        }
    }
}
