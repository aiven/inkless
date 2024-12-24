// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.storage.internals.log.BatchMetadata;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FindProducerStateRequest;
import io.aiven.inkless.control_plane.FindProducerStateResponse;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

public class FindProducerStateJobTest extends SharedPostgreSQLTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456;

    static final String TOPIC_0 = "topic0";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, new TopicPartition(TOPIC_0, 0));

    final Time time = new MockTime();

    @BeforeEach
    void createTopics() {
        new TopicsAndPartitionsCreateJob(
            Time.SYSTEM,
            hikariDataSource,
            Set.of(
                new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2)
            ),
            durationMs -> {}
        ).run();
    }

    @Test
    void noRequests() {
        final FindProducerStateJob job = new FindProducerStateJob(time, hikariDataSource, List.of(), Instant.ofEpochMilli(1), durationMs -> {});
        assertThat(job.call()).isEmpty();
    }

    @Test
    void singleEntry() {
        final long producerId = 1L;
        final CommitFileJob commitJob = new CommitFileJob(
            time, hikariDataSource, "obj1", BROKER_ID, FILE_SIZE,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 0, 1234, 0, 11, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 0, 11)
                )
            ),
            durationMs -> {
            }
        );
        assertThat(commitJob.call()).isNotEmpty();

        final List<FindProducerStateRequest> requests = List.of(
            new FindProducerStateRequest(T0P0, producerId, (short) 3)
        );
        final FindProducerStateJob job = new FindProducerStateJob(time, hikariDataSource, requests, Instant.ofEpochMilli(1), durationMs -> {});
        final var responses0 = job.call();
        final BatchMetadata firstMeta = new BatchMetadata(11, 11, 11, time.milliseconds());
        assertThat(responses0)
            .isNotEmpty()
            .containsExactly(
                new FindProducerStateResponse(T0P0.topicPartition(), 1, (short) 3, List.of(firstMeta))
            )
            .flatExtracting(FindProducerStateResponse::batches)
            .hasSize(1);
    }

    @Test
    void moreThanFiveEntries() {
        final long producerId = 1L;
        final CommitFileJob commitJob0 = new CommitFileJob(
            time, hikariDataSource, "obj1", BROKER_ID, FILE_SIZE,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 0, 1234, 0, 11, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 0, 11)
                )
            ),
            durationMs -> {
            }
        );
        assertThat(commitJob0.call()).isNotEmpty();

        final List<FindProducerStateRequest> requests0 = List.of(
            new FindProducerStateRequest(T0P0, producerId, (short) 3)
        );
        final FindProducerStateJob job = new FindProducerStateJob(time, hikariDataSource, requests0, Instant.ofEpochMilli(1), durationMs -> {});
        final var responses = job.call();
        final BatchMetadata firstMeta = new BatchMetadata(11, 11, 11, time.milliseconds());
        assertThat(responses)
            .isNotEmpty()
            .containsExactly(
                new FindProducerStateResponse(T0P0.topicPartition(), 1, (short) 3, List.of(firstMeta))
            )
            .flatExtracting(FindProducerStateResponse::batches)
            .hasSize(1);

        // commit 5 more batches
        final CommitFileJob commitJob1 = new CommitFileJob(
            time, hikariDataSource, "obj2", BROKER_ID, FILE_SIZE,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 1234, 1239, 12, 17, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 12, 17)
                ),
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 1240, 1249, 18, 27, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 18, 27)
                ),
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 1250, 1259, 28, 37, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 28, 37)
                ),
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 1260, 1269, 38, 47, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 38, 47)
                ),
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 1270, 1279, 48, 57, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 48, 57)
                )
            ),
            durationMs -> {
            }
        );
        assertThat(commitJob1.call()).isNotEmpty();

        final List<FindProducerStateRequest> requests1 = List.of(
            new FindProducerStateRequest(T0P0, producerId, (short) 3)
        );
        final FindProducerStateJob job1 = new FindProducerStateJob(time, hikariDataSource, requests1, Instant.ofEpochMilli(1), durationMs -> {});
        final var responses1 = job1.call();
        assertThat(responses1)
            .isNotEmpty()
            .flatExtracting(FindProducerStateResponse::batches)
            .hasSize(5)
            .doesNotContain(firstMeta);
    }

    @Test
    void noEntriesFound() {
        // Given a producerId that has committed batches
        final long producerId = 1L;
        final CommitFileJob commitJob = new CommitFileJob(
            time, hikariDataSource, "obj1", BROKER_ID, FILE_SIZE,
            List.of(
                new CommitFileJob.CommitBatchRequestJson(
                    CommitBatchRequest.idempotent(T0P0, 0, 1234, 0, 11, time.milliseconds(), TimestampType.CREATE_TIME, producerId, (short) 3, 0, 11)
                )
            ),
            durationMs -> {
            }
        );
        assertThat(commitJob.call()).isNotEmpty();

        // When querying for a different producerId
        final List<FindProducerStateRequest> requests0 = List.of(
            new FindProducerStateRequest(T0P0, producerId + 1, (short) 3)
        );
        final FindProducerStateJob job0 = new FindProducerStateJob(time, hikariDataSource, requests0, TimeUtils.now(time), durationMs -> {});
        final var responses0 = job0.call();
        // Then no entries should be found
        assertThat(responses0).flatExtracting(FindProducerStateResponse::batches).isEmpty();

        // When querying for a different epoch
        final List<FindProducerStateRequest> requests1 = List.of(
            new FindProducerStateRequest(T0P0, producerId, (short) 2)
        );
        final FindProducerStateJob job1 = new FindProducerStateJob(time, hikariDataSource, requests1, TimeUtils.now(time), durationMs -> {});
        final var responses1 = job1.call();
        // Then no entries should be found
        assertThat(responses1).flatExtracting(FindProducerStateResponse::batches).isEmpty();

        // When querying for a time outside the expiration period
        time.sleep(60_000);
        final List<FindProducerStateRequest> requests2 = List.of(
            new FindProducerStateRequest(T0P0, producerId, (short) 3)
        );
        final FindProducerStateJob job2 = new FindProducerStateJob(time, hikariDataSource, requests2, TimeUtils.now(time), durationMs -> {});
        final var responses2 = job2.call();
        // Then no entries should be found
        assertThat(responses2).flatExtracting(FindProducerStateResponse::batches).isEmpty();
    }
}
