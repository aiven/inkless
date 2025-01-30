// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.Time;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;

class FindBatchesJobTest extends SharedPostgreSQLTest {
    static final int BROKER_ID = 11;
    static final long FILE_SIZE = 123456;

    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final TopicIdPartition T0P0 = new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0);

    Time time = new MockTime();

    @BeforeEach
    void createTopics() {
        final Set<CreateTopicAndPartitionsRequest> createTopicAndPartitionsRequests = Set.of(
            new CreateTopicAndPartitionsRequest(TOPIC_ID_0, TOPIC_0, 2),
            new CreateTopicAndPartitionsRequest(TOPIC_ID_1, TOPIC_1, 1)
        );
        new TopicsAndPartitionsCreateJob(Time.SYSTEM, jooqCtx, createTopicAndPartitionsRequests, duration -> {}).run();
    }

    @Test
    void simpleFind() {
        final String objectKey1 = "obj1";

        final CommitFileJob commitJob = new CommitFileJob(
            time, jooqCtx, objectKey1, BROKER_ID, FILE_SIZE,
            List.of(
                CommitBatchRequest.of(T0P0, 0, 1234, 0, 11, 1000, TimestampType.CREATE_TIME)
            ),
            duration -> {}
        );
        assertThat(commitJob.call()).isNotEmpty();

        final FindBatchesJob job = new FindBatchesJob(
            time, jooqCtx,
            List.of(
                // This will produce a normal find result with some batches.
                new FindBatchRequest(new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0), 0, 1000),
                // This will be on the border, offset matching HWM, will produce no bathes, but still a successful result.
                new FindBatchRequest(new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0), 0, 1000),
                // This will result in the out-of-range error.
                new FindBatchRequest(new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1), 10, 1000)
            ),
            true, 2000,
            duration -> {}, duration -> {});
        final List<FindBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(1L, objectKey1, BatchMetadata.of(T0P0, 0, 1234, 0, 11, time.milliseconds(), 1000, TimestampType.CREATE_TIME))
            ), 0, 12),
            new FindBatchResponse(Errors.NONE, List.of(), 0, 0),
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 0)
        );
    }
}
