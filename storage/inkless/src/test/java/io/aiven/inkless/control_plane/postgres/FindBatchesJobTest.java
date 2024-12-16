// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.metadata.PartitionRecord;
import org.apache.kafka.common.metadata.TopicRecord;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;

import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.CommitBatchRequest;
import io.aiven.inkless.control_plane.FindBatchRequest;
import io.aiven.inkless.control_plane.FindBatchResponse;
import io.aiven.inkless.control_plane.MetadataView;
import io.aiven.inkless.test_utils.SharedPostgreSQLTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class FindBatchesJobTest extends SharedPostgreSQLTest {
    static final String TOPIC_0 = "topic0";
    static final String TOPIC_1 = "topic1";
    static final Uuid TOPIC_ID_0 = new Uuid(10, 12);
    static final Uuid TOPIC_ID_1 = new Uuid(555, 333);
    static final TopicPartition T0P0 = new TopicPartition(TOPIC_0, 0);
    static final TopicPartition T0P1 = new TopicPartition(TOPIC_0, 1);
    static final TopicPartition T1P0 = new TopicPartition(TOPIC_1, 0);

    @Mock
    Time time;

    @BeforeEach
    void createTopics() {
        final MetadataView metadataView = mock(MetadataView.class);
        when(metadataView.isInklessTopic(anyString())).thenReturn(true);

        final MetadataDelta delta = new MetadataDelta.Builder().setImage(MetadataImage.EMPTY).build();
        delta.replay(new TopicRecord().setName(TOPIC_0).setTopicId(TOPIC_ID_0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_0).setPartitionId(0));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_0).setPartitionId(1));
        delta.replay(new TopicRecord().setName(TOPIC_1).setTopicId(TOPIC_ID_1));
        delta.replay(new PartitionRecord().setTopicId(TOPIC_ID_1).setPartitionId(0));
        new TopicsCreateJob(Time.SYSTEM, metadataView, hikariDataSource, delta.topicsDelta().changedTopics())
            .run();
    }

    @Test
    void simpleFind() {
        final String objectKey1 = "obj1";

        when(time.milliseconds()).thenReturn(123456L);

        final CommitFileJob commitJob = new CommitFileJob(
            time, hikariDataSource, objectKey1,
            List.of(
                new CommitFileJob.CommitBatchRequestExtra(new CommitBatchRequest(T0P0, 0, 1234, 12, 1000), TOPIC_ID_0, TimestampType.CREATE_TIME)
            )
        );
        assertThat(commitJob.call()).isNotEmpty();

        final FindBatchesJob job = new FindBatchesJob(
            time, hikariDataSource,
            List.of(
                // This will produce a normal find result with some batches.
                new FindBatchRequest(new TopicIdPartition(TOPIC_ID_0, 0, TOPIC_0), 0, 1000),
                // This will be on the border, offset matching HWM, will produce no bathes, but still a successful result.
                new FindBatchRequest(new TopicIdPartition(TOPIC_ID_0, 1, TOPIC_0), 0, 1000),
                // This will result in the out-of-range error.
                new FindBatchRequest(new TopicIdPartition(TOPIC_ID_1, 0, TOPIC_1), 10, 1000)
            ),
            true, 2000);
        final List<FindBatchResponse> result = job.call();

        assertThat(result).containsExactlyInAnyOrder(
            new FindBatchResponse(Errors.NONE, List.of(
                new BatchInfo(objectKey1, 0, 1234, 0, 12, TimestampType.CREATE_TIME, 123456L, 1000)), 0, 12
            ),
            new FindBatchResponse(Errors.NONE, List.of(), 0, 0),
            new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, 0, 0)
        );
    }
}
