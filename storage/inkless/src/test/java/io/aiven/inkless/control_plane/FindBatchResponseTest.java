package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;


class FindBatchResponseTest {

    final TopicIdPartition topicIdPartition = new TopicIdPartition(Uuid.randomUuid(), 0, "topic");

    @Test
    void estimatedSizeReturnsZeroIfOffsetIsNotIncluded() {
        var response = new FindBatchResponse(
            Errors.NONE, List.of(batchInfo(10L, 100L, 200L)), 0L, 100L
        );
        assertEquals(0, response.estimatedByteSize(0L));
        assertEquals(0, response.estimatedByteSize(300L));
    }

    @Test
    void estimatedSizeReturnsZeroIfThereAreNoBatches() {
        var response = new FindBatchResponse(
            Errors.NONE, List.of(), 0L, 100L
        );
        assertEquals(0, response.estimatedByteSize(0L));
    }

    @Test
    void estimatedSizeReturnsMinusOneInCaseOfErrors() {
        var response = new FindBatchResponse(
            Errors.UNKNOWN_TOPIC_OR_PARTITION, List.of(), 0, 100
        );
        assertEquals(-1, response.estimatedByteSize(0));
    }

    @Test
    void estimatedSizeWithBatchesStartingFromZero() {
        var response = new FindBatchResponse(
            Errors.NONE, List.of(
                batchInfo(100, 0, 99),
                batchInfo(100, 100, 199)
            ), 0, 100
        );
        assertEquals(200, response.estimatedByteSize(0));
        assertEquals(150, response.estimatedByteSize(50));
        assertEquals(101, response.estimatedByteSize(99));
        assertEquals(47, response.estimatedByteSize(153));
        assertEquals(1, response.estimatedByteSize(199));
        assertEquals(0, response.estimatedByteSize(200));
    }

    @Test
    void estimatedSizeWithBatchesStartingFrom100() {
        var response = new FindBatchResponse(
            Errors.NONE, List.of(
            batchInfo(100, 100, 199),
            batchInfo(100, 200, 299),
            batchInfo(100, 300, 399)
        ), 0, 100
        );
        assertEquals(0, response.estimatedByteSize(99));
        assertEquals(300, response.estimatedByteSize(100));
        assertEquals(201, response.estimatedByteSize(199));
        assertEquals(150, response.estimatedByteSize(250));
        assertEquals(50, response.estimatedByteSize(350));
        assertEquals(1, response.estimatedByteSize(399));
        assertEquals(0, response.estimatedByteSize(400));
    }

    private BatchInfo batchInfo(long byteSize, long baseOffset, long lastOffset) {
        return new BatchInfo(
            0L, "object",
            BatchMetadata.of(topicIdPartition, 0L, byteSize, baseOffset, lastOffset, 0L, 0L, TimestampType.NO_TIMESTAMP_TYPE)
        );
    }
}
