package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchMetadataTest {

    static final TopicIdPartition TOPIC_ID_PARTITION = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));

    @Test
    void testOffsets() {
        final var batchMetadata = new BatchMetadata(
            TOPIC_ID_PARTITION,
            0,
            10,
            1,
            11,
            0,
            0,
            TimestampType.CREATE_TIME,
            -1,
            (short) -1,
            -1,
            -1
        );
        assertThat(batchMetadata.range().size()).isEqualTo(10);
    }

    @Test
    void invalidRequestOffsets() {
        assertThatThrownBy(() -> new BatchMetadata(
            TOPIC_ID_PARTITION,
            0,
            10,
            10,
            0,
            0,
            0,
            TimestampType.CREATE_TIME,
            -1,
            (short) -1,
            -1,
            -1
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid record offsets, last cannot be less than base: base=10, last=0");
    }
}
