package io.aiven.inkless.control_plane;

import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchInfoTest {
    @Test
    void testOffsets() {
        BatchInfo batchInfo = BatchInfo.of(
            "objectKey",
            0,
            10,
            10,
            1,
            11,
            0,
            0,
            TimestampType.CREATE_TIME
        );
        assertThat(batchInfo.range().size()).isEqualTo(10);
        assertThat(batchInfo.offsetDelta()).isEqualTo(10);
        assertThat(batchInfo.lastOffset()).isEqualTo(20);
    }

    @Test
    void invalidRequestOffsets() {
        assertThatThrownBy(() -> BatchInfo.of(
            "objectKey",
            0,
            10,
            10,
            10,
            0,
            0,
            0,
            TimestampType.CREATE_TIME
        )).isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Invalid request offsets last cannot be less than base: base=10, last=0");
    }
}