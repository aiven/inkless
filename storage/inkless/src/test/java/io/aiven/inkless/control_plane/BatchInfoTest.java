package io.aiven.inkless.control_plane;

import org.apache.kafka.common.record.TimestampType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BatchInfoTest {

    @ParameterizedTest
    @CsvSource({
        "0, CREATE_TIME",
        "1, LOG_APPEND_TIME",
        "-1, NO_TIMESTAMP_TYPE"
    })
    void testTimestampType(short id, TimestampType expected) {
        assertThat(BatchInfo.timestampTypeFromId(id)).isEqualTo(expected);
    }

    @Test
    void unknownTimestampType() {
        assertThatThrownBy(() -> BatchInfo.timestampTypeFromId((short) 2))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Unexpected value: 2");
    }

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