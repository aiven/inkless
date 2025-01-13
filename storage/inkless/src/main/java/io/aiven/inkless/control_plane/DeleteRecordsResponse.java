// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;

import static org.apache.kafka.common.requests.DeleteRecordsResponse.INVALID_LOW_WATERMARK;

public record DeleteRecordsResponse(Errors errors,
                                    long lowWatermark) {
    public static DeleteRecordsResponse success(final long lowWatermark) {
        return new DeleteRecordsResponse(Errors.NONE, lowWatermark);
    }

    public static DeleteRecordsResponse unknownTopicOrPartition() {
        return new DeleteRecordsResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, INVALID_LOW_WATERMARK);
    }

    public static DeleteRecordsResponse offsetOutOfRange() {
        return new DeleteRecordsResponse(Errors.OFFSET_OUT_OF_RANGE, INVALID_LOW_WATERMARK);
    }
}
