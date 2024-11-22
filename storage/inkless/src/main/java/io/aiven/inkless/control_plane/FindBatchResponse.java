// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.protocol.Errors;

import java.util.List;

public record FindBatchResponse(Errors errors,
                                List<BatchInfo> batches,
                                long logStartOffset,
                                long highWatermark) {
    public static final long UNKNOWN_OFFSET = -1L;

    public static FindBatchResponse success(final List<BatchInfo> batches,
                                            final long logStartOffset,
                                            final long highWatermark) {
        return new FindBatchResponse(Errors.NONE, batches, logStartOffset, highWatermark);
    }

    public static FindBatchResponse offsetOutOfRange(final long logStartOffset, final long highWatermark) {
        return new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, logStartOffset, highWatermark);
    }

    public static FindBatchResponse unknownTopicOrPartition() {
        return new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, UNKNOWN_OFFSET, UNKNOWN_OFFSET);
    }

    public static FindBatchResponse unknownServerError() {
        return new FindBatchResponse(Errors.UNKNOWN_SERVER_ERROR, null, UNKNOWN_OFFSET, UNKNOWN_OFFSET);
    }
}
