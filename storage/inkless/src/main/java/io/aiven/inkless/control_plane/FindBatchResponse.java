// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import java.util.List;

import org.apache.kafka.common.protocol.Errors;

public record FindBatchResponse(Errors errors,
                                List<BatchInfo> batches,
                                long highWatermark) {
    public static FindBatchResponse success(final List<BatchInfo> batches, final long highWatermark) {
        return new FindBatchResponse(Errors.NONE, batches, highWatermark);
    }

    public static FindBatchResponse offsetOutOfRange(final long highWatermark) {
        return new FindBatchResponse(Errors.OFFSET_OUT_OF_RANGE, null, highWatermark);
    }

    public static FindBatchResponse unknownTopicOrPartition() {
        return new FindBatchResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION, null, -1);
    }

    public static FindBatchResponse unknownServerError() {
        return new FindBatchResponse(Errors.UNKNOWN_SERVER_ERROR, null, -1);
    }
}
