package io.aiven.inkless.common;

import java.util.List;

public class FindBatchResponse {
    public final List<BatchInfo> batches;
    public final long highWatermark;

    public FindBatchResponse(final List<BatchInfo> batches,
                             final long highWatermark) {
        this.batches = batches;
        this.highWatermark = highWatermark;
    }

    @Override
    public String toString() {
        return "FindBatchResponse["
            + "batches=" + this.batches
            + ", highWatermark=" + this.highWatermark
            + "]";
    }
}
