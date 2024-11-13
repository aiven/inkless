// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.List;

import io.aiven.inkless.control_plane.CommitBatchRequest;

/**
 * The result of closing a batch buffer.
 *
 * @param commitBatchRequests commit batch requests matching in order the batches in {@code data}.
 * @param requestIds          produce request IDs matching in order the batches in {@code data}.
 */
record BatchBufferCloseResult(List<CommitBatchRequest> commitBatchRequests,
                              List<Integer> requestIds,
                              byte[] data) {
}
