package io.aiven.inkless.reader;

import java.nio.ByteBuffer;

import io.aiven.inkless.common.BatchCoordinates;

class BatchWithData {
    public final BatchCoordinates batchCoordinates;
    public final ByteBuffer byteBuffer;

    BatchWithData(final BatchCoordinates batchCoordinates,
                  final ByteBuffer byteBuffer) {
        this.batchCoordinates = batchCoordinates;
        this.byteBuffer = byteBuffer;
    }
}
