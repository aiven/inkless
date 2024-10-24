package io.aiven.cps.common;

public class FindBatchResponse {
    public final String fileName;
    public final int byteOffset;
    public final int byteSize;
    public final long batchBaseOffset;
    public final int numberOfRecords;

    public FindBatchResponse(final String fileName,
                             final int byteOffset, final int byteSize,
                             final long batchBaseOffset, final int numberOfRecords) {
        this.fileName = fileName;
        this.byteOffset = byteOffset;
        this.byteSize = byteSize;
        this.batchBaseOffset = batchBaseOffset;
        this.numberOfRecords = numberOfRecords;
    }
}
