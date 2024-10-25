package kafka.server.inkless_common;

public class FindBatchResponse {
    public final String filePath;
    public final int byteOffset;
    public final int byteSize;
    public final long batchBaseOffset;
    public final long numberOfRecords;

    public FindBatchResponse(final String filePath,
                             final int byteOffset, final int byteSize,
                             final long batchBaseOffset, final long numberOfRecords) {
        this.filePath = filePath;
        this.byteOffset = byteOffset;
        this.byteSize = byteSize;
        this.batchBaseOffset = batchBaseOffset;
        this.numberOfRecords = numberOfRecords;
    }

    @Override
    public String toString() {
        return "FindBatchResponse["
            + "filePath=" + this.filePath
            + ", byteOffset=" + this.byteOffset
            + ", byteSize=" + this.byteSize
            + ", batchBaseOffset=" + this.batchBaseOffset
            + ", numberOfRecords=" + this.numberOfRecords
            + "]";
    }
}
