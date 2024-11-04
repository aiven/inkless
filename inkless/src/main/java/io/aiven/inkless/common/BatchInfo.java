package io.aiven.inkless.common;

import java.util.Objects;

public class BatchInfo {
    // Logical coordinates.
    public final BatchCoordinates coordinates;

    // Physical coordinates.
    public final String filePath;
    public final int byteOffset;
    public final int byteSize;

    // Other metadata
    public final long numberOfRecords;

    public BatchInfo(final BatchCoordinates coordinates,
                     final String filePath,
                     final int byteOffset,
                     final int byteSize,
                     final long numberOfRecords) {
        this.coordinates = coordinates;
        this.filePath = filePath;
        this.byteOffset = byteOffset;
        this.byteSize = byteSize;
        this.numberOfRecords = numberOfRecords;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final BatchInfo batchInfo = (BatchInfo) o;
        return byteOffset == batchInfo.byteOffset
            && byteSize == batchInfo.byteSize
            && numberOfRecords == batchInfo.numberOfRecords
            && Objects.equals(coordinates, batchInfo.coordinates)
            && Objects.equals(filePath, batchInfo.filePath);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + coordinates.hashCode();
        result = prime * result + filePath.hashCode();
        result = prime * result + Integer.hashCode(byteOffset);
        result = prime * result + Integer.hashCode(byteSize);
        result = prime * result + Long.hashCode(numberOfRecords);
        return result;
    }

    @Override
    public String toString() {
        return "BatchInfo["
            + "coordinates=" + coordinates
            + ", filePath=" + filePath
            + ", byteOffset=" + byteOffset
            + ", byteSize=" + byteSize
            + ", numberOfRecords=" + numberOfRecords
            + "]";
    }
}
