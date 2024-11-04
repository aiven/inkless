package io.aiven.inkless.reader;

import java.util.List;
import java.util.Objects;

import io.aiven.inkless.common.BatchInfo;

public class FileFetch {
    public final String filePath;
    public final long startOffset;
    public final long rangeSize;
    public final List<BatchInfo> batches;

    public FileFetch(final String filePath,
                     final long startOffset,
                     final long rangeSize,
                     final List<BatchInfo> batches) {
        this.filePath = filePath;
        this.startOffset = startOffset;
        this.rangeSize = rangeSize;
        this.batches = batches;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final FileFetch fileFetch = (FileFetch) o;
        return startOffset == fileFetch.startOffset
            && rangeSize == fileFetch.rangeSize
            && Objects.equals(filePath, fileFetch.filePath)
            && Objects.equals(batches, fileFetch.batches);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = prime + filePath.hashCode();
        result = prime * result + Long.hashCode(startOffset);
        result = prime * result + Long.hashCode(rangeSize);
        result = prime * result + batches.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "FileFetch["
            + "filePath=" + filePath
            + ", startOffset=" + startOffset
            + ", rangeSize=" + rangeSize
            + ", batches=" + batches
            + "]";
    }
}
