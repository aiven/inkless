/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.merge;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.MergedFileBatch;

public class MergeBatchesInputStream extends InputStream {

    public record MergeMetadata(List<MergedFileBatch> mergedFileBatch, long mergedFileSize) {}

    private final List<BatchAndStream> batchAndStreams;
    private boolean closed = false;
    private int currentStream = 0;


    public static class Builder {
        private final List<BatchAndStream> batchAndStreams;

        public Builder() {
            this.batchAndStreams = new ArrayList<>();
        }

        public Builder addBatch(final BatchAndStream batchAndStream) {
            this.batchAndStreams.add(batchAndStream);
            return this;
        }

        public MergeBatchesInputStream build() {
            batchAndStreams.sort(BatchAndStream.TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR);
            return new MergeBatchesInputStream(batchAndStreams);
        }

        public MergeMetadata mergeMetadata() {
            batchAndStreams.sort(BatchAndStream.TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR);
            final List<MergedFileBatch> mergedFileBatches = new ArrayList<>();
            long fileSize = 0;
            for (final BatchAndStream bf : batchAndStreams) {
                var batchSize = bf.batchLength();
                mergedFileBatches.add(new MergedFileBatch(
                        new BatchMetadata(
                                bf.parentBatch().metadata().magic(),
                                bf.parentBatch().metadata().topicIdPartition(),
                                fileSize,
                                batchSize,
                                bf.parentBatch().metadata().baseOffset(),
                                bf.parentBatch().metadata().lastOffset(),
                                bf.parentBatch().metadata().logAppendTimestamp(),
                                bf.parentBatch().metadata().batchMaxTimestamp(),
                                bf.parentBatch().metadata().timestampType()
                        ),
                        List.of(bf.parentBatch().batchId())
                ));
                fileSize += batchSize;
            }
            return new MergeMetadata(mergedFileBatches, fileSize);
        }
    }

    private MergeBatchesInputStream(List<BatchAndStream> batchAndStreams) {
        this.batchAndStreams = batchAndStreams;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (currentStream >= batchAndStreams.size() || closed) {
            return -1;
        }
        BatchAndStream stream;
        int totalRead = 0;
        int read;
        int offset = off;
        int nBytesToRead;
        do {
            stream = batchAndStreams.get(currentStream);
            nBytesToRead = Math.min(len, len - totalRead);
            read = stream.read(b, offset, nBytesToRead);
            if (read == -1) {
                currentStream += 1;
                if (currentStream == batchAndStreams.size()) {
                    if (totalRead == 0) {
                        return -1;
                    } else {
                        return totalRead;
                    }
                }
            } else {
                totalRead += read;
                offset += read;
            }
        } while (totalRead < len);

        return totalRead;
    }

    @Override
    public int read() throws IOException {
        throw new UnsupportedOperationException("Reading a single byte is not supported");
    }

    public void close() throws IOException {
        if (closed) { return; }
        for (var stream : batchAndStreams) {
            stream.close();
        }
        closed = true;
    }
}
