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


import java.io.EOFException;
import java.io.IOException;
import java.util.Comparator;

import io.aiven.inkless.control_plane.BatchInfo;

public class BatchAndStream {
    private final InputStreamWithPosition inputStreamWithPosition;
    private final BatchInfo batch;
    private int bytesRead = 0;

    public BatchAndStream(BatchInfo batch,
                          InputStreamWithPosition inputStreamWithPosition) {
        this.batch = batch;
        this.inputStreamWithPosition = inputStreamWithPosition;
    }

    public long batchLength() {
        return batch.metadata().byteSize();
    }

    private long batchStartOffset() {
        return batch.metadata().byteOffset();
    }

    public BatchInfo parentBatch() {
        return batch;
    }

    /**
     * The comparator that sorts batches by the following criteria (in order):
     * <ol>
     *     <li>Topic ID.</li>
     *     <li>Partition.</li>
     *     <li>Base offset.</li>
     * </ol>
     */
    static final Comparator<BatchAndStream> TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR;

    static {
        final Comparator<BatchAndStream> topicIdComparator =
            Comparator.comparing(bf -> bf.parentBatch().metadata().topicIdPartition().topicId());
        final Comparator<BatchAndStream> partitionComparator =
            Comparator.comparing(bf -> bf.parentBatch().metadata().topicIdPartition().partition());
        final Comparator<BatchAndStream> offsetComparator =
            Comparator.comparing(bf -> bf.parentBatch().metadata().baseOffset());
        TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR =
            topicIdComparator.thenComparing(partitionComparator).thenComparing(offsetComparator);
    }

    public int read(byte[] b, int off, int nBytesToRead) throws IOException {
        if (bytesRead == batchLength()) {
            return -1;
        } else if (bytesRead > batchLength()) {
            throw new RuntimeException("Desynchronization between batches and files");
        }

        inputStreamWithPosition.open();
        if (inputStreamWithPosition.position() < batchStartOffset()) {
            // We're facing a gap, need to fast-forward.
            final long gapSize = batchStartOffset() - inputStreamWithPosition.position();
            try {
                inputStreamWithPosition.skipNBytes(gapSize);
            } catch (final EOFException e) {
                throw new RuntimeException("Desynchronization between batches and files");
            }
        }

        final var actualAmountOfBytesToRead = Math.toIntExact(Math.min(nBytesToRead, batchLength() - bytesRead));
        final var read = inputStreamWithPosition.read(b, off, actualAmountOfBytesToRead);
        bytesRead += read;

        if (inputStreamWithPosition.closeIfFullyRead() && bytesRead != batchLength()) {
            throw new RuntimeException("Desynchronization between batches and files");
        }

        return read;
    }

    public void close() throws IOException {
        inputStreamWithPosition.close();
    }
}
