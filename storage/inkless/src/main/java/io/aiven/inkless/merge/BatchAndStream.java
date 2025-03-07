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
import java.io.InputStream;
import java.util.Comparator;

import io.aiven.inkless.control_plane.BatchInfo;

public class BatchAndStream {
    private final long offset;
    private final long length;
    private int bytesRead = 0;
    private final InputStreamWithPosition inputStreamWithPosition;
    private final BatchInfo batch;

    public BatchAndStream(BatchInfo batch,
                          InputStreamWithPosition inputStreamWithPosition) {
        this.offset = batch.metadata().byteOffset();
        this.length = batch.metadata().byteSize();
        this.batch = batch;
        this.inputStreamWithPosition = inputStreamWithPosition;
    }

    public long length() {
        return length;
    }

    public long offset() {
        return offset;
    }

    public BatchInfo getParentBatch() {
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
            Comparator.comparing(bf -> bf.getParentBatch().metadata().topicIdPartition().topicId());
        final Comparator<BatchAndStream> partitionComparator =
            Comparator.comparing(bf -> bf.getParentBatch().metadata().topicIdPartition().partition());
        final Comparator<BatchAndStream> offsetComparator =
            Comparator.comparing(bf -> bf.getParentBatch().metadata().baseOffset());
        TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR =
            topicIdComparator.thenComparing(partitionComparator).thenComparing(offsetComparator);
    }

    public int read(byte[] b, int off, int nBytesToRead) throws IOException {
        if (bytesRead == length) {
            return -1;
        } else if (bytesRead > length) {
            throw new RuntimeException("Desynchronization between batches and files");
        }

        // ignore auto closing of the input stream, we'll close them manually
        final InputStream inputStream = inputStreamWithPosition.inputStream();
        if (inputStreamWithPosition.position() < offset) {
            // We're facing a gap, need to fast-forward.
            final long gapSize = offset - inputStreamWithPosition.position();
            try {
                inputStream.skipNBytes(gapSize);
            } catch (final EOFException e) {
                throw new RuntimeException("Desynchronization between batches and files");
            }
            inputStreamWithPosition.advance(gapSize);
        }

        final var actualAmountOfBytesToRead = Math.toIntExact(Math.min(nBytesToRead, length - bytesRead));
        final var read = inputStream.read(b, off, actualAmountOfBytesToRead);
        bytesRead += read;

        inputStreamWithPosition.advance(read);

        if (inputStreamWithPosition.closeIfFullyRead() && bytesRead != length) {
            throw new RuntimeException("Desynchronization between batches and files");
        }

        return read;
    }

    public void forceClose() {
        inputStreamWithPosition.forceClose();
    }
}
