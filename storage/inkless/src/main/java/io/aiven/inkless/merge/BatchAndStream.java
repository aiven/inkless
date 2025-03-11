package io.aiven.inkless.merge;

import org.apache.kafka.common.TopicIdPartition;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Comparator;

import io.aiven.inkless.control_plane.BatchInfo;

public class BatchAndStream {
    final long offset;
    final long length;
    int bytesRead = 0;
    final InputStreamWithPosition inputStreamWithPosition;
    final TopicIdPartition topicIdPartition;
    final BatchInfo batchInfo;

    public BatchAndStream(BatchInfo batch,
                          InputStreamWithPosition inputStreamWithPosition) {
        this.offset = batch.metadata().byteOffset();
        this.length = batch.metadata().byteSize();
        this.batchInfo = batch;
        this.inputStreamWithPosition = inputStreamWithPosition;
        this.topicIdPartition = batch.metadata().topicIdPartition();
    }

    public long offset() {
        return offset;
    }
    public long getLength() {
        return length;
    }

    public BatchInfo getParentBatch() {
        return batchInfo;
    }
    public TopicIdPartition topicIdPartition() {
        return topicIdPartition;
    }

    public InputStreamWithPosition getInputStreamWithPosition() {
        return inputStreamWithPosition;
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
            Comparator.comparing(bf -> bf.topicIdPartition().topicId());
        final Comparator<BatchAndStream> partitionComparator =
            Comparator.comparing(bf -> bf.topicIdPartition().partition());
        final Comparator<BatchAndStream> offsetComparator =
            Comparator.comparing(bf -> bf.getParentBatch().metadata().baseOffset());
        TOPIC_ID_PARTITION_BASE_OFFSET_COMPARATOR =
            topicIdComparator.thenComparing(partitionComparator).thenComparing(offsetComparator);
    }

    public byte[] readNBytes(final int nBytesToRead) throws IOException {
        if (bytesRead + nBytesToRead > length) {
            throw new IndexOutOfBoundsException();
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
        } else if (inputStreamWithPosition.position() > offset) {
            throw new RuntimeException("Desynchronization between batches and files");
        }

        final var actualAmountOfBytesToRead = Math.toIntExact(Math.min(nBytesToRead, length - bytesRead));
        var outputBytes = inputStream.readNBytes(actualAmountOfBytesToRead);
        bytesRead += actualAmountOfBytesToRead;

        inputStreamWithPosition.advance(actualAmountOfBytesToRead);

        if (inputStreamWithPosition.closeIfFullyRead() && bytesRead != length) {
            throw new RuntimeException("Desynchronization between batches and files");
        }

        return outputBytes;
    }
}
