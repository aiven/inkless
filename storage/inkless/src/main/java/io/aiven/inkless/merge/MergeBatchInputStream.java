package io.aiven.inkless.merge;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class MergeBatchInputStream {

    private final List<BatchAndStream> batchAndStreams;
    private boolean closed = false;
    private static final int DEFAULT_BUFFER_SIZE = 8192;

    public MergeBatchInputStream(List<BatchAndStream> streams) {
        if (streams == null || streams.isEmpty()) {
            throw new IllegalArgumentException("streams cannot be null or empty");
        }
        this.batchAndStreams = new ArrayList<>(streams);
    }

    public long transferTo(OutputStream out) throws IOException {
        Objects.requireNonNull(out, "out");
        long transferred = 0;

        for (final BatchAndStream bf : batchAndStreams) {
            var batchSize = bf.getLength();
            var read = bf.readNBytes((int) batchSize);
            out.write(read, 0, read.length);
            transferred += read.length;
        }

        return transferred;
    }


    public void close() throws IOException {
        if (closed) { return; }
        for (var stream : batchAndStreams) {
            stream.inputStreamWithPosition.forceClose();
        }
        closed = true;
    }
}
