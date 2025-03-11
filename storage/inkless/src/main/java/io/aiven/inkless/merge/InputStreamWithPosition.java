package io.aiven.inkless.merge;

import org.apache.kafka.common.utils.Utils;

import java.io.InputStream;
import java.util.function.Supplier;

// One InputStream per file.
// Note that BoundedInputStream is by default unbound, we use it only for counting the current position.
public class InputStreamWithPosition {
    private final Supplier<InputStream> inputStreamSupplier;
    private final long size;
    private long position = 0;
    private InputStream source = null;

    public InputStreamWithPosition(final Supplier<InputStream> inputStreamSupplier, final long size) {
        this.inputStreamSupplier = inputStreamSupplier;
        this.size = size;
    }

    long position() {
        return position;
    }

    void advance(final long offset) {
        this.position += offset;
    }

    public InputStream inputStream() {
        if (source == null) {
            source = inputStreamSupplier.get();
        }
        return source;
    }

    public boolean closeIfFullyRead() {
        if (position >= size) {
            close();
            return true;
        }
        return false;
    }

    public void forceClose() {
        if (position < size) {
            close();
        }
    }

    private void close() {
        if (source != null) {
            Utils.closeQuietly(source, "object storage input stream");
        }
    }
}