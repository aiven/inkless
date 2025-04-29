package io.aiven.inkless.merge;

import org.apache.kafka.common.utils.ByteBufferInputStream;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

public class MockInputStream extends InputStream {
    public static byte[] generateData(final int size, final String signature) {
        final byte[] signatureBytes = signature.getBytes();
        final ByteBuffer buffer = ByteBuffer.allocate(size);
        while (buffer.position() < size) {
            if (signatureBytes.length <= buffer.remaining()) {
                buffer.put(signatureBytes);
            } else {
                buffer.put(Arrays.copyOf(signatureBytes, buffer.remaining()));
            }
        }
        return buffer.array();
    }

    private boolean isBuilding = true;
    private final ByteBuffer buffer;
    private boolean wasClosed = false;
    private Integer endGap = null;  // possible gap in the very end of file
    private ByteBufferInputStream stream = null;

    MockInputStream(final int size) {
        buffer = ByteBuffer.allocate(size);
    }

    void addBatch(final byte[] data) {
        if (!isBuilding) {
            throw new IllegalStateException();
        }
        buffer.put(data);
        endGap = null;
    }

    void addGap(final int size) {
        if (!isBuilding) {
            throw new IllegalStateException();
        }
        buffer.put(new byte[size]);
        endGap = size;
    }

    void finishBuilding() {
        if (!isBuilding) {
            throw new IllegalStateException();
        }
        isBuilding = false;
        buffer.position(0);
        stream = new ByteBufferInputStream(buffer);
    }

    @Override
    public void skipNBytes(final long n) {
        if (isBuilding) {
            throw new IllegalStateException();
        }
        buffer.position(buffer.position() + (int) n);
    }

    @Override
    public int read() {
        return stream.read();
    }

    @Override
    public void close() throws IOException {
        if (isBuilding) {
            throw new IllegalStateException();
        }
        if (wasClosed) {
            throw new IllegalStateException();
        }
        if (endGap != null) {
            final int remaining = buffer.remaining();
            if (remaining >= endGap) {
                buffer.position(buffer.position() + endGap);
            }
        }
        stream.close();
        wasClosed = true;
    }

    boolean isClosed() {
        return wasClosed;
    }

    void assertClosedAndDataFullyConsumed() {
        assertThat(wasClosed).isTrue();
        assertThat(buffer.position()).isEqualTo(buffer.capacity());
    }
}