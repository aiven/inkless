// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

public record ByteRange(long offset, long size) {
    public ByteRange {
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative, " + offset + " given");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size cannot be negative, " + size + " given");
        }
        if (offset + size < 0) {
            throw new IllegalArgumentException("size too large, overflows final offset");
        }
    }

    public boolean empty() {
        return size == 0;
    }

    public int bufferOffset() {
        if (offset > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Offset " + offset + " more than " + Integer.MAX_VALUE + " and is not usable for indexing buffers");
        }
        return (int) offset;
    }

    public int bufferSize() {
        if (size > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Size " + size + " more than " + Integer.MAX_VALUE + " and is not usable for sizing buffers");
        }
        return (int) size;
    }

    public long endOffset() {
        return offset + size - 1;
    }

    public static ByteRange maxRange() {
        return new ByteRange(0L, Long.MAX_VALUE);
    }

    public static ByteRange intersect(ByteRange a, ByteRange b) {
        if (a.empty()) return a;
        if (b.empty()) return b;
        long minOffset = Long.max(a.offset, b.offset);
        long maxOffset = Long.min(a.offset + a.size, b.offset + b.size);
        return new ByteRange(minOffset, Long.max(0, maxOffset - minOffset));
    }

    public boolean contains(ByteRange range) {
        return this.offset <= range.offset && this.endOffset() >= range.endOffset();
    }
}
