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

    public long endOffset() {
        return offset + size - 1;
    }

    public static ByteRange maxRange() {
        return new ByteRange(0L, Long.MAX_VALUE);
    }

    public static ByteRange merge(ByteRange a, ByteRange b) {
        if (a.empty()) return b;
        if (b.empty()) return a;
        long minOffset = Long.min(a.offset, b.offset);
        long maxOffset = Long.max(a.offset + a.size, b.offset + b.size);
        return new ByteRange(minOffset, maxOffset - minOffset);
    }

    public boolean contains(ByteRange range) {
        return this.offset <= range.offset && this.endOffset() >= range.endOffset();
    }
}
