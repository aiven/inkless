// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

public record ByteRange(int offset, int size) {
    public ByteRange {
        if (offset < 0) {
            throw new IllegalArgumentException("offset cannot be negative, " + offset + " given");
        }
        if (size < 0) {
            throw new IllegalArgumentException("size cannot be negative, " + size + " given");
        }
    }

    public boolean empty() {
        return size == 0;
    }

    public int endOffset() {
        return offset + size - 1;
    }

    public static ByteRange maxRange() {
        return new ByteRange(0, Integer.MAX_VALUE);
    }
}
