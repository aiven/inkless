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
package io.aiven.inkless.common;

import java.util.Objects;

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

    public static ByteRange union(final ByteRange a, final ByteRange b) {
        Objects.requireNonNull(a, "a cannot be null");
        Objects.requireNonNull(b, "b cannot be null");

        if (a.empty()) {
            return b;
        }
        if (b.empty()) {
            return a;
        }
        
        final long minOffset = Long.min(a.offset, b.offset);
        final long maxEndOffset = Long.max(a.offset + a.size, b.offset + b.size);
        return new ByteRange(minOffset, maxEndOffset - minOffset);
    }

    public boolean contains(ByteRange range) {
        return this.offset <= range.offset && this.endOffset() >= range.endOffset();
    }
}
