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

import java.io.IOException;
import org.apache.kafka.common.utils.Utils;

import java.io.InputStream;
import java.util.function.Supplier;

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

    void skipNBytes(final long offset) throws IOException {
        if (source == null) {
            throw new IllegalStateException("Stream is closed");
        }
        this.source.skipNBytes(offset);
        this.position += offset;
    }

    int read(byte[] b, int off, int len) throws IOException {
        if (source == null) {
            throw new IllegalStateException("Stream is closed");
        }
        var bytesRead = this.source.read(b, off, len);
        this.position += bytesRead;
        return bytesRead;
    }

    public void open() {
        if (source == null) {
            source = inputStreamSupplier.get();
        }
    }

    public boolean closeIfFullyRead() throws IOException {
        if (position >= size) {
            close();
            return true;
        }
        return false;
    }

    public void close() throws IOException {
        if (source != null) {
            source.close();
        }
        source = null;
    }
}