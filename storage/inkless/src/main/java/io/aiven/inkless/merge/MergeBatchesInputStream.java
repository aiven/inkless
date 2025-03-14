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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

public class MergeBatchesInputStream extends InputStream {

    private final List<BatchAndStream> batchAndStreams;
    private boolean closed = false;
    private int currentStream = 0;

    public MergeBatchesInputStream(List<BatchAndStream> streams) {
        if (streams == null || streams.isEmpty()) {
            throw new IllegalArgumentException("streams cannot be null or empty");
        }
        this.batchAndStreams = new ArrayList<>(streams);
    }

    @Override
    public int read() throws IOException {
        byte[] b = new byte[1];
        int bytesRead = read(b, 0, 1);
        if (bytesRead == -1) {
            return -1;
        } else {
            return b[0] & 0xFF;
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (currentStream >= batchAndStreams.size()) {
            return -1;
        }
        BatchAndStream stream;
        int totalRead = 0;
        int read;
        int offset = off;
        int nBytesToRead;
        do {
            stream = batchAndStreams.get(currentStream);
            nBytesToRead = Math.min(len, len - totalRead);
            read = stream.read(b, offset, nBytesToRead);
            if (read == -1) {
                currentStream += 1;
                if (currentStream == batchAndStreams.size()) {
                    if (totalRead == 0) {
                        return -1;
                    } else {
                        return totalRead;
                    }
                }
            } else {
                totalRead += read;
                offset += read;
            }
        } while (totalRead < len);

        return totalRead;
    }

    public void close() throws IOException {
        if (closed) { return; }
        for (var stream : batchAndStreams) {
            stream.forceClose();
        }
        closed = true;
    }
}
