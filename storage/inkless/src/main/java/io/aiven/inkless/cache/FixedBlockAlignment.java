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
package io.aiven.inkless.cache;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;

/**
 * Strategy which breaks files into blocks of a specified fixed size for caching and fetching.
 */
public class FixedBlockAlignment implements KeyAlignmentStrategy {

    private final int blockSize;

    public FixedBlockAlignment(int blockSize) {
        this.blockSize = blockSize;
    }

    @Override
    public Set<ByteRange> align(List<ByteRange> ranges) {
        if (ranges == null) {
            return Collections.emptySet();
        }
        HashSet<ByteRange> keys = new HashSet<>();
        for (ByteRange requestRange : ranges) {
            // Rely on integer division to map bytes to blocks.
            long firstBlock = requestRange.offset() / blockSize;
            long lastBlock = requestRange.endOffset() / blockSize;
            // Iterate over the set of blocks which cover the requested range.
            for (long blockIndex = firstBlock; blockIndex <= lastBlock; blockIndex++) {
                keys.add(new ByteRange(blockSize * blockIndex, blockSize));
            }
        }
        return Collections.unmodifiableSet(keys);
    }
}
