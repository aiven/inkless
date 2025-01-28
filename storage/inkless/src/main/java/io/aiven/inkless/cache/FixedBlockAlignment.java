// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
            if (firstBlock != lastBlock) {
                // TODO INK-77: For ranges which cross multiple blocks, issue multiple requests and concatenate them later.
                keys.add(requestRange);
            }
        }
        return Collections.unmodifiableSet(keys);
    }
}
