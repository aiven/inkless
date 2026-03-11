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
import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;

/**
 * Strategy that collapses all byte ranges into a single bounding range
 * from min(offset) to max(offset + size).
 *
 * <p>Used for the cold (lagging consumer) path where making one HTTP request
 * for the bounding range is cheaper than multiple requests to skip small gaps
 * of interleaved data from other partitions.
 */
public class BoundingRangeAlignment implements KeyAlignmentStrategy {

    @Override
    public Set<ByteRange> align(List<ByteRange> ranges) {
        if (ranges == null || ranges.isEmpty()) {
            return Collections.emptySet();
        }

        long minOffset = Long.MAX_VALUE;
        long maxEnd = Long.MIN_VALUE;

        for (final ByteRange range : ranges) {
            minOffset = Math.min(minOffset, range.offset());
            maxEnd = Math.max(maxEnd, range.offset() + range.size());
        }

        return Set.of(new ByteRange(minOffset, maxEnd - minOffset));
    }
}
