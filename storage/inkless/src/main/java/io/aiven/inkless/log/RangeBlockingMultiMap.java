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
package io.aiven.inkless.log;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Keeps multiple {@link ByteRange} values under a single {@link ObjectKey}.
 */
class RangeBlockingMultiMap {
    private final Map<ObjectKey, List<ByteRange>> map = new HashMap<>();

    /**
     * Puts the range under the key.
     * @param objectKey the key to store the range under (must not be null)
     * @param range the range to store (must not be null).
     * @return whether it was the first value under the key.
     */
    synchronized boolean put(final ObjectKey objectKey, final ByteRange range) {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        Objects.requireNonNull(range, "range cannot be null");

        if (map.containsKey(objectKey)) {
            map.get(objectKey).add(range);
            return false;
        } else {
            final ArrayList<ByteRange> ranges = new ArrayList<>();
            ranges.add(range);
            map.put(objectKey, ranges);
            return true;
        }
    }

    /**
     * Removes and returns all ranges associated with the given key.
     * @param objectKey the key to remove (must not be null)
     * @return the list of ranges that were associated with the key, or null if the key was not present
     */
    synchronized List<ByteRange> remove(final ObjectKey objectKey) {
        Objects.requireNonNull(objectKey, "objectKey cannot be null");
        return map.remove(objectKey);
    }
}
