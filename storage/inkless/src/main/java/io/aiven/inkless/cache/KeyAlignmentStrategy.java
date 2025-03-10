/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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

import java.util.List;
import java.util.Set;

import io.aiven.inkless.common.ByteRange;

/**
 * Computes a set of aligned cache keys that cover the requested byte ranges.
 * Aligned keys often equal other aligned keys, improving cache hit rates.
 */
public interface KeyAlignmentStrategy {

    Set<ByteRange> align(List<ByteRange> ranges);
}
