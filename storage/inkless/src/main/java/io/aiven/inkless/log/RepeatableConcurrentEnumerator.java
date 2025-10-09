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

import java.util.Enumeration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

class RepeatableConcurrentEnumerator<V> {
    private final ConcurrentHashMap<?, V> map;
    private Enumeration<V> enumeration = null;

    RepeatableConcurrentEnumerator(final ConcurrentHashMap<?, V> map) {
        this.map = Objects.requireNonNull(map, "map cannot be null");
    }

    synchronized V next() {
        if (map.isEmpty()) {
            return null;
        }

        if (enumeration == null || !enumeration.hasMoreElements()) {
            enumeration = map.elements();
        }
        if (enumeration.hasMoreElements()) {
            return enumeration.nextElement();
        } else {
            return null;
        }
    }
}
