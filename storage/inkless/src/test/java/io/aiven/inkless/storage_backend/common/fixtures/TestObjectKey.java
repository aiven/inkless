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
package io.aiven.inkless.storage_backend.common.fixtures;

import java.util.Objects;

import io.aiven.inkless.common.ObjectKey;

public class TestObjectKey implements ObjectKey {
    private final String key;

    public TestObjectKey(final String key) {
        this.key = Objects.requireNonNull(key, "key cannot be null");
    }

    @Override
    public String value() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }
}
