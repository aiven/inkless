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
package io.aiven.inkless.storage_backend.common;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import io.aiven.inkless.common.ObjectKey;

@CoverageIgnore
public class KeyNotFoundException extends StorageBackendException {
    public KeyNotFoundException(final StorageBackend storage, final ObjectKey key) {
        super(getMessage(storage, key));
    }

    public KeyNotFoundException(final StorageBackend storage, final ObjectKey key, final Exception e) {
        super(getMessage(storage, key), e);
    }

    private static String getMessage(final StorageBackend storage, final ObjectKey key) {
        return "Key " + key + " does not exists in storage " + storage;
    }
}
