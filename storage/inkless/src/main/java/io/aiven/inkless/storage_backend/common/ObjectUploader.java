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
package io.aiven.inkless.storage_backend.common;

import java.io.Closeable;
import java.io.InputStream;

import io.aiven.inkless.common.ObjectKey;

public interface ObjectUploader extends Closeable {

    /**
     * Uploads an object to object storage.
     * An exception must be thrown in case the number of bytes streamed from {@code inputStream}
     * is different from {@code length}.
     * @param key                      key of the object to upload.
     * @param inputStream              data of the object that will be uploaded.
     * @param length                   length of the data that will be uploaded.
     * @throws StorageBackendException if there are errors during the upload.
     */
    void upload(ObjectKey key, InputStream inputStream, long length) throws StorageBackendException;

}
