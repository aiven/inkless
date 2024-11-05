/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.log.remote.storage;

import org.apache.kafka.common.Uuid;

import java.util.concurrent.Future;

/**
 * An abstraction for object and cache storage
 * Implementations of this interface should be thread-safe and eventually consistent.
 */
public interface InklessStorageManager {

    /**
     * Open storage for writing. Closing the object should finalize and persist the object with the hinted durability.
     * @param objectId A unique ID for an object
     * @param hint Hints as to how the storage will be used, to allow the implementation to provide an optimized result.
     * @return A future object which can accept writes. Exceptions are passed via the future if there is a problem starting to write, such as storage is unavailable.
     */
    Future<WritableObject> write(Uuid objectId, UsageHint hint);

    /**
     * Open storage for reading.
     * @param objectId A unique ID for an object
     * @param offset Offset within the file to begin reading bytes. Equivalently, the number of bytes at the start of the
     * @param length Hint about the expected number of bytes which will be read from the object.
     * @return A future object which can accept reads. Exceptions are passed via the future if there is a problem starting to read, such as the object is missing.
     */
    Future<ReadableObject> read(Uuid objectId, long offset, long length);

    /**
     * Delete object, releasing it when no longer necessary.
     * @param objectId A unique ID for an object
     * @return A future which should succeed if the object was deleted by this operation, or does not exist. Exceptions passed via the future indicate that deletion failed and should be retried.
     */
    Future<Void> delete(Uuid objectId);
}
