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
package io.aiven.inkless.consume;

import io.aiven.inkless.generated.FileExtent;

/**
 * Result type for file extent fetch operations that can either succeed with data or fail with an error.
 *
 * <p>This type explicitly models the success/failure states of fetching file extents, avoiding
 * the use of sentinel values (like empty FileExtent) to signal failures. This makes the code
 * more type-safe and self-documenting.
 *
 * <p>Usage in fetch pipeline:
 * <ul>
 *   <li>FetchPlanner returns CompletableFuture&lt;FileExtentResult&gt; for each object fetch</li>
 *   <li>Reader.allOfFileExtents converts exceptions to Failure instances</li>
 *   <li>FetchCompleter pattern-matches on Success/Failure to build responses</li>
 * </ul>
 */
public sealed interface FileExtentResult {
    /**
     * Successful file extent fetch with data.
     *
     * @param extent the fetched file extent containing object data
     */
    record Success(FileExtent extent) implements FileExtentResult {}

    /**
     * Failed file extent fetch with error information.
     *
     * <p>Common failure scenarios:
     * <ul>
     *   <li>RejectedExecutionException: lagging consumer executor saturated</li>
     *   <li>StorageBackendException: remote storage fetch failed</li>
     *   <li>FileFetchException: file fetch or processing failed</li>
     * </ul>
     *
     * @param error the exception that caused the fetch to fail
     */
    record Failure(Throwable error) implements FileExtentResult {}
}
