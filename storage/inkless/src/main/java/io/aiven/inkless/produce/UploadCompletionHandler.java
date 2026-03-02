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
package io.aiven.inkless.produce;

import java.util.function.BiFunction;

import io.aiven.inkless.common.ObjectKey;

/**
 * Handler for processing upload completion results.
 *
 * <p>This interface extends {@link BiFunction} to provide a domain-specific contract
 * for handling upload completions in the async commit pipeline. Implementations
 * receive the uploaded object key (or null on failure) and any error that occurred.
 *
 * <p>Designed for use with {@code CompletableFuture.handleAsync()}:
 * <pre>{@code
 * uploadFuture.handleAsync(handler, executor);
 * }</pre>
 *
 * @param <R> the type of result produced by handling the upload completion
 */
@FunctionalInterface
public interface UploadCompletionHandler<R> extends BiFunction<ObjectKey, Throwable, R> {

    /**
     * Handles the completion of an upload operation.
     *
     * @param objectKey the key of the uploaded object, or null if the upload failed
     * @param error the error that occurred during upload, or null if successful
     * @return the result of handling the upload completion
     */
    R onUploadComplete(ObjectKey objectKey, Throwable error);

    @Override
    default R apply(ObjectKey objectKey, Throwable error) {
        return onUploadComplete(objectKey, error);
    }
}
