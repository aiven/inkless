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
package io.aiven.inkless.control_plane;

import java.time.Instant;
import java.util.List;

import io.aiven.inkless.common.ObjectFormat;

public record FileMergeWorkItem(long workItemId,
                                Instant createdAt,
                                List<File> files) {

    public record File(long fileId,
                       String objectKey,
                       ObjectFormat format,
                       long size,
                       long usedSize,
                       List<BatchInfo> batches) {
    }
}
