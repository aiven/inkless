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
package io.aiven.inkless.control_plane.postgres.converters;

import org.jooq.generated.enums.FileReasonT;
import org.jooq.impl.AbstractConverter;

import io.aiven.inkless.control_plane.FileReason;

public class FileReasonTToFileReasonConverter extends AbstractConverter<FileReasonT, FileReason> {
    public FileReasonTToFileReasonConverter() {
        super(FileReasonT.class, FileReason.class);
    }

    @Override
    public FileReason from(final FileReasonT databaseObject) {
        if (databaseObject == null) {
            return null;
        }
        return FileReason.fromName(databaseObject.getLiteral());
    }

    @Override
    public FileReasonT to(final FileReason userObject) {
        if (userObject == null) {
            return null;
        }
        return FileReasonT.lookupLiteral(userObject.name);
    }
}
