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
package io.aiven.inkless.control_plane.postgres.converters;

import org.jooq.impl.AbstractConverter;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class OffsetDateTimeToInstantConverter extends AbstractConverter<OffsetDateTime, Instant> {
    public OffsetDateTimeToInstantConverter() {
        super(OffsetDateTime.class, Instant.class);
    }

    @Override
    public Instant from(final OffsetDateTime databaseObject) {
        return databaseObject == null
            ? null
            : databaseObject.toInstant();
    }

    @Override
    public OffsetDateTime to(final Instant userObject) {
        return userObject == null
            ? null
            : OffsetDateTime.ofInstant(userObject, ZoneOffset.UTC);
    }
}
