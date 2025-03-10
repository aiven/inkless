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

import org.apache.kafka.common.record.TimestampType;

import org.jooq.impl.AbstractConverter;


public class ShortToTimestampTypeConverter extends AbstractConverter<Short, TimestampType> {
    public ShortToTimestampTypeConverter() {
        super(Short.class, TimestampType.class);
    }

    @Override
    public TimestampType from(final Short databaseObject) {
        if (databaseObject == null) {
            return null;
        }
        return switch (databaseObject) {
            case -1 -> TimestampType.NO_TIMESTAMP_TYPE;
            case 0 -> TimestampType.CREATE_TIME;
            case 1 -> TimestampType.LOG_APPEND_TIME;
            default -> throw new IllegalStateException("Unexpected value: " + databaseObject);
        };
    }

    @Override
    public Short to(final TimestampType userObject) {
        return userObject == null
            ? null
            : (short) userObject.id;
    }
}
