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

import org.apache.kafka.common.Uuid;

import org.jooq.impl.AbstractConverter;

import java.util.UUID;

public class UUIDtoUuidConverter extends AbstractConverter<UUID, Uuid> {
    public UUIDtoUuidConverter() {
        super(UUID.class, Uuid.class);
    }

    @Override
    public Uuid from(final UUID databaseObject) {
        return databaseObject == null
            ? null
            : new Uuid(databaseObject.getMostSignificantBits(), databaseObject.getLeastSignificantBits());
    }

    @Override
    public UUID to(final Uuid userObject) {
        return userObject == null
            ? null
            : new UUID(userObject.getMostSignificantBits(), userObject.getLeastSignificantBits());
    }
}
