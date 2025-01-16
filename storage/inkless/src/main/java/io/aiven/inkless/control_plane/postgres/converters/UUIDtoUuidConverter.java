// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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
