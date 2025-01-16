// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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
