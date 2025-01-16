// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
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
