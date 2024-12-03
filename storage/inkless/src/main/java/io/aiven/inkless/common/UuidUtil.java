// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import org.apache.kafka.common.Uuid;

import java.util.UUID;

public class UuidUtil {
    public static UUID toJava(final Uuid uuid) {
        return new UUID(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    public static Uuid fromJava(final UUID uuid) {
        return new Uuid(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }
}
