// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.flywaydb.core.Flyway;

class Migrations {
    static void migrate(final PostgresControlPlaneConfig controlPlaneConfig) {
        final Flyway flyway = Flyway.configure().dataSource(
            controlPlaneConfig.connectionString(),
            controlPlaneConfig.username(),
            controlPlaneConfig.password()).load();
        flyway.migrate();
    }
}
