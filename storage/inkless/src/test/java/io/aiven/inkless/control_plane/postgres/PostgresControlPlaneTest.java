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
package io.aiven.inkless.control_plane.postgres;

import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.control_plane.AbstractControlPlaneTest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

@Testcontainers
class PostgresControlPlaneTest extends AbstractControlPlaneTest {
    @Container
    static InklessPostgreSQLContainer pgContainer = PostgreSQLTestContainer.container();

    @Override
    protected ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo) {
        pgContainer.createDatabase(testInfo);

        final var controlPlane = new PostgresControlPlane(time);
        final Map<String, String> configs = new HashMap<>(BASE_CONFIG);
        configs.putAll(Map.of(
            "connection.string", pgContainer.getUserJdbcUrl(),
            "username", pgContainer.getUsername(),
            "password", pgContainer.getPassword()
        ));
        return new ControlPlaneAndConfigs(controlPlane, configs);
    }

    @Override
    protected void tearDownControlPlane() throws IOException {
        controlPlane.close();
        pgContainer.tearDown();
    }
}
