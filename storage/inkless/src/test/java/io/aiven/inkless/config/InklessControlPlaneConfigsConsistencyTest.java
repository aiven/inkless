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
package io.aiven.inkless.config;

import org.apache.kafka.server.config.InklessControlPlaneConfigs;

import org.junit.jupiter.api.Test;

import java.util.Set;

import io.aiven.inkless.control_plane.postgres.PostgresConnectionConfig;
import io.aiven.inkless.control_plane.postgres.PostgresControlPlaneConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * `InklessControlPlaneConfigs`, in `:server-common`, duplicates these three property names as
 * literals because `:server` cannot depend on `:storage:inkless`, where the pieces below live.
 * This guards against the two definitions drifting apart.
 */
class InklessControlPlaneConfigsConsistencyTest {
    @Test
    void namesMatchInklessConfigComposition() {
        final String prefix = InklessConfig.PREFIX + InklessConfig.CONTROL_PLANE_PREFIX;

        assertThat(InklessControlPlaneConfigs.PREFIX).isEqualTo(prefix);
        assertThat(InklessControlPlaneConfigs.CONNECTION_STRING_CONFIG)
            .isEqualTo(prefix + PostgresConnectionConfig.CONNECTION_STRING_CONFIG);
        assertThat(InklessControlPlaneConfigs.READ_CONNECTION_STRING_CONFIG)
            .isEqualTo(prefix + PostgresControlPlaneConfig.READ_CONFIG_PREFIX + PostgresConnectionConfig.CONNECTION_STRING_CONFIG);
        assertThat(InklessControlPlaneConfigs.WRITE_CONNECTION_STRING_CONFIG)
            .isEqualTo(prefix + PostgresControlPlaneConfig.WRITE_CONFIG_PREFIX + PostgresConnectionConfig.CONNECTION_STRING_CONFIG);
        assertThat(InklessControlPlaneConfigs.RECONFIGURABLE_CONFIGS).isEqualTo(Set.of(
            InklessControlPlaneConfigs.CONNECTION_STRING_CONFIG,
            InklessControlPlaneConfigs.READ_CONNECTION_STRING_CONFIG,
            InklessControlPlaneConfigs.WRITE_CONNECTION_STRING_CONFIG));
    }
}
