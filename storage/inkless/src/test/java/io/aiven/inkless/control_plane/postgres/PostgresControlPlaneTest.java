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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.aiven.inkless.control_plane.AbstractControlPlaneTest;
import io.aiven.inkless.control_plane.ControlPlaneRequestRejectedException;
import io.aiven.inkless.control_plane.CreateTopicAndPartitionsRequest;
import io.aiven.inkless.control_plane.InitDisklessLogProducerState;
import io.aiven.inkless.control_plane.InitDisklessLogRequest;
import io.aiven.inkless.test_utils.InklessPostgreSQLContainer;
import io.aiven.inkless.test_utils.PostgreSQLTestContainer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

    @Test
    void initDisklessLogRejectsAValueThatADomainDoesNotAllow() {
        final Uuid topicId = new Uuid(70, 70);
        controlPlane.createTopicAndPartitions(Set.of(new CreateTopicAndPartitionsRequest(topicId, "rejected", 2)));

        // `timestamp_t` allows values from -5. Partition 0 fails with partition 1 because the call is one transaction.
        assertThatThrownBy(() -> controlPlane.initDisklessLog(List.of(
            new InitDisklessLogRequest(topicId, "rejected", 0, 0, 10, List.of()),
            new InitDisklessLogRequest(topicId, "rejected", 1, 0, 10,
                List.of(new InitDisklessLogProducerState(7L, (short) 0, 0, 0, 9L, -6L)))
        ))).isInstanceOf(ControlPlaneRequestRejectedException.class);
    }
}
