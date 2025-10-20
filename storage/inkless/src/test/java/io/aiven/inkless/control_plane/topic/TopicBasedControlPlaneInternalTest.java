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
package io.aiven.inkless.control_plane.topic;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import io.aiven.inkless.control_plane.AbstractControlPlaneTest;

import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

import static org.mockito.Mockito.mock;

class TopicBasedControlPlaneInternalTest extends AbstractControlPlaneTest {
    @TempDir
    Path dbDir;

    @Override
    protected ControlPlaneAndConfigs createControlPlane(final TestInfo testInfo) {
        final RecordWriter recordWriter = mock(RecordWriter.class);

        final var controlPlane = new TopicBasedControlPlaneInternal(time, recordWriter);
        final Map<String, String> configs = new HashMap<>(BASE_CONFIG);
        configs.put("database.dir", dbDir.toString());
        return new ControlPlaneAndConfigs(controlPlane, configs);
    }

    @Override
    protected void tearDownControlPlane() throws IOException {
    }
}
