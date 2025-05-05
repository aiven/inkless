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

import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;

import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlaneException;
import io.aiven.inkless.control_plane.DeleteFilesRequest;

public class DeleteFilesJob implements Runnable {
    private final Time time;
    private final DSLContext jooqCtx;
    private final Consumer<Long> durationCallback;

    final Set<String> objectKeyPaths;

    public DeleteFilesJob(Time time,
                          DSLContext jooqCtx,
                          DeleteFilesRequest request,
                          Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.durationCallback = durationCallback;
        this.objectKeyPaths = request.objectKeyPaths();
    }

    @Override
    public void run() {
        if (objectKeyPaths.isEmpty()) {
            return;
        }

        try {
            TimeUtils.measureDurationMs(time, this::runOnce, durationCallback);
        } catch (final Exception e) {
            // TODO add retry with backoff
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    private void runOnce() {
        jooqCtx.transaction((final Configuration conf) -> {
            final String[] paths = this.objectKeyPaths.toArray(new String[0]);
            Routines.deleteFilesV1(conf, paths);
        });
    }
}
