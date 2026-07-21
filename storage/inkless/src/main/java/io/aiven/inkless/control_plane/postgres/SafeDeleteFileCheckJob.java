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

import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static org.jooq.generated.tables.Files.FILES;

/**
 * The job of ensuring an object key is not referenced by any file, so it can be safely deleted.
 */
public class SafeDeleteFileCheckJob implements Callable<Boolean> {
    private final Time time;
    private final DSLContext jooqCtx;
    private final Consumer<Long> durationCallback;

    final String objectKeyPath;

    public SafeDeleteFileCheckJob(final Time time,
                                  final DSLContext jooqCtx,
                                  final String objectKeyPath,
                                  final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.durationCallback = durationCallback;
        this.objectKeyPath = objectKeyPath;
    }

    @Override
    public Boolean call() throws Exception {
        if (objectKeyPath == null || objectKeyPath.isEmpty()) {
            return true;
        }
        return JobUtils.run(this::runOnce, time, durationCallback);
    }

    private boolean runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) ->
            conf.dsl()
                .select(FILES.FILE_ID)
                .from(FILES)
                .where(FILES.OBJECT_KEY.eq(objectKeyPath))
                .forShare()
                .fetch()
                .isEmpty()
        );
    }
}
