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

import org.jooq.Configuration;
import org.jooq.DSLContext;

import java.util.concurrent.Callable;

import static org.jooq.generated.tables.Files.FILES;

/**
 * The job of ensuring an object key is not referenced by any file, so it can be safely deleted.
 */
public class SafeDeleteFileCheckJob implements Callable<Boolean> {
    private final DSLContext jooqCtx;

    final String objectKeyPath;

    public SafeDeleteFileCheckJob(final DSLContext jooqCtx,
                                  final String objectKeyPath) {
        this.jooqCtx = jooqCtx;
        this.objectKeyPath = objectKeyPath;
    }

    @Override
    public Boolean call() throws Exception {
        if (objectKeyPath == null || objectKeyPath.isEmpty()) {
            return true;
        }

        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO add retry with backoff
            throw new RuntimeException(e);
        }
    }

    private boolean runOnce() {
        return jooqCtx.transactionResult((final Configuration conf) ->
            conf.dsl()
                .select(FILES.FILE_ID)
                .from(FILES)
                .where(FILES.OBJECT_KEY.eq(objectKeyPath))
                .fetch()
                .isEmpty()
        );
    }
}
