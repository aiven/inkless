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

import org.apache.kafka.common.utils.Time;

import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

import io.aiven.inkless.control_plane.FileToDelete;

import static org.jooq.generated.Tables.FILES;
import static org.jooq.generated.Tables.FILES_TO_DELETE;

public class FindFilesToDeleteJob implements Callable<List<FileToDelete>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FindFilesToDeleteJob.class);

    private final Time time;
    private final DSLContext jooqCtx;

    public FindFilesToDeleteJob(final Time time, final DSLContext jooqCtx) {
        this.time = time;
        this.jooqCtx = jooqCtx;
    }

    @Override
    public List<FileToDelete> call() {
        try {
            return runOnce();
        } catch (final Exception e) {
            // TODO retry with backoff
            throw new RuntimeException(e);
        }
    }

    private List<FileToDelete> runOnce() {
        final var fetchResult = jooqCtx.select(
                FILES.FILE_ID,
                FILES.OBJECT_KEY,
                FILES_TO_DELETE.MARKED_FOR_DELETION_AT
            ).from(FILES_TO_DELETE)
            .innerJoin(FILES)
            .using(FILES.FILE_ID)
            .fetchStream();
        return fetchResult.map(r -> new FileToDelete(
                r.get(FILES.OBJECT_KEY),
                r.get(FILES_TO_DELETE.MARKED_FOR_DELETION_AT)
            ))
            .toList();
    }
}
