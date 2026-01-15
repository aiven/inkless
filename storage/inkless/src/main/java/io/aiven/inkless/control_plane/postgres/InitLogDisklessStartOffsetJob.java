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

import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.control_plane.InitLogDisklessStartOffsetRequest;

import static org.jooq.generated.Tables.LOGS;

public class InitLogDisklessStartOffsetJob implements Runnable {
    private final Time time;
    private final DSLContext jooqCtx;
    private final Set<InitLogDisklessStartOffsetRequest> requests;
    private final Consumer<Long> durationCallback;

    InitLogDisklessStartOffsetJob(final Time time,
                                  final DSLContext jooqCtx,
                                  final Set<InitLogDisklessStartOffsetRequest> requests,
                                  final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.requests = requests;
        this.durationCallback = durationCallback;
    }

    @Override
    public void run() {
        if (requests.isEmpty()) {
            return;
        }
        JobUtils.run(this::runOnce, time, durationCallback);
    }

    private void runOnce() {
        jooqCtx.transaction((final Configuration conf) -> {
            var insertStep = conf.dsl().insertInto(LOGS,
                LOGS.TOPIC_ID,
                LOGS.PARTITION,
                LOGS.TOPIC_NAME,
                LOGS.LOG_START_OFFSET,
                LOGS.HIGH_WATERMARK,
                LOGS.BYTE_SIZE,
                LOGS.DISKLESS_START_OFFSET);
            for (final var request : requests) {
                insertStep = insertStep.values(
                    request.topicId(),
                    request.partition(),
                    request.topicName(),
                    request.logStartOffset(),
                    request.disklessStartOffset(),
                    0L,
                    request.disklessStartOffset());
            }
            insertStep.onConflictDoNothing().execute();
        });
    }
}
