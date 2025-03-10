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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.UuidUtil;

class DeleteTopicJob implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTopicJob.class);

    private final Time time;
    private final DSLContext jooqCtx;
    private final Set<Uuid> topicIds;
    private final Consumer<Long> durationCallback;

    DeleteTopicJob(final Time time,
                   DSLContext jooqCtx,
                   final Set<Uuid> topicIds,
                   final Consumer<Long> durationCallback) {
        this.time = time;
        this.jooqCtx = jooqCtx;
        this.topicIds = topicIds;
        this.durationCallback = durationCallback;
    }

    @Override
    public void run() {
        if (topicIds.isEmpty()) {
            return;
        }

        try {
            runOnce();
        } catch (final Exception e) {
            // TODO add retry with backoff
            throw new RuntimeException(e);
        }
    }

    private void runOnce() {
        jooqCtx.transaction((final Configuration conf) -> {
            final Instant now = TimeUtils.now(time);
            final UUID[] topicIds = this.topicIds.stream().map(UuidUtil::toJava).toArray(UUID[]::new);
            Routines.deleteTopicV1(conf, now, topicIds);
        });
    }
}
