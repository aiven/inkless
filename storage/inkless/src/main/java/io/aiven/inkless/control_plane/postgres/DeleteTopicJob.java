// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
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
