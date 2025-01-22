// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.generated.Routines;

import java.util.Set;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
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
            throw new RuntimeException(e);
        }
    }

    private void runOnce() {
        jooqCtx.transaction((final Configuration conf) -> {
            final String[] paths = this.objectKeyPaths.toArray(new String[0]);
            Routines.deleteFilesV1(conf, paths);
        });
    }
}
