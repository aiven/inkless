// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Instant;

@FunctionalInterface
public interface CommitTickScheduler {
    void schedule(Instant sessionStarted, WriterState currentState);
}
