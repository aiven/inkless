// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.time.Instant;

class SessionUtils {
    static boolean canSessionAddMoreData(final WriteSession session,
                                         final WriterState.StateContext context) {
        return session.totalSize() < context.maxBufferSize();
    }

    static boolean hasSessionReachedCommitTimeout(final WriteSession session,
                                                  final WriterState.StateContext context) {
        final Instant whenToCommit = session.sessionStarted().plus(context.commitInterval());
        final Instant now = context.now();
        return whenToCommit.isBefore(now) || whenToCommit.equals(now);
    }
}
