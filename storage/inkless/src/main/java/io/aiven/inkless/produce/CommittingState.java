// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.MemoryRecords;

import io.aiven.inkless.control_plane.CommitBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The committing state of {@link Writer}.
 *
 * <p>There's no writing happening, the current session is being committed.
 * An {@code Add} event will start writing without interrupting the ongoing commit.
 * {@code Commit tick} events are ignored.
 * A {@code Commit finished} event will mark the end of the commit process.
 */
class CommittingState extends AbstractWriterState {
    private static final Logger LOGGER = LoggerFactory.getLogger(CommittingState.class);

    private final WriteSession sessionBeingCommitted;

    CommittingState(final StateContext config,
                    final WriteSession sessionBeingCommitted) {
        super(config);
        this.sessionBeingCommitted = sessionBeingCommitted;

        final BatchBufferCloseResult closeResult = sessionBeingCommitted.closeAndPrepareForCommit();
        config.committer().commit(closeResult.commitBatchRequests(), closeResult.data());
    }

    @Override
    public AddResult add(final Map<TopicPartition, MemoryRecords> entriesPerPartition) {
        return new CommittingAndWritingState(context, sessionBeingCommitted).add(entriesPerPartition);
    }

    @Override
    public CommitFinishedResult commitFinished(final List<CommitBatchResponse> commitBatchResponses,
                                               final Throwable error) {
        LOGGER.debug("CommitFinished received, transferring into Empty");
        sessionBeingCommitted.finishCommit(commitBatchResponses, error);
        return new CommitFinishedResult(new EmptyState(context));
    }
}
