// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.produce;

import java.util.List;

import io.aiven.inkless.control_plane.CommitBatchResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract class AbstractWriterState implements WriterState {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractWriterState.class);

    protected final StateContext context;

    protected AbstractWriterState(final StateContext context) {
        this.context = context;
    }

    @Override
    public CommitTickResult commitTick() {
        LOGGER.debug("Received Tick in {}, ignoring", this);
        return new CommitTickResult(this);
    }

    @Override
    public CommitFinishedResult commitFinished(final List<CommitBatchResponse> commitBatchResponses,
                                               final Throwable error) {
        LOGGER.error("Received CommitFinished in {}, this wasn't expected, ignoring", this);
        return new CommitFinishedResult(this);
    }
}
