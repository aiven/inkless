package io.aiven.inkless.cache;

import io.aiven.inkless.control_plane.BatchCoordinate;

public class LogTruncationException extends StaleCacheEntryException {
    public LogTruncationException(LogFragment logFragment, BatchCoordinate batch) {
        super("Log start offset of new batch ("+ batch.logStartOffset() +") > current log start offset (" + logFragment.logStartOffset() + ")");
    }
}
