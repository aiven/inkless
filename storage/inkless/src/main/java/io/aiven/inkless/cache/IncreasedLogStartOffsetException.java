package io.aiven.inkless.cache;

public class IncreasedLogStartOffsetException extends StaleCacheEntryException {
    public IncreasedLogStartOffsetException(LogFragment logFragment, CacheBatchCoordinate batch) {
        super("Log start offset of new batch ("+ batch.logStartOffset() +") > current log start offset (" + logFragment.logStartOffset() + ")");
    }
}
