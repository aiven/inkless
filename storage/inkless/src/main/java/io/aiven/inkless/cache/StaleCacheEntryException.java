package io.aiven.inkless.cache;

/* When this Exception is raised by a LogFragment, it means that the data contained in the LogFragment
   is stale and it needs to be invalidated. */
public class StaleCacheEntryException extends Exception {
    public StaleCacheEntryException(String message) {
        super(message);
    }
}
