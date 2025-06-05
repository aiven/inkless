package io.aiven.inkless.cache;

public class StaleCacheEntryException extends Exception {
    public StaleCacheEntryException(String message) {
        super(message);
    }
}
