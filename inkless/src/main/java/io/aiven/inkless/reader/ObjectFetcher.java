package io.aiven.inkless.reader;

public interface ObjectFetcher {
    byte[] fetch(String objectKey, final long offset, final long size);
}
