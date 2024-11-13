// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class InklessThreadFactory implements ThreadFactory {
    private final String namePrefix;
    private final boolean daemon;

    private final AtomicInteger threadNumber = new AtomicInteger(0);

    public InklessThreadFactory(final String namePrefix, final boolean daemon) {
        this.namePrefix = namePrefix;
        this.daemon = daemon;
    }

    @Override
    public Thread newThread(final Runnable r) {
        final Thread thread = new Thread(r, namePrefix + threadNumber.getAndIncrement());
        thread.setDaemon(daemon);
        return thread;
    }
}
