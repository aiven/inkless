/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package io.aiven.inkless.storage_backend.gcs;

import com.google.auth.Credentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * A credentials provider that can automatically reload credentials from a file
 * when the file is modified during runtime.
 */
public class ReloadableCredentialsProvider implements AutoCloseable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadableCredentialsProvider.class);

    private Credentials currentCredentials;
    private final String credentialsPath;
    private final File credentialsFile;
    private final String credentialsJson;
    private final Boolean defaultCredentials;

    private WatchService watchService;
    private java.util.concurrent.ScheduledExecutorService scheduledExecutorService;
    private Consumer<Credentials> credentialsUpdateCallback;

    /**
     * Creates a new ReloadableCredentialsProvider.
     * Auto-reload is automatically enabled when using file-based credentials.
     *
     * @param defaultCredentials use default credentials if true
     * @param credentialsJson JSON credentials string (mutually exclusive with path)
     * @param credentialsPath path to credentials file (mutually exclusive with JSON)
     * @throws IOException if credentials cannot be loaded initially
     */
    public ReloadableCredentialsProvider(final Boolean defaultCredentials,
                                       final String credentialsJson,
                                       final String credentialsPath) throws IOException {
        this.defaultCredentials = defaultCredentials;
        this.credentialsJson = credentialsJson;
        this.credentialsPath = credentialsPath;
        final boolean watchEnabled = credentialsPath != null;
        if (watchEnabled) {
            this.credentialsFile = new File(credentialsPath);
        } else {
            this.credentialsFile = null;
        }

        // Load initial credentials
        loadInitialCredentials();
    }

    public void enableWatching(final int interval) throws IOException {
        // Start file watcher if using path-based credentials
        if (this.credentialsFile != null && watchService == null) {
            startFileWatcher(interval);
        }
    }

    public void enableWatching() throws IOException {
        enableWatching(10);
    }

    /**
     * Gets the current credentials. This method returns the most recently loaded credentials,
     * which may have been reloaded from the file system if file watching is enabled.
     *
     * @return current credentials
     */
    public Credentials getCredentials() {
        return currentCredentials;
    }

    /**
     * Sets a callback that will be invoked whenever credentials are reloaded.
     * This can be used to update the GCS Storage client with new credentials.
     *
     * @param callback the callback to invoke when credentials are updated
     */
    public void setCredentialsUpdateCallback(final Consumer<Credentials> callback) {
        this.credentialsUpdateCallback = callback;
    }

    private void loadInitialCredentials() throws IOException {
        LOGGER.debug("Loading initial GCS credentials");
        currentCredentials = CredentialsBuilder.build(
            defaultCredentials, credentialsJson, credentialsPath);
    }

    /**
     * Manually reload credentials from the configured source.
     *
     * @throws IOException if credentials cannot be reloaded
     */
    public void reloadCredentials() throws IOException {
        LOGGER.debug("Reloading GCS credentials");
        currentCredentials = CredentialsBuilder.build(
                defaultCredentials, credentialsJson, credentialsPath);
        LOGGER.info("GCS credentials have been reloaded successfully");

        if (credentialsUpdateCallback != null) {
            try {
                credentialsUpdateCallback.accept(currentCredentials);
            } catch (final Exception e) {
                LOGGER.warn("Error in credentials update callback", e);
            }
        }
    }

    private void startFileWatcher(final int interval) throws IOException {
        watchService = FileSystems.getDefault().newWatchService();

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(r -> {
            final Thread t = new Thread(r, "gcs-credentials-watcher");
            t.setDaemon(true);
            return t;
        });

        final AtomicReference<WatchKey> watchKeyReference = new AtomicReference<>(subscribeToCredentialChanges());
        scheduledExecutorService.scheduleWithFixedDelay(() -> {
            final WatchKey watchKey = watchKeyReference.get();
            if (watchKey != null) {
                final List<WatchEvent<?>> watchEvents = watchKey.pollEvents();

                watchEvents.stream().filter(watchEvent -> {
                    @SuppressWarnings("unchecked")
                    final Path path = ((WatchEvent<Path>) watchEvent).context();
                    return this.credentialsFile.toPath().getFileName().equals(path);
                }).findFirst().ifPresent(watchEvent -> {
                    LOGGER.info("{}: {}, Modified: {}",
                            watchEvent.kind(), watchEvent.context(), this.credentialsFile.lastModified());
                    try {
                        reloadCredentials();
                    } catch (final Exception e) {
                        LOGGER.error("Failed to reload credentials from file", e);
                    }
                });
                if (!watchKey.reset()) {
                    watchKeyReference.compareAndSet(watchKey, subscribeToCredentialChanges());
                }
            } else {
                watchKeyReference.set(subscribeToCredentialChanges());
            }
        }, 0, interval, TimeUnit.SECONDS);
    }

    private WatchKey subscribeToCredentialChanges() {
        try {
            if (credentialsFile == null) {
                return null;
            }
            Path parentPath = credentialsFile.toPath().toAbsolutePath().getParent();
            if (parentPath == null) {
                LOGGER.error("Failed to get parent path for credentials file");
                return null;
            }
            return parentPath
                    .register(watchService, StandardWatchEventKinds.ENTRY_MODIFY,
                            StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
        } catch (final IOException e) {
            LOGGER.error("Failed to subscribe to credential changes", e);
            return null;
        }
    }

    @Override
    public void close() throws IOException {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }
        if (watchService != null) {
            watchService.close();
        }
    }
}
