// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.delete;

import org.apache.kafka.common.utils.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.common.SharedState;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.DeleteFilesRequest;
import io.aiven.inkless.control_plane.FileToDelete;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.common.StorageBackendException;

public class FileCleaner implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileCleaner.class);

    final Time time;
    final ControlPlane controlPlane;
    final StorageBackend storage;
    final ObjectKeyCreator objectKeyCreator;
    final Duration retentionPeriod;

    public FileCleaner(SharedState sharedState) {
        this(
            sharedState.time(),
            sharedState.controlPlane(),
            sharedState.storage(),
            sharedState.objectKeyCreator(),
            sharedState.config().fileCleanerRetentionPeriod()
        );
    }

    // package-private constructor for testing
    FileCleaner(Time time,
                ControlPlane controlPlane,
                StorageBackend storage,
                ObjectKeyCreator objectKeyCreator,
                Duration retentionPeriod) {
        this.time = time;
        this.controlPlane = controlPlane;
        this.storage = storage;
        this.objectKeyCreator = objectKeyCreator;
        this.retentionPeriod = retentionPeriod;
    }


    @Override
    public void run() {
        try {
            final var now = TimeUtils.now(time);
            LOGGER.info("Running file cleaner at {}", now);

            // find all files that are marked for deletion
            final Set<String> objectKeyPaths = controlPlane.getFilesToDelete()
                .stream()
                .filter(f -> Duration.between(f.markedForDeletionAt(), now).compareTo(retentionPeriod) > 0)
                .map(FileToDelete::objectKey).collect(Collectors.toSet());
            if (objectKeyPaths.isEmpty()) {
                LOGGER.info("No files to delete");
                return;
            }

            // delete files from storage backend
            final Set<ObjectKey> objectKeys = objectKeyPaths.stream()
                .map(objectKeyCreator::from)
                .collect(Collectors.toSet());
            storage.delete(objectKeys);
            // update control plane
            final DeleteFilesRequest request = new DeleteFilesRequest(objectKeyPaths);
            controlPlane.deleteFiles(request);

            LOGGER.info("Deleted {} files", objectKeyPaths.size());
            // TODO add control-plane exception handling to always retry
        } catch (StorageBackendException e) {
            LOGGER.warn("Failed to delete files", e);
        }
    }
}
