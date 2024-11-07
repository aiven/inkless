// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common;

import org.apache.kafka.common.Configurable;

public interface StorageBackend extends Configurable, ObjectUploader, ObjectFetcher, ObjectDeleter {
}
