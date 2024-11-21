// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.config;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import io.aiven.inkless.storage_backend.common.StorageBackend;

public class InklessConfig extends AbstractConfig {
    public static final String PREFIX = "inkless.";

    public static final String OBJECT_KEY_PREFIX_CONFIG = "object.key.prefix";
    private static final String OBJECT_KEY_PREFIX_DOC = "The object storage key prefix.";

    public static final String PRODUCE_PREFIX = "produce.";

    public static final String PRODUCE_COMMIT_INTERVAL_MS_CONFIG = PRODUCE_PREFIX + "commit.interval.ms";
    private static final String PRODUCE_COMMIT_INTERVAL_MS_DOC = "The interval with which produced data are committed.";
    private static final int PRODUCE_COMMIT_INTERVAL_MS_DEFAULT = 250;

    public static final String PRODUCE_BUFFER_MAX_BYTES_CONFIG = PRODUCE_PREFIX + "buffer.max.bytes";
    private static final String PRODUCE_BUFFER_MAX_BYTES_DOC = "The max size of the buffer to accumulate produce requests. "
        + "This is a best effort limit that cannot always be strictly enforced.";
    private static final int PRODUCE_BUFFER_MAX_BYTES_DEFAULT = 8 * 1024 * 1024;  // 8 MiB

    public static final String PRODUCE_MAX_UPLOAD_ATTEMPTS_CONFIG = PRODUCE_PREFIX + "max.upload.attempts";
    private static final String PRODUCE_MAX_UPLOAD_ATTEMPTS_DOC = "The max number of attempts to upload a file to the object storage.";
    private static final int PRODUCE_MAX_UPLOAD_ATTEMPTS_DEFAULT = 3;

    public static final String PRODUCE_UPLOAD_BACKOFF_MS_CONFIG = PRODUCE_PREFIX + "upload.backoff.ms";
    private static final String PRODUCE_UPLOAD_BACKOFF_MS_DOC = "The number of millisecond to back off for before the next upload attempt.";
    private static final int PRODUCE_UPLOAD_BACKOFF_MS_DEFAULT = 10;

    public static final String STORAGE_PREFIX = "storage.";

    public static final String STORAGE_BACKEND_CLASS_CONFIG = STORAGE_PREFIX + "backend.class";
    private static final String STORAGE_BACKEND_CLASS_DOC = "The storage backend implementation class";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            OBJECT_KEY_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.MEDIUM,
            OBJECT_KEY_PREFIX_DOC
        );

        configDef.define(
            PRODUCE_COMMIT_INTERVAL_MS_CONFIG,
            ConfigDef.Type.INT,
            PRODUCE_COMMIT_INTERVAL_MS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.HIGH,
            PRODUCE_COMMIT_INTERVAL_MS_DOC
        );

        configDef.define(
            PRODUCE_BUFFER_MAX_BYTES_CONFIG,
            ConfigDef.Type.INT,
            PRODUCE_BUFFER_MAX_BYTES_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.HIGH,
            PRODUCE_BUFFER_MAX_BYTES_DOC
        );

        configDef.define(
            PRODUCE_MAX_UPLOAD_ATTEMPTS_CONFIG,
            ConfigDef.Type.INT,
            PRODUCE_MAX_UPLOAD_ATTEMPTS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.MEDIUM,
            PRODUCE_MAX_UPLOAD_ATTEMPTS_DOC
        );

        configDef.define(
            PRODUCE_UPLOAD_BACKOFF_MS_CONFIG,
            ConfigDef.Type.INT,
            PRODUCE_UPLOAD_BACKOFF_MS_DEFAULT,
            ConfigDef.Range.atLeast(0),
            ConfigDef.Importance.MEDIUM,
            PRODUCE_UPLOAD_BACKOFF_MS_DOC
        );

        configDef.define(
            STORAGE_BACKEND_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            ConfigDef.NO_DEFAULT_VALUE,
            ConfigDef.Importance.HIGH,
            STORAGE_BACKEND_CLASS_DOC
        );

        return configDef;
    }

    public InklessConfig(final AbstractConfig config) {
        this(config.originalsWithPrefix(InklessConfig.PREFIX));
    }

    // Visible for testing
    InklessConfig(final Map<String, ?> props) {
        super(configDef(), props);
    }

    public String objectKeyPrefix() {
        return getString(OBJECT_KEY_PREFIX_CONFIG);
    }

    public StorageBackend storage() {
        final Class<?> storageClass = getClass(STORAGE_BACKEND_CLASS_CONFIG);
        final StorageBackend storage = Utils.newInstance(storageClass, StorageBackend.class);
        storage.configure(this.originalsWithPrefix(STORAGE_PREFIX));
        return storage;
    }

    public Duration commitInterval() {
        return Duration.ofMillis(getInt(PRODUCE_COMMIT_INTERVAL_MS_CONFIG));
    }

    public int produceBufferMaxBytes() {
        return getInt(PRODUCE_BUFFER_MAX_BYTES_CONFIG);
    }

    public int produceMaxUploadAttempts() {
        return getInt(PRODUCE_MAX_UPLOAD_ATTEMPTS_CONFIG);
    }
    public Duration produceUploadBackoff() {
        return Duration.ofMillis(getInt(PRODUCE_UPLOAD_BACKOFF_MS_CONFIG));
    }
}
