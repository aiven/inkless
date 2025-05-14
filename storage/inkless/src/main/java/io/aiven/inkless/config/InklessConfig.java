/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
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
package io.aiven.inkless.config;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.Utils;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import io.aiven.inkless.common.config.validators.Subclass;
import io.aiven.inkless.control_plane.ControlPlane;
import io.aiven.inkless.control_plane.InMemoryControlPlane;
import io.aiven.inkless.storage_backend.common.StorageBackend;
import io.aiven.inkless.storage_backend.in_memory.InMemoryStorage;

public class InklessConfig extends AbstractConfig {
    public static final String PREFIX = "inkless.";

    public static final String CONTROL_PLANE_PREFIX = "control.plane.";

    public static final String CONTROL_PLANE_CLASS_CONFIG = CONTROL_PLANE_PREFIX + "class";
    private static final String CONTROL_PLANE_CLASS_DOC = "The control plane implementation class";
    private static final String CONTROL_PLANE_CLASS_DEFAULT = InMemoryControlPlane.class.getCanonicalName();

    public static final String OBJECT_KEY_PREFIX_CONFIG = "object.key.prefix";
    private static final String OBJECT_KEY_PREFIX_DOC = "The object storage key prefix. It cannot start of finish with a slash.";

    public static final String OBJECT_KEY_LOG_PREFIX_MASKED_CONFIG = "object.key.log.prefix.masked";
    private static final String OBJECT_KEY_LOG_PREFIX_MASKED_DOC = "Whether to log full object key path, or mask the prefix.";

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
    private static final String STORAGE_BACKEND_CLASS_DEFAULT = InMemoryStorage.class.getCanonicalName();

    public static final String CONSUME_PREFIX = "consume.";

    public static final String CONSUME_CACHE_BLOCK_BYTES_CONFIG = CONSUME_PREFIX + "cache.block.bytes";
    private static final String CONSUME_CACHE_BLOCK_BYTES_DOC = "The number of bytes to fetch as a single block from object storage when serving fetch requests.";
    private static final int CONSUME_CACHE_BLOCK_BYTES_DEFAULT = 16 * 1024 * 1024;  // 16 MiB

    public static final String CONSUME_CACHE_MAX_COUNT_CONFIG = CONSUME_PREFIX + "cache.max.count";
    private static final String CONSUME_CACHE_MAX_COUNT_DOC = "The maximum number of objects to cache in memory.";
    private static final int CONSUME_CACHE_MAX_COUNT_DEFAULT = 1000;

    public static final String FILE_CLEANER_INTERVAL_MS_CONFIG = "file.cleaner.interval.ms";
    private static final String FILE_CLEANER_INTERVAL_MS_DOC = "The interval with which to clean up files marked for deletion.";
    private static final int FILE_CLEANER_INTERVAL_MS_DEFAULT = 5 * 60 * 1000;  // 5 minutes

    public static final String FILE_CLEANER_RETENTION_PERIOD_MS_CONFIG = "file.cleaner.retention.period.ms";
    private static final String FILE_CLEANER_RETENTION_PERIOD_MS_DOC = "The retention period for files marked for deletion.";
    private static final int FILE_CLEANER_RETENTION_PERIOD_MS_DEFAULT = 60 * 1000;  // 1 minute

    public static final String FILE_MERGER_INTERVAL_MS_CONFIG = "file.merger.interval.ms";
    private static final String FILE_MERGER_INTERVAL_MS_DOC = "The interval with which to merge files.";
    private static final int FILE_MERGER_INTERVAL_MS_DEFAULT = 60 * 1000;  // 1 minute

    public static final String FILE_MERGER_TEMP_DIR_CONFIG = "file.merger.temp.dir";
    private static final String FILE_MERGER_TEMP_DIR_DOC = "The temporary directory for file merging.";
    private static final String FILE_MERGER_TEMP_DIR_DEFAULT = "/tmp/inkless/merger";

    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            CONTROL_PLANE_CLASS_CONFIG,
            ConfigDef.Type.CLASS,
            CONTROL_PLANE_CLASS_DEFAULT,
            new Subclass(ControlPlane.class),
            ConfigDef.Importance.HIGH,
            CONTROL_PLANE_CLASS_DOC
        );

        configDef.define(
            OBJECT_KEY_PREFIX_CONFIG,
            ConfigDef.Type.STRING,
            "",
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.MEDIUM,
            OBJECT_KEY_PREFIX_DOC
        );
        configDef.define(
            OBJECT_KEY_LOG_PREFIX_MASKED_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.LOW,
            OBJECT_KEY_LOG_PREFIX_MASKED_DOC
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
            STORAGE_BACKEND_CLASS_DEFAULT,
            ConfigDef.Importance.HIGH,
            STORAGE_BACKEND_CLASS_DOC
        );

        configDef.define(
                CONSUME_CACHE_BLOCK_BYTES_CONFIG,
                ConfigDef.Type.INT,
                CONSUME_CACHE_BLOCK_BYTES_DEFAULT,
                ConfigDef.Importance.LOW,
                CONSUME_CACHE_BLOCK_BYTES_DOC
        );

        configDef.define(
            FILE_CLEANER_INTERVAL_MS_CONFIG,
            ConfigDef.Type.INT,
            FILE_CLEANER_INTERVAL_MS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            FILE_CLEANER_INTERVAL_MS_DOC
        );

        configDef.define(
            FILE_CLEANER_RETENTION_PERIOD_MS_CONFIG,
            ConfigDef.Type.INT,
            FILE_CLEANER_RETENTION_PERIOD_MS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            FILE_CLEANER_RETENTION_PERIOD_MS_DOC
        );

        configDef.define(
            FILE_MERGER_INTERVAL_MS_CONFIG,
            ConfigDef.Type.INT,
            FILE_MERGER_INTERVAL_MS_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            FILE_MERGER_INTERVAL_MS_DOC
        );
        configDef.define(
            FILE_MERGER_TEMP_DIR_CONFIG,
            ConfigDef.Type.STRING,
            FILE_MERGER_TEMP_DIR_DEFAULT,
            new ConfigDef.NonNullValidator(),
            ConfigDef.Importance.LOW,
            FILE_MERGER_TEMP_DIR_DOC
        );
        configDef.define(
            CONSUME_CACHE_MAX_COUNT_CONFIG,
            ConfigDef.Type.LONG,
            CONSUME_CACHE_MAX_COUNT_DEFAULT,
            ConfigDef.Range.atLeast(1),
            ConfigDef.Importance.LOW,
            CONSUME_CACHE_MAX_COUNT_DOC
        );

        return configDef;
    }

    public InklessConfig(final AbstractConfig config) {
        this(config.originalsWithPrefix(InklessConfig.PREFIX));
    }

    public InklessConfig(final Map<String, ?> props) {
        super(configDef(), props);
    }

    @SuppressWarnings("unchecked")
    public Class<ControlPlane> controlPlaneClass() {
        return (Class<ControlPlane>) getClass(CONTROL_PLANE_CLASS_CONFIG);
    }

    public Map<String, Object> controlPlaneConfig() {
        return originalsWithPrefix(CONTROL_PLANE_PREFIX);
    }

    public String objectKeyPrefix() {
        return getString(OBJECT_KEY_PREFIX_CONFIG);
    }

    public boolean objectKeyLogPrefixMasked() {
        return getBoolean(OBJECT_KEY_LOG_PREFIX_MASKED_CONFIG);
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

    public int fetchCacheBlockBytes() {
        return getInt(CONSUME_CACHE_BLOCK_BYTES_CONFIG);
    }

    public Duration fileCleanerInterval() {
        return Duration.ofMillis(getInt(FILE_CLEANER_INTERVAL_MS_CONFIG));
    }

    public Duration fileCleanerRetentionPeriod() {
        return Duration.ofMillis(getInt(FILE_CLEANER_RETENTION_PERIOD_MS_CONFIG));
    }

    public Duration fileMergerInterval() {
        return Duration.ofMillis(getInt(FILE_MERGER_INTERVAL_MS_CONFIG));
    }

    public Path fileMergeWorkDir() {
        final String path = getString(FILE_MERGER_TEMP_DIR_CONFIG);
        return Path.of(path);
    }

    public Long cacheMaxCount() {
        return getLong(CONSUME_CACHE_MAX_COUNT_CONFIG);
    }
}
