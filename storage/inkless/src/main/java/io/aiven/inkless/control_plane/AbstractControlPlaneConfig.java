// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.time.Duration;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;

public abstract class AbstractControlPlaneConfig extends AbstractConfig {
    public static final String FILE_MERGE_SIZE_THRESHOLD_BYTES_CONFIG = "file.merge.size.threshold.bytes";
    private static final String FILE_MERGE_SIZE_THRESHOLD_BYTES_DOC = "The total minimum volume of files to be merged together.";

    public static final String FILE_MERGE_LOCK_PERIOD_MS_CONFIG = "file.merge.lock.period.ms";
    private static final String FILE_MERGE_LOCK_PERIOD_MS_DOC = "The period of time when the file merge job is locked (assumed being performed).";

    protected static ConfigDef baseConfigDef() {
        final var configDef = new ConfigDef();

        configDef.define(
            FILE_MERGE_SIZE_THRESHOLD_BYTES_CONFIG,
            ConfigDef.Type.LONG,
            100 * 1024 * 1024,
            atLeast(1),
            ConfigDef.Importance.MEDIUM,
            FILE_MERGE_SIZE_THRESHOLD_BYTES_DOC
        );
        configDef.define(
            FILE_MERGE_LOCK_PERIOD_MS_CONFIG,
            ConfigDef.Type.LONG,
            Duration.ofMinutes(60).toMillis(),
            atLeast(1),
            ConfigDef.Importance.MEDIUM,
            FILE_MERGE_LOCK_PERIOD_MS_DOC
        );

        return configDef;
    }

    public AbstractControlPlaneConfig(final ConfigDef definition, final Map<?, ?> originals) {
        super(definition, originals);
    }

    public long fileMergeSizeThresholdBytes() {
        return getLong(FILE_MERGE_SIZE_THRESHOLD_BYTES_CONFIG);
    }

    public Duration fileMergeLockPeriod() {
        return Duration.ofMillis(getLong(FILE_MERGE_LOCK_PERIOD_MS_CONFIG));
    }
}
