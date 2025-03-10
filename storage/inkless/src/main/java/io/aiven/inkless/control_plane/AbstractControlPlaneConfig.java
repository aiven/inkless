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
