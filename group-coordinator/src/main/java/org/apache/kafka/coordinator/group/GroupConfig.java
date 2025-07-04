/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Range.atLeast;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;
import static org.apache.kafka.common.config.ConfigDef.ValidString.in;

/**
 * Group configuration related parameters and supporting methods like validation, etc. are
 * defined in this class.
 */
public final class GroupConfig extends AbstractConfig {

    public static final String CONSUMER_SESSION_TIMEOUT_MS_CONFIG = "consumer.session.timeout.ms";

    public static final String CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG = "consumer.heartbeat.interval.ms";

    public static final String SHARE_SESSION_TIMEOUT_MS_CONFIG = "share.session.timeout.ms";

    public static final String SHARE_HEARTBEAT_INTERVAL_MS_CONFIG = "share.heartbeat.interval.ms";

    public static final String SHARE_RECORD_LOCK_DURATION_MS_CONFIG = "share.record.lock.duration.ms";

    public static final String SHARE_AUTO_OFFSET_RESET_CONFIG = "share.auto.offset.reset";
    public static final String SHARE_AUTO_OFFSET_RESET_DEFAULT = ShareGroupAutoOffsetResetStrategy.LATEST.name();
    public static final String SHARE_AUTO_OFFSET_RESET_DOC = "The strategy to initialize the share-partition start offset. " +
        "<ul><li>earliest: automatically reset the offset to the earliest offset</li>" +
        "<li>latest: automatically reset the offset to the latest offset</li>" +
        "<li>by_duration:&lt;duration&gt;: automatically reset the offset to a configured duration from the current timestamp. " +
        "&lt;duration&gt; must be specified in ISO8601 format (PnDTnHnMn.nS). " +
        "Negative duration is not allowed.</li>" +
        "<li>anything else: throw exception to the share consumer.</li></ul>";

    public static final String SHARE_ISOLATION_LEVEL_CONFIG = "share.isolation.level";
    public static final String SHARE_ISOLATION_LEVEL_DEFAULT = IsolationLevel.READ_UNCOMMITTED.toString();
    public static final String SHARE_ISOLATION_LEVEL_DOC = "Controls how to read records written transactionally. " +
        "If set to \"read_committed\", the share group will only deliver transactional records which have been committed. " +
        "If set to \"read_uncommitted\", the share group will return all messages, even transactional messages which have been aborted. " +
        "Non-transactional records will be returned unconditionally in either mode.";

    public static final String STREAMS_SESSION_TIMEOUT_MS_CONFIG = "streams.session.timeout.ms";

    public static final String STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG = "streams.heartbeat.interval.ms";

    public static final String STREAMS_NUM_STANDBY_REPLICAS_CONFIG = "streams.num.standby.replicas";

    public final int consumerSessionTimeoutMs;

    public final int consumerHeartbeatIntervalMs;

    public final int shareSessionTimeoutMs;

    public final int shareHeartbeatIntervalMs;

    public final int shareRecordLockDurationMs;

    public final String shareAutoOffsetReset;

    public final int streamsSessionTimeoutMs;

    public final int streamsHeartbeatIntervalMs;

    public final int streamsNumStandbyReplicas;

    public final String shareIsolationLevel;

    private static final ConfigDef CONFIG = new ConfigDef()
        .define(CONSUMER_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.CONSUMER_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(SHARE_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.SHARE_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.SHARE_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(SHARE_RECORD_LOCK_DURATION_MS_CONFIG,
            INT,
            ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DEFAULT,
            atLeast(1000),
            MEDIUM,
            ShareGroupConfig.SHARE_GROUP_RECORD_LOCK_DURATION_MS_DOC)
        .define(SHARE_AUTO_OFFSET_RESET_CONFIG,
            STRING,
            SHARE_AUTO_OFFSET_RESET_DEFAULT,
            new ShareGroupAutoOffsetResetStrategy.Validator(),
            MEDIUM,
            SHARE_AUTO_OFFSET_RESET_DOC)
        .define(SHARE_ISOLATION_LEVEL_CONFIG,
            STRING,
            SHARE_ISOLATION_LEVEL_DEFAULT,
            in(IsolationLevel.READ_COMMITTED.toString(), IsolationLevel.READ_UNCOMMITTED.toString()),
            MEDIUM,
            SHARE_ISOLATION_LEVEL_DOC)
        .define(STREAMS_SESSION_TIMEOUT_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_SESSION_TIMEOUT_MS_DOC)
        .define(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DEFAULT,
            atLeast(1),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_HEARTBEAT_INTERVAL_MS_DOC)
        .define(STREAMS_NUM_STANDBY_REPLICAS_CONFIG,
            INT,
            GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT,
            atLeast(0),
            MEDIUM,
            GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DOC);

    public GroupConfig(Map<?, ?> props) {
        super(CONFIG, props, false);
        this.consumerSessionTimeoutMs = getInt(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        this.consumerHeartbeatIntervalMs = getInt(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareSessionTimeoutMs = getInt(SHARE_SESSION_TIMEOUT_MS_CONFIG);
        this.shareHeartbeatIntervalMs = getInt(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.shareRecordLockDurationMs = getInt(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
        this.shareAutoOffsetReset = getString(SHARE_AUTO_OFFSET_RESET_CONFIG);
        this.streamsSessionTimeoutMs = getInt(STREAMS_SESSION_TIMEOUT_MS_CONFIG);
        this.streamsHeartbeatIntervalMs = getInt(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        this.streamsNumStandbyReplicas = getInt(STREAMS_NUM_STANDBY_REPLICAS_CONFIG);
        this.shareIsolationLevel = getString(SHARE_ISOLATION_LEVEL_CONFIG);
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public static Optional<Type> configType(String configName) {
        return Optional.ofNullable(CONFIG.configKeys().get(configName)).map(c -> c.type);
    }

    public static Set<String> configNames() {
        return CONFIG.names();
    }

    /**
     * Check that property names are valid
     */
    public static void validateNames(Properties props) {
        Set<String> names = configNames();
        for (Object name : props.keySet()) {
            if (!names.contains(name)) {
                throw new InvalidConfigurationException("Unknown group config name: " + name);
            }
        }
    }

    /**
     * Validates the values of the given properties.
     */
    @SuppressWarnings({"CyclomaticComplexity", "NPathComplexity"})
    private static void validateValues(Map<?, ?> valueMaps, GroupCoordinatorConfig groupCoordinatorConfig, ShareGroupConfig shareGroupConfig) {
        int consumerHeartbeatInterval = (Integer) valueMaps.get(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        int consumerSessionTimeout = (Integer) valueMaps.get(CONSUMER_SESSION_TIMEOUT_MS_CONFIG);
        int shareHeartbeatInterval = (Integer) valueMaps.get(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        int shareSessionTimeout = (Integer) valueMaps.get(SHARE_SESSION_TIMEOUT_MS_CONFIG);
        int shareRecordLockDurationMs = (Integer) valueMaps.get(SHARE_RECORD_LOCK_DURATION_MS_CONFIG);
        int streamsSessionTimeoutMs = (Integer) valueMaps.get(STREAMS_SESSION_TIMEOUT_MS_CONFIG);
        int streamsHeartbeatIntervalMs = (Integer) valueMaps.get(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        int streamsNumStandbyReplicas = (Integer) valueMaps.get(STREAMS_NUM_STANDBY_REPLICAS_CONFIG);
        if (consumerHeartbeatInterval < groupCoordinatorConfig.consumerGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (consumerHeartbeatInterval > groupCoordinatorConfig.consumerGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (consumerSessionTimeout < groupCoordinatorConfig.consumerGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (consumerSessionTimeout > groupCoordinatorConfig.consumerGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.CONSUMER_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (shareHeartbeatInterval < groupCoordinatorConfig.shareGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (shareHeartbeatInterval > groupCoordinatorConfig.shareGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(SHARE_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (shareSessionTimeout < groupCoordinatorConfig.shareGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(SHARE_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (shareSessionTimeout > groupCoordinatorConfig.shareGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(SHARE_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.SHARE_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (shareRecordLockDurationMs < shareGroupConfig.shareGroupMinRecordLockDurationMs()) {
            throw new InvalidConfigurationException(SHARE_RECORD_LOCK_DURATION_MS_CONFIG + " must be greater than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MIN_RECORD_LOCK_DURATION_MS_CONFIG);
        }
        if (shareRecordLockDurationMs > shareGroupConfig.shareGroupMaxRecordLockDurationMs()) {
            throw new InvalidConfigurationException(SHARE_RECORD_LOCK_DURATION_MS_CONFIG + " must be less than or equal to " +
                ShareGroupConfig.SHARE_GROUP_MAX_RECORD_LOCK_DURATION_MS_CONFIG);
        }
        if (streamsHeartbeatIntervalMs < groupCoordinatorConfig.streamsGroupMinHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MIN_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (streamsHeartbeatIntervalMs > groupCoordinatorConfig.streamsGroupMaxHeartbeatIntervalMs()) {
            throw new InvalidConfigurationException(STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (streamsSessionTimeoutMs < groupCoordinatorConfig.streamsGroupMinSessionTimeoutMs()) {
            throw new InvalidConfigurationException(STREAMS_SESSION_TIMEOUT_MS_CONFIG + " must be greater than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MIN_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (streamsSessionTimeoutMs > groupCoordinatorConfig.streamsGroupMaxSessionTimeoutMs()) {
            throw new InvalidConfigurationException(STREAMS_SESSION_TIMEOUT_MS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_SESSION_TIMEOUT_MS_CONFIG);
        }
        if (streamsNumStandbyReplicas > groupCoordinatorConfig.streamsGroupMaxNumStandbyReplicas()) {
            throw new InvalidConfigurationException(STREAMS_NUM_STANDBY_REPLICAS_CONFIG + " must be less than or equal to " +
                GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_CONFIG);
        }
        if (consumerSessionTimeout <= consumerHeartbeatInterval) {
            throw new InvalidConfigurationException(CONSUMER_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                CONSUMER_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (shareSessionTimeout <= shareHeartbeatInterval) {
            throw new InvalidConfigurationException(SHARE_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                SHARE_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
        if (streamsSessionTimeoutMs <= streamsHeartbeatIntervalMs) {
            throw new InvalidConfigurationException(STREAMS_SESSION_TIMEOUT_MS_CONFIG + " must be greater than " +
                STREAMS_HEARTBEAT_INTERVAL_MS_CONFIG);
        }
    }

    /**
     * Check that the given properties contain only valid consumer group config names and that all values can be
     * parsed and are valid.
     */
    public static void validate(Properties props, GroupCoordinatorConfig groupCoordinatorConfig, ShareGroupConfig shareGroupConfig) {
        validateNames(props);
        Map<?, ?> valueMaps = CONFIG.parse(props);
        validateValues(valueMaps, groupCoordinatorConfig, shareGroupConfig);
    }

    /**
     * Create a group config instance using the given properties and defaults.
     */
    public static GroupConfig fromProps(Map<?, ?> defaults, Properties overrides) {
        Properties props = new Properties();
        props.putAll(defaults);
        props.putAll(overrides);
        return new GroupConfig(props);
    }

    /**
     * The default share group auto offset reset strategy.
     */
    public static ShareGroupAutoOffsetResetStrategy defaultShareAutoOffsetReset() {
        return ShareGroupAutoOffsetResetStrategy.fromString(SHARE_AUTO_OFFSET_RESET_DEFAULT);
    }

    /**
     * The default share group isolation level.
     */
    public static IsolationLevel defaultShareIsolationLevel() {
        return IsolationLevel.valueOf(SHARE_ISOLATION_LEVEL_DEFAULT.toUpperCase(Locale.ROOT));
    }

    /**
     * The consumer group session timeout in milliseconds.
     */
    public int consumerSessionTimeoutMs() {
        return consumerSessionTimeoutMs;
    }

    /**
     * The consumer group heartbeat interval in milliseconds.
     */
    public int consumerHeartbeatIntervalMs() {
        return consumerHeartbeatIntervalMs;
    }

    /**
     * The share group session timeout in milliseconds.
     */
    public int shareSessionTimeoutMs() {
        return shareSessionTimeoutMs;
    }

    /**
     * The share group heartbeat interval in milliseconds.
     */
    public int shareHeartbeatIntervalMs() {
        return shareHeartbeatIntervalMs;
    }

    /**
     * The share group record lock duration milliseconds.
     */
    public int shareRecordLockDurationMs() {
        return shareRecordLockDurationMs;
    }

    /**
     * The share group auto offset reset strategy.
     */
    public ShareGroupAutoOffsetResetStrategy shareAutoOffsetReset() {
        return ShareGroupAutoOffsetResetStrategy.fromString(shareAutoOffsetReset);
    }

    /**
     * The streams group session timeout in milliseconds.
     */
    public int streamsSessionTimeoutMs() {
        return streamsSessionTimeoutMs;
    }

    /**
     * The streams group heartbeat interval in milliseconds.
     */
    public int streamsHeartbeatIntervalMs() {
        return streamsHeartbeatIntervalMs;
    }

    /**
     * The number of streams standby replicas for each task.
     */
    public int streamsNumStandbyReplicas() {
        return streamsNumStandbyReplicas;
    }

    /**
     * The share group isolation level.
     */
    public IsolationLevel shareIsolationLevel() {
        if (shareIsolationLevel == null) {
            throw new IllegalArgumentException("Share isolation level is null");
        }
        try {
            return IsolationLevel.valueOf(shareIsolationLevel.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown Share isolation level: " + shareIsolationLevel);
        }
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtml(4, config -> "groupconfigs_" + config));
    }
}
