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
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.InvalidConfigurationException;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Invariant tests for topic type transitions between CLASSIC, TIERED, and DISKLESS.
 *
 * <p>Topic types (3 states):
 * <ul>
 *   <li>CLASSIC: diskless=false, remote=false — local-only storage</li>
 *   <li>TIERED:  diskless=false, remote=true  — tiered storage (remote log copy)</li>
 *   <li>DISKLESS: diskless=true, remote=true  — diskless with remote storage</li>
 * </ul>
 *
 * <p>Forbidden state: diskless=true, remote=false (diskless without remote storage).
 *
 * <p>Key invariants:
 * <ul>
 *   <li>Diskless requires remote storage: diskless.enable=true always requires remote.storage.enable=true.</li>
 *   <li>Diskless is irreversible: once enabled, cannot be disabled (DISKLESS → TIERED not supported).</li>
 *   <li>CLASSIC → DISKLESS requires passing through TIERED (allow-from-classic only works on tiered topics).</li>
 *   <li>Disabling remote storage requires delete-on-disable (TIERED → CLASSIC).</li>
 *   <li>Disabling remote storage on a diskless topic is forbidden.</li>
 * </ul>
 */
public class TopicTypeTransitionInvariantsTest {

    // --- Creation transitions ---

    @ParameterizedTest(name = "create: {0}")
    @MethodSource("creationScenarios")
    void testCreationTransitions(
        String description,
        Properties requestedConfigs,
        boolean disklessAllowFromClassic,
        boolean consolidationEnabled,
        boolean expectValid,
        String expectedError
    ) {
        Map<String, String> existingConfigs = Map.of();
        if (expectValid) {
            assertDoesNotThrow(() -> validate(existingConfigs, requestedConfigs, disklessAllowFromClassic, consolidationEnabled),
                "Expected VALID but got rejection for: " + description);
        } else {
            InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class,
                () -> validate(existingConfigs, requestedConfigs, disklessAllowFromClassic, consolidationEnabled),
                "Expected REJECTED but was allowed for: " + description);
            assertEquals(expectedError, ex.getMessage());
        }
    }

    // --- Alter transitions ---

    @ParameterizedTest(name = "alter: {0}")
    @MethodSource("alterScenarios")
    void testAlterTransitions(
        String description,
        Map<String, String> existingConfigs,
        Properties requestedConfigs,
        boolean disklessAllowFromClassic,
        boolean consolidationEnabled,
        boolean expectValid,
        String expectedError
    ) {
        if (expectValid) {
            assertDoesNotThrow(() -> validate(existingConfigs, requestedConfigs, disklessAllowFromClassic, consolidationEnabled),
                "Expected VALID but got rejection for: " + description);
        } else {
            InvalidConfigurationException ex = assertThrows(InvalidConfigurationException.class,
                () -> validate(existingConfigs, requestedConfigs, disklessAllowFromClassic, consolidationEnabled),
                "Expected REJECTED but was allowed for: " + description);
            assertEquals(expectedError, ex.getMessage());
        }
    }

    // --- Scenario providers ---

    static Stream<Arguments> creationScenarios() {
        return Stream.of(
            // Valid creation cases
            creation("CLASSIC (no configs)",
                props(), false, false, true, null),
            creation("TIERED (remote=true only)",
                props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"), false, false, true, null),
            creation("DISKLESS (diskless=true, remote=true, consolidation enabled)",
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                false, true, true, null),

            // Forbidden state: diskless.enable=true without remote.storage.enable (requires consolidation to enforce)
            creation("REJECTED: diskless.enable=true without remote.storage.enable (forbidden state)",
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"), false, true, false, DISKLESS_REQUIRES_RS_ERROR),
            creation("REJECTED: diskless.enable=true, remote.storage.enable=false (mutual exclusion fires first)",
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                false, true, false, MUTUAL_EXCLUSION_ERROR),

            // No-op / explicit false cases
            creation("CLASSIC with explicit diskless=false",
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"), false, false, true, null),
            creation("CLASSIC with explicit remote=false",
                props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"), false, false, true, null)
        );
    }

    static Stream<Arguments> alterScenarios() {
        return Stream.of(
            // ===== From CLASSIC =====
            alter("CLASSIC → TIERED: enable remote",
                existingClassic(), props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                false, false, true, null),
            alter("CLASSIC → DISKLESS: allowed with allow-from-classic + consolidation (single request)",
                existingClassic(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                true, true, true, null),
            alter("CLASSIC → DISKLESS: blocked without allow-from-classic",
                existingClassic(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                false, false, false, ENABLE_DISKLESS_ERROR),
            alter("CLASSIC → DISKLESS: allowed if remote.storage.enable was never set (controller will auto-enable)",
                existingClassic(), props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"),
                true, true, true, null),
            // When remote.storage.enable is explicitly false, operator must include remote.storage.enable=true
            alter("CLASSIC (explicit remote.storage.enable=false) → DISKLESS: allowed when remote.storage.enable=true included",
                existingClassicWithExplicitRs(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                true, true, true, null),
            alter("CLASSIC (explicit remote.storage.enable=false) → DISKLESS: blocked without remote.storage.enable=true (mutual exclusion)",
                existingClassicWithExplicitRs(), props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true"),
                true, true, false, MUTUAL_EXCLUSION_ERROR),
            alter("CLASSIC → no-op: keep remote=false",
                existingClassic(), props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                false, false, true, null),

            // ===== From TIERED =====
            alter("TIERED → DISKLESS: allowed with allow-from-classic",
                existingTiered(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                true, false, true, null),
            alter("TIERED → DISKLESS: blocked without allow-from-classic",
                existingTiered(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                false, false, false, ENABLE_DISKLESS_ERROR),
            alter("TIERED → DISKLESS: blocked if trying to disable remote.storage.enable simultaneously (mutual exclusion fires first)",
                existingTiered(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                true, true, false, MUTUAL_EXCLUSION_ERROR),
            alter("TIERED → CLASSIC: blocked without delete-on-disable",
                existingTiered(), props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                false, false, false, DISABLE_REMOTE_WITHOUT_DELETE_ERROR),
            alter("TIERED → CLASSIC: allowed with delete-on-disable",
                existingTiered(),
                props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false", TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "true"),
                false, false, true, null),
            alter("TIERED → no-op: keep remote=true",
                existingTiered(), props(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                false, false, true, null),

            // ===== From DISKLESS (diskless=true, remote=true) =====
            alter("DISKLESS → TIERED: blocked (cannot disable diskless)",
                existingDiskless(), props(TopicConfig.DISKLESS_ENABLE_CONFIG, "false"),
                false, false, false, DISABLE_DISKLESS_ERROR),
            alter("DISKLESS → CLASSIC: blocked (cannot disable diskless)",
                existingDiskless(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "false", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                false, false, false, DISABLE_DISKLESS_ERROR),
            alter("DISKLESS → forbidden state: blocked (cannot disable remote.storage.enable on diskless, mutual exclusion fires first)",
                existingDiskless(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false"),
                false, true, false, MUTUAL_EXCLUSION_ERROR),
            alter("DISKLESS → no-op: keep both enabled",
                existingDiskless(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"),
                false, true, true, null),
            alter("DISKLESS → no-op: unrelated config update",
                existingDiskless(),
                props(TopicConfig.DISKLESS_ENABLE_CONFIG, "true", TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true",
                    TopicConfig.RETENTION_MS_CONFIG, "86400000"),
                false, true, true, null)
        );
    }

    // --- Error messages (must match LogConfig validation) ---

    private static final String ENABLE_DISKLESS_ERROR =
        "It is invalid to enable diskless on an already existing topic.";
    private static final String DISKLESS_REQUIRES_RS_ERROR =
        "Diskless topics must have remote storage enabled. Set remote.storage.enable=true when enabling diskless.";
    private static final String DISABLE_DISKLESS_ERROR =
        "It is invalid to disable diskless.";
    private static final String MUTUAL_EXCLUSION_ERROR =
        "It is not valid to set a value for both diskless.enable and remote.storage.enable unless it's for diskless switch or consolidation.";
    private static final String DISABLE_REMOTE_WITHOUT_DELETE_ERROR =
        "It is invalid to disable remote storage without deleting remote data. "
            + "If you want to keep the remote data and turn to read only, please set `remote.storage.enable=true,remote.log.copy.disable=true`. "
            + "If you want to disable remote storage and delete all remote data, please set `remote.storage.enable=false,remote.log.delete.on.disable=true`.";

    // --- Existing config state factories ---

    /** Topic with no explicit diskless or remote configs (implicit defaults: both false). */
    private static Map<String, String> existingClassic() {
        return Map.of(TopicConfig.RETENTION_MS_CONFIG, "604800000");
    }

    /** Topic with remote.storage.enable=false explicitly set (operator previously configured it). */
    private static Map<String, String> existingClassicWithExplicitRs() {
        return Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
    }

    /** Topic with remote.storage.enable=true (tiered). */
    private static Map<String, String> existingTiered() {
        return Map.of(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true");
    }

    /** Topic with diskless=true and remote=true (the only valid diskless state). */
    private static Map<String, String> existingDiskless() {
        return Map.of(
            TopicConfig.DISKLESS_ENABLE_CONFIG, "true",
            TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true"
        );
    }

    // --- Helpers ---

    private static Properties props(String... keyValues) {
        Properties p = new Properties();
        for (int i = 0; i < keyValues.length; i += 2) {
            p.setProperty(keyValues[i], keyValues[i + 1]);
        }
        return p;
    }

    private static Arguments creation(String desc, Properties requested,
                                      boolean allowFromClassic, boolean consolidation,
                                      boolean valid, String error) {
        return Arguments.of(desc, requested, allowFromClassic, consolidation, valid, error);
    }

    private static Arguments alter(String desc, Map<String, String> existing, Properties requested,
                                   boolean allowFromClassic, boolean consolidation,
                                   boolean valid, String error) {
        return Arguments.of(desc, existing, requested, allowFromClassic, consolidation, valid, error);
    }

    /**
     * Calls LogConfig.validate with:
     * - remoteLogStorageSystemEnabled = true (system supports RS)
     * - disklessStorageSystemEnabled = true (system supports diskless)
     * - the given flags for allow-from-classic and consolidation
     */
    private void validate(Map<String, String> existingConfigs, Properties requestedProps,
                          boolean disklessAllowFromClassic, boolean consolidationEnabled) {
        Properties configuredProps = new Properties();
        configuredProps.setProperty(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "false");
        configuredProps.setProperty(TopicConfig.DISKLESS_ENABLE_CONFIG, "false");
        configuredProps.setProperty(TopicConfig.REMOTE_LOG_DELETE_ON_DISABLE_CONFIG, "false");
        configuredProps.setProperty(TopicConfig.REMOTE_LOG_COPY_DISABLE_CONFIG, "false");
        existingConfigs.forEach(configuredProps::setProperty);
        requestedProps.forEach((k, v) -> configuredProps.setProperty((String) k, (String) v));

        LogConfig.validate(
            existingConfigs,
            requestedProps,
            configuredProps,
            /* isRemoteLogStorageSystemEnabled */ true,
            disklessAllowFromClassic,
            /* isDisklessStorageSystemEnabled */ true,
            consolidationEnabled
        );
    }
}
