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
package io.aiven.inkless.doc;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.TopicConfig;

/**
 * Class to generate documentation for diskless topic-level configurations.
 */
public class TopicConfigsDocs {
    /**
     * Returns a ConfigDef with all diskless topic-level configurations.
     */
    public static ConfigDef configDef() {
        final ConfigDef configDef = new ConfigDef();

        configDef.define(
            TopicConfig.INKLESS_ENABLE_CONFIG,
            ConfigDef.Type.BOOLEAN,
            false,
            ConfigDef.Importance.HIGH,
            TopicConfig.INKLESS_ENABLE_DOC
        );

        configDef.define(
                TopicConfig.DISKLESS_ENABLE_CONFIG,
                ConfigDef.Type.BOOLEAN,
                false,
                ConfigDef.Importance.HIGH,
                TopicConfig.DISKLESS_ENABLE_DOC
        );

        return configDef;
    }

    public static void main(String[] args) {
        System.out.println("Diskless Topic Configurations");
        System.out.println("============================");
        System.out.println();
        System.out.println("This document describes the topic-level configurations for diskless.");
        System.out.println();

        final ConfigDef configDef = TopicConfigsDocs.configDef();
        System.out.println(configDef.toEnrichedRst());
    }
}
