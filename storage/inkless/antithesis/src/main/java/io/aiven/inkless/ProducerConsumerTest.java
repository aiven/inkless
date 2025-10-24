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
package io.aiven.inkless;

import com.antithesis.sdk.Lifecycle;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class ProducerConsumerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerConsumerTest.class);

    public static void main(final String[] args) throws IOException {
        final String configFile = args[0];
        LOGGER.info("Using config file {}", configFile);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        final JsonNode config = mapper.readTree(new File(configFile));
        final String bootstrapServers = config.get("bootstrap_servers").asText();
        LOGGER.info("Bootstrap servers: {}", bootstrapServers);
        
        Lifecycle.setupComplete(null);
    }
}
