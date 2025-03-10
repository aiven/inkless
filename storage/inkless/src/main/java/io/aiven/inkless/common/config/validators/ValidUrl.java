/*
 * Inkless
 * Copyright (C) 2025 Aiven OY
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
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

/**
 * {@link ConfigDef.Validator} implementation that verifies that a config value is
 * a valid URL with http or https schemes.
 */
public class ValidUrl implements ConfigDef.Validator {
    private static final List<String> SUPPORTED_SCHEMAS = List.of("http", "https");

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            try {
                final var url = new URL((String) value);
                if (!SUPPORTED_SCHEMAS.contains(url.getProtocol())) {
                    throw new ConfigException(name, value, "URL must have scheme from the list " + SUPPORTED_SCHEMAS);
                }
            } catch (final MalformedURLException e) {
                throw new ConfigException(name, value, "Must be a valid URL");
            }
        }
    }

    @CoverageIgnore
    @Override
    public String toString() {
        return "Valid URL as defined in rfc2396";
    }
}
