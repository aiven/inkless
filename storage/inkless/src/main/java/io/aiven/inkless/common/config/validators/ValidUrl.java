// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.groupcdg.pitest.annotations.CoverageIgnore;

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
