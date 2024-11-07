// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ValidUrlTest {

    private final ConfigDef.Validator urlValidator = new ValidUrl();

    @Test
    void invalidScheme() {
        assertThatThrownBy(() -> urlValidator.ensureValid("test", "ftp://host")).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value ftp://host for configuration test: "
                + "URL must have scheme from the list [http, https]");
    }

    @Test
    void invalidHost() {
        assertThatThrownBy(() -> urlValidator.ensureValid("test", "host")).isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value host for configuration test: Must be a valid URL");
    }

    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> urlValidator.ensureValid("test", null));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http", "https"})
    void validSchemes(final String scheme) {
        assertThatNoException().isThrownBy(() -> urlValidator.ensureValid("test", scheme + "://host"));
    }
}
