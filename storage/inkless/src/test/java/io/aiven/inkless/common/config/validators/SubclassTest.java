// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SubclassTest {
    @Test
    void validSubclass() {
        assertThatNoException().isThrownBy(() -> Subclass.of(Object.class).ensureValid("test", String.class));
    }

    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> Subclass.of(Object.class).ensureValid("test", null));
    }

    @Test
    void invalidSubclass() {
        assertThatThrownBy(() -> Subclass.of(String.class).ensureValid("test", Object.class))
            .isInstanceOf(ConfigException.class)
            .hasMessage("test should be a subclass of java.lang.String");
    }
}
