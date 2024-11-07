// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NullTest {
    @Test
    void nullIsValid() {
        assertThatNoException().isThrownBy(() -> Null.or(ConfigDef.Range.between(1, 2)).ensureValid("test", null));
    }

    @Test
    void nonNullCorrectValueIsValid() {
        assertThatNoException().isThrownBy(() -> Null.or(ConfigDef.Range.between(1, 2)).ensureValid("test", 1));
    }

    @Test
    void invalidValue() {
        assertThatThrownBy(() -> Null.or(ConfigDef.Range.between(1, 2)).ensureValid("test", 5))
            .isInstanceOf(ConfigException.class)
            .hasMessage("Invalid value 5 for configuration test: Value must be no more than 2");
    }

    @Test
    void testToString() {
        assertThat(Null.or(ConfigDef.Range.between(1, 2)).toString())
            .isEqualTo("null or [1,...,2]");
    }
}
