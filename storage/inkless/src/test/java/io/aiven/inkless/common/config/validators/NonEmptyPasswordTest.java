// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class NonEmptyPasswordTest {

    private final ConfigDef.Validator nonEmptyPasswordValidator = new NonEmptyPassword();

    @Test
    void emptyPassword() {
        assertThatThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", new Password("   ")))
            .isInstanceOf(ConfigException.class)
            .hasMessage("password value must not be empty");
    }

    @Test
    void nullPassword() {
        assertThatNoException().isThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", null));
    }

    @Test
    void validPassword() {
        assertThatNoException()
            .isThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", new Password("pass")));
    }

    @Test
    void nullPasswordValue() {
        assertThatThrownBy(() -> nonEmptyPasswordValidator.ensureValid("password", new Password(null)))
            .isInstanceOf(ConfigException.class)
            .hasMessage("password value must not be empty");
    }
}
