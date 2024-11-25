// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;

import com.groupcdg.pitest.annotations.CoverageIgnore;

import java.util.Objects;

public class NonEmptyPassword implements ConfigDef.Validator {
    @Override
    public void ensureValid(final String name, final Object value) {
        if (Objects.isNull(value)) {
            return;
        }
        final var pwd = (Password) value;
        if (pwd.value() == null || pwd.value().isBlank()) {
            throw new ConfigException(name + " value must not be empty");
        }
    }

    @CoverageIgnore
    @Override
    public String toString() {
        return "Non-empty password text";
    }
}
