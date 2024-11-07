// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;

/**
 * {@link ConfigDef.Validator} implementation that is used to combine another validator
 * with allowing null as a valid value. Useful for cases like
 * <pre>
 * {@code Null.or(ConfigDef.Range.between(1, Long.MAX_VALUE))}
 * </pre>
 * where existing validator that does not allow null values con be used but null is still a default value.
 */
public class Null implements ConfigDef.Validator {

    private final ConfigDef.Validator validator;

    public static ConfigDef.Validator or(final ConfigDef.Validator validator) {
        return new Null(validator);
    }

    private Null(final ConfigDef.Validator validator) {
        this.validator = validator;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null) {
            validator.ensureValid(name, value);
        }
    }

    @Override
    public String toString() {
        return "null or " + validator.toString();
    }
}

