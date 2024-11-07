// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.common.config.validators;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.groupcdg.pitest.annotations.CoverageIgnore;

/**
 * {@link ConfigDef.Validator} implementation that verifies
 * that a config value is a subclass of a specified class.
 */
public class Subclass implements ConfigDef.Validator {
    private final Class<?> parentClass;

    public static Subclass of(final Class<?> parentClass) {
        return new Subclass(parentClass);
    }

    public Subclass(final Class<?> parentClass) {
        this.parentClass = parentClass;
    }

    @Override
    public void ensureValid(final String name, final Object value) {
        if (value != null && !(parentClass.isAssignableFrom((Class<?>) value))) {
            throw new ConfigException(name + " should be a subclass of " + parentClass.getCanonicalName());
        }
    }

    @CoverageIgnore
    @Override
    public String toString() {
        return "Any implementation of " + parentClass.getName();
    }
}
