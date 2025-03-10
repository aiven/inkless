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
