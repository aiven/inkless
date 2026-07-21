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
