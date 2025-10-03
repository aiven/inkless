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
package io.aiven.inkless.common.metrics;

import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Value;

import java.util.function.Supplier;

/**
 * Implementation of {@link Value} that allows fetching a value from provided {@code Long} {@link Supplier}
 * to avoid unnecessary calls to {@link Sensor#record()} that under the hood has a synchronized block and affects
 * performance because of that.
 */
public class MeasurableValue<T extends Number> extends Value {
    private final Supplier<T> value;

    public MeasurableValue(final Supplier<T> value) {
        this.value = value;
    }

    @Override
    public double measure(final MetricConfig config, final long now) {
        return value.get().doubleValue();
    }
}
