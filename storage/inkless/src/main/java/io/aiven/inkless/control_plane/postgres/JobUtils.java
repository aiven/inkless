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
package io.aiven.inkless.control_plane.postgres;

import org.apache.kafka.common.utils.Time;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

import io.aiven.inkless.TimeUtils;
import io.aiven.inkless.control_plane.ControlPlaneException;

public class JobUtils {
    public static void run(final Runnable runnable) {
        try {
            runnable.run();
        } catch (final Exception e) {
            // TODO add retry with backoff
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static void run(final Runnable runnable, final Time time, final Consumer<Long> durationCallback) {
        try {
            TimeUtils.measureDurationMs(time, runnable, durationCallback);
        } catch (final Exception e) {
            // TODO add retry with backoff
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> T run(final Callable<T> callable) {
        try {
            return callable.call();
        } catch (final Exception e) {
            // TODO add retry with backoff
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }

    public static <T> T run(final Callable<T> callable, final Time time, final Consumer<Long> durationCallback) {
        try {
            return TimeUtils.measureDurationMs(time, callable, durationCallback);
        } catch (final Exception e) {
            // TODO add retry with backoff
            if (e instanceof ControlPlaneException) {
                throw (ControlPlaneException) e;
            } else {
                throw new RuntimeException(e);
            }
        }
    }
}
