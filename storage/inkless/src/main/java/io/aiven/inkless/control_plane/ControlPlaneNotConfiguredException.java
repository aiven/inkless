/*
 * Inkless
 * Copyright (C) 2024 - 2026 Aiven OY
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
package io.aiven.inkless.control_plane;

import org.apache.kafka.common.config.ConfigException;

/**
 * Signals that a {@link ControlPlane} implementation has no connection configured, as opposed to
 * some other invalid configuration.
 *
 * <p>{@link ControlPlaneDelegateReconciler} treats only this exception as "no control plane is
 * configured," the state a dynamic reconfiguration can legitimately produce by emptying a
 * connection string. A {@code ControlPlane} implementation MUST NOT throw this for any other
 * reason: a plain {@link ConfigException}, such as a typo in an unrelated key, must reach the
 * reconciler as itself so the real cause stays visible instead of being reported as unconfigured.
 */
public final class ControlPlaneNotConfiguredException extends ControlPlaneException {
    public ControlPlaneNotConfiguredException(final String message) {
        super(message);
    }

    public ControlPlaneNotConfiguredException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
