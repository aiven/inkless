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

/**
 * Signals that no control plane is configured, so a call failed without contacting one.
 *
 * <p>Distinct from a plain {@link ControlPlaneException} so callers can answer with a retriable
 * error and a single log line instead of an unexpected-failure stack trace.
 */
public class ControlPlaneUnavailableException extends ControlPlaneException {
    public ControlPlaneUnavailableException(final String message) {
        super(message);
    }

    public ControlPlaneUnavailableException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
