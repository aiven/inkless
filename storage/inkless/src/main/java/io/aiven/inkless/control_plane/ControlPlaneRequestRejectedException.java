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
 * Signals that the control plane rejected the content of a request, such as a value that a database
 * constraint doesn't allow.
 *
 * <p>Distinct from a plain {@link ControlPlaneException} so that a caller can isolate the request that
 * failed. A retry of the same request fails the same way, while a connection or pool failure doesn't
 * depend on the request.
 */
public class ControlPlaneRequestRejectedException extends ControlPlaneException {
    public ControlPlaneRequestRejectedException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
