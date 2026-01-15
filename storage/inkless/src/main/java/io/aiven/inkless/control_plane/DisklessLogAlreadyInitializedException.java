/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
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

import org.apache.kafka.common.Uuid;

/**
 * Exception thrown when attempting to initialize a diskless log that has already been initialized.
 */
public class DisklessLogAlreadyInitializedException extends ControlPlaneException {

    private final Uuid topicId;
    private final int partition;

    public DisklessLogAlreadyInitializedException(final Uuid topicId, final int partition) {
        super(String.format("Diskless log already initialized for topic %s partition %d",
            topicId, partition));
        this.topicId = topicId;
        this.partition = partition;
    }

    public Uuid topicId() {
        return topicId;
    }

    public int partition() {
        return partition;
    }
}
