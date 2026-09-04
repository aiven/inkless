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

/**
 * Work done by one {@link ControlPlane#purgeDeletedLogs(int)} call.
 *
 * @param batchesDeleted batches removed from soft-deleted logs
 * @param logsPurged     log rows dropped after they had no remaining batches
 * @param filesMarked    files newly marked for deletion because they became empty
 * @param moreRemain     true if any soft-deleted log is still present after this call,
 *                       including rows another broker holds with {@code SKIP LOCKED}
 * @param capReached     true if {@code moreRemain} and this call hit the window
 *                       {@code LIMIT} or the batch budget
 */
public record PurgeDeletedLogsResponse(
    long batchesDeleted,
    int logsPurged,
    int filesMarked,
    boolean moreRemain,
    boolean capReached
) {
    public static PurgeDeletedLogsResponse empty() {
        return new PurgeDeletedLogsResponse(0, 0, 0, false, false);
    }

    /**
     * True if this call deleted, purged, and marked nothing.
     * Ignores {@code moreRemain}, which is cluster-wide and stays true when another
     * broker holds the remaining rows.
     */
    public boolean isEmpty() {
        return batchesDeleted == 0 && logsPurged == 0 && filesMarked == 0;
    }
}
