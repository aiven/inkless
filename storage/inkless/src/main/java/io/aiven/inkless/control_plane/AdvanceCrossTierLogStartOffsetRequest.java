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

import org.apache.kafka.common.Uuid;

/**
 * Reports the cross-tier (remote) log start offset for a partition to the control plane.
 *
 * <p>The cross-tier log start offset is the lowest offset still physically readable across the
 * remote + local tiers, as observed by the partition's classic leader. The control plane only ever
 * advances the stored value (see {@code advance_cross_tier_log_start_v1}).
 */
public record AdvanceCrossTierLogStartOffsetRequest(Uuid topicId,
                                                    int partition,
                                                    long remoteLogStartOffset) {
}
