/*
 * Inkless
 * Copyright (C) 2026 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published
 * by the Free Software Foundation, either version 3 of the License, or
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
package io.aiven.inkless.consume;

import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.internal.Records;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;

public final class InklessFetchPartitionData extends FetchPartitionData {
    public final OptionalLong startingOffsetTimestamp;
    public final OptionalLong highWatermarkTimestamp;

    public InklessFetchPartitionData(final Errors error,
                                     final long highWatermark,
                                     final long logStartOffset,
                                     final Records records,
                                     final Optional<FetchResponseData.EpochEndOffset> divergingEpoch,
                                     final OptionalLong lastStableOffset,
                                     final Optional<List<FetchResponseData.AbortedTransaction>> abortedTransactions,
                                     final OptionalInt preferredReadReplica,
                                     final boolean isReassignmentFetch,
                                     final OptionalLong startingOffsetTimestamp,
                                     final OptionalLong highWatermarkTimestamp) {
        super(
            error,
            highWatermark,
            logStartOffset,
            records,
            divergingEpoch,
            lastStableOffset,
            abortedTransactions,
            preferredReadReplica,
            isReassignmentFetch);
        this.startingOffsetTimestamp = Objects.requireNonNull(startingOffsetTimestamp, "startingOffsetTimestamp");
        this.highWatermarkTimestamp = Objects.requireNonNull(highWatermarkTimestamp, "highWatermarkTimestamp");
    }
}
