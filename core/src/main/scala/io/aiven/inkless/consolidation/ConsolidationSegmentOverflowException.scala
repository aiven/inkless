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

package io.aiven.inkless.consolidation

import org.apache.kafka.common.InvalidRecordException

/**
 * A consolidation fetch served a single record block larger than the follower's `segment.bytes`, so the
 * follower append rejected it with `RecordBatchTooLargeException`. This happens when a partition holds a
 * batch larger than the current `segment.bytes` (e.g. `max.message.bytes` was lowered below `segment.bytes`
 * over time, or a coalesced diskless unit). See `DisklessLeaderEndPoint.clampRecordsToSegment`.
 *
 * Extends [[InvalidRecordException]] (same `InvalidConfigurationException` family as
 * `RecordBatchTooLargeException`) so [[kafka.server.AbstractFetcherThread]] treats it as a soft,
 * retriable per-partition error rather than a hard failure: the partition stays parked at the same offset
 * and retries with backoff, so raising `segment.bytes` resumes consolidation without a become-follower or
 * broker restart.
 */
class ConsolidationSegmentOverflowException(message: String, cause: Throwable)
  extends InvalidRecordException(message, cause)
