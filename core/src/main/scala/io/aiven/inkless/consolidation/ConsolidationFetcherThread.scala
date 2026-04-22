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

import io.aiven.inkless.consume.ConcatenatedRecords
import kafka.server.{FailedPartitions, KafkaConfig, ReplicaFetcherThread, ReplicaManager, ReplicaQuota}
import org.apache.kafka.common.record.{MemoryRecords, Records}
import org.apache.kafka.server.LeaderEndPoint

class ConsolidationFetcherThread(name: String,
                                 leader: LeaderEndPoint,
                                 brokerConfig: KafkaConfig,
                                 failedPartitions: FailedPartitions,
                                 replicaMgr: ReplicaManager,
                                 quota: ReplicaQuota,
                                 logPrefix: String) extends ReplicaFetcherThread(name, leader, brokerConfig, failedPartitions, replicaMgr, quota, logPrefix) {

  override def toMemoryRecords(records: Records): MemoryRecords = {
    (records: @unchecked) match {
      case r: ConcatenatedRecords => r.toMemoryRecords
      case _ => super.toMemoryRecords(records)
    }
  }
}
