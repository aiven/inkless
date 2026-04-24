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

import io.aiven.inkless.consume.{FetchHandler, FetchOffsetHandler}
import kafka.server.{AbstractFetcherManager, KafkaConfig, ReplicaManager, ReplicationQuotaManager}
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.server.network.BrokerEndPoint

class ConsolidationFetcherManager(brokerConfig: KafkaConfig,
                                  replicaManager: ReplicaManager,
                                  quotaManager: ReplicationQuotaManager,
                                  fetchHandler: FetchHandler,
                                  fetchOffsetHandler: FetchOffsetHandler)
  extends AbstractFetcherManager[ConsolidationFetcherThread](name = "ConsolidationFetcherManager on broker " + brokerConfig.brokerId,
    clientId = "Consolidation",
    numFetchers = brokerConfig.numReplicaFetchers) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): ConsolidationFetcherThread = {
    val threadName = s"ConsolidationFetcherThread-$fetcherId-${sourceBroker.id}"
    val logContext = new LogContext(s"[ConsolidationFetcher replicaId=${brokerConfig.brokerId}, leaderId=${sourceBroker.id}, " +
      s"fetcherId=$fetcherId] ")
    val disklessLeaderEndPoint = new DisklessLeaderEndPoint(
      sourceBroker,
      fetchHandler,
      fetchOffsetHandler,
      replicaManager,
      brokerConfig,
      quotaManager,
      () => replicaManager.metadataCache.metadataVersion(),
      replicaManager.brokerEpochSupplier
    )
    new ConsolidationFetcherThread(threadName, disklessLeaderEndPoint, brokerConfig, failedPartitions, replicaManager,
      quotaManager, logContext.logPrefix)
  }

  def shutdown(): Unit = {
    info("shutting down")
    closeAllFetchers()
    info("shutdown completed")
  }
}
