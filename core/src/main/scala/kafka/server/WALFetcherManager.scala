package kafka.server

import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.network.BrokerEndPoint

class WALFetcherManager(brokerConfig: KafkaConfig,
                        protected val replicaManager: ReplicaManager,
                        metrics: Metrics,
                        time: Time,
                        metadataVersionSupplier: () => MetadataVersion,
                        brokerEpochSupplier: () => Long) extends AbstractFetcherManager[ReplicaFetcherThread](
  name = "WALFetcherManager on broker " + brokerConfig.brokerId,
  clientId = "Replica",
  numFetchers = brokerConfig.disklessTsUnificationFetchers) {

  override def createFetcherThread(fetcherId: Int, sourceBroker: BrokerEndPoint): Nothing = ???
}
