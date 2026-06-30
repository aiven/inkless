# Inkless
# Copyright (C) 2024 - 2026 Aiven OY
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import uuid

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.inkless.consolidation_verifier import ConsolidationVerifier
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer


class ConsolidationPipelineTest(Test):
    """Test for a born-consolidated topic: WAL -> local log -> remote, then prune.

    Asserts the consolidation gauges are exported via JMX, the tiered-storage
    prefix grows in MinIO, the WAL is pruned in the Postgres control plane, and
    every acked record is still consumable afterwards.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a
    # stale topic name would let old rows skew the control-plane queries.
    TOPIC_PREFIX = "consolidation-pipeline"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Sized to roll several segments by size (1 MiB floor); only closed segments tier.
    NUM_RECORDS = 300000

    def __init__(self, test_context):
        super(ConsolidationPipelineTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_consolidation_pipeline(self, metadata_quorum):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            # Enable consolidation per-test; the constructor also flips the controller.
            consolidation=True,
            # Run the WAL pruner / file cleaner fast; cache TTL must stay <= retention/2.
            server_prop_overrides=[
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
            ],
            # JMX is started later on brokers only (see start_jmx); the controller
            # never exposes these MBeans.
            topics={
                self.TOPIC: {
                    "partitions": self.NUM_PARTITIONS,
                    "replication-factor": self.REPLICATION_FACTOR,
                    "configs": {
                        "diskless.enable": "true",
                        "remote.storage.enable": "true",
                        "min.insync.replicas": 2,
                        # Roll segments by size/time so they close and get tiered.
                        "segment.bytes": 1048576,
                        "segment.ms": 5000,
                    },
                },
            },
        )
        self.kafka.start()

        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()

        baseline_tiered = verifier.tiered_object_count()
        self.logger.info("Baseline tiered-storage object count: %d" % baseline_tiered)
        baseline_wal = verifier.wal_object_count()
        self.logger.info("Baseline WAL object count: %d" % baseline_wal)

        # Start JmxTool on the brokers before producing so we sample the whole run.
        verifier.start_jmx()

        # Produce the dataset, unthrottled.
        producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC, max_messages=self.NUM_RECORDS,
                                      throughput=-1)
        producer.start()

        wait_until(lambda: producer.num_acked >= self.NUM_RECORDS or producer.worker_errors,
                   timeout_sec=180, backoff_sec=1,
                   err_msg="Producer failed to produce %d records." % self.NUM_RECORDS)
        assert not producer.worker_errors, "Producer errors: %s" % producer.worker_errors
        producer.stop()
        acked = producer.num_acked
        producer.free()
        self.logger.info("Produced and acked %d records" % acked)

        # Wait for the WAL -> local log fetcher to catch up.
        wait_until(lambda: verifier.local_lag() == 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Consolidation local lag did not drain to 0.")
        self.logger.info("Consolidation local lag drained to 0")

        # Wait for data to reach remote: the tiered-storage prefix grows.
        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Tiered-storage object count did not grow above baseline.")
        self.logger.info("Tiered-storage object count grew to %d" % verifier.tiered_object_count())

        # Wait for the control plane to prune the WAL.
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="WAL was not pruned (log_start_offset did not advance) in the Postgres control plane.")
        self.logger.info("Control plane pruned WAL: batch_count=%d, min_log_start_offset=%d" %
                         (verifier.wal_batch_count(self.TOPIC), verifier.min_log_start_offset(self.TOPIC)))

        wal_after_prune = verifier.wal_object_count()
        self.logger.info("WAL object count after prune: %d (baseline %d, delta %+d)" %
                         (wal_after_prune, baseline_wal, wal_after_prune - baseline_wal))

        # Assert the consolidation gauges were exported and scraped (peaks logged
        # for diagnostics; we don't assert lag rose, as it is not reliably observable).
        self.kafka.read_jmx_output_all_nodes()
        jmx_keys = self.kafka.maximum_jmx_value
        local_lag_key = ConsolidationVerifier.find_aggregate_key(jmx_keys, ConsolidationVerifier.LOCAL_LAG)
        total_lag_key = ConsolidationVerifier.find_aggregate_key(jmx_keys, ConsolidationVerifier.TOTAL_LAG)
        deletable_key = ConsolidationVerifier.find_aggregate_key(jmx_keys, ConsolidationVerifier.DELETABLE_MESSAGES)
        local_lag_peak = jmx_keys.get(local_lag_key, 0) if local_lag_key else 0
        total_lag_peak = jmx_keys.get(total_lag_key, 0) if total_lag_key else 0
        deletable_peak = jmx_keys.get(deletable_key, 0) if deletable_key else 0
        self.logger.info("Consolidation JMX peaks: local_lag=%s total_lag=%s deletable=%s" %
                         (local_lag_peak, total_lag_peak, deletable_peak))
        assert local_lag_key is not None, \
            ("ConsolidationLocalLag was not exported/scraped via JMX. Expected a "
             "%s 'name=ConsolidationLocalLag' Value column; got keys: %s"
             % (ConsolidationVerifier.JMX_PACKAGE, sorted(jmx_keys)))

        # Read every acked record back (from remote). Track unique offsets to avoid a false pass if
        # the consumer skips ranges (e.g. due to data loss) or re-reads duplicates during retries/rebalances.
        seen_offsets = set()
        max_offset = {"value": -1}

        def on_record_consumed(event, _node):
            try:
                offset = int(event.get("offset", -1))
            except (TypeError, ValueError):
                return
            if offset >= 0:
                seen_offsets.add(offset)
                max_offset["value"] = max(max_offset["value"], offset)

        consumer = VerifiableConsumer(
            self.test_context, num_nodes=1, kafka=self.kafka,
            topic=self.TOPIC, group_id="consolidation-pipeline-group",
            on_record_consumed=on_record_consumed
        )
        consumer.start()
        wait_until(lambda: (max_offset["value"] >= (acked - 1) and len(seen_offsets) >= acked) or consumer.worker_errors,
                   timeout_sec=180, backoff_sec=1,
                   err_msg="Consumer failed to read back all %d acked records." % acked)
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        total_consumed = consumer.total_consumed()
        consumer.free()
        assert len(seen_offsets) == acked, (
            "Expected to read %d unique offsets [0..%d], but saw %d (max_offset=%d)" %
            (acked, acked - 1, len(seen_offsets), max_offset["value"])
        )
        self.logger.info("Read back %d records after consolidation + prune" % total_consumed)
