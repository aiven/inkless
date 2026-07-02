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


class ReadFromRemoteAfterPruneTest(Test):
    """Born-consolidated topic durability: after data is consolidated to remote and
    the diskless WAL is pruned, reads must survive losing every local copy.

    Stops all brokers, deletes the topic's partition dirs, and restarts, forcing
    reads from the remote tier (MinIO) + diskless control plane (Postgres). Gates
    post-wipe on: ListOffsets(earliest) reports 0 (not the pruned diskless start), a
    fetch from 0 returns offset 0, a bounded read from 0 is strictly contiguous with
    correct content, and a full read-back returns every record.
    """

    # Unique per run: Postgres/MinIO persist across runs, so a stale name would
    # mix old rows into the control-plane queries.
    TOPIC_PREFIX = "read-from-remote-after-prune"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Enough to roll several segments (1 MiB floor); only tiered closed segments
    # can be served from remote after the wipe.
    NUM_RECORDS = 300000

    def __init__(self, test_context):
        super(ReadFromRemoteAfterPruneTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_read_from_remote_after_prune(self, metadata_quorum):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            consolidation=True,
            # Run the WAL pruner / file cleaner fast; cache TTL must stay <= retention/2.
            server_prop_overrides=[
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
            ],
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
                        # Evict local segments soon after upload so data lives in
                        # remote before the wipe.
                        "local.retention.ms": 5000,
                    },
                },
            },
        )
        # Collect broker data-dir logs to debug a post-restart failure.
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.start()

        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()

        baseline_tiered = verifier.tiered_object_count()
        self.logger.info("Baseline tiered-storage object count: %d" % baseline_tiered)

        verifier.start_jmx()

        acked = verifier.produce(self.TOPIC, self.NUM_RECORDS)
        self.logger.info("Produced and acked %d records" % acked)

        # 1) WAL -> local log fetcher catches up.
        wait_until(lambda: verifier.local_lag() == 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Consolidation local lag did not drain to 0.")
        self.logger.info("Consolidation local lag drained to 0")

        # 2) Data reached remote: the tiered-storage prefix grew.
        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Tiered-storage object count did not grow above baseline.")
        self.logger.info("Tiered-storage object count grew to %d" % verifier.tiered_object_count())

        # 3) WAL pruned: early (tiered) batches gone and diskless log-start advanced.
        #    The WAL need not drain to zero; the durability proof only needs the
        #    early offsets to be remote-only.
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="WAL was not pruned in the Postgres control plane.")
        self.logger.info("Control plane pruned the WAL: batch_count=%d, "
                         "min_log_start_offset=%d, wal_object_count=%d" %
                         (verifier.wal_batch_count(self.TOPIC),
                          verifier.min_log_start_offset(self.TOPIC),
                          verifier.wal_object_count()))

        # Diagnostic only: born-consolidated topics tier via the consolidation
        # pipeline, so earliest-local can stay at 0; the wipe below forces the
        # remote read.
        self.logger.info("Earliest-local offset before wipe: %d (diagnostic only)"
                         % verifier.offset_at(self.TOPIC, time_spec=-4))

        # Confirm the consolidation gauges were exported/scraped (diagnostic).
        self.kafka.read_jmx_output_all_nodes()
        jmx_keys = self.kafka.maximum_jmx_value
        local_lag_key = ConsolidationVerifier.find_aggregate_key(jmx_keys, ConsolidationVerifier.LOCAL_LAG)
        assert local_lag_key is not None, \
            ("ConsolidationLocalLag was not exported/scraped via JMX. Expected a "
             "%s 'name=ConsolidationLocalLag' Value column; got keys: %s"
             % (ConsolidationVerifier.JMX_PACKAGE, sorted(jmx_keys)))

        # --- Durability test: destroy every local copy, then read remote ---

        # Stop ALL brokers before wiping so no surviving replica serves from a
        # local log on restart (a spurious pass). The controller and internal
        # topics on disk are left intact.
        self.logger.info("Stopping all brokers to wipe local copies of %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=120)

        for node in self.kafka.nodes:
            verifier.wipe_topic_local_logs(node, self.TOPIC)

        self.logger.info("Restarting all brokers with empty local logs for %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.start_node(node, timeout_sec=120)

        # Wait until the partition has a leader again before reading.
        wait_until(lambda: verifier.partition_has_leader(self.TOPIC, 0),
                   timeout_sec=120, backoff_sec=2,
                   err_msg="Partition %s-0 did not get a leader after the restart." % self.TOPIC)

        # 1) earliest must read as 0: nothing was deleted by retention, so offset 0
        #    is still durable in remote. The control-plane log_start_offset advanced
        #    to the pruned diskless interval start; recovery must not adopt that as
        #    the whole-log start.
        earliest = verifier.wait_for_offset(self.TOPIC, time_spec=-2)
        diskless_log_start = verifier.min_log_start_offset(self.TOPIC)
        self.logger.info("After wipe+restart: earliest offset=%d, diskless log_start_offset=%d"
                         % (earliest, diskless_log_start))
        assert earliest == 0, (
            "earliest offset is %d, not 0 after wipe+restart; diskless log_start_offset=%d. The "
            "consolidated remote prefix below the diskless start is no longer readable."
            % (earliest, diskless_log_start))

        # 2) earliest=0 is only advertised; verify a fetch from 0 actually returns
        #    offset 0 from remote rather than silently skipping to the diskless start.
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=0)
        self.logger.info("First offset actually served by a fetch from 0: %d "
                         "(diskless log_start_offset=%d)" % (first_served, diskless_log_start))
        assert first_served == 0, (
            "a fetch from offset 0 returned offset %d, not 0; diskless log_start_offset=%d. The "
            "consolidated remote prefix [0, diskless_start) is durable but not served."
            % (first_served, diskless_log_start))

        # 2b) A bounded read from 0 is strictly contiguous with matching content: the single
        #     producer wrote its sequence number as each value, so value == offset. Catches
        #     gaps/dupes/reorder/corruption that the aggregate count below cannot.
        spot_count = min(acked, 20000)
        spot_records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=0, max_messages=spot_count)
        assert len(spot_records) >= spot_count, (
            "bounded read from 0 returned only %d of %d records from remote; part of the "
            "consolidated prefix is not readable." % (len(spot_records), spot_count))
        for i, (offset, value) in enumerate(spot_records[:spot_count]):
            assert offset == i, (
                "non-contiguous read from 0 at position %d: offset=%d (gap/dupe/reorder)." % (i, offset))
            assert value == offset, (
                "content mismatch at offset %d: value=%d; the record served from remote does "
                "not match what was produced." % (offset, value))
        self.logger.info("Bounded contiguous read from 0 OK: %d records with correct content"
                         % spot_count)

        # 3) End-to-end: read every record back from remote + control plane.
        #
        # Do NOT pass on_record_consumed: it forces VerifiableConsumer --verbose
        # (one JSON line/record) which the SSH worker can't keep up with for 300k
        # records; the periodic records_consumed summaries still drive total_consumed().
        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC,
                                      group_id="read-from-remote-%s" % uuid.uuid4().hex[:8],
                                      reset_policy="earliest")
        consumer.start()
        wait_until(lambda: consumer.total_consumed() >= acked or consumer.worker_errors,
                   timeout_sec=240, backoff_sec=1,
                   err_msg=("Consumer read back only %d of %d records after wiping local logs; "
                            "remaining records must be served from remote storage."
                            % (consumer.total_consumed(), acked)))
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        total = consumer.total_consumed()
        consumer.free()
        self.logger.info("Read back %d records from remote after wiping local logs (acked=%d)" %
                         (total, acked))
        assert total >= acked, \
            "Data loss after wiping local logs: expected >= %d records from remote but got %d" % (acked, total)
