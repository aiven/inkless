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
    """Tier-1 test: verify the consolidation pipeline of a born-consolidated topic.

    A consolidating topic stores data in object storage via the diskless WAL,
    then a pipeline drains it: diskless WAL -> local log -> tiered/remote storage,
    after which the WAL is pruned. The generic Group A tests prove produce/consume
    still works, but assert nothing about this pipeline. This test does:

      - JMX:  the io.aiven.inkless.consolidation gauges are exported and scraped,
              and ConsolidationLocalLag is readable and settles to 0. We do not
              assert the lag rose above 0 -- it is not reliably observable here
              (see the NOTE on JMX). The peaks are logged for diagnostics.
      - MinIO: the tiered-storage/ prefix grows (data physically moved to remote).
      - Postgres: WAL `batches` rows are pruned / `log_start_offset` advances.
      - Correctness: every acked record is still consumable afterwards.

    Requires `--globals` with `consolidation=true` and the inkless Postgres/MinIO
    dependencies started by tests/docker/run_consolidation_tests.sh.

    NOTE on JMX: the consolidation gauges are fetch-backlog gauges -- they are
    only updated inside ConsolidationFetcherThread.processPartitionData (i.e.
    while the fetcher is actively pulling WAL data) and measure how far behind
    the fetcher is *right now*. We therefore only assert that the gauges are
    exported and scraped (and that ConsolidationLocalLag settles to 0), not that
    they ever rose above 0, because that is not reliably observable here:
      - ConsolidationLocalLag tends to ~0. Even with an unthrottled producer the
        diskless write path (object-store WAL + min.insync.replicas) caps the
        effective rate to a few thousand rec/s, which the WAL->local fetcher
        generally keeps up with, so the local backlog rarely grows between the
        1s JMX samples.
      - ConsolidationTotalLag/DeletableMessages only update on a fetch while
        remoteLogEndOffset >= 0, but remote tiering tends to make remoteLEO >= 0
        only after the fetcher has caught up and stopped fetching, so they are
        unlikely to be sampled non-zero.
    The actual observed peaks are logged each run for diagnostics.
      - We start JmxTool BEFORE producing. The broker-level aggregate gauges are
        registered at ReplicaManager startup (independent of any topic), so
        JmxTool's --wait succeeds immediately and we sample the whole run.
      - JmxTool runs on the brokers only. Wiring it into KafkaService
        construction propagates the object names to the isolated KRaft
        controller, which never exposes these MBeans, so --wait would hang
        controller startup; verifier.start_jmx() targets broker nodes only.
    """

    # The Postgres/MinIO control plane persists across runs (the dependency
    # containers are not torn down between test runs). Querying the WAL/prune
    # state by topic_name would then aggregate over stale rows from previous
    # runs (e.g. a run that created the topic but never pruned leaves a
    # log_start_offset=0 row), so min(log_start_offset) is dragged to 0 forever.
    # Use a fresh topic name per run so the control-plane queries see only this
    # run's partition. (Resolved to a unique value in __init__.)
    TOPIC_PREFIX = "consolidation-pipeline"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Must be large enough that the local log exceeds segment.bytes (1 MiB floor)
    # several times so segments roll *by size* during the produce burst. Only
    # closed (rolled) segments are tiered to remote; a smaller dataset stays in a
    # single active segment that never rolls (the burst ends before segment.ms
    # can trigger a time-based roll, which is only evaluated on a later append),
    # so nothing would ever be tiered. ~13 bytes/record => 300k records ~= 3.8 MiB
    # ~= 3+ rolled segments.
    NUM_RECORDS = 300000

    def __init__(self, test_context):
        super(ConsolidationPipelineTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_consolidation_pipeline(self, metadata_quorum):
        if not self.test_context.globals.get("consolidation", False):
            self.logger.warn("Skipping ConsolidationPipelineTest: requires '--globals "
                             "consolidation=true' and the inkless Postgres/MinIO dependencies.")
            return

        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            # Drive the WAL pruner and file cleaner fast enough to observe in-test.
            # The broker enforces consume.batch.coordinate.cache.ttl.ms <=
            # file.cleaner.retention.period.ms / 2 (see SharedState), so the cache
            # TTL is lowered alongside the retention period.
            server_prop_overrides=[
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
            ],
            # NOTE: JMX is intentionally not wired in here. KafkaService propagates
            # jmx_object_names to the isolated KRaft controller, which never exposes
            # the consolidation MBeans, so JmxTool --wait would hang controller
            # startup. We start JmxTool on the brokers later via verifier.start_jmx().
            topics={
                self.TOPIC: {
                    "partitions": self.NUM_PARTITIONS,
                    "replication-factor": self.REPLICATION_FACTOR,
                    "configs": {
                        "diskless.enable": "true",
                        "remote.storage.enable": "true",
                        "min.insync.replicas": 2,
                        # Roll segments by size and time so they close and get tiered
                        # to remote within the test window (only closed segments tier).
                        # segment.bytes has a 1 MiB floor enforced by LogConfig.
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

        # Start JmxTool on the brokers BEFORE producing. The broker-level
        # aggregate ConsolidationMetrics gauges are registered at ReplicaManager
        # startup (independent of any topic), so JmxTool's --wait succeeds
        # immediately and we capture the whole produce window. (Still brokers
        # only -- the isolated KRaft controller never exposes these MBeans; see
        # start_jmx().) Starting before produce means we sample across the whole
        # run, which is the best chance of observing any transient lag.
        verifier.start_jmx()

        # --- produce a known dataset ---
        # Unthrottled (throughput=-1) just to finish quickly; the diskless write
        # path (object-store WAL + min.insync.replicas) caps the effective rate
        # anyway. NUM_RECORDS is sized so the local log crosses the 1 MiB
        # segment.bytes floor several times and rolls segments (only closed
        # segments tier) -- see NUM_RECORDS.
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
        self.logger.info("Produced and acked %d records" % acked)

        # --- the pipeline drains: diskless WAL -> local log ---
        # Only gate on local lag: it reliably returns to 0 once the fetcher
        # catches up. total_lag/deletable are updated solely on fetch events and
        # can freeze at a non-zero value after the fetcher idles, so gating on
        # them would hang.
        wait_until(lambda: verifier.local_lag() == 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Consolidation local lag did not drain to 0.")
        self.logger.info("Consolidation local lag drained to 0")

        # Data physically moved to remote: the tiered-storage prefix grew.
        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Tiered-storage object count did not grow above baseline.")
        self.logger.info("Tiered-storage object count grew to %d" % verifier.tiered_object_count())

        # --- WAL pruning in the control plane ---
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="WAL batches were not pruned in the Postgres control plane.")
        self.logger.info("Control plane pruned WAL: batch_count=%d, min_log_start_offset=%d" %
                         (verifier.wal_batch_count(self.TOPIC), verifier.min_log_start_offset(self.TOPIC)))

        self.logger.info("WAL object count after prune: %d" % verifier.wal_object_count())

        # --- the consolidation gauges are exposed via JMX and behave sanely ---
        # We assert the broker actually exports the io.aiven.inkless.consolidation
        # MBeans and that JmxTool scraped them (the key is present in the parsed
        # CSV), plus the local-lag drain gate above proved the gauge is readable
        # and settles to 0. We do NOT assert the lag ever rose above 0: that is
        # not reliably observable here. The diskless produce path (object-store
        # WAL + min.insync.replicas) caps throughput to a few thousand rec/s,
        # which the WAL->local fetcher keeps up with, so ConsolidationLocalLag
        # stays ~0; and ConsolidationTotalLag/DeletableMessages only update on a
        # fetch while remoteLogEndOffset >= 0, but remote tiering completes after
        # the fetcher has idled, so they are never sampled non-zero. The real
        # proof that the pipeline moved + pruned data is the MinIO growth and the
        # Postgres prune above, plus the consume-back below. The peaks are logged
        # for diagnostics only.
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

        # --- correctness: every acked record is still consumable (from remote) ---
        consumed = {"count": 0}

        def on_record(event, node):
            consumed["count"] += 1

        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC, group_id="consolidation-pipeline-group",
                                      on_record_consumed=on_record)
        consumer.start()
        wait_until(lambda: consumer.total_consumed() >= acked or consumer.worker_errors,
                   timeout_sec=180, backoff_sec=1,
                   err_msg="Consumer failed to read back all %d acked records." % acked)
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        self.logger.info("Read back %d records after consolidation + prune" % consumer.total_consumed())
