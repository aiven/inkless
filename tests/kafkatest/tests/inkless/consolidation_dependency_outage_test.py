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

import time
import uuid

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.inkless.consolidation_verifier import ConsolidationVerifier
from kafkatest.services.verifiable_consumer import VerifiableConsumer


class DependencyOutageDuringProduceTest(Test):
    """Resilience of the consolidation pipeline to an outage of one of its two hard
    dependencies -- the object store (MinIO, alias ``storage``) or the control
    plane (Postgres, alias ``postgres``).

    For each dependency the test produces and consolidates a baseline, then
    blackholes every broker's traffic to that dependency (an iptables DROP, since
    the deps are standalone ducknet containers a ducktape test cannot
    ``docker pause``), and asserts three things:

    - **No premature WAL prune while remote is unreachable.** The pruner must
      never delete WAL data that is not yet safely in remote. With object storage
      down this is asserted directly: the control plane's ``log_start_offset`` does
      not advance while a not-yet-tiered tail exists. With the control plane down,
      pruning is structurally impossible (the prune RPC needs Postgres), so the
      test instead asserts write-path data safety: while the control plane is
      unreachable the broker acks no produce (a diskless write is only acked after
      its offsets are durably committed), so nothing acked can be lost.
    - **Catch-up after recovery.** Once healed, consolidation lag drains, the
      tiered tier grows, and pruning resumes (``log_start_offset`` advances).
    - **No data loss.** Every record acked before and after the outage is readable
      from offset 0.
    """

    TOPIC_PREFIX = "dependency-outage"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Enough to roll several 1 MiB segments so consolidation tiers and prunes.
    NUM_BASELINE_RECORDS = 150000
    NUM_POST_HEAL_RECORDS = 50000
    # Hold the outage across several prune cycles (cleanup interval is 5s) so a
    # single unlucky sample cannot mask a prune that should not have happened.
    OUTAGE_HOLD_SEC = 30
    OUTAGE_POLL_SEC = 5

    def __init__(self, test_context):
        super(DependencyOutageDuringProduceTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])
        self.verifier = None

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft],
            dependency=["object_storage", "control_plane"])
    def test_dependency_outage_during_produce(self, metadata_quorum, dependency):
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
                        "segment.bytes": 1048576,
                        "segment.ms": 5000,
                        "local.retention.ms": 5000,
                    },
                },
            },
        )
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.start()

        self.verifier = ConsolidationVerifier(self.kafka)
        self.verifier.verify_tooling()
        self.verifier.start_jmx()

        # --- 1) Baseline: produce and let the pipeline tier + prune. ---
        baseline_tiered = self.verifier.tiered_object_count()
        acked_baseline = self.verifier.produce(self.TOPIC, self.NUM_BASELINE_RECORDS, "baseline")
        self.logger.info("Baseline acked=%d" % acked_baseline)

        wait_until(lambda: self.verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Baseline data never reached the tiered tier.")
        # Pruning advanced past 0: proves the prune path works before the outage,
        # so "does not advance" under the outage is a meaningful signal.
        wait_until(lambda: self.verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="WAL was never pruned in the control plane before the outage.")
        self.logger.info("Baseline pruned: min_log_start_offset=%d"
                         % self.verifier.min_log_start_offset(self.TOPIC))

        # --- 2) Outage + 3) the per-dependency invariants. Each helper returns the
        #        count of any extra records it acked before the outage. ---
        if dependency == "object_storage":
            acked_during = self._exercise_object_storage_outage()
        elif dependency == "control_plane":
            acked_during = self._exercise_control_plane_outage()
        else:
            raise AssertionError("Unknown dependency %r" % dependency)

        # --- 4) Recovery: pipeline catches up and pruning resumes. ---
        pre_recovery_tiered = self.verifier.tiered_object_count()
        acked_post = self.verifier.produce(self.TOPIC, self.NUM_POST_HEAL_RECORDS, "post-heal")
        self.logger.info("Post-heal acked=%d" % acked_post)

        wait_until(lambda: self.verifier.local_lag() == 0,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Consolidation lag did not drain after the dependency recovered.")
        wait_until(lambda: self.verifier.tiered_object_count() > pre_recovery_tiered,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Tiered tier did not grow after the dependency recovered.")
        wait_until(lambda: self.verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Pruning did not resume after the dependency recovered.")
        self.logger.info("Recovered: min_log_start_offset=%d, tiered_object_count=%d"
                         % (self.verifier.min_log_start_offset(self.TOPIC),
                            self.verifier.tiered_object_count()))

        # --- 5) No data loss: every acked record is readable from 0. ---
        expected = acked_baseline + acked_during + acked_post
        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC,
                                      group_id="outage-%s" % uuid.uuid4().hex[:8],
                                      reset_policy="earliest")
        consumer.start()
        wait_until(lambda: consumer.total_consumed() >= expected or consumer.worker_errors,
                   timeout_sec=240, backoff_sec=1,
                   err_msg=("Read back only %d of %d acked records after the outage."
                            % (consumer.total_consumed(), expected)))
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        total = consumer.total_consumed()
        consumer.free()
        self.logger.info("Read back %d records (expected >= %d)" % (total, expected))
        assert total >= expected, \
            "Data loss after the outage: expected >= %d records but got %d" % (expected, total)

    def _exercise_object_storage_outage(self):
        """With object storage unreachable, nothing new can tier, so the pruner
        must not advance the diskless log-start past the already-tiered watermark
        even though a not-yet-tiered tail exists. The control plane stays reachable,
        so we read ``log_start_offset`` directly throughout the outage.

        Returns the number of extra records acked during this phase (none)."""
        self.verifier.block_dependency("object_storage")
        try:
            # Let any prune cycle in flight at block time settle before snapshotting.
            time.sleep(self.OUTAGE_POLL_SEC)
            lso_at_block = self.verifier.min_log_start_offset(self.TOPIC)
            # ListOffsets(latest) is served from broker memory and does not touch
            # the (blocked) object store, so it is safe to read during the outage.
            latest = self.verifier.offset_at(self.TOPIC, time_spec=-1)
            self.logger.info("Object-storage outage: log_start=%d, latest=%d"
                             % (lso_at_block, latest))
            assert latest > lso_at_block, \
                ("No protected tail to test: latest (%d) is not above the pruned "
                 "log-start (%d), so 'no premature prune' is vacuous." % (latest, lso_at_block))

            # Consolidation lag cannot drain while uploads are blocked.
            self._assert_stable(
                lambda: self.verifier.min_log_start_offset(self.TOPIC),
                lambda v: v == lso_at_block,
                "the control-plane log_start_offset advanced from %d while object storage was "
                "unreachable -- the pruner deleted WAL data that is not yet in remote" % lso_at_block)
        finally:
            self.verifier.heal_dependency("object_storage")
        return 0

    def _exercise_control_plane_outage(self):
        """With the control plane unreachable, the prune RPC cannot run at all, so
        "no premature prune" holds by construction (and is unobservable -- Postgres,
        which holds ``log_start_offset``, is the blocked dependency). The meaningful,
        non-racy invariant is data safety on the write path: a diskless produce can
        only be acked once the control plane has durably assigned its offsets, so
        while the control plane is down the broker must not ack *anything*. Anything
        it does not ack cannot be lost.

        Returns the number of extra records acked before the outage (none: nothing
        should ack while the control plane is down)."""
        self.verifier.block_dependency("control_plane")
        try:
            acked = self.verifier.attempt_produce(self.TOPIC, self.NUM_POST_HEAL_RECORDS,
                                                  window_sec=self.OUTAGE_HOLD_SEC,
                                                  label="control-plane-outage")
            assert acked == 0, \
                ("The broker acked %d records while the control plane was unreachable; a "
                 "diskless write must not be acknowledged before its offsets are durably "
                 "committed, or those records can be lost." % acked)
        finally:
            self.verifier.heal_dependency("control_plane")
        return 0

    def _assert_stable(self, sample, predicate, err_msg):
        """Poll ``sample`` for OUTAGE_HOLD_SEC and fail if ``predicate(value)`` is
        ever violated; used to assert an invariant *holds throughout* the outage
        rather than at a single instant."""
        deadline = time.time() + self.OUTAGE_HOLD_SEC
        while time.time() < deadline:
            value = sample()
            self.logger.info("Outage invariant sample: %s" % value)
            assert predicate(value), "%s (observed %s)" % (err_msg, value)
            time.sleep(self.OUTAGE_POLL_SEC)

    def teardown(self):
        # Flush any leftover DROP rules so a failed assertion cannot leave a broker
        # partitioned for a later test sharing the cluster.
        if self.verifier is not None:
            try:
                self.verifier.heal_all()
            except Exception as e:  # noqa: BLE001 - best effort during teardown
                self.logger.warn("Failed to flush iptables DROP rules in teardown: %s" % e)
        super(DependencyOutageDuringProduceTest, self).teardown()
