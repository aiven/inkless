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
    """Consolidation pipeline resilience to an outage of one of its two hard
    dependencies: the object store (MinIO, alias ``storage``) or the control plane
    (Postgres, alias ``postgres``).

    Per dependency: produce and consolidate a baseline, blackhole every broker's
    traffic to that dependency with an iptables DROP (the deps are standalone
    ducknet containers, so a ducktape test can't docker-pause them), and check:

    - No premature WAL prune while remote is unreachable. Object storage down:
      log_start_offset must not advance while an un-tiered tail exists. Control
      plane down: pruning can't run at all (it needs Postgres), so instead assert
      the broker acks no produce -- a diskless write is acked only once its offsets
      are durably committed, so nothing acked can be lost.
    - Catch-up after recovery: lag drains, the tiered tier grows, pruning resumes.
    - No data loss: every record acked before and after the outage is readable
      from offset 0.
    """

    TOPIC_PREFIX = "dependency-outage"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Enough to roll several 1 MiB segments so consolidation tiers and prunes.
    NUM_BASELINE_RECORDS = 150000
    NUM_POST_HEAL_RECORDS = 50000
    # Short produce right before blocking object storage to guarantee an
    # un-tiered tail (latest > latest_tiered) at snapshot time.
    NUM_PRE_OUTAGE_TAIL_RECORDS = 500
    # Hold the outage across several 5s prune cycles so one lucky sample can't
    # mask a prune that shouldn't have happened.
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
        # Confirm pruning works before the outage, so "does not advance" during
        # it is a meaningful signal.
        wait_until(lambda: self.verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="WAL was never pruned in the control plane before the outage.")
        self.logger.info("Baseline pruned: min_log_start_offset=%d"
                         % self.verifier.min_log_start_offset(self.TOPIC))

        # --- 2/3) Outage + per-dependency invariants. Each helper returns how many
        #          extra records it acked during the phase. ---
        if dependency == "object_storage":
            acked_during = self._exercise_object_storage_outage()
        elif dependency == "control_plane":
            acked_during = self._exercise_control_plane_outage()
        else:
            raise AssertionError("Unknown dependency %r" % dependency)

        # --- 4) Recovery: pipeline catches up and pruning resumes. ---
        pre_recovery_tiered = self.verifier.tiered_object_count()
        # Snapshot log-start so the post-recovery check requires it to advance;
        # ">0" alone is vacuous since baseline pruning already passed 0.
        pre_recovery_lso = self.verifier.min_log_start_offset(self.TOPIC)
        acked_post = self.verifier.produce(self.TOPIC, self.NUM_POST_HEAL_RECORDS, "post-heal")
        self.logger.info("Post-heal acked=%d (pre-recovery min_log_start_offset=%d)"
                         % (acked_post, pre_recovery_lso))

        wait_until(lambda: self.verifier.local_lag() == 0,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Consolidation lag did not drain after the dependency recovered.")
        wait_until(lambda: self.verifier.tiered_object_count() > pre_recovery_tiered,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Tiered tier did not grow after the dependency recovered.")
        wait_until(lambda: self.verifier.min_log_start_offset(self.TOPIC) > pre_recovery_lso,
                   timeout_sec=240, backoff_sec=2,
                   err_msg=("Pruning did not resume after the dependency recovered: "
                            "min_log_start_offset stayed at %d." % pre_recovery_lso))
        self.logger.info("Recovered: min_log_start_offset=%d, tiered_object_count=%d"
                         % (self.verifier.min_log_start_offset(self.TOPIC),
                            self.verifier.tiered_object_count()))

        # --- 5) No data loss: every acked record is readable from 0. ---
        # Track unique consumed offsets, not total_consumed(), so a rebalance/retry
        # that re-reads duplicates can't inflate the count and mask a gap.
        expected = acked_baseline + acked_during + acked_post
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

        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC,
                                      group_id="outage-%s" % uuid.uuid4().hex[:8],
                                      reset_policy="earliest",
                                      on_record_consumed=on_record_consumed)
        consumer.start()
        wait_until(lambda: (max_offset["value"] >= expected - 1
                            and len(seen_offsets) >= expected) or consumer.worker_errors,
                   timeout_sec=240, backoff_sec=1,
                   err_msg="Consumer did not read back all %d acked records within timeout." % expected)
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        total = consumer.total_consumed()
        consumer.free()
        self.logger.info("Read back %d records, %d unique offsets (expected %d)"
                         % (total, len(seen_offsets), expected))
        # Every acked offset [0..expected-1] must be present (no gaps = no loss).
        # Extras above expected-1 are legitimate: un-acked outage-window records
        # the broker held in the WAL can commit after recovery (at-least-once).
        # == would flake on those; total_consumed() >= expected masks gaps.
        missing = set(range(expected)) - seen_offsets
        assert not missing, \
            ("Data loss after the outage: %d acked offsets missing from read-back "
             "(expected %d unique [0..%d], saw %d unique, max=%d, first missing=%d)"
             % (len(missing), expected, expected - 1, len(seen_offsets),
                max_offset["value"], min(missing)))

    def _exercise_object_storage_outage(self):
        """Object storage unreachable: no new data can tier, so the tiered watermark
        (highestOffsetInRemoteStorage) is frozen. The pruner may advance log_start
        to watermark + 1 but not past (would drop WAL not yet in remote).
        Inclusivity: highestOffsetInRemoteStorage is inclusive, log_start_offset is
        exclusive, so post-prune state is log_start == watermark + 1 and the
        contract is log_start - 1 <= watermark. A pre-block produce keeps an
        un-tiered tail so the check isn't vacuous.

        Returns the pre-block tail count, counted toward no-data-loss."""
        # Produce a small un-tiered tail before blocking so latest > latest_tiered
        # at snapshot time. Stays in the open segment, can't tier before the block.
        tail_acked = self.verifier.produce(self.TOPIC, self.NUM_PRE_OUTAGE_TAIL_RECORDS,
                                           "pre-outage-tail")
        self.verifier.block_dependency("object_storage")
        try:
            # Let any in-flight prune cycle settle before snapshotting.
            time.sleep(self.OUTAGE_POLL_SEC)
            # latest-tiered (-5) is highestOffsetInRemoteStorage; latest (-1) is the log
            # end. Both come from broker memory, so they're safe to read while the store
            # is blocked. wait_for_offset retries until the get-offsets output parses
            # (offset_at returns -1 until then), so the precondition isn't flaky. Both are
            # partition 0, while min_log_start_offset is a min over partitions -- comparing
            # them is coherent only because NUM_PARTITIONS == 1.
            tiered_at_block = self.verifier.wait_for_offset(self.TOPIC, time_spec=-5)
            latest = self.verifier.wait_for_offset(self.TOPIC, time_spec=-1)
            self.logger.info("Object-storage outage: latest_tiered=%d, latest=%d, log_start=%d"
                             % (tiered_at_block, latest,
                                self.verifier.min_log_start_offset(self.TOPIC)))
            assert latest > tiered_at_block, \
                ("No un-tiered tail to protect: latest (%d) is not above the tiered watermark "
                 "(%d), so 'no premature prune' is vacuous." % (latest, tiered_at_block))

            # Re-read the watermark each poll and track its running max: it cannot rise
            # from new uploads while blocked, but a pre-block copy may update it lazily.
            watermark = {"max": tiered_at_block}

            def _within_watermark(log_start):
                current = self.verifier.offset_at(self.TOPIC, time_spec=-5)
                if current < 0:
                    # Unparseable get-offsets output; keep last known watermark.
                    # Stale = stricter predicate = false positive, not false negative.
                    self.logger.info("watermark re-read returned %s during outage; "
                                     "keeping max at %d" % (current, watermark["max"]))
                elif current > watermark["max"]:
                    watermark["max"] = current
                # Post-prune state is log_start == watermark + 1; do not tighten
                # to <= watermark (off by one, fails once the pruner catches up).
                return log_start - 1 <= watermark["max"]

            self._assert_stable(
                lambda: self.verifier.min_log_start_offset(self.TOPIC),
                _within_watermark,
                "log_start_offset advanced past watermark + 1 while object storage was "
                "unreachable: exclusive WAL prune frontier exceeded inclusive "
                "last-in-remote offset + 1, pruner dropped WAL data not yet in remote")
        finally:
            self.verifier.heal_dependency("object_storage")
        return tail_acked

    def _exercise_control_plane_outage(self):
        """Control plane unreachable: the prune RPC can't run, and log_start_offset
        lives in the blocked Postgres, so "no premature prune" is neither at risk nor
        observable here. The testable invariant is write-path safety: a diskless
        produce is acked only after the control plane durably assigns its offsets, so
        the broker must ack nothing while Postgres is down.

        Returns the number of extra records acked during the outage (none)."""
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
        ever violated, i.e. assert the invariant holds throughout the outage, not
        just at one instant."""
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
