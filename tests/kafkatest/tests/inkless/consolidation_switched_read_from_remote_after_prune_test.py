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


class SwitchedReadFromRemoteAfterPruneTest(Test):
    """Durability test for a *switched* (classic -> diskless) consolidated topic.

    The switched-topic analogue of :class:`ReadFromRemoteAfterPruneTest`: offset 0 was
    written by the classic log under the classic epoch (the diskless interval carries
    ``last_classic_epoch + 1``), so serving it from remote after a local-log loss
    requires resolving the classic epoch for the boundary offset.

    Sequence: produce a classic prefix; switch to consolidating diskless (sealing at
    ``S``); produce + consolidate a diskless tail and prune the WAL above the seal; wait
    until the remote tier covers through the seal; then stop all brokers, wipe the
    partition dirs, and restart empty. Post-wipe, assert earliest reads as 0, a fetch
    from 0 is served, a window straddling the seal is contiguous with correct content,
    and a full read-back returns every record from remote + the un-pruned WAL.

    Requires at least one post-switch diskless append (its higher epoch rolls a fresh
    segment at the seal, closing + tiering the boundary classic segment). A partition
    that goes fully idle after the switch keeps its classic tail only on local disk;
    out of scope here.
    """

    # Unique per run: Postgres/MinIO persist across runs, so a stale name would
    # mix old rows into the control-plane queries.
    TOPIC_PREFIX = "switched-read-from-remote-after-prune"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Classic prefix: enough to roll several segments (1 MiB floor) so a large
    # contiguous part tiers and is locally deleted.
    NUM_CLASSIC_RECORDS = 150000
    # Diskless tail: ~13 B/record => ~4 MiB, exceeding segment.bytes (2 MiB) so the
    # tail rolls an inactive segment. Only rolled segments tier, so this is what lifts
    # highestOffsetInRemoteStorage past the seal and lets the WAL pruner advance.
    NUM_DISKLESS_RECORDS = 300000

    def __init__(self, test_context):
        super(SwitchedReadFromRemoteAfterPruneTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switched_read_from_remote_after_prune(self, metadata_quorum):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            consolidation=True,
            # log.diskless.enable=false: keep the cluster default classic (else new
            #   topics are born diskless and trip the diskless/remote validator).
            # diskless.allow.from.classic.enable=true: open the classic->diskless
            #   switch bridge (validated on every node).
            # The rest run the WAL pruner / file cleaner fast; cache TTL <= retention/2.
            server_prop_overrides=[
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
            ],
            # Switch-completion gauges live on the broker; the controller never
            # exposes them, so scraping it would hang start_jmx_tool's --wait.
            jmx_object_names=list(ConsolidationVerifier.SWITCH_JMX_OBJECT_NAMES),
            jmx_attributes=list(ConsolidationVerifier.SWITCH_JMX_ATTRIBUTES),
        )
        # server_prop_overrides only reach the brokers, but the controller also
        # validates the switch bridge: mirror it onto the controller and disable
        # JMX there (it has no switch gauges).
        if getattr(self.kafka, "isolated_controller_quorum", None):
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + [
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
            ]
            ctrl.jmx_object_names = None
            ctrl.jmx_attributes = []

        # Collect broker data-dir logs to debug a post-restart failure.
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.start()

        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()

        baseline_tiered = verifier.tiered_object_count()
        self.logger.info("Baseline tiered-storage object count: %d" % baseline_tiered)

        # --- 1) Classic phase: produce a classic prefix on a plain classic topic ---
        verifier.create_classic_topic(self.TOPIC, self.NUM_PARTITIONS, self.REPLICATION_FACTOR)

        classic_acked = verifier.produce(self.TOPIC, self.NUM_CLASSIC_RECORDS, "classic")
        seal_offset = classic_acked
        self.logger.info("Produced classic prefix: acked=%d (seal offset will be %d)"
                         % (classic_acked, seal_offset))

        # --- 2) Switch the classic topic straight to consolidating diskless ---
        #
        # One combined alter: the leader seals the classic log at `seal_offset` and
        # starts the consolidation fetcher; short local.retention.ms lets the tiered
        # classic prefix be deleted locally. Separate alters fail: remote-first trips
        # the mutual-exclusion validator; remote after a diskless-only switch is a
        # config-only delta that never starts consolidation.
        #
        # segment.bytes = 2 MiB (test-only; prod defaults to 1 GiB). It must be > the
        # 1 MiB batch ceiling, else a consolidation append (a batch up to max.request.size
        # 1 MiB + framing) fails with RecordBatchTooLargeException and kills the fetcher;
        # and < the diskless tail, so the tail rolls an inactive segment (only rolled
        # segments tier, which is what lets the WAL pruner advance past the seal.
        # segment.ms can't help, it only rolls on a later append this bounded tail lacks).
        self.logger.info("Switching topic %s to consolidating diskless (combined alter)" % self.TOPIC)
        self.kafka.alter_topic_configs(self.TOPIC, {
            "diskless.enable": "true",
            "remote.storage.enable": "true",
            "local.retention.ms": "5000",
            "segment.bytes": str(2 * 1024 * 1024),
        })
        verifier.wait_for_switch_complete(self.TOPIC, self.NUM_PARTITIONS)
        self.logger.info("Classic-to-consolidated switch completed for %s" % self.TOPIC)

        # --- 3) Tier the classic prefix ---
        #
        # The classic RLM uploads the closed classic segments [0, seal) (with their
        # classic epochs) to MinIO; local.retention.ms then deletes the local copies.
        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Classic tiered-storage object count did not grow above baseline "
                           "after enabling remote storage on the switched topic.")
        self.logger.info("Classic tiered-storage object count grew to %d"
                         % verifier.tiered_object_count())

        # earliest-local past 0 proves the closed classic segments tiered AND were
        # locally deleted. This is an intermediate signal; the gate that the whole
        # prefix (incl. the boundary tail) is durable is computed after the diskless
        # tail drives the boundary segment to roll (see step 4).
        classic_remote_watermark = verifier.wait_for_local_log_truncation(self.TOPIC)
        self.logger.info("Classic guaranteed-remote watermark (earliest-local): %d"
                         % classic_remote_watermark)
        assert classic_remote_watermark > 0, \
            "Classic prefix never tiered: earliest-local stayed at 0 after enabling remote storage."

        # --- 4) Diskless phase: produce the tail, consolidate, and prune the WAL ---
        #
        # Snapshot the tiered count BEFORE producing: consolidation tiers the diskless
        # interval during the produce, so a baseline taken afterwards could already
        # include the first diskless segment and the "grew above baseline" wait could
        # never fire.
        pre_diskless_tiered = verifier.tiered_object_count()
        self.logger.info("Tiered-storage object count before diskless tail: %d" % pre_diskless_tiered)

        diskless_acked = verifier.produce(self.TOPIC, self.NUM_DISKLESS_RECORDS, "diskless")
        self.logger.info("Produced diskless tail: acked=%d" % diskless_acked)

        # Consolidation tiered the diskless interval, growing the remote tier again.
        wait_until(lambda: verifier.tiered_object_count() > pre_diskless_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Diskless consolidation did not grow the tiered-storage object count.")
        self.logger.info("Tiered-storage object count grew to %d after consolidation"
                         % verifier.tiered_object_count())

        # The WAL pruner advances the diskless log-start above the seal (the diskless
        # interval, and thus the control-plane log_start_offset, begins at the seal).
        # The OR accepts either prune representation -- deleted batch rows (count -> 0)
        # or an advanced log_start_offset -- so it must stay an OR, not an AND (a prune
        # may only surface one). The count==0 leg can't fire prematurely here: the
        # tiered-count-grew gate above already proves batches were written, so 0 means
        # pruned, not "not yet produced" (wal_batch_count returns -1 when unregistered).
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > seal_offset,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Diskless WAL was not pruned above the seal in the control plane.")
        self.logger.info("Control plane pruned the diskless WAL: batch_count=%d, "
                         "min_log_start_offset=%d, seal=%d" %
                         (verifier.wal_batch_count(self.TOPIC),
                          verifier.min_log_start_offset(self.TOPIC),
                          seal_offset))

        # Wait until the remote tier covers through the seal (latest-tiered/-5 =
        # highestOffsetInRemoteStorage >= seal), proving the classic prefix incl. the
        # boundary tail is durable and recoverable after the wipe. Not earliest-local/-4:
        # that tracks local deletion (periodic retention sweep), not durability.
        remote_watermark = verifier.wait_for_tiered_offset_at_least(self.TOPIC, seal_offset)
        self.logger.info("Remote tier covers through the seal: latest-tiered=%d, seal=%d"
                         % (remote_watermark, seal_offset))
        assert remote_watermark >= seal_offset, (
            "latest-tiered (%d) did not reach the seal (%d): the classic prefix incl. the "
            "boundary tail is not durable in remote, so it would be lost on a local wipe."
            % (remote_watermark, seal_offset))

        # --- 5) Destroy every local copy, then read the classic remote prefix ---

        # Stop ALL brokers before wiping so no surviving replica serves the classic
        # prefix from a local log on restart. The controller and internal topics on
        # disk are left intact.
        self.logger.info("Stopping all brokers to wipe local copies of %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=120)

        for node in self.kafka.nodes:
            verifier.wipe_topic_local_logs(node, self.TOPIC)

        self.logger.info("Restarting all brokers with empty local logs for %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.start_node(node, timeout_sec=120)

        wait_until(lambda: verifier.partition_has_leader(self.TOPIC, 0),
                   timeout_sec=120, backoff_sec=2,
                   err_msg="Partition %s-0 did not get a leader after the restart." % self.TOPIC)

        # --- 6a) earliest must read as 0 (the true whole-log start) ---
        #
        # Nothing was deleted by retention, so the classic prefix at offset 0 is still
        # durable in remote. The control-plane log_start_offset advanced to the pruned
        # diskless interval start; recovery must not adopt that as the whole-log start.
        earliest = verifier.wait_for_offset(self.TOPIC, time_spec=-2)
        diskless_log_start = verifier.min_log_start_offset(self.TOPIC)
        self.logger.info("After wipe+restart: earliest offset=%d, diskless log_start_offset=%d, seal=%d"
                         % (earliest, diskless_log_start, seal_offset))
        assert earliest == 0, (
            "earliest offset is %d, not 0 after wipe+restart; diskless log_start_offset=%d, seal=%d. "
            "The classic remote prefix [0, seal) is no longer readable."
            % (earliest, diskless_log_start, seal_offset))

        # --- 6b) a fetch from offset 0 must actually return offset 0 from remote ---
        #
        # Switched-topic-specific: offset 0 lives in the classic interval under the
        # classic leader epoch, so serving it after the wipe needs the rebuild to
        # resolve that epoch and fetch the classic remote segment.
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=0)
        self.logger.info("First offset actually served by a fetch from 0: %d "
                         "(diskless log_start_offset=%d, seal=%d)"
                         % (first_served, diskless_log_start, seal_offset))
        assert first_served == 0, (
            "a fetch from offset 0 returned offset %d, not 0 (seal=%d). The classic remote prefix "
            "is durable but not served; the rebuild did not resolve the classic epoch."
            % (first_served, seal_offset))

        # --- 6c) a bounded contiguous read from 0 stays in the classic remote tier ---
        #
        # Cap the read well below both the seal and the watermark so these records come
        # only from the classic remote tier.
        read_count = min(remote_watermark, 20000)
        first_offset, num_read = verifier.read_contiguous_from(self.TOPIC, from_offset=0,
                                                              max_messages=read_count)
        self.logger.info("Bounded contiguous read from 0: first_offset=%d, num_read=%d (requested %d)"
                         % (first_offset, num_read, read_count))
        assert first_offset == 0, (
            "bounded read from offset 0 started at %d, not 0." % first_offset)
        assert num_read >= read_count, (
            "bounded read from 0 returned only %d of %d records below the remote watermark (%d): "
            "part of the tiered classic prefix is not readable from remote."
            % (num_read, read_count, remote_watermark))

        # --- 6d) contiguous read straddling the seal: no gaps/dupes + correct content ---
        #
        # Frame a window around the seal, entirely below remote_watermark so every record
        # is served from remote/WAL. Assert the offsets are strictly contiguous across
        # the classic->diskless boundary (no gap, dupe, or reorder) and that each value
        # matches what was produced: VerifiableProducer writes the per-producer sequence
        # as the value, and a fresh producer wrote the diskless tail, so the value series
        # resets to 0 exactly at the seal.
        window = 5000
        boundary_start = max(0, seal_offset - window)
        boundary_count = min(2 * window, remote_watermark - boundary_start)
        assert boundary_start < seal_offset < boundary_start + boundary_count, (
            "cannot frame a cross-boundary read: seal=%d, start=%d, count=%d, watermark=%d"
            % (seal_offset, boundary_start, boundary_count, remote_watermark))
        boundary_records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=boundary_start, max_messages=boundary_count)
        assert len(boundary_records) >= boundary_count, (
            "cross-boundary read returned only %d of %d records spanning the seal (%d); "
            "part of the classic->diskless boundary is not readable from remote."
            % (len(boundary_records), boundary_count, seal_offset))
        for i, (offset, value) in enumerate(boundary_records[:boundary_count]):
            expected_offset = boundary_start + i
            assert offset == expected_offset, (
                "non-contiguous read across the seal at position %d: offset=%d, expected=%d "
                "(gap/dupe/reorder near seal=%d)." % (i, offset, expected_offset, seal_offset))
            expected_value = offset if offset < seal_offset else offset - seal_offset
            assert value == expected_value, (
                "content mismatch at offset %d: value=%d, expected=%d (seal=%d); the record "
                "served from remote does not match what was produced."
                % (offset, value, expected_value, seal_offset))
        self.logger.info("Cross-boundary read OK: %d contiguous records [%d, %d) with correct "
                         "content across seal=%d" % (boundary_count, boundary_start,
                          boundary_start + boundary_count, seal_offset))

        # --- 6e) full read-back: every record returns from remote + WAL ---
        #
        # Read it all back: the classic prefix (remote) and the diskless interval (remote +
        # un-pruned WAL) must survive the wipe. Periodic VerifiableConsumer summaries drive
        # total_consumed (--verbose can't be sustained for 100k+ records).
        expected_total = self.NUM_CLASSIC_RECORDS + self.NUM_DISKLESS_RECORDS
        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC,
                                      group_id="switched-read-from-remote-%s" % uuid.uuid4().hex[:8],
                                      reset_policy="earliest")
        consumer.start()
        wait_until(lambda: consumer.total_consumed() >= expected_total or consumer.worker_errors,
                   timeout_sec=240, backoff_sec=1,
                   err_msg=("Consumer read back only %d of %d records after wiping local logs; "
                            "the classic prefix and diskless interval must both be served from "
                            "remote/WAL." % (consumer.total_consumed(), expected_total)))
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        total = consumer.total_consumed()
        consumer.free()
        self.logger.info("Read back %d records after wiping local logs (expected >= %d: classic %d + diskless %d)"
                         % (total, expected_total, self.NUM_CLASSIC_RECORDS, self.NUM_DISKLESS_RECORDS))
        assert total >= expected_total, (
            "Data loss after wiping local logs: expected >= %d records (classic %d + diskless %d) "
            "from remote/WAL but got %d." % (expected_total, self.NUM_CLASSIC_RECORDS,
             self.NUM_DISKLESS_RECORDS, total))
