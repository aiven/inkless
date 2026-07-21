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


class DeleteRecordsAcrossTiersTest(Test):
    """``DeleteRecords`` on a *switched* (hybrid) consolidating topic must advance the
    topic's earliest readable offset to *exactly* the requested boundary, in a way every
    broker agrees on, and must not reclaim any records the caller did not ask to delete.
    This guards two distinct product bugs -- Problem A (cross-broker EARLIEST
    consistency) and Problem B (no over-reclaim of the classic prefix); see
    ``DELETE_RECORDS_DISKLESS_ROUTING_IMPL.md`` at the repo root for the full design.

    Problem A -- cross-broker EARLIEST consistency. ``ListOffsets(EARLIEST)`` for a
    consolidating partition used to be served from the broker-local classic
    ``UnifiedLog.logStartOffset`` while it was still below the seal. With managed
    replicas enabled (``diskless.managed.rf.enable=true``, which consolidation
    requires) ``InklessTopicMetadataTransformer`` advertises a *hash-selected replica*
    as the partition leader for locality-aware reads -- deterministically the same
    replica for every client, and generally a *follower*, not the real KRaft leader
    that holds the writable log. That follower's local classic log start is frozen at
    the switch, so a ``DeleteRecords`` (which advances only the real leader's log start
    and the control plane) left the client-visible earliest stuck at ``0`` forever. The
    fix routes ``ListOffsets(EARLIEST)`` for consolidating topics to the control plane
    on *every* broker (``COALESCE(remote_log_start_offset, log_start_offset)`` from
    ``list_offsets_v1``), which the classic leader keeps current, so the earliest
    advances on whichever replica the transformer routed the client to, and is one
    broker-agnostic value.

    Problem B -- no over-reclaim of the classic prefix. With retention unset, a
    ``DeleteRecords`` before ``delete_before < S`` must remove only ``[0, delete_before)``
    and leave ``[delete_before, S)`` readable, so the earliest settles at *exactly*
    ``delete_before``. The consolidation cleanup used to reclaim the whole classic prefix
    ``[0, S)`` down to the seal instead: a freshly-elected leader's local
    ``logStartOffset`` is pinned at ``S`` (and only ever increments), so the RLM
    remote-retention reclaim floor and the become-leader report used ``S`` rather than
    the true cross-tier earliest. The fix drives both the consolidating partition's
    whole-log start (``DisklessLeaderEndPoint``) and the RLM reclaim floor /
    become-leader report (``RemoteLogManager``) from the broker-agnostic control-plane
    cross-tier earliest via ``ReplicaManager.crossTierEarliestOffset``.

    The scenario: produce a classic prefix, switch to a consolidating diskless topic
    (sealing at ``S``), produce + consolidate a diskless tail, and let the classic
    prefix tier to remote with its local copies evicted -- so the prefix ``[0, S)``
    lives in the remote tier. ``retention.ms``/``retention.bytes`` are held unset
    throughout so the only thing that can move the earliest offset is the
    ``DeleteRecords`` request. A ``DeleteRecords`` before ``delete_before = S // 2``
    (inside that remote prefix) must then:

    - return the requested boundary as the partition's new log start,
    - advance the client-visible earliest to *exactly* ``delete_before``, identical on
      every broker (served from the control plane, not a replica's local log), and
    - leave the surviving prefix ``[delete_before, S)`` readable and contiguous with the
      originally produced content.

    Leader routing for the write side of ``DeleteRecords``: the op is leader-routed and
    its local leg requires the real KRaft leader, so the broker the transformer points
    the admin client at forwards the affected partitions to their real leader
    (``DisklessDeleteRecordsForwarder``); the leader runs both the local-log and the
    diskless (control-plane) legs authoritatively.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a stale
    # topic name would let old rows/objects skew the control-plane and mc queries.
    TOPIC_PREFIX = "delete-records-across-tiers"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Classic prefix. segment.bytes is floored at 1 MiB and records are ~13 B, so a
    # closed classic segment holds ~80k records. The delete boundary (seal // 2) must
    # sit strictly inside the sealed classic region so the reclaimed range is
    # remote-backed and the classic log start stays below the seal. Sizing it this way
    # also clears whole ~1 MiB segments below the boundary while leaving several whole
    # surviving segments in [delete_before, seal) -- so a correct implementation drops
    # only the reclaimed ones and keeps the rest (the Problem B over-reclaim guard).
    NUM_CLASSIC_RECORDS = 400000
    # Diskless tail: ~13 B/record => ~4 MiB, exceeding segment.bytes (2 MiB) so the
    # tail rolls an inactive segment. Only rolled segments tier, so this is what lifts
    # highestOffsetInRemoteStorage past the seal and lets the WAL pruner advance.
    NUM_DISKLESS_RECORDS = 300000
    # How many surviving-prefix records to spot-check for readability/content.
    SPOT_CHECK = 20000

    def __init__(self, test_context):
        super(DeleteRecordsAcrossTiersTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    def _start_cluster(self):
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
            # The rest run the WAL pruner / file cleaner / remote-log task fast.
            server_prop_overrides=[
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
                ["remote.log.manager.task.interval.ms", "5000"],
                ["log.retention.check.interval.ms", "5000"],
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
        self.kafka.start()

    def _switch_and_drain(self, verifier, baseline_tiered):
        """Produce a classic prefix, switch to consolidating diskless, tier the classic
        prefix to remote (with local copies evicted), then produce + consolidate a
        diskless tail and wait until the remote tier covers through the seal. Returns
        ``(seal_offset, total_acked)``."""
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
        # classic prefix be deleted locally. segment.bytes = 2 MiB (> the 1 MiB batch
        # ceiling so a consolidation append does not trip RecordBatchTooLargeException,
        # and < the diskless tail so the tail rolls an inactive, tierable segment).
        # retention.ms/.bytes are left at their (long/unlimited) defaults so only the
        # DeleteRecords below can move the earliest offset.
        self.logger.info("Switching topic %s to consolidating diskless (combined alter)" % self.TOPIC)
        self.kafka.alter_topic_configs(self.TOPIC, {
            "diskless.enable": "true",
            "remote.storage.enable": "true",
            "local.retention.ms": "5000",
            "segment.bytes": str(2 * 1024 * 1024),
        })
        verifier.wait_for_switch_complete(self.TOPIC, self.NUM_PARTITIONS)
        self.logger.info("Classic-to-consolidated switch completed for %s" % self.TOPIC)

        # --- 3) Tier the classic prefix and evict the local copies ---
        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Classic tiered-storage object count did not grow above baseline "
                           "after enabling remote storage on the switched topic.")
        classic_remote_watermark = verifier.wait_for_local_log_truncation(self.TOPIC)
        assert classic_remote_watermark > 0, \
            "Classic prefix never tiered: earliest-local stayed at 0 after enabling remote storage."
        self.logger.info("Classic prefix tiered; earliest-local watermark=%d" % classic_remote_watermark)

        # --- 4) Diskless phase: produce the tail and consolidate it to remote ---
        pre_diskless_tiered = verifier.tiered_object_count()
        diskless_acked = verifier.produce(self.TOPIC, self.NUM_DISKLESS_RECORDS, "diskless")
        self.logger.info("Produced diskless tail: acked=%d" % diskless_acked)

        wait_until(lambda: verifier.tiered_object_count() > pre_diskless_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Diskless consolidation did not grow the tiered-storage object count.")

        # The whole classic prefix [0, seal) -- including the boundary tail -- is durable
        # in remote once the latest-tiered offset reaches the seal, so the delete boundary
        # we pick inside the prefix is remote-backed.
        remote_watermark = verifier.wait_for_tiered_offset_at_least(self.TOPIC, seal_offset)
        self.logger.info("Remote tier covers through the seal: latest-tiered=%d, seal=%d"
                         % (remote_watermark, seal_offset))

        total_acked = classic_acked + diskless_acked
        return seal_offset, total_acked

    def _rolling_restart(self, verifier):
        """Restart every broker one at a time, waiting for the partition to regain a
        leader and full ISR before moving to the next. This forces each replica to become
        a freshly-rebuilt leader at some point (local ``logStartOffset`` re-pinned at the
        seal) and moves RLM leadership across the cluster -- the leadership-change /
        metadata-propagation-timing scenario the reclaim fail-safe must survive."""
        for node in list(self.kafka.nodes):
            self.logger.info("Rolling restart: bouncing %s" % node.account.hostname)
            self.kafka.restart_node(node, clean_shutdown=True)
            wait_until(lambda: verifier.partition_has_leader(self.TOPIC),
                       timeout_sec=120, backoff_sec=2,
                       err_msg="partition %s-0 had no leader after restarting %s"
                               % (self.TOPIC, node.account.hostname))
            wait_until(lambda: len(self.kafka.isr_idx_list(self.TOPIC, 0)) >= self.REPLICATION_FACTOR,
                       timeout_sec=180, backoff_sec=3,
                       err_msg="ISR for %s-0 did not recover to %d after restarting %s"
                               % (self.TOPIC, self.REPLICATION_FACTOR, node.account.hostname))
            self.logger.info("Broker %s back in ISR for %s-0" % (node.account.hostname, self.TOPIC))

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_delete_records_survives_leader_failover(self, metadata_quorum):
        """After a cross-tier ``DeleteRecords`` on a switched consolidating topic, a
        leadership change must NOT over-reclaim the surviving classic prefix.

        A freshly-elected leader's local ``logStartOffset`` is pinned at the seal ``S``
        (and only ever increments), and that same broker becomes the partition's RLM
        leader. If it used the local seal -- rather than the control-plane cross-tier
        earliest, or the fail-safe remote-earliest floor when that is momentarily
        unavailable -- as its remote-retention reclaim floor, it would irreversibly delete
        ``[delete_before, S)`` from remote. This is the metadata-propagation-timing /
        rapid-failover path behind the RLM ``isConsolidatingDisklessPartition`` fail-safe.

        The test performs the same cross-tier delete as ``test_delete_records_across_tiers``,
        then rolls every broker (so each replica becomes a fresh leader and RLM leadership
        moves), and re-asserts the earliest still settles at *exactly* the delete boundary
        and the surviving prefix ``[delete_before, S)`` is still readable and contiguous --
        i.e. the failover did not over-reclaim."""
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()
        baseline_tiered = verifier.tiered_object_count()

        seal_offset, total_acked = self._switch_and_drain(verifier, baseline_tiered)

        delete_before = seal_offset // 2
        assert delete_before > 0, "seal too small to pick a delete boundary: %d" % seal_offset

        low_watermark = verifier.delete_records(self.TOPIC, before_offset=delete_before)
        self.logger.info("Requested DeleteRecords before offset %d (seal=%d); low_watermark=%d"
                         % (delete_before, seal_offset, low_watermark))
        assert low_watermark == delete_before, (
            "DeleteRecords returned low_watermark=%d; expected the requested boundary %d"
            % (low_watermark, delete_before))

        # Baseline (pre-failover) invariants, identical to the base test.
        pre_failover_earliest = verifier.wait_for_consistent_earliest_across_brokers(
            self.TOPIC, timeout_sec=300)
        assert pre_failover_earliest == delete_before, (
            "pre-failover earliest %d did not settle at the requested boundary %d (seal=%d)"
            % (pre_failover_earliest, delete_before, seal_offset))
        pre_failover_tiered = verifier.wait_for_tiered_count_stable()
        self.logger.info("Pre-failover earliest agreed at %d; settled tiered count=%d"
                         % (pre_failover_earliest, pre_failover_tiered))

        # Force fresh leadership across the whole cluster.
        self._rolling_restart(verifier)

        # Post-failover: a broker that over-reclaimed using its local seal would push the
        # earliest to S and make [delete_before, S) unreadable. Assert it stayed put.
        post_failover_earliest = verifier.wait_for_consistent_earliest_across_brokers(
            self.TOPIC, timeout_sec=300)
        assert post_failover_earliest == delete_before, (
            "after leader failover the earliest settled at %d, not the delete boundary %d "
            "(seal=%d); a freshly-elected leader over-reclaimed [delete_before, seal) using its "
            "local seal as the reclaim floor instead of the cross-tier earliest / fail-safe"
            % (post_failover_earliest, delete_before, seal_offset))
        self.logger.info("Post-failover earliest still agreed at exactly %d" % post_failover_earliest)

        # The surviving prefix is still served from its exact boundary...
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=delete_before)
        assert first_served == delete_before, (
            "after failover a fetch from the delete boundary %d returned offset %d (seal=%d); the "
            "surviving classic prefix was reclaimed" % (delete_before, first_served, seal_offset))

        # ...and reads back contiguous with the correct content.
        spot = min(seal_offset - delete_before, self.SPOT_CHECK)
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=delete_before, max_messages=spot, timeout_ms=240000)
        assert len(records) >= spot, (
            "after failover a bounded read from the surviving boundary %d returned only %d of %d "
            "records; the surviving classic prefix was reclaimed" % (delete_before, len(records), spot))
        for i, (offset, value) in enumerate(records[:spot]):
            expected_offset = delete_before + i
            assert offset == expected_offset, (
                "non-contiguous read at position %d after failover: offset=%d, expected %d"
                % (i, offset, expected_offset))
            assert value == offset, (
                "content mismatch at offset %d after failover: value=%d, expected %d"
                % (offset, value, offset))
        self.logger.info("Surviving classic prefix from %d intact and readable after failover (%d records)"
                         % (delete_before, spot))

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_delete_records_across_tiers_born_consolidated(self, metadata_quorum):
        """The cross-tier ``DeleteRecords`` guarantees must also hold for a
        *born-consolidated* topic (diskless + remote enabled at creation, no classic
        prefix, no seal), not just a switched one.

        Born-consolidated has no seal-pinned classic prefix, so it was never subject to
        the Problem B over-reclaim; the point here is to prove Problem A (one
        broker-agnostic earliest served from the control plane) and a correct cross-tier
        reclaim hold for this topology, which historically had a separate ``DeleteRecords``
        limitation. Produce a diskless stream, let consolidation tier a remote prefix,
        delete inside that prefix, and assert the earliest advances to *exactly* the
        boundary on every broker and the survivor is readable."""
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()

        # Born consolidated: diskless + remote at creation, no switch.
        verifier.create_consolidated_topic(self.TOPIC, self.NUM_PARTITIONS, self.REPLICATION_FACTOR)
        acked = verifier.produce(self.TOPIC, self.NUM_DISKLESS_RECORDS, "born-consolidated")
        self.logger.info("Produced born-consolidated stream: acked=%d" % acked)

        # Wait until consolidation has tiered a remote prefix, so the delete boundary we
        # pick is remote-backed (a genuine cross-tier delete, not a WAL-only one).
        tier_target = acked // 2
        remote_watermark = verifier.wait_for_tiered_offset_at_least(self.TOPIC, tier_target)
        self.logger.info("Remote tier covers through %d (target %d)" % (remote_watermark, tier_target))

        delete_before = tier_target // 2  # strictly inside the tiered/remote prefix
        assert delete_before > 0, "not enough tiered data to pick a delete boundary (acked=%d)" % acked

        # Pre-delete: earliest is one broker-agnostic value below the boundary.
        pre_earliest = verifier.earliest_on_each_broker(self.TOPIC)
        pre_values = set(pre_earliest.values())
        assert len(pre_values) == 1, (
            "pre-delete earliest diverged across brokers: %s (ListOffsets(EARLIEST) not served "
            "from the control plane for a born-consolidated topic)" % pre_earliest)
        pre_value = next(iter(pre_values))
        assert 0 <= pre_value < delete_before, (
            "pre-delete earliest %d is not below the delete boundary %d; cannot exercise a "
            "cross-tier delete" % (pre_value, delete_before))

        low_watermark = verifier.delete_records(self.TOPIC, before_offset=delete_before)
        self.logger.info("Requested DeleteRecords before offset %d (acked=%d); low_watermark=%d"
                         % (delete_before, acked, low_watermark))
        assert low_watermark == delete_before, (
            "DeleteRecords returned low_watermark=%d; expected the requested boundary %d"
            % (low_watermark, delete_before))

        agreed_earliest = verifier.wait_for_consistent_earliest_across_brokers(
            self.TOPIC, timeout_sec=300)
        assert agreed_earliest == delete_before, (
            "post-delete earliest %d did not settle at exactly the requested boundary %d for the "
            "born-consolidated topic" % (agreed_earliest, delete_before))
        self.logger.info("Post-delete earliest agreed across all brokers at exactly %d" % agreed_earliest)

        first_served = verifier.first_served_offset(self.TOPIC, from_offset=delete_before)
        assert first_served == delete_before, (
            "a fetch from the delete boundary %d returned offset %d; the surviving prefix was "
            "reclaimed instead of served" % (delete_before, first_served))

        spot = min(acked - delete_before, self.SPOT_CHECK)
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=delete_before, max_messages=spot, timeout_ms=240000)
        assert len(records) >= spot, (
            "bounded read from the surviving boundary %d returned only %d of %d records; the "
            "surviving prefix was reclaimed" % (delete_before, len(records), spot))
        for i, (offset, value) in enumerate(records[:spot]):
            expected_offset = delete_before + i
            assert offset == expected_offset, (
                "non-contiguous read at position %d: offset=%d, expected %d" % (i, offset, expected_offset))
            assert value == offset, (
                "content mismatch at offset %d: value=%d, expected %d" % (offset, value, offset))
        self.logger.info("Born-consolidated surviving prefix from %d read back contiguous (%d records)"
                         % (delete_before, spot))

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_delete_records_follower_read_guard(self, metadata_quorum):
        """A consumer that the metadata transformer routes to a *follower* must not be served
        records below the cross-tier earliest after a ``DeleteRecords``, whether it is an old
        (pre-KIP-392) or a modern client.

        With managed replicas the transformer advertises a hash-selected replica -- usually a
        follower, not the real KRaft leader -- as the partition leader, and the follower then
        serves the read from its local classic log (an old consumer via the
        ``isOlderConsumer && switched`` relaxation, a modern consumer because ``allowReplica``
        is true for it once routed to a non-leader). A follower's local ``logStartOffset`` is
        frozen at the switch (``DeleteRecords`` advances only the real leader's log start and
        the control plane), so a consumer resuming from a position inside the now-deleted range
        ``[0, delete_before)`` could read those logically-deleted records straight off the
        follower. The fix consults the control-plane cross-tier earliest on the follower read
        path and returns ``OFFSET_OUT_OF_RANGE`` for anything below it, so the consumer resets
        (``auto.offset.reset=earliest``) to the delete boundary.

        Both client generations are exercised: the 2.2 console consumer the ducker image ships
        (Fetch < v11, empty ``clientMetadata``) and the current one (Fetch v11+). Each asserts
        a seek below the boundary never yields an offset ``< delete_before`` (guard rejects it;
        the consumer resets to ``delete_before``), and the old client additionally checks a read
        at the boundary is still served (the guard does not over-reject valid data)."""
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()
        baseline_tiered = verifier.tiered_object_count()

        seal_offset, total_acked = self._switch_and_drain(verifier, baseline_tiered)

        delete_before = seal_offset // 2
        assert delete_before > 0, "seal too small to pick a delete boundary: %d" % seal_offset

        low_watermark = verifier.delete_records(self.TOPIC, before_offset=delete_before)
        self.logger.info("Requested DeleteRecords before offset %d (seal=%d); low_watermark=%d"
                         % (delete_before, seal_offset, low_watermark))
        assert low_watermark == delete_before, (
            "DeleteRecords returned low_watermark=%d; expected the requested boundary %d"
            % (low_watermark, delete_before))

        agreed_earliest = verifier.wait_for_consistent_earliest_across_brokers(
            self.TOPIC, timeout_sec=300)
        assert agreed_earliest == delete_before, (
            "post-delete earliest %d did not settle at exactly the requested boundary %d (seal=%d)"
            % (agreed_earliest, delete_before, seal_offset))
        self.logger.info("Earliest settled at %d across all brokers" % agreed_earliest)

        spot = min(seal_offset - delete_before, self.SPOT_CHECK)

        # 1) Old consumer resuming from *inside* the deleted range must not be served any
        # record below the cross-tier earliest. With the guard it gets OFFSET_OUT_OF_RANGE
        # and resets (auto.offset.reset=earliest) to the boundary; without it the follower
        # would hand back the logically-deleted prefix from its frozen local log.
        #
        # The 2.2 client cannot print offsets (no print.offset before KIP-431), so we read the
        # record values instead; in the classic prefix [0, seal) the producer wrote value == offset,
        # so a returned value is the record's offset.
        probe = delete_before // 2
        below = verifier.read_values_with_old_client(
            self.TOPIC, from_offset=probe, max_messages=spot, timeout_ms=240000)
        stale = [v for v in below if v < delete_before]
        assert not stale, (
            "a pre-KIP-392 consumer seeking to %d (inside the deleted range [0, %d)) was served "
            "%d record(s) below the cross-tier earliest, e.g. %s; the follower served "
            "logically-deleted data instead of rejecting the read"
            % (probe, delete_before, len(stale), stale[:5]))
        if below:
            assert below[0] == delete_before, (
                "old consumer reset to offset %d, not the delete boundary %d"
                % (below[0], delete_before))
            for i, value in enumerate(below):
                expected = delete_before + i
                assert value == expected, (
                    "non-contiguous/mismatched old-client read at position %d: value=%d, "
                    "expected %d (value == offset in the classic prefix)" % (i, value, expected))
            self.logger.info("Old consumer reset to the boundary %d and read %d contiguous records"
                             % (delete_before, len(below)))
        else:
            self.logger.info("Old consumer returned nothing for a below-earliest seek; the key "
                             "invariant (no record below %d) still holds" % delete_before)

        # 1b) A modern consumer (Fetch v11+, sends clientMetadata) is routed to the same
        # follower and served from its local log, so the broadened guard must reject its
        # below-earliest reads too. Reset to earliest so an out-of-range seek lands on the
        # boundary rather than the log end.
        modern_below = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=probe, max_messages=spot, timeout_ms=240000,
            consumer_properties={"auto.offset.reset": "earliest"})
        modern_stale = [(o, v) for (o, v) in modern_below if o < delete_before]
        assert not modern_stale, (
            "a modern consumer seeking to %d (inside the deleted range [0, %d)) was served %d "
            "record(s) below the cross-tier earliest, e.g. %s; the follower served "
            "logically-deleted data to a modern client" % (probe, delete_before, len(modern_stale), modern_stale[:5]))
        if modern_below:
            assert modern_below[0][0] == delete_before, (
                "modern consumer reset to offset %d, not the delete boundary %d"
                % (modern_below[0][0], delete_before))
            self.logger.info("Modern consumer reset to the boundary %d and read %d records"
                             % (delete_before, len(modern_below)))
        else:
            self.logger.info("Modern consumer returned nothing for a below-earliest seek; the key "
                             "invariant (no record below %d) still holds" % delete_before)

        # 2) The guard must not over-reject: an old consumer reading from exactly the
        # surviving boundary is served that offset with the originally produced content
        # (value == offset in the classic prefix).
        at = verifier.read_values_with_old_client(
            self.TOPIC, from_offset=delete_before, max_messages=spot, timeout_ms=240000)
        assert at and at[0] == delete_before, (
            "a pre-KIP-392 consumer reading from the surviving boundary %d got %s; the guard "
            "over-rejected a valid at-earliest read" % (delete_before, at[:1]))
        for i, value in enumerate(at):
            expected = delete_before + i
            assert value == expected, (
                "non-contiguous/mismatched old-client read at position %d: value=%d, "
                "expected %d (value == offset in the classic prefix)" % (i, value, expected))
        self.logger.info("Old consumer read the surviving prefix from exactly %d (%d records)"
                         % (delete_before, len(at)))

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_delete_records_across_tiers(self, metadata_quorum):
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()
        baseline_tiered = verifier.tiered_object_count()

        seal_offset, total_acked = self._switch_and_drain(verifier, baseline_tiered)

        # Precondition: every broker agrees the earliest is still below the delete
        # boundary (the classic remote prefix is intact). We assert *agreement*, not a
        # specific value: the control-plane cross-tier earliest is the same on every
        # broker, and it must be well below the boundary so the DeleteRecords is a
        # genuine cross-tier delete of remote data.
        delete_before = seal_offset // 2
        assert delete_before > 0, "seal too small to pick a delete boundary: %d" % seal_offset

        pre_earliest = verifier.earliest_on_each_broker(self.TOPIC)
        pre_values = set(pre_earliest.values())
        assert len(pre_values) == 1, (
            "pre-delete earliest diverged across brokers: %s (ListOffsets(EARLIEST) is not "
            "served from the broker-agnostic control plane)" % pre_earliest)
        pre_value = next(iter(pre_values))
        assert 0 <= pre_value < delete_before, (
            "pre-delete earliest %d is not below the delete boundary %d; cannot exercise a "
            "cross-tier delete of the remote prefix" % (pre_value, delete_before))
        self.logger.info("Pre-delete earliest agreed across brokers at %d (boundary %d)"
                         % (pre_value, delete_before))

        # Settle the pre-delete peak: the diskless tail keeps tiering after it drains
        # locally, so a snapshot here can still be climbing. Capturing a settled peak
        # keeps later diagnostics meaningful.
        tiered_peak = verifier.wait_for_tiered_count_stable()
        self.logger.info("Settled pre-delete tiered-storage object count: %d" % tiered_peak)

        # Delete the first half of the classic prefix. The boundary is strictly inside
        # the sealed classic region [0, seal), which is tiered to remote, so the
        # reclaimed range [0, delete_before) is remote-backed: this exercises the
        # local-log -> RemoteLogManager cross-tier delete on the real leader (reached
        # via DisklessDeleteRecordsForwarder).
        #
        # delete_records parses the per-partition result and raises on a partition error
        # (kafka-delete-records.sh reports those on stdout yet still exits 0). The returned
        # low_watermark is the partition's new log start as the broker computed it.
        low_watermark = verifier.delete_records(self.TOPIC, before_offset=delete_before)
        self.logger.info("Requested DeleteRecords before offset %d (seal=%d, total=%d); low_watermark=%d"
                         % (delete_before, seal_offset, total_acked, low_watermark))
        assert low_watermark == delete_before, (
            "DeleteRecords returned low_watermark=%d; expected the requested boundary %d "
            "(the broker did not advance the log start to the delete offset)"
            % (low_watermark, delete_before))

        # Problem A + B assertion: the client-visible earliest advances from below the
        # boundary to a single, stable value that is the SAME on every broker (Problem A:
        # served from the control plane, not a hash-selected follower's frozen local log)
        # and settles at EXACTLY the delete boundary (Problem B: only [0, delete_before)
        # is reclaimed -- with retention unset nothing else may move the earliest, and the
        # consolidation cleanup must not over-reclaim the classic prefix down to the seal).
        # Before the fixes this stayed at 0 (Problem A) or ran ahead to the seal (Problem B).
        agreed_earliest = verifier.wait_for_consistent_earliest_across_brokers(
            self.TOPIC, timeout_sec=300)
        self.logger.info("Post-delete earliest agreed across all brokers at a stable %d"
                         % agreed_earliest)
        assert agreed_earliest == delete_before, (
            "post-delete earliest %d did not settle at exactly the requested boundary %d "
            "(seal=%d); the cleanup over-reclaimed the classic prefix past the delete boundary "
            "even though retention is unset" % (agreed_earliest, delete_before, seal_offset))

        # The advertised floor is backed by real, readable data: a fetch from the boundary
        # serves that exact offset (not a skip-ahead to the seal).
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=delete_before)
        assert first_served == delete_before, (
            "a fetch from the delete boundary %d returned offset %d (seal=%d); the surviving "
            "classic prefix was reclaimed instead of served"
            % (delete_before, first_served, seal_offset))
        self.logger.info("Fetch from the delete boundary %d served that offset" % delete_before)

        # The surviving prefix [delete_before, seal) comes back contiguous with the correct
        # content. VerifiableProducer writes each record's per-producer sequence as its value;
        # the classic producer wrote [0, seal) so value == offset below the seal.
        spot = min(seal_offset - delete_before, self.SPOT_CHECK)
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=delete_before, max_messages=spot, timeout_ms=240000)
        assert len(records) >= spot, (
            "bounded read from the surviving boundary %d returned only %d of %d records; the "
            "surviving classic prefix was reclaimed" % (delete_before, len(records), spot))
        for i, (offset, value) in enumerate(records[:spot]):
            expected_offset = delete_before + i
            assert offset == expected_offset, (
                "non-contiguous read at position %d: offset=%d, expected %d (gap/dupe/reorder)"
                % (i, offset, expected_offset))
            assert value == offset, (
                "content mismatch at offset %d: value=%d, expected %d; the surviving classic "
                "record does not match what was produced" % (offset, value, offset))
        self.logger.info("Surviving classic prefix from %d read back contiguous with correct content (%d records)"
                         % (delete_before, spot))
