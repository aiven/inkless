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


class ConsolidationEnabledOnUntieredDisklessTest(Test):
    """Upgrade test: a topic reaches the untiered-diskless state before
    consolidation exists, then consolidation is turned on for it.

    A topic can only be *untiered* diskless while the cluster-wide consolidation
    feature (``diskless.remote.storage.consolidation.enable``) is off -- with it on,
    ``validateDisklessRequiresRemoteStorage`` forces every diskless topic to enable
    remote storage. So this is the realistic upgrade: run plain diskless (WAL only),
    then flip the feature on cluster-wide and enable remote storage on the existing
    diskless topic to start consolidating it. The feature is a static
    ``ServerConfigs`` flag, so flipping it needs a restart (done in-flight).

    Sequence (three cluster states):
      A. Consolidation OFF. Create a classic topic, produce a prefix, switch
         ``diskless.enable=true`` alone (sealing at ``S``), produce a diskless tail,
         and assert the intermediate state (diskless, no remote, nothing tiered,
         WAL holds data).
      B. Flip the consolidation feature on cluster-wide and restart; the existing
         untiered-diskless topic survives.
      C. Alter ``remote.storage.enable=true`` (the "start consolidation" transition):
         the TopicConfigHandler remote-flip hook starts the consolidation fetchers
         without a leader change. Assert the remote tier grows, the WAL is pruned
         above the seal, and reads stay contiguous and lossless across the seal
         (nothing is wiped, so the upgrade must not lose data).

    Read-from-remote-after-prune durability is covered by
    :class:`ReadFromRemoteAfterPruneTest` and
    :class:`SwitchedReadFromRemoteAfterPruneTest`; the novel behaviour here is the
    upgrade transition itself.
    """

    # Unique per run: Postgres/MinIO persist across runs, so a stale name would
    # mix old rows into the control-plane queries.
    TOPIC_PREFIX = "enable-consolidation-on-untiered-diskless"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Classic prefix: enough to roll several segments (1 MiB floor) so a large
    # contiguous part tiers once consolidation starts.
    NUM_CLASSIC_RECORDS = 150000
    # Diskless tail: enough that consolidation tiers it and the WAL pruner advances
    # the diskless log-start above the seal.
    NUM_DISKLESS_RECORDS = 150000

    CONSOLIDATION_FEATURE_CONFIG = "diskless.remote.storage.consolidation.enable"

    RESTART_TIMEOUT_SEC = 120

    def __init__(self, test_context):
        super(ConsolidationEnabledOnUntieredDisklessTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_enable_consolidation_on_untiered_diskless(self, metadata_quorum):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            # Keep the remote-storage system + RSM plugin wired so phase C can tier
            # without re-rendering config; the feature itself is OFF below for phase A.
            consolidation=True,
            # log.diskless.enable=false: keep the cluster default classic.
            # diskless.allow.from.classic.enable=true: open the classic->diskless
            #   switch bridge.
            # diskless.remote.storage.consolidation.enable=false: feature off for
            #   phase A so validateDisklessRequiresRemoteStorage allows an untiered
            #   diskless topic; phase B flips it true.
            # auto.leader.rebalance.enable=false: pin leadership so phase C proves the
            #   remote-flip hook starts consolidation, not an incidental leader change.
            # The rest run the WAL pruner / file cleaner fast; cache TTL <= retention/2.
            server_prop_overrides=[
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
                [self.CONSOLIDATION_FEATURE_CONFIG, "false"],
                ["auto.leader.rebalance.enable", "false"],
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
        # server_prop_overrides only reach the brokers; mirror the switch bridge,
        # classic default, the (initially off) feature, and leader pinning onto the
        # controller (KRaft drives preferred-leader rebalancing), and disable its JMX.
        if getattr(self.kafka, "isolated_controller_quorum", None):
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + [
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
                [self.CONSOLIDATION_FEATURE_CONFIG, "false"],
                ["auto.leader.rebalance.enable", "false"],
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

        # =================================================================
        # Phase A: consolidation OFF -- reach the untiered-diskless state
        # =================================================================

        # --- A1) Create a plain classic topic and produce a classic prefix ---
        verifier.create_classic_topic(self.TOPIC, self.NUM_PARTITIONS, self.REPLICATION_FACTOR)
        classic_acked = verifier.produce(self.TOPIC, self.NUM_CLASSIC_RECORDS, "classic")
        seal_offset = classic_acked
        self.logger.info("Produced classic prefix: acked=%d (seal offset will be %d)"
                         % (classic_acked, seal_offset))

        # --- A2) Switch classic -> *untiered* diskless (diskless.enable only) ---
        #
        # Legal only with consolidation off (else remote storage is forced too). The
        # leader seals at `seal_offset`; the diskless interval is served from the WAL.
        self.logger.info("Switching topic %s to UNTIERED diskless (diskless.enable only)" % self.TOPIC)
        self.kafka.alter_topic_configs(self.TOPIC, {
            "diskless.enable": "true",
        })
        verifier.wait_for_switch_complete(self.TOPIC, self.NUM_PARTITIONS)
        self.logger.info("Classic-to-untiered-diskless switch completed for %s" % self.TOPIC)

        # --- A3) Produce a diskless tail into the WAL ---
        diskless_acked = verifier.produce(self.TOPIC, self.NUM_DISKLESS_RECORDS, "diskless")
        self.logger.info("Produced diskless tail: acked=%d" % diskless_acked)

        # --- A4) Assert the intermediate untiered-diskless state ---
        config = self.kafka.describe_topic_config(self.TOPIC)
        assert config.get("diskless.enable") == "true", \
            "Topic %s is not diskless after the switch: %s" % (self.TOPIC, config)
        assert config.get("remote.storage.enable") != "true", \
            ("Topic %s already has remote storage enabled before phase C: %s" % (self.TOPIC, config))
        # Nothing tiers while consolidation is off and no topic has remote storage.
        untiered_count = verifier.tiered_object_count()
        assert untiered_count == baseline_tiered, (
            "tiered object count grew from %d to %d while consolidation was OFF: the untiered "
            "intermediate state is not clean." % (baseline_tiered, untiered_count))
        # The diskless tail must still be in the WAL: with no consolidation there is
        # nothing to make it durable, so it cannot have been pruned.
        wal_batches = verifier.wal_batch_count(self.TOPIC)
        self.logger.info("Untiered-diskless state: tiered=%d (baseline), WAL batches=%d"
                         % (untiered_count, wal_batches))
        assert wal_batches > 0, (
            "diskless WAL for %s is empty in the untiered state: the tail was not retained."
            % self.TOPIC)

        # =================================================================
        # Phase B: flip the consolidation feature ON cluster-wide (restart)
        # =================================================================
        self.logger.info("Enabling the consolidation feature cluster-wide and restarting")
        self._enable_consolidation_feature_and_restart()
        wait_until(lambda: verifier.partition_has_leader(self.TOPIC, 0),
                   timeout_sec=self.RESTART_TIMEOUT_SEC, backoff_sec=2,
                   err_msg="Partition %s-0 did not get a leader after enabling consolidation." % self.TOPIC)
        self.logger.info("Cluster restarted with consolidation enabled; %s still online" % self.TOPIC)

        # The restart flipped only the cluster-wide consolidation feature; the topic's
        # own config must be untouched. Assert it explicitly so a later Phase C timeout
        # is attributable to consolidation not starting, not to a config lost across the
        # restart (e.g. a dropped diskless.enable, or remote silently already on).
        post_restart_config = self.kafka.describe_topic_config(self.TOPIC)
        assert post_restart_config.get("diskless.enable") == "true", (
            "diskless.enable is not true after the consolidation-feature restart (got %s): "
            "the untiered-diskless topic did not survive the restart."
            % post_restart_config.get("diskless.enable"))
        assert post_restart_config.get("remote.storage.enable") != "true", (
            "remote.storage.enable is already true before Phase C (got %s): the topic was not "
            "untiered-diskless going into the consolidation start."
            % post_restart_config.get("remote.storage.enable"))

        # =================================================================
        # Phase C: enable remote storage -> the topic starts consolidating
        # =================================================================
        pre_consolidation_tiered = verifier.tiered_object_count()
        self.logger.info("Tiered-storage object count before enabling remote storage: %d"
                         % pre_consolidation_tiered)

        # The "start consolidation" transition (diskless stays on, remote storage
        # turns on): the TopicConfigHandler remote-flip hook starts the consolidation
        # fetchers without a leader change.
        #
        # Raise segment.bytes off the 1 MiB floor: consolidation appends each WAL
        # batch to the local log, and a batch (max.request.size 1 MiB + framing) can
        # exceed 1 MiB, which a 1 MiB segment rejects with RecordBatchTooLargeException
        # and permanently fails the fetcher. 8 MiB clears it; segments still roll by
        # segment.ms. (Test-only artifact of the tiny floor; prod defaults to 1 GiB.)
        self.logger.info("Enabling remote storage on %s to start consolidation" % self.TOPIC)
        self.kafka.alter_topic_configs(self.TOPIC, {
            "remote.storage.enable": "true",
            "local.retention.ms": "5000",
            "segment.bytes": str(8 * 1024 * 1024),
        })

        # --- C1) The remote tier grows: classic prefix + diskless tail tier ---
        wait_until(lambda: verifier.tiered_object_count() > pre_consolidation_tiered,
                   timeout_sec=240, backoff_sec=2,
                   err_msg=("Consolidation did not start after enabling remote storage on the "
                            "previously-untiered diskless topic: the tiered-storage object count "
                            "never grew above %d." % pre_consolidation_tiered))
        self.logger.info("Tiered-storage object count grew to %d after enabling consolidation"
                         % verifier.tiered_object_count())

        # --- C2) The diskless WAL is pruned above the seal in the control plane ---
        #
        # The OR accepts either prune representation -- deleted batch rows (count -> 0)
        # or an advanced log_start_offset -- so it must stay an OR, not an AND (a prune
        # may only surface one). The count==0 leg can't fire prematurely: the C1
        # tiered-count-grew gate above already proves batches were written, so 0 means
        # pruned, not "not yet produced" (wal_batch_count returns -1 when unregistered).
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > seal_offset,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Diskless WAL was not pruned above the seal after consolidation started.")
        self.logger.info("Control plane pruned the diskless WAL: batch_count=%d, "
                         "min_log_start_offset=%d, seal=%d" %
                         (verifier.wal_batch_count(self.TOPIC),
                          verifier.min_log_start_offset(self.TOPIC),
                          seal_offset))

        # --- C3) Data integrity: no data lost across the classic/diskless boundary ---
        #
        # Nothing is wiped, so the whole log [0, total) must stay readable end-to-end:
        # earliest is still 0 (log_start_offset advancing above the seal is not the
        # whole-log start), a fetch from 0 starts at 0, a window across the seal is
        # strictly contiguous with correct content, and a full read-back returns every record.
        total_produced = classic_acked + diskless_acked
        earliest = verifier.wait_for_offset(self.TOPIC, time_spec=-2)
        assert earliest == 0, (
            "earliest offset is %d, not 0 after the upgrade: the classic prefix [0, %d) is no "
            "longer addressable." % (earliest, seal_offset))

        # A fetch from 0 must actually start at offset 0 (cheap single-record read).
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=0)
        assert first_served == 0, (
            "a fetch from offset 0 returned offset %d, not 0 after the upgrade." % first_served)

        # A window straddling the seal must be strictly contiguous with matching content
        # (no gap/dupe/reorder across the classic->diskless boundary). Each producer wrote
        # its sequence as the value, so the series resets to 0 at the seal.
        window = 5000
        boundary_start = max(0, seal_offset - window)
        boundary_count = min(2 * window, total_produced - boundary_start)
        boundary_records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=boundary_start, max_messages=boundary_count)
        assert len(boundary_records) >= boundary_count, (
            "cross-boundary read returned only %d of %d records spanning the seal (%d)."
            % (len(boundary_records), boundary_count, seal_offset))
        for i, (offset, value) in enumerate(boundary_records[:boundary_count]):
            expected_offset = boundary_start + i
            assert offset == expected_offset, (
                "non-contiguous read across the seal at position %d: offset=%d, expected=%d "
                "(gap/dupe/reorder near seal=%d)." % (i, offset, expected_offset, seal_offset))
            expected_value = offset if offset < seal_offset else offset - seal_offset
            assert value == expected_value, (
                "content mismatch at offset %d: value=%d, expected=%d (seal=%d)."
                % (offset, value, expected_value, seal_offset))
        self.logger.info("Cross-boundary read OK: %d contiguous records [%d, %d) across seal=%d"
                         % (boundary_count, boundary_start, boundary_start + boundary_count,
                            seal_offset))

        # Full-count read-back via VerifiableConsumer: it tolerates the large
        # (~300k) record count where kafka-console-consumer can idle-timeout
        # mid-stream, and its periodic summaries drive total_consumed().
        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC,
                                      group_id="enable-consolidation-%s" % uuid.uuid4().hex[:8],
                                      reset_policy="earliest")
        consumer.start()
        wait_until(lambda: consumer.total_consumed() >= total_produced or consumer.worker_errors,
                   timeout_sec=240, backoff_sec=1,
                   err_msg=("Consumer read back only %d of %d records after enabling consolidation; "
                            "the classic prefix and diskless tail must both stay readable."
                            % (consumer.total_consumed(), total_produced)))
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        num_read = consumer.total_consumed()
        consumer.free()
        self.logger.info("Read back %d records after the upgrade (expected %d)"
                         % (num_read, total_produced))
        assert num_read >= total_produced, (
            "read back only %d of %d records after the upgrade: enabling consolidation on the "
            "untiered-diskless topic lost data." % (num_read, total_produced))

    # -----------------------------------------------------------------------
    # Helpers: cluster-wide consolidation feature flip (static config -> restart)
    # -----------------------------------------------------------------------

    def _enable_consolidation_feature_and_restart(self):
        """Flip ``diskless.remote.storage.consolidation.enable`` true on brokers and
        controller, then restart. It is a static ``ServerConfigs`` flag and
        ``start_node`` re-renders config from ``server_prop_overrides``, so mutating
        the list and restarting suffices. Stop brokers before the controller and
        start the controller first so brokers always have one to talk to; on-disk
        metadata survives, so the untiered-diskless topic is preserved."""
        ctrl = getattr(self.kafka, "isolated_controller_quorum", None)

        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=self.RESTART_TIMEOUT_SEC)
        if ctrl:
            for node in ctrl.nodes:
                ctrl.stop_node(node, clean_shutdown=True, timeout_sec=self.RESTART_TIMEOUT_SEC)

        self._set_override(self.kafka, self.CONSOLIDATION_FEATURE_CONFIG, "true")
        if ctrl:
            self._set_override(ctrl, self.CONSOLIDATION_FEATURE_CONFIG, "true")

        if ctrl:
            for node in ctrl.nodes:
                ctrl.start_node(node, timeout_sec=self.RESTART_TIMEOUT_SEC)
        for node in self.kafka.nodes:
            self.kafka.start_node(node, timeout_sec=self.RESTART_TIMEOUT_SEC)

    @staticmethod
    def _set_override(service, key, value):
        """Set ``key=value`` in ``server_prop_overrides``, replacing any existing
        entry for ``key`` so the rendered config is unambiguous."""
        service.server_prop_overrides = [
            override for override in service.server_prop_overrides if override[0] != key
        ] + [[key, value]]
