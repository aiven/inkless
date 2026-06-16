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

import re
import uuid

from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.inkless.consolidation_verifier import ConsolidationVerifier
from kafkatest.services.verifiable_producer import VerifiableProducer


class ConsolidationEnabledOnUntieredDisklessTest(Test):
    """Upgrade test for the *cross-mode* path that lands a topic in the
    untiered-diskless state before consolidation exists, then turns
    consolidation on for it.

    Unlike the classic -> consolidated switch (covered by
    :class:`SwitchedReadFromRemoteAfterPruneTest`), a topic can only become
    *untiered* diskless while the cluster-wide consolidation feature
    (``diskless.remote.storage.consolidation.enable``) is **off**: with
    consolidation on, ``validateDisklessRequiresRemoteStorage`` forces every
    diskless topic to also enable remote storage, so a diskless-only topic is
    rejected at creation/alter. The only way to reach "diskless but not
    tiering" is therefore a cluster that started with consolidation disabled.

    This is exactly the realistic upgrade story: a deployment runs plain
    diskless (WAL only, no tiering), then the operator turns on the
    consolidation feature cluster-wide and finally enables remote storage on an
    existing diskless topic to start consolidating it. Because
    ``diskless.remote.storage.consolidation.enable`` is a static server config
    (see ``ServerConfigs``), flipping it requires a controller + broker restart,
    which this test performs in-flight.

    Sequence (three distinct cluster states):

      A. **Consolidation OFF.** Start the cluster with the diskless storage
         system on, the classic->diskless switch bridge open
         (``diskless.allow.from.classic.enable=true``), the cluster default
         flipped back to classic (``log.diskless.enable=false``), and the
         consolidation feature explicitly **disabled**
         (``diskless.remote.storage.consolidation.enable=false``) -- while still
         keeping the remote-storage system + RSM plugin wired (so we can tier
         later without re-templating). Then:
           1. Create a plain classic topic and produce a classic prefix.
           2. Alter ``diskless.enable=true`` *alone* (no remote storage). With
              consolidation off this is the legal classic -> *untiered* diskless
              switch: the leader seals the classic log at ``S = classic_count``
              and the diskless interval ``[S, ...)`` is served from the WAL.
           3. Produce a diskless tail into the WAL.
           4. Assert the intermediate state: ``diskless.enable=true``,
              ``remote.storage.enable`` not set, nothing tiered, WAL holds data.

      B. **Flip consolidation ON (restart).** Stop the brokers and the isolated
         controller, set ``diskless.remote.storage.consolidation.enable=true``
         on both, and restart. The existing untiered-diskless topic survives the
         flip (existing topic configs are not re-validated on metadata replay).

      C. **Start consolidating.** Alter ``remote.storage.enable=true`` (plus a
         short ``local.retention.ms``) on the diskless topic. This is the
         ``isValidConsolidationModeTransitionOnUpdate`` "start consolidation"
         transition, and the ``TopicConfigHandler`` remote-flip hook kicks off
         the consolidation fetchers without needing a leader change. Assert:
           a. the remote tier grows (the classic prefix ``[0, S)`` and the
              diskless tail ``[S, ...)`` are tiered to remote);
           b. the diskless WAL is pruned above the seal in the control plane;
           c. the data is intact -- a contiguous read from offset ``0`` returns
              every produced record (nothing is wiped in this test, so the
              upgrade must not drop or reorder data).

    This test deliberately does **not** wipe local logs and read back from
    remote: the read-from-remote-after-prune durability guarantee is already
    covered by :class:`ReadFromRemoteAfterPruneTest` (born-consolidated) and
    :class:`SwitchedReadFromRemoteAfterPruneTest` (classic->consolidated). The
    novel behaviour here is the *upgrade transition* itself -- that an existing
    untiered-diskless topic actually begins consolidating once the feature is
    enabled cluster-wide and remote storage is turned on for it.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a
    # stale topic name would let old rows skew the control-plane queries.
    TOPIC_PREFIX = "enable-consolidation-on-untiered-diskless"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Classic prefix: sized to roll several segments by size (1 MiB floor) so a
    # large, contiguous part of it tiers to remote once consolidation starts.
    NUM_CLASSIC_RECORDS = 150000
    # Diskless tail: large enough that consolidation tiers it and the WAL pruner
    # advances the diskless log-start above the seal.
    NUM_DISKLESS_RECORDS = 150000

    CONSOLIDATION_FEATURE_CONFIG = "diskless.remote.storage.consolidation.enable"

    # Switch-completion JMX gauges (broker-side). The topic config flips to
    # diskless.enable=true before leaders finish sealing and before
    # InitDisklessLogManager commits the diskless start offset, so we gate on
    # these rather than on config visibility alone.
    SEALED_LEADER_PARTITIONS_JMX_OBJECT = "kafka.server:type=ReplicaManager,name=SealedPartitionsCount"
    INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT = \
        "kafka.server:type=InitDisklessLogManager,name=InFlightPartitions"
    SWITCH_JMX_OBJECT_NAMES = [
        SEALED_LEADER_PARTITIONS_JMX_OBJECT,
        INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT,
    ]
    SWITCH_JMX_ATTRIBUTES = ["Value"]

    SWITCH_TIMEOUT_SEC = 180
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
            # consolidation=True keeps the remote-storage system + RSM/tiered
            # plugin wired (the template bundles them under this flag), so we can
            # tier in phase C without re-rendering the broker config. The
            # consolidation *feature* itself is then explicitly turned OFF for
            # phase A via the override below -- this is the whole point of the
            # test: reach "diskless but not tiering" first, then flip it on.
            consolidation=True,
            # log.diskless.enable=false: cluster default back to classic so the
            #   "classic" topic is born classic (consolidation=True would
            #   otherwise default new topics to diskless).
            # diskless.allow.from.classic.enable=true: opens the classic ->
            #   diskless switch bridge (validated on every node).
            # diskless.remote.storage.consolidation.enable=false: turn the
            #   consolidation FEATURE off for phase A. With it off,
            #   validateDisklessRequiresRemoteStorage does not run, so a
            #   diskless-only (untiered) topic is legal. Phase B flips it true.
            # The remaining knobs run the WAL pruner / file cleaner fast; cache
            #   TTL must stay <= retention/2.
            # auto.leader.rebalance.enable=false: the consolidation-flip restart re-elects whichever
            # replica is in-sync first (not necessarily the preferred leader). With auto-rebalance on,
            # the controller later moves leadership back to the preferred replica, and that incidental
            # leader change -- not the remote.storage.enable config flip -- is what would re-enter the
            # consolidation-start path. Pinning leadership makes the test assert the config-flip hook
            # itself starts consolidation, deterministically.
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
            jmx_object_names=list(self.SWITCH_JMX_OBJECT_NAMES),
            jmx_attributes=list(self.SWITCH_JMX_ATTRIBUTES),
        )
        # server_prop_overrides only reach the brokers; mirror the switch bridge,
        # the classic cluster default, and the (initially off) consolidation
        # feature onto the isolated controller quorum, and disable JMX there.
        if getattr(self.kafka, "isolated_controller_quorum", None):
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + [
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
                [self.CONSOLIDATION_FEATURE_CONFIG, "false"],
                # Preferred-leader rebalancing is driven by the controller in KRaft.
                ["auto.leader.rebalance.enable", "false"],
            ]
            ctrl.jmx_object_names = None
            ctrl.jmx_attributes = []

        # Collect the broker data-dir logs so a post-restart failure is debuggable.
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
        self._create_classic_topic(self.TOPIC)
        classic_acked = self._produce(self.NUM_CLASSIC_RECORDS, "classic")
        seal_offset = classic_acked
        self.logger.info("Produced classic prefix: acked=%d (seal offset will be %d)"
                         % (classic_acked, seal_offset))

        # --- A2) Switch classic -> *untiered* diskless (diskless.enable only) ---
        #
        # Legal only because consolidation is off: with it on,
        # validateDisklessRequiresRemoteStorage would force remote.storage.enable
        # too. The leader seals the classic log at `seal_offset`; the diskless
        # interval [seal, ...) is served from the WAL (no tiering yet).
        self.logger.info("Switching topic %s to UNTIERED diskless (diskless.enable only)" % self.TOPIC)
        self.kafka.alter_topic_configs(self.TOPIC, {
            "diskless.enable": "true",
        })
        self._wait_for_switch_complete(self.TOPIC)
        self.logger.info("Classic-to-untiered-diskless switch completed for %s" % self.TOPIC)

        # --- A3) Produce a diskless tail into the WAL ---
        diskless_acked = self._produce(self.NUM_DISKLESS_RECORDS, "diskless")
        self.logger.info("Produced diskless tail: acked=%d" % diskless_acked)

        # --- A4) Assert the intermediate untiered-diskless state ---
        config = self.kafka.describe_topic_config(self.TOPIC)
        assert config.get("diskless.enable") == "true", \
            "Topic %s is not diskless after the switch: %s" % (self.TOPIC, config)
        assert config.get("remote.storage.enable") != "true", \
            ("Topic %s already has remote storage enabled before phase C; the untiered "
             "intermediate state was not reached: %s" % (self.TOPIC, config))
        # Nothing should have tiered while consolidation is off and no topic has
        # remote storage enabled.
        untiered_count = verifier.tiered_object_count()
        assert untiered_count == baseline_tiered, (
            "Tiered-storage object count grew from %d to %d while consolidation was OFF and no "
            "topic had remote storage enabled -- the untiered intermediate state is not clean."
            % (baseline_tiered, untiered_count))
        # The diskless tail must be sitting in the WAL (it cannot have been pruned
        # because there is no consolidation/tiering to make it durable yet).
        wal_batches = verifier.wal_batch_count(self.TOPIC)
        self.logger.info("Untiered-diskless state: tiered=%d (baseline), WAL batches=%d"
                         % (untiered_count, wal_batches))
        assert wal_batches > 0, (
            "Diskless WAL for %s is empty in the untiered state: the diskless tail was not "
            "retained in the WAL (it must not be pruned while there is no consolidation)."
            % self.TOPIC)

        # =================================================================
        # Phase B: flip the consolidation feature ON cluster-wide (restart)
        # =================================================================
        self.logger.info("Enabling the consolidation feature cluster-wide and restarting")
        self._enable_consolidation_feature_and_restart()
        wait_until(lambda: self._partition_has_leader(self.TOPIC, 0),
                   timeout_sec=self.RESTART_TIMEOUT_SEC, backoff_sec=2,
                   err_msg="Partition %s-0 did not get a leader after enabling consolidation." % self.TOPIC)
        self.logger.info("Cluster restarted with consolidation enabled; %s still online" % self.TOPIC)

        # =================================================================
        # Phase C: enable remote storage -> the topic starts consolidating
        # =================================================================
        pre_consolidation_tiered = verifier.tiered_object_count()
        self.logger.info("Tiered-storage object count before enabling remote storage: %d"
                         % pre_consolidation_tiered)

        # The "start consolidation" transition (isValidConsolidationModeTransitionOnUpdate):
        # diskless stays enabled, remote storage becomes enabled. The
        # TopicConfigHandler remote-flip hook starts the consolidation fetchers
        # without needing a leader change.
        #
        # segment.bytes is also raised off the 1 MiB floor here. Consolidation appends
        # each WAL record batch to the local log, and a record batch must fit within
        # segment.bytes. A single batch is bounded by the producer's max.request.size
        # (1 MiB default), so with batch framing overhead it lands just over 1 MiB --
        # enough for a 1 MiB segment to reject it with RecordBatchTooLargeException,
        # which fails the partition's consolidation fetcher permanently. 8 MiB clears
        # that ~1 MiB ceiling with ample margin. The classic prefix already rolled (and
        # will tier) as small 1 MiB segments during phase A, so raising the floor now
        # only affects the segments consolidation creates for the diskless tail; they
        # still roll by segment.ms=5000 so tiering/pruning proceeds. (In production
        # segment.bytes defaults to 1 GiB, so this is a test-only artifact of the
        # deliberately tiny floor.)
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
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > seal_offset,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Diskless WAL was not pruned above the seal after consolidation started.")
        self.logger.info("Control plane pruned the diskless WAL: batch_count=%d, "
                         "min_log_start_offset=%d, seal=%d" %
                         (verifier.wal_batch_count(self.TOPIC),
                          verifier.min_log_start_offset(self.TOPIC),
                          seal_offset))

        # --- C3) Data integrity: a contiguous read from 0 returns every record ---
        #
        # Nothing is wiped in this test, so the whole log [0, total) must still be
        # readable end-to-end across the classic prefix / diskless tail boundary
        # after the upgrade. (earliest must also still be 0 -- the diskless
        # control-plane log_start_offset advancing above the seal must not be
        # mistaken for the start of the whole log.)
        total_produced = classic_acked + diskless_acked
        earliest = self._wait_for_offset(self.TOPIC, time_spec=-2)
        assert earliest == 0, (
            "After the upgrade the earliest offset is %d, not 0: the classic prefix [0, %d) is no "
            "longer addressable." % (earliest, seal_offset))
        first_offset, num_read = self._read_contiguous_from(self.TOPIC, from_offset=0,
                                                            max_messages=total_produced)
        self.logger.info("Contiguous read from 0: first_offset=%d, num_read=%d (expected %d)"
                         % (first_offset, num_read, total_produced))
        assert first_offset == 0, (
            "Contiguous read from offset 0 started at %d, not 0, after enabling consolidation."
            % first_offset)
        assert num_read >= total_produced, (
            "Contiguous read from offset 0 returned only %d of %d produced records after the "
            "upgrade: enabling consolidation on the untiered-diskless topic lost data."
            % (num_read, total_produced))

    # -----------------------------------------------------------------------
    # Helpers: topic / produce
    # -----------------------------------------------------------------------

    def _create_classic_topic(self, topic):
        """Plain classic (non-diskless, non-tiered) topic with a small
        ``segment.bytes`` so the classic prefix rolls several closed segments.

        ``diskless.enable`` is deliberately left unset: the cluster default is
        forced to ``false`` (see ``log.diskless.enable`` in the broker
        overrides), so the topic is born classic. Remote storage is not enabled
        either -- it is turned on only in phase C to start consolidation."""
        self.kafka.create_topic({
            "topic": topic,
            "partitions": self.NUM_PARTITIONS,
            "replication-factor": self.REPLICATION_FACTOR,
            "configs": {
                "min.insync.replicas": 2,
                # 1 MiB is the enforced minimum for segment.bytes.
                "segment.bytes": 1048576,
                # Force a roll every 5s so produced records flow through closed
                # segments quickly even before the size floor is reached.
                "segment.ms": 5000,
            },
        })

    def _produce(self, num_records, label):
        producer = VerifiableProducer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC, max_messages=num_records,
                                      throughput=-1)
        producer.start()
        wait_until(lambda: producer.num_acked >= num_records or producer.worker_errors,
                   timeout_sec=180, backoff_sec=1,
                   err_msg="Producer failed to produce %d %s records." % (num_records, label))
        assert not producer.worker_errors, "Producer errors (%s): %s" % (label, producer.worker_errors)
        producer.stop()
        acked = producer.num_acked
        producer.free()
        return acked

    # -----------------------------------------------------------------------
    # Helpers: cluster-wide consolidation feature flip (static config -> restart)
    # -----------------------------------------------------------------------

    def _enable_consolidation_feature_and_restart(self):
        """Flip ``diskless.remote.storage.consolidation.enable`` from false to
        true on the brokers and the isolated controller, then restart.

        It is a static ``ServerConfigs`` flag, so it only takes effect on
        startup. ``start_node`` re-renders ``kafka.properties`` from the current
        ``server_prop_overrides``, so mutating the list and restarting is enough.
        Stop the brokers before the controller and start the controller before
        the brokers so the brokers always have a controller to talk to; the
        on-disk metadata (``__cluster_metadata``) survives, so the existing
        untiered-diskless topic is preserved across the flip.
        """
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
        """Set ``key=value`` in a service's ``server_prop_overrides``, replacing
        any existing entry for ``key`` so the rendered config is unambiguous."""
        service.server_prop_overrides = [
            override for override in service.server_prop_overrides if override[0] != key
        ] + [[key, value]]

    # -----------------------------------------------------------------------
    # Helpers: switch completion (broker-side JMX)
    # -----------------------------------------------------------------------

    def _wait_for_switch_complete(self, topic, timeout_sec=None):
        """Wait until the classic-to-diskless init work has drained.

        The topic config becomes visible before leaders finish sealing their
        classic logs and before InitDisklessLogManager commits the diskless
        start offsets. Gate on broker-side JMX (sealed leader partitions present
        and zero in-flight init partitions) so the post-switch produce does not
        race the switch-pending state.
        """
        if timeout_sec is None:
            timeout_sec = self.SWITCH_TIMEOUT_SEC

        wait_until(lambda: self._diskless_config_visible(topic),
                   timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Topic %s did not show diskless.enable=true within %ds"
                           % (topic, timeout_sec))

        expected_sealed = self.NUM_PARTITIONS

        def check():
            try:
                self.kafka.read_jmx_output_all_nodes()
                sealed_key = "%s:Value" % self.SEALED_LEADER_PARTITIONS_JMX_OBJECT
                in_flight_key = "%s:Value" % self.INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT
                sealed = 0.0
                in_flight = 0.0
                for time_to_stats in self.kafka.jmx_stats:
                    if time_to_stats:
                        latest = max(time_to_stats.keys())
                        sealed += time_to_stats[latest].get(sealed_key, 0)
                        in_flight += time_to_stats[latest].get(in_flight_key, 0)
                self.logger.info("Switch state for %s: sealed leader partitions=%d/%d, "
                                 "in-flight init partitions=%d"
                                 % (topic, int(sealed), expected_sealed, int(in_flight)))
                return sealed >= expected_sealed and in_flight == 0
            except Exception as e:  # noqa: BLE001 - JMX reads race broker startup
                self.logger.debug("Failed to read switch JMX state for %s: %s" % (topic, e))
                return False

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=5,
                   err_msg=("Topic %s did not finish the classic-to-diskless switch within %ds "
                            "(expected >= %d sealed leader partitions and zero in-flight init "
                            "partitions)" % (topic, timeout_sec, expected_sealed)))

    def _diskless_config_visible(self, topic):
        try:
            return self.kafka.describe_topic_config(topic).get("diskless.enable") == "true"
        except Exception:  # noqa: BLE001 - describe can race the config propagation
            return False

    # -----------------------------------------------------------------------
    # Helpers: offsets / reads
    # -----------------------------------------------------------------------

    def _partition_has_leader(self, topic, partition):
        try:
            return self.kafka.leader(topic, partition=partition) is not None
        except Exception as e:  # noqa: BLE001 - leader lookup races broker startup
            self.logger.debug("Leader lookup for %s-%d not ready yet: %s" % (topic, partition, e))
            return False

    def _read_contiguous_from(self, topic, from_offset=0, max_messages=1, partition=0,
                              timeout_ms=120000):
        """Fetch up to ``max_messages`` records starting at ``from_offset`` via
        ``kafka-console-consumer.sh`` and return ``(first_offset, num_read)``.

        ``first_offset`` is -1 if no record is returned within the timeout."""
        node = self.kafka.nodes[0]
        cmd = ("%s --bootstrap-server %s --topic %s --partition %d --offset %d "
               "--max-messages %d --timeout-ms %d "
               "--property print.offset=true --property print.key=false "
               "--property print.value=false" % (
                   self.kafka.path.script("kafka-console-consumer.sh", node),
                   self.kafka.bootstrap_servers(),
                   topic,
                   partition,
                   from_offset,
                   max_messages,
                   timeout_ms,
               ))
        first_offset = -1
        num_read = 0
        try:
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                # print.offset=true emits e.g. "Offset:231900"; fall back to any
                # bare integer in case the formatter output differs by version.
                match = re.search(r"Offset:(\d+)", line) or re.search(r"\b(\d+)\b", line)
                if match:
                    if first_offset < 0:
                        first_offset = int(match.group(1))
                    num_read += 1
        except Exception as e:  # noqa: BLE001 - best effort; tool may time out
            self.logger.warn("Failed to read from %s-%d at offset %d: %s"
                             % (topic, partition, from_offset, str(e)))
        return first_offset, num_read

    def _offset_at(self, topic, time_spec, partition=0):
        """Return the offset for ``topic``/``partition`` at the given
        ``kafka-get-offsets.sh --time`` spec.

        ``-1`` = latest, ``-2`` = earliest (log start), ``-4`` = earliest-local.
        Returns ``-1`` if the offset cannot be parsed yet."""
        node = self.kafka.nodes[0]
        cmd = "%s --bootstrap-server %s --topic %s --partitions %d --time %s" % (
            self.kafka.path.script("kafka-get-offsets.sh", node),
            self.kafka.bootstrap_servers(),
            topic,
            partition,
            time_spec,
        )
        try:
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                parts = line.strip().split(":")
                if len(parts) == 3 and parts[0] == topic and parts[1] == str(partition):
                    return int(parts[2])
        except Exception as e:  # noqa: BLE001 - best effort; tool may not be ready
            self.logger.warn("Failed to read offset (time=%s) for %s-%d: %s"
                             % (time_spec, topic, partition, str(e)))
        return -1

    def _wait_for_offset(self, topic, time_spec, partition=0, timeout_sec=60):
        """Poll ``_offset_at`` until it parses a real (>= 0) offset and return it."""
        result = {"offset": -1}

        def _ready():
            result["offset"] = self._offset_at(topic, time_spec=time_spec, partition=partition)
            return result["offset"] >= 0

        wait_until(_ready, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Could not read offset (time=%s) for %s-%d."
                           % (time_spec, topic, partition))
        return result["offset"]
