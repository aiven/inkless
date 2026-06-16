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


class SwitchedReadFromRemoteAfterPruneTest(Test):
    """Durability test for a *switched* (classic -> diskless) consolidated topic:
    after the classic prefix has been tiered to remote, the topic is switched to
    diskless, the diskless tail is consolidated to remote, the diskless WAL is
    pruned, and every local copy is destroyed -- yet a fetch from offset ``0``
    must still serve the classic remote prefix.

    This is the switched-topic analogue of :class:`ReadFromRemoteAfterPruneTest`
    (born-consolidated). It exercises a path that the born-consolidated test
    cannot: the records at and below offset ``0`` were written by the *classic*
    log with the *classic* leader epoch (a switched topic's diskless interval
    carries ``last_classic_epoch + 1``), so reconstructing their location in the
    remote tier after a local-log loss requires resolving the **classic** epoch
    for the boundary offset before the tiered-storage segment can be fetched.

    Sequence:
      1. Create a *plain* classic topic (``diskless.enable=false``, **no**
         ``remote.storage.enable``) with a small ``segment.bytes`` so the classic
         prefix rolls several closed segments. Produce a classic prefix large
         enough to roll several segments.

         Why plain classic (not classic-tiered)? It keeps the classic prefix
         *un-tiered* until the switch, so the closed classic segments ``[0, S)``
         are uploaded to remote (with their *classic* leader epochs) only once
         consolidation starts -- exactly the data path this test exercises.
      2. Switch the topic to consolidating diskless with a **single combined
         alter** that sets ``diskless.enable=true`` and ``remote.storage.enable=
         true`` together (plus a short ``local.retention.ms``), then wait for the
         switch to complete. This is the canonical "classic -> consolidated"
         transition (see ``DisklessAndRemoteStorageConfigsTest``): the controller
         marks the switch pending and the leader seals the classic log at offset
         ``S = classic_count`` while the topic is already consolidating; the
         diskless interval is ``[S, ...)``. Once the seal commits, the leader
         starts the consolidation fetcher for the diskless tail.

         (Applying the two configs in separate alters does not work: enabling
         remote *first* trips the mutual-exclusion validator, and enabling it
         *after* a diskless-only switch never starts consolidation because that is
         a config-only delta with no leader/partition change to trigger it.)
      3. Tier the classic prefix: with ``remote.storage.enable=true`` now on, the
         classic RLM uploads the closed classic segments ``[0, S)`` to remote and
         the short ``local.retention.ms`` deletes the local copies, so the
         earliest-local offset advances past 0 -- record it as a
         *guaranteed-remote watermark* (every offset below it is remote-only at
         that instant).
      4. Produce a diskless tail; wait until the consolidation pipeline tiers it
         to remote and the diskless WAL is pruned above the seal.
      5. Stop all brokers, ``rm -rf`` the topic's partition directories on every
         broker, and restart with empty local logs.
      6. Assert the switched-topic remote-read guarantees:
           a. ``ListOffsets(earliest)`` reports ``0`` (the true start of the
              whole log), not the diskless start.
           b. A fetch starting at offset ``0`` actually returns offset ``0``,
              served from the *classic* remote tier (this is the assertion that
              fails if classic-epoch resolution / the tiered-storage rebuild is
              not wired into the consolidation read path).
           c. A bounded contiguous read from offset ``0`` returns records
              starting at ``0`` -- staying strictly below the guaranteed-remote
              watermark so it never crosses into the (intentionally local-only)
              un-tiered classic tail.

    .. note::

       Unlike the born-consolidated test, this test does **not** assert that
       *every* produced record is readable after the wipe. A classic topic
       always has an active (un-rolled) segment at the seal point whose records
       were never uploaded to remote; consolidation only ever tiers the diskless
       interval ``[S, ...)`` and never re-uploads the classic prefix. Wiping all
       replicas therefore legitimately drops that un-tiered classic tail, so a
       full read-back count would be short *by design*. The offset-level checks
       above isolate exactly the switched-topic remote-read fix without depending
       on that inherently-local data.

       See ``docs/inkless/CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md``.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a
    # stale topic name would let old rows skew the control-plane queries.
    TOPIC_PREFIX = "switched-read-from-remote-after-prune"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Classic prefix: sized to roll several segments by size (1 MiB floor) so a
    # large, contiguous part of it tiers to remote and is locally deleted.
    NUM_CLASSIC_RECORDS = 150000
    # Diskless tail: large enough that consolidation tiers it and the WAL pruner
    # advances the diskless log-start above the seal.
    NUM_DISKLESS_RECORDS = 150000

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
            # Enable consolidation per-test; the constructor also flips the controller.
            consolidation=True,
            # Run the WAL pruner / file cleaner fast; cache TTL must stay <= retention/2.
            # diskless.allow.from.classic.enable opens the classic -> diskless switch
            # bridge (validated on every node via ControllerConfigurationValidator).
            # log.diskless.enable=false flips the *cluster default* for new topics back
            # to classic: consolidation=True otherwise defaults topics to diskless, which
            # would both make our "classic" topic born-diskless and trip the
            # diskless.enable/remote.storage.enable mutual-exclusion validator at creation.
            # The consolidation pipeline itself is gated on
            # diskless.remote.storage.consolidation.enable (still on), not this default.
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
            jmx_object_names=list(self.SWITCH_JMX_OBJECT_NAMES),
            jmx_attributes=list(self.SWITCH_JMX_ATTRIBUTES),
        )
        # The classic -> diskless switch is gated on the controller, which
        # validates diskless.allow.from.classic.enable against
        # remote.log.storage.system.enable (already on via consolidation=True).
        # server_prop_overrides only reach the brokers, so mirror the switch
        # bridge onto the isolated controller quorum and disable JMX there.
        if getattr(self.kafka, "isolated_controller_quorum", None):
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + [
                ["log.diskless.enable", "false"],
                ["diskless.allow.from.classic.enable", "true"],
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

        # --- 1) Classic phase: produce a classic prefix on a *plain* classic topic ---
        #
        # The topic is born plain classic (remote storage off) on purpose: see the
        # class docstring. Keeping it un-tiered until the switch means the closed
        # classic segments [0, seal) are uploaded to remote (with their classic
        # leader epochs) only once consolidation starts in step 2.
        self._create_classic_topic(self.TOPIC)

        classic_acked = self._produce(self.NUM_CLASSIC_RECORDS, "classic")
        seal_offset = classic_acked
        self.logger.info("Produced classic prefix: acked=%d (seal offset will be %d)"
                         % (classic_acked, seal_offset))

        # --- 2) Switch the classic topic straight to consolidating diskless ---
        #
        # Canonical "classic -> consolidated" transition: a single combined alter
        # sets diskless.enable=true and remote.storage.enable=true together (plus a
        # short local.retention.ms so the classic prefix is locally deleted after
        # it tiers). The topic is already consolidating at the pending delta, but
        # the leader still seals the classic log at `seal_offset`; once the seal
        # commits, the leader starts the consolidation fetcher for the diskless
        # tail. Applying the two configs in separate alters does NOT work (remote
        # first trips the mutual-exclusion validator; remote after a diskless-only
        # switch is a config-only delta that never starts consolidation).
        self.logger.info("Switching topic %s to consolidating diskless (combined alter)" % self.TOPIC)
        self.kafka.alter_topic_configs(self.TOPIC, {
            "diskless.enable": "true",
            "remote.storage.enable": "true",
            "local.retention.ms": "5000",
        })
        self._wait_for_switch_complete(self.TOPIC)
        self.logger.info("Classic-to-consolidated switch completed for %s" % self.TOPIC)

        # --- 3) Tier the classic prefix ---
        #
        # With remote.storage.enable=true now on, the classic RLM uploads the
        # closed classic segments [0, seal) to the shared MinIO tier with their
        # *classic* leader epochs; the short local.retention.ms then deletes the
        # local copies, advancing the earliest-local offset above 0.
        #
        # The classic tier grew: closed classic segments were uploaded to MinIO.
        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Classic tiered-storage object count did not grow above baseline "
                           "after enabling remote storage on the switched topic.")
        self.logger.info("Classic tiered-storage object count grew to %d"
                         % verifier.tiered_object_count())

        # Wait until the earliest-local offset advances past 0 -- proof that the
        # classic closed segments were uploaded to remote AND deleted from local
        # disk. Every offset below this watermark is remote-only at this instant,
        # so a contiguous read from 0 up to it after the wipe stays entirely in
        # the classic remote tier (never touching the un-tiered classic tail).
        remote_watermark = self._wait_for_local_log_truncation(self.TOPIC)
        self.logger.info("Classic guaranteed-remote watermark (earliest-local): %d"
                         % remote_watermark)
        assert remote_watermark > 0, \
            "Classic prefix never tiered: earliest-local stayed at 0 after enabling remote storage."

        # --- 4) Diskless phase: produce the tail, consolidate, and prune the WAL ---
        #
        # Snapshot the tiered count BEFORE producing the tail. The producer for
        # 150k records runs for tens of seconds, and consolidation starts tiering
        # the diskless interval *while it is still producing* -- so a baseline taken
        # after the produce would already include the first tiered diskless segment
        # and the "grew above baseline" wait below could never observe the growth.
        pre_diskless_tiered = verifier.tiered_object_count()
        self.logger.info("Tiered-storage object count before diskless tail: %d" % pre_diskless_tiered)

        diskless_acked = self._produce(self.NUM_DISKLESS_RECORDS, "diskless")
        self.logger.info("Produced diskless tail: acked=%d" % diskless_acked)

        # The remote tier grew again as consolidation tiered the diskless interval.
        wait_until(lambda: verifier.tiered_object_count() > pre_diskless_tiered,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Diskless consolidation did not grow the tiered-storage object count.")
        self.logger.info("Tiered-storage object count grew to %d after consolidation"
                         % verifier.tiered_object_count())

        # The diskless WAL pruner advanced the diskless log-start *above the seal*:
        # this is the switched-topic analogue of the born test's "log-start > 0".
        # (For a switched topic the diskless interval -- and thus the control-plane
        # log_start_offset -- begins at the seal, so we require it to move past it.)
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > seal_offset,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="Diskless WAL was not pruned above the seal in the control plane.")
        self.logger.info("Control plane pruned the diskless WAL: batch_count=%d, "
                         "min_log_start_offset=%d, seal=%d" %
                         (verifier.wal_batch_count(self.TOPIC),
                          verifier.min_log_start_offset(self.TOPIC),
                          seal_offset))

        # --- 5) Destroy every local copy, then read the classic remote prefix ---

        # Stop ALL brokers before wiping so that, on restart, no surviving replica
        # can serve the classic prefix from its local log (which would let the
        # read bypass remote and pass spuriously). The controller is isolated and
        # stays up, as do __cluster_metadata, __remote_log_metadata and
        # __consumer_offsets on disk (we wipe only this topic's directories).
        self.logger.info("Stopping all brokers to wipe local copies of %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=120)

        for node in self.kafka.nodes:
            self._wipe_topic_local_logs(node, self.TOPIC)

        self.logger.info("Restarting all brokers with empty local logs for %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.start_node(node, timeout_sec=120)

        wait_until(lambda: self._partition_has_leader(self.TOPIC, 0),
                   timeout_sec=120, backoff_sec=2,
                   err_msg="Partition %s-0 did not get a leader after the restart." % self.TOPIC)

        # --- 6a) earliest must read as 0 (the true start of the whole log) ---
        #
        # No records were ever deleted by retention (retention.ms is the multi-day
        # default), so the classic prefix starting at offset 0 is still durable in
        # the remote tier. The diskless control-plane log_start_offset legitimately
        # advanced to the start of the pruned diskless interval; the read/recovery
        # path must NOT adopt that as the start of the whole log.
        earliest = self._wait_for_offset(self.TOPIC, time_spec=-2)
        diskless_log_start = verifier.min_log_start_offset(self.TOPIC)
        self.logger.info("After wipe+restart: earliest offset=%d, diskless log_start_offset=%d, seal=%d"
                         % (earliest, diskless_log_start, seal_offset))
        assert earliest == 0, (
            "SWITCHED CONSOLIDATION DURABILITY BUG: after wiping local logs and restarting, the "
            "partition's earliest offset is %d, not 0. The diskless control-plane log_start_offset "
            "is %d (the start of the pruned *diskless* interval, at/above the seal %d), but the "
            "classic prefix [0, %d) was tiered to the classic remote tier and is still durable in "
            "object storage. Expected earliest=0 so the classic remote prefix remains readable. "
            "See docs/inkless/CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md."
            % (earliest, diskless_log_start, seal_offset, seal_offset))

        # --- 6b) a fetch from offset 0 must actually return offset 0 from remote ---
        #
        # This is the switched-topic-specific assertion: offset 0 lives in the
        # classic interval with the *classic* leader epoch (the diskless interval
        # carries last_classic_epoch + 1). Serving it after a local-log loss
        # requires resolving the classic epoch for the boundary offset and
        # fetching the corresponding classic remote segment. If that rebuild is
        # not wired into the consolidation read path, the broker silently serves
        # from the diskless start instead and the first record returned is the
        # diskless log-start rather than 0.
        first_served = self._first_served_offset(self.TOPIC, from_offset=0)
        self.logger.info("First offset actually served by a fetch from 0: %d "
                         "(diskless log_start_offset=%d, seal=%d)"
                         % (first_served, diskless_log_start, seal_offset))
        assert first_served == 0, (
            "SWITCHED CONSOLIDATION DURABILITY BUG: a fetch starting at offset 0 returned a record "
            "at offset %d, not 0. ListOffsets reports earliest=0, but the broker does not actually "
            "serve the classic remote prefix: the records below the seal (%d) were written with the "
            "classic leader epoch, and rebuilding the wiped local log from the diskless WAL clears "
            "the leader-epoch cache, so reads below the diskless start cannot be mapped to their "
            "classic remote segments unless the tiered-storage rebuild resolves the classic epoch. "
            "See docs/inkless/CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md."
            % (first_served, seal_offset))

        # --- 6c) a bounded contiguous read from 0 stays in the classic remote tier ---
        #
        # Read a capped number of records strictly below the guaranteed-remote
        # watermark recorded before the switch. Every one of these offsets was
        # tiered-and-locally-deleted while the topic was classic, so after the
        # wipe they can only come from the classic remote tier, and they form a
        # contiguous range that never reaches the un-tiered classic tail (the
        # active segment at the seal, which is local-only by design).
        read_count = min(remote_watermark, 20000)
        first_offset, num_read = self._read_contiguous_from(self.TOPIC, from_offset=0,
                                                            max_messages=read_count)
        self.logger.info("Bounded contiguous read from 0: first_offset=%d, num_read=%d (requested %d)"
                         % (first_offset, num_read, read_count))
        assert first_offset == 0, (
            "Bounded read from offset 0 started at %d, not 0: the classic remote prefix is not "
            "served from its true start after the wipe." % first_offset)
        assert num_read >= read_count, (
            "Bounded contiguous read from offset 0 returned only %d of %d records below the "
            "guaranteed-remote watermark (%d): part of the tiered classic prefix is not readable "
            "from remote after wiping local logs." % (num_read, read_count, remote_watermark))

    # -----------------------------------------------------------------------
    # Helpers: topic / produce
    # -----------------------------------------------------------------------

    def _create_classic_topic(self, topic):
        """Plain classic (non-diskless, non-tiered) topic with a small
        ``segment.bytes`` so the classic prefix rolls several closed segments.

        Remote storage is deliberately **not** enabled here: it is turned on as
        part of the single combined switch alter (see the class docstring), so the
        closed classic segments tier to remote only once consolidation starts,
        carrying their classic leader epochs.

        ``diskless.enable`` is deliberately left unset: the cluster default is
        forced to ``false`` (see ``log.diskless.enable`` in the broker overrides),
        so the topic is born classic."""
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
    # Helpers: wipe / offsets / reads
    # -----------------------------------------------------------------------

    def _wipe_topic_local_logs(self, node, topic):
        """Delete the topic's partition directories from both broker data dirs.

        Only the topic's ``<topic>-<partition>`` directories are removed -- never
        the whole persistent root -- so the broker keeps its KRaft identity and
        the internal topics (``__cluster_metadata``, ``__remote_log_metadata``,
        ``__consumer_offsets``) needed to rebuild the partition and locate the
        remote segments. ``allow_fail`` because a given data dir may not hold a
        replica of this partition.
        """
        for data_dir in (KafkaService.DATA_LOG_DIR_1, KafkaService.DATA_LOG_DIR_2):
            node.account.ssh("rm -rf -- %s/%s-*" % (data_dir, topic), allow_fail=True)
        self.logger.info("Wiped local logs for %s on %s" % (topic, node.account.hostname))

    def _partition_has_leader(self, topic, partition):
        try:
            return self.kafka.leader(topic, partition=partition) is not None
        except Exception as e:  # noqa: BLE001 - leader lookup races broker startup
            self.logger.debug("Leader lookup for %s-%d not ready yet: %s" % (topic, partition, e))
            return False

    def _wait_for_local_log_truncation(self, topic, partition=0, timeout_sec=180):
        """Wait until the topic's earliest-local offset advances past 0 and
        return it. Proves some closed segments were tiered to remote AND deleted
        locally (so reads from offset 0 below this watermark come from remote)."""
        result = {"offset": -1}

        def check():
            result["offset"] = self._offset_at(topic, time_spec=-4, partition=partition)
            self.logger.info("Topic %s-%d earliest-local offset: %d"
                             % (topic, partition, result["offset"]))
            return result["offset"] > 0

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg=("earliest-local offset for %s-%d did not advance past 0 within %ds; "
                            "classic tiering or local retention is not progressing"
                            % (topic, partition, timeout_sec)))
        return result["offset"]

    def _first_served_offset(self, topic, partition=0, from_offset=0, timeout_ms=120000):
        """Return the offset of the first record a fetch starting at
        ``from_offset`` actually returns, using ``kafka-console-consumer.sh``.

        Unlike ``kafka-get-offsets.sh`` (which only reports the *advertised*
        earliest offset), this performs a real fetch, so it reveals whether the
        broker truly serves ``from_offset`` or silently skips ahead to the local
        start. Returns -1 if no record is returned within the timeout."""
        first, _ = self._read_contiguous_from(topic, from_offset=from_offset,
                                               max_messages=1, partition=partition,
                                               timeout_ms=timeout_ms)
        return first

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
        """Poll ``_offset_at`` until it parses a real (>= 0) offset and return it.

        Reading offsets can race broker startup right after the restart, so retry
        until the tool answers rather than asserting on a transient -1."""
        result = {"offset": -1}

        def _ready():
            result["offset"] = self._offset_at(topic, time_spec=time_spec, partition=partition)
            return result["offset"] >= 0

        wait_until(_ready, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Could not read offset (time=%s) for %s-%d after the restart."
                           % (time_spec, topic, partition))
        return result["offset"]
