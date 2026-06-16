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
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer


class ReadFromRemoteAfterPruneTest(Test):
    """Tier-1 durability test for a born-consolidated topic: once data is
    consolidated to remote storage and the diskless WAL is pruned, neither the
    local log nor the WAL is needed to serve reads.

    This is the single most important consolidation guarantee, and it is the
    one thing :class:`ConsolidationPipelineTest` does *not* prove: that test
    consumes every record back after the prune, but it never destroys the local
    log, so its final read can be (and almost certainly is) served from the
    local log / page cache rather than from remote storage.

    Here we additionally **destroy every local copy** of the topic's data: we
    stop all brokers, ``rm -rf`` the topic's partition directories from both
    broker data dirs, then restart. Because the remote (tiered-storage) bucket
    is shared MinIO reachable by every broker, and the diskless control plane
    lives in Postgres, the cluster *should* be able to reconstruct the partition
    and serve all records from remote.

    .. warning::

       **This test currently FAILS: it is a known-bug reproducer.**

       The diskless control-plane ``log_start_offset`` legitimately advances to
       the start of the *diskless* interval when the WAL is pruned (e.g.
       ~232414). The bug is that, after a local-log loss, the read path can treat
       that diskless start as the start of the *whole* log, orphaning the records
       below it (offsets ``0 .. diskless_start-1``) that were consolidated to the
       classic remote tier and are still durable in object storage.

       The fix has two parts and this test gates on both:

       1. ``ListOffsets(earliest)`` must report ``0`` (the true log start), not
          the diskless start. *(Fixed: ``ConsolidationFetcherThread`` preserves
          the lower ``logStartOffset`` on reset.)*
       2. A fetch from offset ``0`` must actually return offset ``0``, served
          from the remote tier -- not silently skip to the local start.
          *(Still broken: the consolidation fetcher does not rebuild the remote
          leader-epoch/aux state, so reads below the diskless start are not
          mapped to their remote segments.)*

       The test asserts the correct behaviour for both, so it fails clearly
       while either part is broken and will pass once a consolidating partition
       can serve its consolidated remote data from offset 0 after a local wipe.

       See ``docs/inkless/CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md``.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a
    # stale topic name would let old rows skew the control-plane queries.
    TOPIC_PREFIX = "read-from-remote-after-prune"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Sized to roll several segments by size (1 MiB floor); only closed segments
    # tier, and only tiered segments can be served from remote after the wipe.
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
            # Enable consolidation per-test; the constructor also flips the controller.
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
                        # Delete local segments shortly after they are uploaded to
                        # remote, so the data physically lives in remote before the
                        # wipe (belt-and-suspenders to the explicit rm below).
                        "local.retention.ms": 5000,
                    },
                },
            },
        )
        # Collect the broker data-dir logs so a post-restart failure is debuggable.
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.start()

        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()

        baseline_tiered = verifier.tiered_object_count()
        self.logger.info("Baseline tiered-storage object count: %d" % baseline_tiered)

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

        # 3) The WAL pruner has run: the early (tiered) batches are gone from the
        #    control plane and the diskless log-start has advanced. We do NOT
        #    require the WAL to drain to *zero* batches -- the most-recent records
        #    sit in a still-open segment that has not tiered yet, so their WAL
        #    batches legitimately persist and the count never reaches 0. The
        #    durability proof below does not depend on a fully empty WAL: it
        #    relies on the early offsets being remote-only (step 4 + the wipe).
        wait_until(lambda: verifier.wal_batch_count(self.TOPIC) == 0
                   or verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=180, backoff_sec=2,
                   err_msg="WAL was not pruned in the Postgres control plane.")
        self.logger.info("Control plane pruned the WAL: batch_count=%d, "
                         "min_log_start_offset=%d, wal_object_count=%d" %
                         (verifier.wal_batch_count(self.TOPIC),
                          verifier.min_log_start_offset(self.TOPIC),
                          verifier.wal_object_count()))

        # The earliest-local offset is logged only as a diagnostic. We do NOT
        # gate on it: born-consolidated topics do not move data off local disk
        # via the classic local.retention path (the consolidation pipeline
        # manages tiering itself), so earliest-local can legitimately stay at 0
        # even though the data is in remote. The explicit wipe below -- not local
        # retention -- is what forces the post-restart read to come from remote.
        self.logger.info("Earliest-local offset before wipe: %d (diagnostic only)"
                         % self._earliest_local_offset(self.TOPIC))

        # Confirm the consolidation gauges were exported/scraped (diagnostic).
        self.kafka.read_jmx_output_all_nodes()
        jmx_keys = self.kafka.maximum_jmx_value
        local_lag_key = ConsolidationVerifier.find_aggregate_key(jmx_keys, ConsolidationVerifier.LOCAL_LAG)
        assert local_lag_key is not None, \
            ("ConsolidationLocalLag was not exported/scraped via JMX. Expected a "
             "%s 'name=ConsolidationLocalLag' Value column; got keys: %s"
             % (ConsolidationVerifier.JMX_PACKAGE, sorted(jmx_keys)))

        # --- The durability test: destroy every local copy, then read remote ---

        # Stop ALL brokers before wiping so that, on restart, no surviving
        # replica can serve the data from its local log (which would let the read
        # bypass remote and make the test pass spuriously). The controller is a
        # separate isolated node and stays up, as do __cluster_metadata,
        # __remote_log_metadata, and __consumer_offsets on disk (we wipe only
        # this topic's partition directories).
        self.logger.info("Stopping all brokers to wipe local copies of %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.stop_node(node, clean_shutdown=True, timeout_sec=120)

        for node in self.kafka.nodes:
            self._wipe_topic_local_logs(node, self.TOPIC)

        self.logger.info("Restarting all brokers with empty local logs for %s" % self.TOPIC)
        for node in self.kafka.nodes:
            self.kafka.start_node(node, timeout_sec=120)

        # Wait until the partition has a leader again before reading.
        wait_until(lambda: self._partition_has_leader(self.TOPIC, 0),
                   timeout_sec=120, backoff_sec=2,
                   err_msg="Partition %s-0 did not get a leader after the restart." % self.TOPIC)

        # --- Deterministic bug check (fails fast while the bug exists) ---
        #
        # No records were ever deleted (no DeleteRecords, and retention.ms is the
        # multi-day default; local.retention only evicts *local* copies, never the
        # remote tier), so the true start of the log is offset 0 and it must
        # remain so after the wipe -- the records below the diskless log-start are
        # in the classic remote tier and are still durable in object storage.
        #
        # The Postgres control-plane `log_start_offset` legitimately advanced to
        # the start of the *diskless* interval when the WAL was pruned. The bug is
        # that the read/recovery path adopts that diskless value as the *whole*
        # log's start, so a fresh (wiped) replica reports earliest = diskless
        # log-start instead of 0 and refuses to serve the consolidated remote
        # records below it. Assert the correct behaviour: earliest must be 0.
        earliest = self._wait_for_offset(self.TOPIC, time_spec=-2)
        diskless_log_start = verifier.min_log_start_offset(self.TOPIC)
        self.logger.info("After wipe+restart: earliest offset=%d, diskless log_start_offset=%d"
                         % (earliest, diskless_log_start))
        assert earliest == 0, (
            "CONSOLIDATION DURABILITY BUG: after wiping local logs and restarting, the "
            "partition's earliest offset is %d, not 0. The diskless control-plane "
            "log_start_offset is %d -- correctly the start of the *diskless* interval "
            "(it advanced when the WAL was pruned) -- but the read/recovery path is "
            "treating it as the start of the *whole* log. Offsets 0..%d were consolidated "
            "to the classic remote tier and are still present in object storage, yet they "
            "are now unreadable: a consumer reset to 'earliest' only sees the un-pruned WAL "
            "tip. Expected earliest=0 so the consolidated remote records remain readable. "
            "See docs/inkless/CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md."
            % (earliest, diskless_log_start, max(earliest - 1, 0)))

        # 2) The earliest offset *reads as* 0, but is offset 0 actually served? A
        #    fetch starting at offset 0 must return a record at offset 0 (from the
        #    remote tier). If the broker silently skips the remote prefix and
        #    serves from the local start instead, the first record we get back is
        #    the diskless log-start, not 0. This is a fast, deterministic check --
        #    it does NOT depend on the (unreliable) VerifiableConsumer record
        #    count -- and it captures the real durability bug: the consolidated
        #    records below the diskless start are not actually fetchable from
        #    remote even though ListOffsets reports earliest=0.
        first_served = self._first_served_offset(self.TOPIC, from_offset=0)
        self.logger.info("First offset actually served by a fetch from 0: %d "
                         "(diskless log_start_offset=%d)" % (first_served, diskless_log_start))
        assert first_served == 0, (
            "CONSOLIDATION DURABILITY BUG: a fetch starting at offset 0 returned a record at "
            "offset %d, not 0. ListOffsets reports earliest=0, but the broker silently serves "
            "from the local start (the diskless log_start_offset, %d) instead of fetching the "
            "consolidated prefix [0, %d) from the remote tier. The records are still durable in "
            "object storage but are not actually readable: rebuilding the partition's local log "
            "from the diskless WAL clears the leader-epoch cache, so reads below the diskless "
            "start cannot be mapped to their remote segments (the tiered-storage "
            "buildRemoteLogAuxState / TierStateMachine rebuild is not wired into the "
            "consolidation fetcher). "
            "See docs/inkless/CONSOLIDATION_READ_FROM_REMOTE_AFTER_PRUNE_BUG.md."
            % (first_served, diskless_log_start, max(first_served, 0)))

        # --- End-to-end durability proof (reached only once the bug is fixed) ---
        #
        # Read every record back. Local logs were wiped and the WAL's early
        # offsets were pruned, so the bulk of the data can only come from the
        # shared remote (tiered-storage) bucket plus the Postgres control plane.
        #
        # Do NOT pass on_record_consumed: it makes VerifiableConsumer run with
        # --verbose, emitting one JSON line per record (300k lines) that the
        # Python worker streams over SSH and parses one-by-one under a lock,
        # which lags far behind real consumption and never catches up within the
        # timeout. Without it the consumer still emits the periodic
        # `records_consumed` summaries that drive total_consumed().
        consumer = VerifiableConsumer(self.test_context, num_nodes=1, kafka=self.kafka,
                                      topic=self.TOPIC,
                                      group_id="read-from-remote-%s" % uuid.uuid4().hex[:8],
                                      reset_policy="earliest")
        consumer.start()
        wait_until(lambda: consumer.total_consumed() >= acked,
                   timeout_sec=240, backoff_sec=1,
                   err_msg=("Consumer read back only %d of %d records after wiping local logs; "
                            "remaining records must be served from remote storage."
                            % (consumer.total_consumed(), acked)))
        consumer.stop()
        total = consumer.total_consumed()
        self.logger.info("Read back %d records from remote after wiping local logs (acked=%d)" %
                         (total, acked))
        assert total >= acked, \
            "Data loss after wiping local logs: expected >= %d records from remote but got %d" % (acked, total)

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _wipe_topic_local_logs(self, node, topic):
        """Delete the topic's partition directories from both broker data dirs.

        Only the topic's ``<topic>-<partition>`` directories are removed -- never
        the whole persistent root -- so the broker keeps its KRaft identity and
        the internal topics (``__cluster_metadata``, ``__remote_log_metadata``,
        ``__consumer_offsets``) that the cluster needs to rebuild the partition
        and locate the remote segments. ``allow_fail`` because a given data dir
        may not hold a replica of this partition.
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

    def _earliest_local_offset(self, topic, partition=0):
        """Return the topic-partition's earliest-local offset via
        ``kafka-get-offsets.sh --time -4`` (OffsetSpec.earliestLocal()).

        Used only as a diagnostic here. Returns -1 if the offset cannot be
        parsed yet."""
        return self._offset_at(topic, time_spec=-4, partition=partition)

    def _first_served_offset(self, topic, partition=0, from_offset=0, timeout_ms=120000):
        """Return the offset of the first record a fetch starting at
        ``from_offset`` actually returns, using ``kafka-console-consumer.sh``.

        Unlike ``kafka-get-offsets.sh`` (which only reports the *advertised*
        earliest offset), this performs a real fetch, so it reveals whether the
        broker truly serves ``from_offset`` or silently skips ahead to the local
        start. Returns -1 if no record is returned within the timeout."""
        node = self.kafka.nodes[0]
        cmd = ("%s --bootstrap-server %s --topic %s --partition %d --offset %d "
               "--max-messages 1 --timeout-ms %d "
               "--property print.offset=true --property print.key=false "
               "--property print.value=false" % (
                   self.kafka.path.script("kafka-console-consumer.sh", node),
                   self.kafka.bootstrap_servers(),
                   topic,
                   partition,
                   from_offset,
                   timeout_ms,
               ))
        try:
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                # print.offset=true emits e.g. "Offset:231900"; fall back to any
                # bare integer in case the formatter output differs by version.
                match = re.search(r"Offset:(\d+)", line) or re.search(r"\b(\d+)\b", line)
                if match:
                    return int(match.group(1))
        except Exception as e:  # noqa: BLE001 - best effort; tool may time out
            self.logger.warn("Failed to read first-served offset for %s-%d from %d: %s" %
                             (topic, partition, from_offset, str(e)))
        return -1

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
            self.logger.warn("Failed to read offset (time=%s) for %s-%d: %s" %
                             (time_spec, topic, partition, str(e)))
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
