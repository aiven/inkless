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


class RetentionBytesReclaimsAcrossTiersTest(Test):
    """``retention.bytes`` on a consolidating topic must reclaim data from the
    remote tier and advance the topic's earliest readable offset.

    This is the size-based counterpart to ``consolidation_retention_across_tiers_test.py``
    (which drives the reclaim with ``retention.ms``). The distinction matters
    because the two limits are enforced by *different* code paths on a
    born-consolidated topic:

    - The Inkless retention enforcer only measures the diskless WAL batches in the
      control plane (``logs.byte_size``). Once the WAL is pruned that size is tiny,
      so ``retention.bytes`` compared against it never binds and never advances the
      topic earliest -- which is why a naive ``retention.bytes`` test was previously
      inconclusive.
    - The whole-log ``retention.bytes`` for a tiered topic is enforced by the classic
      ``RemoteLogManager``: it sums the not-yet-tiered local tail plus the remote
      segments and, when that total exceeds the limit, deletes the oldest *remote*
      segments and raises ``UnifiedLog.logStartOffset``. On a consolidating topic that
      advance is published to the control plane by ``CrossTierLogStartReporter``
      (``remote_log_start_offset``), so ``ListOffsets(EARLIEST)`` moves off-leader too.

    So the reclaim that this test exercises is genuinely cross-tier: after the
    born-consolidated topic has drained (WAL -> local -> remote) and the WAL is
    pruned, the early prefix lives *only* in remote while the topic earliest is
    still 0. Lowering ``retention.bytes`` below the remote size must then:

    - advance the topic's earliest readable offset past 0,
    - leave a contiguous surviving tail (no over-deletion or corruption), and
    - physically remove the reclaimed remote objects (no storage leak).

    Determinism without wall-clock timing: rather than guess the per-record on-disk
    size, the test measures how many bytes actually reached remote for *this* topic
    (the delta over the pre-produce baseline, since this test is the only producer
    in its window) and sets ``retention.bytes`` to half of that. The
    ``RemoteLogManager`` deletes the oldest ~half by size, so the boundary lands well
    inside ``(0, acked)`` regardless of record size, and the newer half survives.
    ``retention.ms`` is held long throughout so the only thing that can reclaim is
    the size limit.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a stale
    # topic name would let old rows/objects skew the control-plane and mc queries.
    TOPIC_PREFIX = "retention-bytes-across-tiers"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Enough that the throttled produce rolls many small closed segments over
    # wall-clock time (segment.ms); only closed, tiered segments end up in remote,
    # which is the tier this test reclaims from. At the VerifiableProducer default
    # (~13 B/record) this is a few MiB of remote data.
    NUM_RECORDS = 400000
    # Long retention.ms so nothing is reclaimed by time; the size limit set below
    # is the only thing that can advance the earliest offset.
    INITIAL_RETENTION_MS = 604800000
    # Throttled produce so closed segments roll over wall-clock time (segment.ms),
    # guaranteeing many small, tiered segments. Keeping the segments small (well
    # below the 1 MiB size cap) is what makes halving the remote size delete many
    # oldest segments rather than risk deleting none (a single oversized oldest
    # segment could exceed the excess and survive).
    PRODUCE_SPAN_SEC = 120
    PRODUCE_THROUGHPUT = NUM_RECORDS // PRODUCE_SPAN_SEC
    # Guard against the degenerate "nothing (meaningfully) tiered" case before we
    # derive a size limit from the measured remote bytes. A drained topic ships
    # several MiB to remote; 2 MiB is a safe floor well under that.
    MIN_REMOTE_BYTES = 2 * 1048576

    def __init__(self, test_context):
        super(RetentionBytesReclaimsAcrossTiersTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    def _start_cluster(self):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            consolidation=True,
            server_prop_overrides=[
                # Run the WAL pruner / file cleaner / remote-log task / retention
                # sweep fast so the pipeline drains and the deleted remote segments
                # are reclaimed within the test window (not the default minutes).
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
                ["inkless.retention.enforcement.interval.ms", "5000"],
                ["remote.log.manager.task.interval.ms", "5000"],
                ["log.retention.check.interval.ms", "5000"],
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
                        # Evict local segments soon after upload so the early prefix
                        # lives only in remote -- retention.bytes must then reclaim
                        # the remote tier, not just the local cache.
                        "local.retention.ms": 5000,
                        # Long time retention: only the size limit set later can
                        # reclaim, so the advance is unambiguously bytes-driven.
                        "retention.ms": str(self.INITIAL_RETENTION_MS),
                        # retention.bytes is left unset (unlimited) until phase 1 is
                        # tiered, so nothing is reclaimed by size during the drain.
                    },
                },
            },
        )
        self.kafka.start()

    def _drain_pipeline(self, verifier, baseline_tiered, acked):
        """Wait until the born-consolidated topic has WAL -> local -> remote drained
        and the WAL is pruned, then return the peak tiered-object count (the
        physical-deletion baseline)."""
        wait_until(lambda: verifier.local_lag() == 0,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Consolidation local lag did not drain to 0.")
        self.logger.info("Consolidation local lag drained to 0")

        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Tiered-storage object count did not grow above baseline.")

        wait_until(lambda: verifier.min_log_start_offset(self.TOPIC) > 0
                   or verifier.wal_batch_count(self.TOPIC) == 0,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="WAL was not pruned in the Postgres control plane.")

        # Remote retention only reclaims what actually reached the remote tier.
        min_tiered = max(acked // 2, 1)
        verifier.wait_for_tiered_offset_at_least(self.TOPIC, min_tiered)
        self.logger.info("Latest-tiered offset reached at least %d (of %d acked)"
                         % (min_tiered, acked))

        # Settle the peak before returning: consolidation keeps tiering after the WAL
        # drains locally, so a snapshot here can still be climbing. A settled peak (and
        # a settled remote byte total for the size limit below) makes the later
        # "count dropped after retention" check meaningful.
        tiered_peak = verifier.wait_for_tiered_count_stable()
        self.logger.info("Pipeline drained: tiered_peak=%d, diskless log_start=%d, wal_batches=%d"
                         % (tiered_peak, verifier.min_log_start_offset(self.TOPIC),
                            verifier.wal_batch_count(self.TOPIC)))

        # The consolidated prefix is remote-only now, but the topic earliest is still
        # 0 (remote serves [0, diskless_start)). This is the precondition that makes
        # the retention below a genuine cross-tier reclaim rather than a WAL prune.
        earliest = verifier.wait_for_offset(self.TOPIC, time_spec=-2)
        assert earliest == 0, (
            "expected earliest==0 before retention (remote still serves the prefix), got %d" % earliest)
        return tiered_peak

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_retention_bytes_reclaims_across_tiers(self, metadata_quorum):
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()
        baseline_tiered = verifier.tiered_object_count()
        baseline_tiered_bytes = verifier.tiered_object_bytes()

        verifier.start_jmx()

        acked = verifier.produce(self.TOPIC, self.NUM_RECORDS, label="produce",
                                 throughput=self.PRODUCE_THROUGHPUT,
                                 timeout_sec=self.PRODUCE_SPAN_SEC + 120)
        self.logger.info("Produced and acked %d records (throttled)" % acked)

        tiered_peak = self._drain_pipeline(verifier, baseline_tiered, acked)

        # Bytes this topic shipped to remote = the growth over the pre-produce
        # baseline (this test is the only producer in its window; the tiered prefix
        # otherwise only accumulates across tests, never shrinks on its own).
        topic_remote_bytes = verifier.tiered_object_bytes() - baseline_tiered_bytes
        self.logger.info("This topic contributed ~%d bytes to remote (%d - %d baseline)"
                         % (topic_remote_bytes, verifier.tiered_object_bytes(), baseline_tiered_bytes))
        assert topic_remote_bytes >= self.MIN_REMOTE_BYTES, (
            "only %d bytes reached remote for %s (need >= %d to set a meaningful size "
            "limit); the pipeline did not tier enough to exercise a cross-tier reclaim"
            % (topic_remote_bytes, self.TOPIC, self.MIN_REMOTE_BYTES))

        # Keep ~half of the remote data: the RemoteLogManager deletes the oldest
        # segments until local_tail + remote <= retention.bytes, so the boundary
        # lands inside (0, acked) and the newer half survives.
        retention_bytes = topic_remote_bytes // 2
        verifier.kafka.alter_topic_config(self.TOPIC, "retention.bytes", str(retention_bytes))
        self.logger.info("Lowered retention.bytes to %d (~half of the %d remote bytes); "
                         "waiting for the size-driven reclaim to reach a stable floor"
                         % (retention_bytes, topic_remote_bytes))

        # 1) retention.bytes advances earliest past 0 to a stable floor strictly
        #    inside the produced range: the oldest remote segments are reclaimed
        #    while the newer survivors (whose size fits under the limit) are kept.
        earliest = verifier.wait_for_earliest_stable(self.TOPIC, timeout_sec=300)
        assert 0 < earliest < acked, (
            "retention.bytes left earliest at %d; expected a cross-tier reclaim of the "
            "oldest remote segments with a surviving tail, i.e. earliest in (0, %d)"
            % (earliest, acked))
        self.logger.info("retention.bytes advanced earliest to a stable %d (of %d acked)"
                         % (earliest, acked))

        # 2) The reclaimed remote segments are physically removed (no storage leak).
        wait_until(lambda: verifier.tiered_object_count() < tiered_peak,
                   timeout_sec=240, backoff_sec=5,
                   err_msg=("tiered-storage object count did not drop below the pre-retention "
                            "peak of %d; the reclaimed remote segments were not deleted "
                            "(storage leak)." % tiered_peak))
        self.logger.info("Tiered-storage object count dropped below peak %d to %d after retention"
                         % (tiered_peak, verifier.tiered_object_count()))

        # 3) A fetch from the new earliest actually serves that offset from remote
        #    (the advertised floor is backed by real, readable data).
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=earliest)
        assert first_served == earliest, (
            "a fetch from the new earliest %d returned offset %d, not %d; the surviving "
            "tail is not served from the reclaim boundary" % (earliest, first_served, earliest))

        # 4) The surviving tail [earliest, end) comes back contiguous with matching
        #    content: the single producer wrote its sequence number as each value,
        #    so value == offset. Catches gaps/dupes/reorder/corruption.
        expected = acked - earliest
        spot = min(expected, 20000)
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=earliest, max_messages=spot, timeout_ms=240000)
        assert len(records) >= spot, (
            "bounded read from the retention boundary %d returned only %d of %d records; "
            "surviving data was lost across tiers" % (earliest, len(records), spot))
        for i, (offset, value) in enumerate(records[:spot]):
            expected_offset = earliest + i
            assert offset == expected_offset, (
                "non-contiguous read at position %d: offset=%d, expected %d (gap/dupe/reorder)"
                % (i, offset, expected_offset))
            assert value == offset, (
                "content mismatch at offset %d: value=%d; the record served after the "
                "size-driven reclaim does not match what was produced." % (offset, value))
        self.logger.info("Surviving tail from %d read back contiguous with correct content (%d records)"
                         % (earliest, spot))
