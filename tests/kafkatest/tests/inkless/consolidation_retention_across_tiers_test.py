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


class RetentionReclaimsAcrossTiersTest(Test):
    """Retention on a consolidating topic must reclaim data from *every* tier.

    Once a born-consolidated topic has drained (WAL -> local -> remote) and the WAL
    is pruned, the early prefix lives only in the remote tier while the topic's
    earliest offset is still 0 (remote serves it). Applying ``retention.ms`` must
    then:

    - advance the topic's earliest readable offset (not just the diskless WAL
      log-start in Postgres),
    - leave the surviving tail intact and contiguous (no over-deletion), and
    - physically remove the deleted remote objects (no storage leak).

    ``retention.ms`` is used (not ``retention.bytes``): on a born-consolidated topic
    cross-tier reclaim is driven by the remote-log retention path, which enforces
    time-based retention in practice; ``retention.bytes`` alone did not advance
    the topic earliest in system tests.

    The outcome is made deterministic with **two age-separated cohorts** and a
    retention *freeze*, rather than trying to land the retention boundary somewhere
    inside a single continuously-produced span (which is impossible to time
    reliably: the drain step alone ages records by an unbounded amount, so any
    boundary that depends on ``now - record_timestamp`` within one span is racy):

    - **Phase 1** (the deletable prefix): consolidated/tiered to remote, then aged
      past ``retention.ms`` during an idle window while the topic still has its long
      initial retention.
    - **Phase 2** (the survivors): a fresh cohort produced *before* the reclaim,
      separated from phase 1 by the idle window so a fresh segment rolls between
      them and the boundary lands cleanly at ``acked1``.
    - **Reclaim**: ``retention.ms`` is lowered so only the aged phase-1 cohort
      expires; ``earliest`` advances to ``acked1`` and the phase-2 tail survives.
    - **Freeze**: the long retention is restored *before* read-back so the surviving
      tail is not creeping under an active ``retention.ms`` window while it is read.

    The control-plane mechanics are unit-covered (``RetentionEnforcerTest``,
    ``EnforceRetentionJobTest``, ``PruneBatchesBelowHighestTieredOffsetV1Test``,
    ...); what is untested below that level is the end-to-end cross-tier reclaim,
    where a bug means either leaked remote data or lost surviving records.
    """

    # Unique per run: the Postgres/MinIO containers persist across runs, so a stale
    # topic name would let old rows/objects skew the control-plane and mc queries.
    TOPIC_PREFIX = "retention-across-tiers"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Phase 1: enough to roll many closed segments (1 MiB floor); only closed, tiered
    # segments end up in remote, which is the tier this test wants to delete from.
    PHASE1_RECORDS = 220000
    # Phase 2: the surviving cohort, produced fresh just before the reclaim.
    PHASE2_RECORDS = 80000
    # Long initial retention so phase 1 consolidates/tiers first; lowered only after
    # phase 1 has aged, then restored to this value before read-back.
    INITIAL_RETENTION_MS = 604800000
    # Reclaim window. Kept comfortably larger than the time it takes the floor to
    # settle after lowering retention, so the freshly produced phase-2 cohort never
    # ages into the window before the long retention is restored.
    RETENTION_MS = 120000
    # Throttled phase-1 produce so closed segments roll over wall-clock time.
    PHASE1_SPAN_SEC = 120
    PHASE1_THROUGHPUT = PHASE1_RECORDS // PHASE1_SPAN_SEC
    # After phase 1 is tiered, idle this long (while retention is still long) so the
    # *entire* phase-1 cohort is older than RETENTION_MS when it is lowered. Drain and
    # the phase-2 produce age phase 1 further, so this only needs to exceed
    # RETENTION_MS; the margin guards against clock skew / a fast drain.
    AGE_AFTER_TIER_SEC = RETENTION_MS // 1000 + 30

    def __init__(self, test_context):
        super(RetentionReclaimsAcrossTiersTest, self).__init__(test_context=test_context)
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
                # Inkless retention enforcement (the path that actually advances
                # the diskless log_start on a born-consolidated topic) defaults to
                # 5 minutes per broker, multiplied by broker count with +/-25%
                # jitter -- with 3 brokers a partition's first enforcement fires
                # 11-19 minutes after scheduling, far outside the test window.
                # 5s base -> 11-19s per-partition first-fire range.
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
                        # lives only in remote -- retention must then reclaim remote.
                        "local.retention.ms": 5000,
                        # Long retention until phase 1 is tiered and aged; lowered
                        # only after the idle window below.
                        "retention.ms": str(self.INITIAL_RETENTION_MS),
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

        tiered_peak = verifier.tiered_object_count()
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

    def _assert_record_value(self, offset, value, phase1_end):
        """``VerifiableProducer`` (``is_int``) restarts its per-producer sequence at 0
        on each run, so phase-2 values are ``offset - phase1_end``."""
        if offset < phase1_end:
            assert value == offset, (
                "content mismatch at offset %d: value=%d (phase-1 record corrupted)" % (offset, value))
        else:
            assert value == offset - phase1_end, (
                "content mismatch at offset %d: value=%d (phase-2 record corrupted)" % (offset, value))

    def _read_back_tail(self, verifier, from_offset, expected, phase1_end):
        """Fetch the surviving tail ``[from_offset, end)`` via an explicit-offset
        console-consumer read.

        A ``VerifiableConsumer`` with ``reset_policy=earliest`` was avoided: while
        ``retention.ms`` is still active the floor can advance between measuring
        earliest and the group joining, so the consumer would start above the
        measured boundary and miss records (or report a misleading 0)."""
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=from_offset, max_messages=expected,
            timeout_ms=240000)
        assert len(records) >= expected, (
            "read back only %d of %d surviving records from [%d, end); the floor may "
            "have advanced again or surviving data was lost across tiers"
            % (len(records), expected, from_offset))
        for i, (offset, value) in enumerate(records[:expected]):
            assert offset == from_offset + i, (
                "non-contiguous read at position %d: offset=%d, expected %d (gap/dupe/reorder)"
                % (i, offset, from_offset + i))
            self._assert_record_value(offset, value, phase1_end)

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_retention_reclaims_across_tiers(self, metadata_quorum):
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()
        baseline_tiered = verifier.tiered_object_count()

        verifier.start_jmx()

        acked1 = verifier.produce(self.TOPIC, self.PHASE1_RECORDS, label="phase1",
                                  throughput=self.PHASE1_THROUGHPUT,
                                  timeout_sec=self.PHASE1_SPAN_SEC + 120)
        self.logger.info("Phase 1 produced and acked %d records (throttled)" % acked1)

        tiered_peak = self._drain_pipeline(verifier, baseline_tiered, acked1)

        self.logger.info("Idling %ds so the whole phase-1 cohort ages past retention.ms=%d "
                         "(topic retention is still long)"
                         % (self.AGE_AFTER_TIER_SEC, self.RETENTION_MS))
        time.sleep(self.AGE_AFTER_TIER_SEC)

        # Produce the surviving cohort *before* the reclaim. The idle window above
        # guarantees a fresh segment has rolled, so phase 2 starts in its own segment
        # and the retention boundary lands cleanly between the cohorts at acked1.
        acked2 = verifier.produce(self.TOPIC, self.PHASE2_RECORDS, label="phase2")
        total_acked = acked1 + acked2
        self.logger.info("Phase 2 produced and acked %d survivor records (total=%d)"
                         % (acked2, total_acked))

        verifier.kafka.alter_topic_config(self.TOPIC, "retention.ms", str(self.RETENTION_MS))
        self.logger.info("Lowered retention.ms to %d; waiting for the reclaim to reach a stable floor"
                         % self.RETENTION_MS)

        # 1) Retention advances the earliest past 0 to a stable floor at the cohort
        #    boundary: the aged phase-1 prefix is reclaimed while the fresh phase-2
        #    cohort (younger than RETENTION_MS) must NOT be deleted (earliest<=acked1).
        earliest = verifier.wait_for_earliest_stable(self.TOPIC, timeout_sec=300)

        # 2) Freeze the floor before validation: restore the long retention so the
        #    surviving phase-2 tail cannot age into the active window during read-back.
        verifier.kafka.alter_topic_config(self.TOPIC, "retention.ms", str(self.INITIAL_RETENTION_MS))
        self.logger.info("Restored retention.ms to %d to freeze the floor at %d"
                         % (self.INITIAL_RETENTION_MS, earliest))

        assert 0 < earliest <= acked1, (
            "retention left earliest at %d; expected a cross-tier reclaim of the phase-1 "
            "prefix with the phase-2 survivors intact, i.e. earliest in (0, %d]"
            % (earliest, acked1))
        self.logger.info("retention.ms advanced earliest to a stable %d (phase-1 boundary of %d)"
                         % (earliest, acked1))

        # 3) The reclaimed remote segments are physically removed (no storage leak).
        wait_until(lambda: verifier.tiered_object_count() < tiered_peak,
                   timeout_sec=240, backoff_sec=5,
                   err_msg=("tiered-storage object count did not drop below the pre-retention "
                            "peak of %d; the reclaimed remote segments were not deleted "
                            "(storage leak)." % tiered_peak))
        self.logger.info("Tiered-storage object count dropped below peak %d to %d after retention"
                         % (tiered_peak, verifier.tiered_object_count()))

        expected = total_acked - earliest
        spot = min(expected, 20000)
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=earliest, max_messages=spot)
        assert len(records) >= spot, (
            "bounded read from the retention boundary %d returned only %d of %d records"
            % (earliest, len(records), spot))
        for i, (offset, value) in enumerate(records[:spot]):
            expected_offset = earliest + i
            assert offset == expected_offset, (
                "non-contiguous read at position %d: offset=%d, expected %d (gap/dupe/reorder)"
                % (i, offset, expected_offset))
            self._assert_record_value(offset, value, acked1)

        # 4) End-to-end: the full surviving tail [earliest, end) comes back.
        self._read_back_tail(verifier, from_offset=earliest, expected=expected, phase1_end=acked1)
