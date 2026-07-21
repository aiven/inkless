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


class CrossTierEarliestBootstrapTest(Test):
    """A consolidating topic must keep ``ListOffsets(EARLIEST)`` pinned to the true
    cross-tier earliest after the WAL prune frontier has advanced well past it.

    EARLIEST is served from the control plane as
    ``COALESCE(remote_log_start_offset, log_start_offset)``. ``log_start_offset`` is the
    diskless WAL prune frontier and climbs as batches tier and are pruned;
    ``remote_log_start_offset`` is the true lowest offset still readable from the remote
    tier and is nullable. If it were left NULL, EARLIEST would COALESCE to the pruned
    frontier and a consumer with ``auto.offset.reset=earliest`` would silently skip the
    still-live remote prefix, i.e. lose data. The become-leader report bootstraps
    ``remote_log_start_offset`` to the true remote earliest (0 for a freshly-tiered
    born-consolidated topic) so this cannot happen.

    This proves the invariant end to end: produce, tier, and prune the WAL well past 0,
    then assert the control plane holds ``remote_log_start_offset = 0`` (never NULL),
    EARLIEST is 0 on every broker despite the advanced frontier, and offset 0 is still
    served from the remote tier with the correct content.

    The genuine reporter/become-leader *fault* path (``remote_log_start_offset`` stuck
    NULL while the WAL is pruned) is left to unit coverage
    (``AbstractControlPlaneTest.getCrossTierLogStart*``, the ``RemoteLogManager``
    become-leader tests): on this code both the become-leader report and the reporter
    re-populate the value, and blackholing the control plane to suppress them also stalls
    the WAL pruner and the EARLIEST read itself, so the NULL-and-pruned state is not
    observable from a system test. The positive assertion below is the regression guard
    for the exact bootstrap the fix relies on."""

    # Unique per run: the Postgres/MinIO containers persist across runs, so a stale
    # topic name would let old rows/objects skew the control-plane and mc queries.
    TOPIC_PREFIX = "cross-tier-earliest-bootstrap"
    NUM_PARTITIONS = 1
    REPLICATION_FACTOR = 3
    # Enough to roll many closed 1 MiB segments so consolidation tiers a remote prefix and
    # the WAL pruner advances log_start_offset well past 0 (the frontier this test needs
    # high for the "EARLIEST stays 0" assertion to be non-vacuous).
    NUM_RECORDS = 300000
    # How many records from offset 0 to spot-check for readability/content.
    SPOT_CHECK = 20000

    def __init__(self, test_context):
        super(CrossTierEarliestBootstrapTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.TOPIC = "%s-%s" % (self.TOPIC_PREFIX, uuid.uuid4().hex[:8])

    def _start_cluster(self):
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=self.num_brokers,
            zk=None,
            controller_num_nodes_override=1,
            consolidation=True,
            # Run the WAL pruner / file cleaner / remote-log task fast so the frontier
            # advances within the test window (not the default minutes).
            server_prop_overrides=[
                ["inkless.consolidation.cleanup.interval.ms", "5000"],
                ["inkless.file.cleaner.interval.ms", "5000"],
                ["inkless.file.cleaner.retention.period.ms", "6000"],
                ["inkless.consume.batch.coordinate.cache.ttl.ms", "2000"],
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
                        # Evict local copies soon after upload so the early prefix lives
                        # only in remote and the read from 0 is a genuine cross-tier read.
                        "local.retention.ms": 5000,
                    },
                },
            },
        )
        self.kafka.start()

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_cross_tier_earliest_survives_wal_prune(self, metadata_quorum):
        self._start_cluster()
        verifier = ConsolidationVerifier(self.kafka)
        verifier.verify_tooling()
        verifier.start_jmx()
        baseline_tiered = verifier.tiered_object_count()

        acked = verifier.produce(self.TOPIC, self.NUM_RECORDS, "bootstrap")
        self.logger.info("Produced born-consolidated stream: acked=%d" % acked)

        wait_until(lambda: verifier.tiered_object_count() > baseline_tiered,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="Consolidation never tiered a remote prefix.")

        # Drive the WAL prune frontier well past 0 so "EARLIEST stays at 0" is a real
        # signal, not vacuously 0 because the frontier is also 0.
        wait_until(lambda: verifier.min_log_start_offset(self.TOPIC) > 0,
                   timeout_sec=240, backoff_sec=2,
                   err_msg="WAL was never pruned in the control plane; frontier stayed at 0.")
        frontier = verifier.min_log_start_offset(self.TOPIC)
        assert frontier > 0, "WAL prune frontier did not advance past 0"
        self.logger.info("WAL prune frontier advanced to log_start_offset=%d" % frontier)

        # 1) The control plane holds remote_log_start_offset = 0 (bootstrapped, never NULL).
        remote_start = verifier.wait_for_remote_log_start_bootstrapped(self.TOPIC, expected=0)
        self.logger.info("Control plane remote_log_start_offset bootstrapped to %d (frontier=%d)"
                         % (remote_start, frontier))

        # 2) EARLIEST is 0 on every broker even though the frontier is at %d: EARLIEST is
        #    COALESCE(remote_log_start_offset=0, log_start_offset=frontier) = 0. A NULL
        #    remote start would make it jump to the frontier and skip the remote prefix.
        per_broker = verifier.earliest_on_each_broker(self.TOPIC)
        assert set(per_broker.values()) == {0}, (
            "EARLIEST is not 0 on every broker despite remote_log_start_offset=0 and a WAL frontier "
            "at %d: %s (EARLIEST fell back to the pruned frontier, so the remote prefix [0, %d) "
            "would be silently skipped)" % (frontier, per_broker, frontier))
        self.logger.info("EARLIEST agreed at 0 across all brokers with the frontier at %d" % frontier)

        # 3) Offset 0 is actually served from the remote tier, contiguous with correct content
        #    (VerifiableProducer writes value == offset for a single born-consolidated run).
        first_served = verifier.first_served_offset(self.TOPIC, from_offset=0)
        assert first_served == 0, (
            "a fetch from offset 0 returned offset %d; the tiered prefix below the WAL frontier %d "
            "was not served" % (first_served, frontier))
        spot = min(acked, self.SPOT_CHECK)
        records = verifier.read_records_with_values_from(
            self.TOPIC, from_offset=0, max_messages=spot, timeout_ms=240000)
        assert len(records) >= spot, (
            "bounded read from offset 0 returned only %d of %d records; the remote prefix below the "
            "WAL frontier %d was not fully served" % (len(records), spot, frontier))
        for i, (offset, value) in enumerate(records[:spot]):
            assert offset == i, (
                "non-contiguous read at position %d: offset=%d, expected %d (gap/dupe/reorder)"
                % (i, offset, i))
            assert value == offset, (
                "content mismatch at offset %d: value=%d, expected %d" % (offset, value, offset))
        self.logger.info("Remote prefix from 0 read back contiguous (%d records) below frontier %d"
                         % (spot, frontier))
