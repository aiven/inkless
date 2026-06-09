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

from ducktape.mark import ignore, matrix
from ducktape.mark.resource import cluster

from kafkatest.services.kafka import quorum
from kafkatest.tests.core.produce_bench_test import ProduceBenchTest


class ConsolidationProduceBenchTest(ProduceBenchTest):
    """Throughput canary for consolidating (diskless + remote) topics.

    Reuses the upstream ``ProduceBenchTest`` workload but flips the cluster
    into consolidation mode *per-test* (``KafkaService.consolidation``) rather
    than via the run-wide ``--globals consolidation`` flag. That makes this
    test discoverable and runnable in the same ducktape session as the
    classic-to-diskless switch tests, which require the classic default
    (``log.diskless.enable=false``) and would break under a run-wide global.

    The workload auto-creates its topics with no per-topic config, so the only
    way to make them consolidating is the broker-level ``log.diskless.enable``
    default that the consolidation broker config sets.
    """

    def __init__(self, test_context):
        super(ConsolidationProduceBenchTest, self).__init__(test_context)
        # Enable consolidation on the brokers and the isolated controller
        # (kept consistent so controller-side config validation matches the
        # brokers). prop_file reads these at render time, so mutating the
        # already-constructed services here -- before start() -- is enough.
        self.kafka.consolidation = True
        controller = getattr(self.kafka, "isolated_controller_quorum", None)
        if controller is not None:
            controller.consolidation = True

    def producer_config(self):
        # The diskless write path batches large object-store WAL writes, so the
        # producer needs bigger batches/buffers and a larger max request size.
        # Applied unconditionally here (the cluster is always consolidating).
        return {
            "batch.size": 1000000,
            "buffer.memory": 128000000,
            "max.request.size": 64000000,
        }

    @ignore
    @cluster(num_nodes=8)
    @matrix(metadata_quorum=quorum.all_non_upgrade)
    def test_produce_bench_transactions(self, metadata_quorum):
        # Transactions are not supported on consolidating (diskless) topics, so
        # the inherited transactional variant is intentionally ignored.
        pass
