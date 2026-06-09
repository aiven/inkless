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

from kafkatest.tests.client.compression_test import CompressionTest


class ConsolidationCompressionTest(CompressionTest):
    """Codec coverage for consolidating (diskless + remote) topics.

    Reuses the upstream ``CompressionTest`` produce/consume/validate flow but
    flips the cluster into consolidation mode *per-test* rather than via the
    run-wide ``--globals consolidation`` flag, so it runs in the same ducktape
    session as the classic-to-diskless switch tests (which need the classic
    default). With consolidation enabled the controller auto-enables
    ``remote.storage.enable=true`` on the created topic, so every compression
    codec (snappy/gzip/lz4/zstd/none) is exercised through the consolidation
    write path -- coverage the single-codec pipeline test does not provide.
    """

    def __init__(self, test_context):
        super(ConsolidationCompressionTest, self).__init__(test_context)
        # prop_file reads these at render time, so mutating the already-built
        # services here (before start()) is enough. The controller is flipped
        # too so its config validation matches the brokers.
        self.kafka.consolidation = True
        controller = getattr(self.kafka, "isolated_controller_quorum", None)
        if controller is not None:
            controller.consolidation = True
