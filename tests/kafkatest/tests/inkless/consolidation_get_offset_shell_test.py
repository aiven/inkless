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

from kafkatest.services.kafka import KafkaService
from kafkatest.tests.core.get_offset_shell_test import GetOffsetShellTest


class ConsolidationGetOffsetShellTest(GetOffsetShellTest):
    """GetOffsetShell (ListOffsets) coverage for consolidating topics.

    Reuses the upstream ``GetOffsetShellTest`` cases but builds the cluster in
    consolidation mode *per-test* rather than via the run-wide ``--globals
    consolidation`` flag, so it runs alongside the classic-to-diskless switch
    tests. With consolidation enabled the controller auto-enables
    ``remote.storage.enable=true`` on the created topics, so the tool's
    earliest/latest offset queries are exercised against consolidating topics.
    """

    def start_kafka(self, security_protocol, interbroker_security_protocol):
        # Same construction as the base, but with consolidation enabled. Passing
        # the kwarg here (rather than mutating afterwards) lets KafkaService
        # forward the flag to the isolated controller quorum automatically.
        self.kafka = KafkaService(
            self.test_context, self.num_brokers,
            None, security_protocol=security_protocol,
            interbroker_security_protocol=interbroker_security_protocol,
            topics=self.topics, consolidation=True)
        self.kafka.start()
