# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.mark.resource import cluster
from ducktape.mark import defaults
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.tests.core.cluster_mirroring_common import ClientService, MirrorConfig, MirrorUtils
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
)


class ClusterMirroringFanOutTest(MirrorUtils, Test):
    """Fan-out topology test: S -> D1, S -> D2, then S crashes and D1 -> D2.

    Verifies that TopicLineages-based LME lookup allows D2 to truncate
    correctly when redirected to fetch from D1 under a new mirror name,
    avoiding full re-replication.
    """

    def __init__(self, test_context):
        super(ClusterMirroringFanOutTest, self).__init__(test_context)
        self.client = ClientService(test_context)
        server_props = [
            ["auto.create.topics.enable", "false"],
            ["default.replication.factor", "2"],
            ["min.insync.replicas", "1"],
            ["offsets.topic.replication.factor", "2"],
            ["transaction.state.log.replication.factor", "2"],
            ["transaction.state.log.min.isr", "1"],
            ["share.coordinator.state.topic.replication.factor", "2"],
            ["share.coordinator.state.topic.min.isr", "1"],
            ["mirror.state.topic.replication.factor", "2"],
            ["mirror.metadata.refresh.interval.ms", "5000"],
            ["mirror.num.replica.fetchers", "2"],
            ["mirror.socket.timeout.ms", "5000"],
        ]
        self.source_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )
        self.dest1_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )
        self.dest2_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )

    def setup(self):
        self.client.start()
        self.client_node = self.client.nodes[0]
        self.source_kafka.start()
        self.dest1_kafka.start()
        self.dest2_kafka.start()

        for kafka in [self.source_kafka, self.dest1_kafka, self.dest2_kafka]:
            self.logger.info(
                "Changing metadata.version on %s to %s", kafka, CLUSTER_MIRRORING_METADATA_VERSION
            )
            kafka.upgrade_metadata_version(CLUSTER_MIRRORING_METADATA_VERSION)
            self.logger.info(
                "Changing mirror.version on %s to %s", kafka, CLUSTER_MIRRORING_VERSION
            )
            kafka.run_features_command(
                "upgrade", "mirror.version", CLUSTER_MIRRORING_VERSION
            )

    def teardown(self):
        self.client.stop()
        for kafka in [self.dest2_kafka, self.dest1_kafka, self.source_kafka]:
            try:
                kafka.stop()
            except Exception:
                self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                for node in kafka.nodes:
                    kafka.stop_node(node, clean_shutdown=False)

    @cluster(num_nodes=10)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_fanout_lme_lookup(self, metadata_quorum):
        """Verify fan-out LME lookup: S -> D1, S -> D2, then D1 -> D2 after S crashes."""
        topic = "t1"

        self.logger.info("Create topic on source and produce first batch")
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 2})
        MirrorUtils.produce_messages(self.logger, self.source_kafka, self.client_node, topic, 5)

        self.logger.info("Create mirror s-to-d1 on D1 from S")
        s_to_d1_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest1_kafka.create_cluster_mirror(
                self.client_node, "s-to-d1", s_to_d1_cfg),
            timeout_sec=120, backoff_sec=2,
            err_msg="Failed to create s-to-d1 mirror",
        )
        wait_until(
            lambda: "Started" in self.dest1_kafka.start_cluster_mirror_topics(
                self.client_node, "s-to-d1", topic),
            timeout_sec=120, backoff_sec=2,
            err_msg="Failed to start s-to-d1 mirror topics",
        )

        self.logger.info("Create mirror s-to-d2 on D2 from S")
        s_to_d2_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest2_kafka.create_cluster_mirror(
                self.client_node, "s-to-d2", s_to_d2_cfg),
            timeout_sec=120, backoff_sec=2,
            err_msg="Failed to create s-to-d2 mirror",
        )
        wait_until(
            lambda: "Started" in self.dest2_kafka.start_cluster_mirror_topics(
                self.client_node, "s-to-d2", topic),
            timeout_sec=120, backoff_sec=2,
            err_msg="Failed to start s-to-d2 mirror topics",
        )

        self.logger.info("Wait for both destinations to catch up")
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest1_kafka, self.client_node,
                                         "s-to-d1", [topic])
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest2_kafka, self.client_node,
                                         "s-to-d2", [topic])

        self.logger.info("Restart source nodes and produce to bump leader epoch")
        for node in self.source_kafka.nodes:
            self.source_kafka.restart_node(node)
        MirrorUtils.produce_messages(self.logger, self.source_kafka, self.client_node, topic, 5)
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest1_kafka, self.client_node,
                                         "s-to-d1", [topic])
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest2_kafka, self.client_node,
                                         "s-to-d2", [topic])

        self.logger.info("Restart source nodes again and produce to bump leader epoch further")
        for node in self.source_kafka.nodes:
            self.source_kafka.restart_node(node)
        MirrorUtils.produce_messages(self.logger, self.source_kafka, self.client_node, topic, 5)
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest1_kafka, self.client_node,
                                         "s-to-d1", [topic])
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest2_kafka, self.client_node,
                                         "s-to-d2", [topic])

        self.logger.info("Crash source cluster S")
        for node in self.source_kafka.nodes:
            self.source_kafka.stop_node(node, clean_shutdown=False)

        self.logger.info("Stop mirroring on D1 and D2 (makes topics writable)")
        self.dest1_kafka.stop_cluster_mirror_topics(self.client_node, "s-to-d1", topic)
        MirrorUtils.wait_mirror_state(self.logger, self.dest1_kafka, self.client_node,
                                       "s-to-d1", [topic], "STOPPED")
        self.dest2_kafka.stop_cluster_mirror_topics(self.client_node, "s-to-d2", topic)
        MirrorUtils.wait_mirror_state(self.logger, self.dest2_kafka, self.client_node,
                                       "s-to-d2", [topic], "STOPPED")
        MirrorUtils.wait_for_metadata_refresh(self.logger, self.dest2_kafka, self.client_node, "s-to-d2")

        self.logger.info("Produce new data on D1 (it is now the new source)")
        MirrorUtils.produce_messages(self.logger, self.dest1_kafka, self.client_node, topic, 5)

        self.logger.info("Create mirror d1-to-d2 on D2 from D1 (fan-out redirection)")
        d1_to_d2_cfg = MirrorConfig(self.dest1_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest2_kafka.create_cluster_mirror(
                self.client_node, "d1-to-d2", d1_to_d2_cfg),
            timeout_sec=120, backoff_sec=2,
            err_msg="Failed to create d1-to-d2 mirror",
        )
        wait_until(
            lambda: "Started" in self.dest2_kafka.start_cluster_mirror_topics(
                self.client_node, "d1-to-d2", topic),
            timeout_sec=120, backoff_sec=2,
            err_msg="Failed to start d1-to-d2 mirror topics",
        )

        self.logger.info("Wait for D2 to catch up from D1 via lineage LME lookup")
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest2_kafka, self.client_node,
                                         "d1-to-d2", [topic],
                                         err_msg="D2 did not catch up from D1 after fan-out redirection")

        self.logger.info("Verify LME lookup: replication did not start from scratch")
        log_path = "%s/info/server.log" % self.dest2_kafka.OPERATIONAL_LOG_DIR
        pattern = "LME lookup result for mirror d1-to-d2"
        found = False
        for node in self.dest2_kafka.nodes:
            for line in node.account.ssh_capture(
                    "grep '%s' %s 2>/dev/null || true" % (pattern, log_path),
                    allow_fail=True):
                line = line.strip()
                if line:
                    self.logger.info("LME lookup log on %s: %s", node.name, line)
                    epoch = int(line.split(topic + "-0=")[1].split("}")[0])
                    assert epoch >= 2, \
                        "Expected LME epoch >= 2, got %d in: %s" % (epoch, line)
                    found = True
        assert found, "No lineage LME lookup log found on any D2 broker"

        self.logger.info("Verify data completeness on D2 (15 original + 5 new)")
        count = MirrorUtils.consume_messages(self.logger, self.dest2_kafka, self.client_node, topic,
                                             max_messages=20, expected_count=20)
        assert count == 20, "Expected 20 messages on D2, got %d" % count
