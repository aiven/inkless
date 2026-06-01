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
import time

from ducktape.mark import parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.core.cluster_mirroring_common import ClientService, MirrorConfig, MirrorUtils
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
    KafkaVersion,
    LATEST_2_1,
    LATEST_3_9,
    LATEST_4_0,
)


class ClusterMirroringCompPlainTest(MirrorUtils, Test):
    """Tests for KIP-1279 Cluster Mirroring across different Kafka versions."""

    DEST_SERVER_PROPS = [
        ["auto.create.topics.enable", "false"],
        ["default.replication.factor", "2"],
        ["min.insync.replicas", "1"],
        ["offsets.topic.replication.factor", "2"],
        ["transaction.state.log.replication.factor", "2"],
        ["transaction.state.log.min.isr", "1"],
        ["share.coordinator.state.topic.replication.factor", "2"],
        ["share.coordinator.state.topic.min.isr", "1"],
        ["mirror.state.topic.replication.factor", "2"],
    ]

    SOURCE_SERVER_PROPS = [
        ["default.replication.factor", "2"],
        ["min.insync.replicas", "1"],
        ["offsets.topic.replication.factor", "2"],
        ["transaction.state.log.replication.factor", "2"],
        ["transaction.state.log.min.isr", "1"],
    ]

    def __init__(self, test_context):
        super(ClusterMirroringCompPlainTest, self).__init__(test_context)

    def teardown(self):
        if hasattr(self, "_original_metadata_quorum"):
            self.test_context.injected_args["metadata_quorum"] = self._original_metadata_quorum
        for attr in ["dest_kafka", "source_kafka"]:
            kafka = getattr(self, attr, None)
            if kafka is not None:
                try:
                    kafka.stop()
                except Exception:
                    self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                    for node in kafka.nodes:
                        kafka.stop_node(node, clean_shutdown=False)
        for attr in ["dest_client", "source_client"]:
            client = getattr(self, attr, None)
            if client is not None:
                client.stop()
        if hasattr(self, "zk"):
            self.zk.stop()

    def setup_source(self, source_version, metadata_quorum):
        # Separate client node with matching CLI version for the older source cluster.
        self.source_client = ClientService(self.test_context, version=source_version)
        self.source_client.start()
        self.source_client_node = self.source_client.nodes[0]
        if metadata_quorum == quorum.zk:
            self.zk = ZookeeperService(self.test_context, num_nodes=1)
            self.zk.start()
            self.source_kafka = KafkaService(
                self.test_context, num_nodes=2, zk=self.zk,
                version=source_version,
                server_prop_overrides=self.SOURCE_SERVER_PROPS,
            )
            self._original_metadata_quorum = self.test_context.injected_args.get("metadata_quorum")
            self.test_context.injected_args["metadata_quorum"] = quorum.isolated_kraft
        else:
            self.source_kafka = KafkaService(
                self.test_context, num_nodes=2, zk=None,
                version=source_version,
                controller_num_nodes_override=1,
                server_prop_overrides=self.SOURCE_SERVER_PROPS,
            )
        self.source_kafka.start()

    def setup_dest(self):
        # Separate client node with DEV_BRANCH CLI for the destination cluster.
        self.dest_client = ClientService(self.test_context)
        self.dest_client.start()
        self.dest_client_node = self.dest_client.nodes[0]
        self.dest_kafka = KafkaService(
            self.test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=self.DEST_SERVER_PROPS,
        )
        self.dest_kafka.start()
        self.logger.info(
            "Changing metadata.version on %s to %s", self.dest_kafka, CLUSTER_MIRRORING_METADATA_VERSION
        )
        self.dest_kafka.upgrade_metadata_version(CLUSTER_MIRRORING_METADATA_VERSION)
        self.logger.info(
            "Changing mirror.version on %s to %s", self.dest_kafka, CLUSTER_MIRRORING_VERSION
        )
        self.dest_kafka.run_features_command(
            "upgrade", "mirror.version", CLUSTER_MIRRORING_VERSION
        )


    @cluster(num_nodes=8)
    @parametrize(source_version=str(LATEST_2_1), metadata_quorum=quorum.zk)
    @parametrize(source_version=str(LATEST_3_9), metadata_quorum=quorum.zk)
    @parametrize(source_version=str(LATEST_4_0), metadata_quorum=quorum.isolated_kraft)
    def test_mirroring(self, source_version, metadata_quorum):
        """Verify migration with data, consumer groups, and topic config sync."""
        self.setup_source(KafkaVersion(source_version), metadata_quorum)
        self.setup_dest()

        num_records = 100
        topics = {
            "my-topic-a": {"partitions": 3, "replication-factor": 2},
            "my-topic-b": {"partitions": 1, "replication-factor": 2},
            "new-topic": {"partitions": 2, "replication-factor": 2},
        }

        self.logger.info("Creating topics on source cluster")
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Restart source cluster to bump the partitions leader epoch")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Producing %d records to each source topic", num_records)
        for t in topics:
            MirrorUtils.produce_records(self.logger, self.source_kafka, t, num_records, self.source_client_node)

        self.logger.info("Creating consumer group on source by consuming my-topic-a")
        MirrorUtils.consume_records(self.logger, self.source_kafka, "my-topic-a", self.source_client_node,
                             max_messages=num_records, group="my-group")

        self.logger.info("Setting dynamic topic config on source")
        self.source_kafka.alter_topic_config("my-topic-a", "retention.ms=100002", node=self.source_client_node)

        self.logger.info("Creating and starting cluster mirror")
        mirror_name = "my-mirror"
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.dest_client_node, mirror_name, mirror_cfg),
            timeout_sec=20, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        for regex in ["my-topic.*", "new-topic"]:
            wait_until(
                lambda r=regex: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                    self.dest_client_node, mirror_name, r),
                timeout_sec=20, backoff_sec=2,
                err_msg="Failed to start mirror topics for %s" % regex,
            )
        self.logger.info("Waiting for all partitions to reach MIRRORING with zero lag")
        MirrorUtils.wait_mirror_lag_zero(self.logger,
            self.dest_kafka, self.dest_client_node, mirror_name, topics=list(topics.keys()))

        self.logger.info("Shutting down the leader of one topic, and send more records to make sure the mirror can still work.")
        leader_node = self.source_kafka.leader("my-topic-b", 0)
        self.source_kafka.stop_node(leader_node, clean_shutdown=False)

        self.logger.info("Producing %d more records to each source topic", num_records)
        for t in topics:
            MirrorUtils.produce_records(self.logger, self.source_kafka, t, num_records, self.source_client_node)

        self.logger.info("Waiting for all partitions to reach MIRRORING with zero lag after one source node down")
        MirrorUtils.wait_mirror_lag_zero(self.logger, self.dest_kafka, self.dest_client_node, mirror_name, topics=list(topics.keys()))

        self.logger.info("Verifying consumer group offset sync")
        MirrorUtils.wait_for_metadata_sync(self.logger, self.dest_kafka, self.dest_client_node, mirror_name, num_cycles=2)
        wait_until(
            lambda: "my-topic-a" in MirrorUtils.describe_consumer_group(
                self.dest_kafka, "my-group", self.dest_client_node),
            timeout_sec=60, backoff_sec=5,
            err_msg="Expected my-topic-a offset to be synced on destination",
        )

        self.logger.info("Verifying topic config sync")
        dest_topic_desc = self.dest_kafka.describe_topic("my-topic-a", node=self.dest_client_node)
        assert "retention.ms=100002" in dest_topic_desc, \
            "Expected retention.ms=100002 synced to destination, got: %s" % dest_topic_desc

        self.logger.info("Stopping mirroring (failover)")
        for regex in ["my-topic.*", "new-topic"]:
            self.dest_kafka.stop_cluster_mirror_topics(
                self.dest_client_node, mirror_name, regex)
        MirrorUtils.wait_mirror_state(self.logger,
            self.dest_kafka, self.dest_client_node, mirror_name, "STOPPED",
            topics=list(topics.keys()))

        self.logger.info("Verifying destination records after failover")
        for topic in topics:
            count = MirrorUtils.consume_records(self.logger, self.dest_kafka, topic, self.dest_client_node,
                                         max_messages=num_records, expected_count=num_records)
            assert count >= num_records, "Expected %d records on %s, got %d" % (num_records, topic, count)

    @cluster(num_nodes=8)
    @parametrize(source_version=str(LATEST_2_1), metadata_quorum=quorum.zk)
    @parametrize(source_version=str(LATEST_3_9), metadata_quorum=quorum.zk)
    @parametrize(source_version=str(LATEST_4_0), metadata_quorum=quorum.isolated_kraft)
    def test_ule_mirroring(self, source_version, metadata_quorum):
        """Verify migration with unclean leader elections."""
        self.logger.info("Create source topic with ULE support enabled")
        topic = "my-topic"
        mirror_name = "new-mirror"
        topics = {topic: {"partitions": 1, "replication-factor": 2}}

        self.setup_source(KafkaVersion(source_version), metadata_quorum)
        self.setup_dest()

        self.source_kafka.create_topic({
            "topic": topic, **topics[topic],
            # this is needed because we started to support manual ULE only in 2.4
            "configs": {"unclean.leader.election.enable": "true"},
        })

        src_broker0 = self.source_kafka.nodes[0]
        src_broker1 = self.source_kafka.nodes[1]

        self.logger.info("Bounce source brokers to trigger leader elections")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Send 1 message via source broker 0")
        MirrorUtils.produce_records(self.logger, self.source_kafka, topic, 1, self.source_client_node,
                             bootstrap_servers=MirrorUtils.broker_bootstrap(src_broker0))

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.dest_client_node, mirror_name, mirror_cfg),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.dest_client_node, mirror_name, topic),
            timeout_sec=60, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        MirrorUtils.wait_mirror_state(
            self.logger, self.dest_kafka, self.dest_client_node, mirror_name, "MIRRORING", [topic],
            err_msg="Mirror did not reach MIRRORING state",
        )

        self.logger.info("Stop source broker 0 (broker 0 becomes stale)")
        self.source_kafka.stop_node(src_broker0)

        self.logger.info("Send 1 message via source broker 1")
        MirrorUtils.produce_records(self.logger, self.source_kafka, topic, 1, self.source_client_node,
                             bootstrap_servers=MirrorUtils.broker_bootstrap(src_broker1))
        MirrorUtils.wait_for_log_convergence(self.logger, self.source_kafka, self.dest_kafka, topics)

        self.logger.info("ULE 1: stop broker 1, start broker 0 (stale), elect it as leader")
        self.source_kafka.stop_node(src_broker1)
        self.source_kafka.start_node(src_broker0)

        self.logger.info("Send 2 messages via source broker 0")
        MirrorUtils.produce_records(self.logger, self.source_kafka, topic, 2, self.source_client_node,
                             bootstrap_servers=MirrorUtils.broker_bootstrap(src_broker0))
        MirrorUtils.wait_for_log_convergence(self.logger, self.source_kafka, self.dest_kafka, topics)

        self.logger.info("Failover: stop mirror so destination topic becomes writable")
        self.dest_kafka.stop_cluster_mirror_topics(self.dest_client_node, mirror_name, topic)
        MirrorUtils.wait_mirror_state(self.logger, self.dest_kafka, self.dest_client_node, mirror_name, "STOPPED", [topic])
