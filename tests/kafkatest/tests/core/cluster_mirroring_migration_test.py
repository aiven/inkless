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

from ducktape.mark import defaults, parametrize
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.core.cluster_mirroring_test import ClientService, MirrorConfig, MirrorHelpers
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
    LATEST_2_1,
    LATEST_3_9,
)


class ClusterMirroringMigrationTest(MirrorHelpers, Test):
    """Tests for KIP-1279 Cluster Mirroring migration from older Kafka versions."""

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
        super(ClusterMirroringMigrationTest, self).__init__(test_context)

    def teardown(self):
        # Restore the quorum injected by @parametrize so retries start clean
        if hasattr(self, "_original_metadata_quorum"):
            self.test_context.injected_args["metadata_quorum"] = self._original_metadata_quorum
        if hasattr(self, "client"):
            self.client.stop()
        for attr in ["dest_kafka", "source_kafka"]:
            kafka = getattr(self, attr, None)
            if kafka is not None:
                try:
                    kafka.stop()
                except Exception:
                    self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                    for node in kafka.nodes:
                        kafka.stop_node(node, clean_shutdown=False)
        if hasattr(self, "zk"):
            self.zk.stop()

    ######### helpers #########

    def setup_dest(self):
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

    def setup_client(self):
        self.client = ClientService(self.test_context, num_nodes=1)
        self.client_node = self.client.nodes[0]

    def run_migration(self):
        num_records = 100
        topics = {
            "my-topic-a": {"partitions": 3, "replication-factor": 2},
            "my-topic-b": {"partitions": 1, "replication-factor": 2},
            "new-topic": {"partitions": 2, "replication-factor": 2},
        }

        self.logger.info("Creating topics on source cluster")
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Producing %d records to each source topic", num_records)
        for t in topics:
            self.produce_records(self.source_kafka.nodes[0], t, num_records)

        self.logger.info("Creating consumer group on source by consuming my-topic-a")
        self.consume_records("my-topic-a", self.source_kafka, max_messages=num_records, group="my-group")

        self.logger.info("Setting dynamic topic config on source")
        # Uses the source node's own binary: --zookeeper for pre-2.6, --bootstrap-server otherwise
        self.source_kafka.alter_topic_config("my-topic-a", "retention.ms=100002")

        self.logger.info("Creating and starting cluster mirrors on destination")
        mirror_name = "my-mirror"
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=20, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        for regex in ["my-topic.*", "new-topic"]:
            wait_until(
                lambda r=regex: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                    self.client_node, mirror_name, r),
                timeout_sec=20, backoff_sec=2,
                err_msg="Failed to start mirror topics for %s" % regex,
            )
        self.logger.info("Waiting for all partitions to reach MIRRORING with zero lag")
        self.wait_mirror_lag_zero(
            self.dest_kafka, mirror_name, topics=list(topics.keys()))

        self.logger.info("Verifying consumer group offset sync")
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)
        group_desc = self.dest_kafka.describe_consumer_group("my-group", self.client_node)
        assert "my-topic-a" in group_desc, \
            "Expected my-topic-a offset to be synced on destination, got: %s" % group_desc

        self.logger.info("Verifying topic config sync")
        dest_topic_desc = self.dest_kafka.describe_topic("my-topic-a", node=self.client_node)
        assert "retention.ms=100002" in dest_topic_desc, \
            "Expected retention.ms=100002 synced to destination, got: %s" % dest_topic_desc

        self.logger.info("Stopping mirroring (failover)")
        for regex in ["my-topic.*", "new-topic"]:
            self.dest_kafka.stop_cluster_mirror_topics(
                self.client_node, mirror_name, regex)
        self.wait_mirror_state(
            self.dest_kafka, mirror_name, "STOPPED",
            topics=list(topics.keys()))

        # Log segment comparison is not suitable here because STOPPING writes
        # epoch bumps and PID reset barriers to the destination segments.
        # Use wait_until because lag zero can be reported before the fetcher
        # discovers the source's actual offsets (sourceOffset=0 gives false lag=0).
        self.logger.info("Verifying destination records after failover")
        for topic in topics:
            wait_until(
                lambda t=topic: self.consume_records(
                    t, self.dest_kafka, max_messages=num_records
                ) == num_records,
                timeout_sec=60, backoff_sec=5,
                err_msg="Expected %d records on destination for %s" % (num_records, topic),
            )

    ######### tests #########

    @cluster(num_nodes=7)
    @parametrize(metadata_quorum=quorum.zk)
    def test_migration_from_zk(self, metadata_quorum):
        """Migrate data from Kafka 2.1 (ZooKeeper mode) to current dev build."""
        self.zk = ZookeeperService(self.test_context, num_nodes=1)
        self.zk.start()

        # 2.1 is the oldest version supported by Kafka 4
        self.source_kafka = KafkaService(
            self.test_context, num_nodes=2, zk=self.zk,
            version=LATEST_2_1,
            server_prop_overrides=self.SOURCE_SERVER_PROPS,
        )
        self.source_kafka.start()

        # Switch to KRaft for the destination; save the original so teardown can restore it
        self._original_metadata_quorum = self.test_context.injected_args.get("metadata_quorum")
        self.test_context.injected_args["metadata_quorum"] = quorum.isolated_kraft
        self.setup_dest()
        self.setup_client()
        self.run_migration()

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_from_kraft(self, metadata_quorum):
        """Migrate data from Kafka 3.9 (KRaft mode) to current dev build."""
        self.source_kafka = KafkaService(
            self.test_context, num_nodes=2, zk=None,
            version=LATEST_3_9,
            controller_num_nodes_override=1,
            server_prop_overrides=self.SOURCE_SERVER_PROPS,
        )
        self.source_kafka.start()

        self.setup_dest()
        self.setup_client()
        self.run_migration()
