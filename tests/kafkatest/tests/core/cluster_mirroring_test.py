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


class ClusterMirroringTest(MirrorUtils, Test):
    """Tests for KIP-1279 Cluster Mirroring using a source and destination cluster."""

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClusterMirroringTest, self).__init__(test_context)
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
        ]
        self.source_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )
        self.dest_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )

    def setup(self):
        self.client.start()
        self.client_node = self.client.nodes[0]
        self.source_kafka.start()
        self.dest_kafka.start()

        for kafka in [self.source_kafka, self.dest_kafka]:
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
        for kafka in [self.dest_kafka, self.source_kafka]:
            try:
                kafka.stop()
            except Exception:
                self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                for node in kafka.nodes:
                    kafka.stop_node(node, clean_shutdown=False)

    def consume_share_records(self, topic, kafka, group, max_messages=None, timeout_ms=30000,
                              expected_count=None, wait_timeout_sec=240):
        # When expected_count is set, retry consumption because the high watermark on
        # destination replicas may not have advanced yet when mirror lag reaches zero.
        cmd = "%s --bootstrap-server %s --topic %s --group %s --timeout-ms %d" % (
            kafka.path.script("kafka-console-share-consumer.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, group, timeout_ms)
        if max_messages is not None:
            cmd += " --max-messages %d" % max_messages
        cmd += " 2>/dev/null"

        count = [0]
        def _try_consume():
            for line in self.client_node.account.ssh_capture(cmd, allow_fail=True):
                if line.strip():
                    count[0] += 1
            self.logger.debug("Share-consumed %d records from %s so far (expected %s)",
                              count[0], topic, expected_count)
            return expected_count is None or count[0] >= expected_count

        if expected_count is not None:
            deadline = time.time() + wait_timeout_sec
            _try_consume()
            while count[0] < expected_count:
                if time.time() >= deadline:
                    raise AssertionError(
                        "Expected %d records on %s, got %d" % (expected_count, topic, count[0]))
                time.sleep(5)
                _try_consume()
        else:
            _try_consume()
        return count[0]

    def run_background_consumer(self, kafka, topic, group):
        """Start a background console consumer."""
        cmd = "%s --bootstrap-server %s --topic %s --group %s" % (
            kafka.path.script("kafka-console-consumer.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, group)
        self.client_node.account.ssh("nohup %s >/dev/null 2>&1 &" % cmd, allow_fail=True)

    def run_background_share_consumer(self, kafka, topic, group):
        """Start a background console share consumer."""
        cmd = "%s --bootstrap-server %s --topic %s --group %s" % (
            kafka.path.script("kafka-console-share-consumer.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, group)
        self.client_node.account.ssh("nohup %s >/dev/null 2>&1 &" % cmd, allow_fail=True)

    def get_offset_shell(self, kafka, topic=None, time=None):
        """Run kafka-get-offsets on the client node."""
        cmd = "%s --bootstrap-server %s" % (
            kafka.path.script("kafka-get-offsets.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol))
        if time is not None:
            cmd += " --time %s" % time
        if topic is not None:
            cmd += " --topic %s" % topic
        output = ""
        for line in self.client_node.account.ssh_capture(cmd, allow_fail=True):
            output += line
        return output

    def kill_background_consumer(self, node, process_class):
        """Kill a background consumer process by class name."""
        node.account.ssh(
            "pkill -SIGKILL -f '%s' || true" % process_class, allow_fail=True)


    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_start_stop(self, metadata_quorum):
        """Verify failover makes destination writable and failback truncates non-mirrored data."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 2})

        self.logger.info("Send 3 records to source")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.logger.info("Consume from destination (expect 3 records)")
        self.consume_records(self.dest_kafka, topic, self.client_node, max_messages=3,
                             expected_count=3)

        self.logger.info("Produce to destination while mirroring (should fail)")
        self.produce_records(self.dest_kafka, topic, 1, self.client_node)

        self.logger.info("Failover: stop mirror so destination topic becomes writable")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, mirror_name, topic)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])

        self.logger.info("Send 1 record to destination (should work now)")
        self.produce_records(self.dest_kafka, topic, 1, self.client_node)

        self.logger.info("Send 2 more records to source (not mirrored)")
        self.produce_records(self.source_kafka, topic, 2, self.client_node)

        self.logger.info("Consume from source (expect 5 records)")
        self.consume_records(self.source_kafka, topic, self.client_node, max_messages=5,
                             expected_count=5)

        self.logger.info("Failback: source mirrors from destination (same mirror name to retrieve LMO)")
        mirror_cfg = MirrorConfig(self.dest_kafka.bootstrap_servers())

        wait_until(
            lambda: self.source_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create reverse cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.source_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start reverse mirror topics",
        )
        self.wait_mirror_lag_zero(self.source_kafka, mirror_name, [topic])

        self.logger.info("Consume from source (non-mirrored data should be truncated)")
        self.consume_records(self.source_kafka, topic, self.client_node,
                             max_messages=5, timeout_ms=5000, expected_count=4)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_pause_resume(self, metadata_quorum):
        """Verify that pausing stops replication and resuming catches up."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 2})

        self.logger.info("Send 3 records to source")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.logger.info("Pause mirroring and send 1 more record to source")
        self.dest_kafka.pause_cluster_mirror_topics(self.client_node, mirror_name, topic)
        self.produce_records(self.source_kafka, topic, 1, self.client_node)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "PAUSED", [topic])

        self.logger.info("Consume from destination while paused (expect 3, the 4th is not mirrored yet)")
        count = self.consume_records(self.dest_kafka, topic, self.client_node,
                                     max_messages=4, timeout_ms=5000)
        assert count == 3, "Expected 3 records on destination while paused, got %d" % count

        self.logger.info("Produce to destination while paused (should fail)")
        self.produce_records(self.dest_kafka, topic, 1, self.client_node)

        self.logger.info("Resume mirroring and wait for it to catch up")
        self.dest_kafka.resume_cluster_mirror_topics(self.client_node, mirror_name, topic)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.logger.info("Consume from destination (expect 4 records after resume)")
        self.consume_records(self.dest_kafka, topic, self.client_node, max_messages=4,
                             expected_count=4)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_config_update(self, metadata_quorum):
        """Verify that updating mirror config triggers reconnection and mirroring continues."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 1})

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])

        self.logger.info("describe_mirror_config: %s",
                         self.dest_kafka.describe_mirror_config(self.client_node, mirror_name))
        self.logger.info("list_cluster_mirror: %s",
                         self.dest_kafka.list_cluster_mirror(self.client_node))
        self.logger.info("describe_cluster_mirror: %s",
                         self.dest_kafka.describe_cluster_mirror(self.client_node))

        self.logger.info("Alter config with invalid config (should fail)")
        self.dest_kafka.alter_mirror_config(self.client_node, mirror_name, "invalid.config=foo")

        self.logger.info("Alter config with valid bootstrap.servers pointing to source broker 1 (triggers reconnection)")
        new_bootstrap = "%s:9092" % self.source_kafka.nodes[1].account.hostname
        self.dest_kafka.alter_mirror_config(self.client_node, mirror_name,
                                            "bootstrap.servers=%s" % new_bootstrap)

        self.logger.info("describe_mirror_config: %s",
                         self.dest_kafka.describe_mirror_config(self.client_node, mirror_name))
        self.logger.info("list_cluster_mirror: %s",
                         self.dest_kafka.list_cluster_mirror(self.client_node))
        self.logger.info("describe_cluster_mirror: %s",
                         self.dest_kafka.describe_cluster_mirror(self.client_node))

        self.logger.info("Verify mirroring still works after config change")
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])

        self.logger.info("Send records to source and verify they arrive at destination")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.consume_records(self.dest_kafka, topic, self.client_node, max_messages=3,
                             expected_count=3)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_mirror_deletion(self, metadata_quorum):
        """Verify that a mirror cannot be deleted while not empty, but can after stopping all topics."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 1})

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])

        self.logger.info("Delete mirror while not empty (should fail)")
        self.dest_kafka.delete_cluster_mirror(self.client_node, mirror_name)
        output = self.dest_kafka.list_cluster_mirror(self.client_node)
        assert mirror_name in output, "Mirror should still exist after failed deletion"

        self.logger.info("Stop mirror topics and wait for STOPPED state")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, mirror_name, topic)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])

        self.logger.info("Delete mirror (should work now)")
        self.dest_kafka.delete_cluster_mirror(self.client_node, mirror_name)
        output = self.dest_kafka.list_cluster_mirror(self.client_node)
        assert mirror_name not in output, "Mirror should be gone after deletion"

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_topic_deletion(self, metadata_quorum):
        """Verify that deleting a source topic transitions mirror partitions to STOPPED."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 2})

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])

        self.logger.info("Delete topic on source and wait for metadata sync")
        self.source_kafka.delete_topic(topic)
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name)

        self.logger.info("Verify mirror partitions transitioned to STOPPED")
        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_topics_filtering(self, metadata_quorum):
        """Verify topic include/exclude filtering, stopped topics not re-discovered, and auto-discovery."""
        mirror_name = "my-mirror"

        self.logger.info("Create 4 topics on source")
        topics_cfg = {
            "orders-us": {"partitions": 1, "replication-factor": 2},
            "orders-eu": {"partitions": 1, "replication-factor": 2},
            "orders-internal": {"partitions": 1, "replication-factor": 2},
            "payments": {"partitions": 1, "replication-factor": 2},
        }
        for t, cfg in topics_cfg.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Send records to source topics")
        self.produce_records(self.source_kafka, "orders-us", 3, self.client_node)
        self.produce_records(self.source_kafka, "orders-eu", 3, self.client_node)
        self.produce_records(self.source_kafka, "orders-internal", 2, self.client_node)
        self.produce_records(self.source_kafka, "payments", 2, self.client_node)

        self.logger.info("Start mirror with regex include and exclude")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, "orders-.*", exclude="orders-internal"),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )

        self.logger.info("Only orders-us and orders-eu should be mirrored")
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name,
                                  ["orders-us", "orders-eu"])

        self.consume_records(self.dest_kafka, "orders-us", self.client_node, max_messages=3,
                             expected_count=3)
        self.consume_records(self.dest_kafka, "orders-eu", self.client_node, max_messages=3,
                             expected_count=3)

        self.logger.info("Verify excluded and non-matching topics don't exist on destination")
        dest_topics = list(self.dest_kafka.list_topics(self.client_node))
        assert "orders-internal" not in dest_topics, \
            "Expected orders-internal to not exist on destination (excluded)"
        assert "payments" not in dest_topics, \
            "Expected payments to not exist on destination (not included)"

        self.logger.info("Stop orders-eu")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, mirror_name, "orders-eu")
        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED",
                               topics={"orders-eu": topics_cfg["orders-eu"]})

        self.logger.info("Send 3 more records to orders-eu on source (should not be mirrored)")
        self.produce_records(self.source_kafka, "orders-eu", 3, self.client_node)
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        count_eu = self.consume_records(self.dest_kafka, "orders-eu", self.client_node, timeout_ms=5000)
        assert count_eu == 3, \
            "Expected 3 records for orders-eu after stop (not 6), got %d" % count_eu

        self.logger.info("Verify orders-eu is not re-discovered after two more metadata refresh cycles")
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.logger.info("describe_cluster_mirror: %s",
                         self.dest_kafka.describe_cluster_mirror(self.client_node))
        count_eu = self.consume_records(self.dest_kafka, "orders-eu", self.client_node, timeout_ms=5000)
        assert count_eu == 3, \
            "Expected orders-eu to remain at 3 records (not re-discovered), got %d" % count_eu

        self.logger.info("Create new topic on source that matches the include pattern")
        self.source_kafka.create_topic({"topic": "orders-jp", "partitions": 1, "replication-factor": 2})
        self.produce_records(self.source_kafka, "orders-jp", 3, self.client_node)

        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING",
                               topics={"orders-jp": {"partitions": 1, "replication-factor": 2}})
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name,
                                  topics={"orders-jp": {"partitions": 1, "replication-factor": 2}})

        self.consume_records(self.dest_kafka, "orders-jp", self.client_node, max_messages=3,
                             expected_count=3)

        self.logger.info("Add payments to include pattern via alter config")
        self.dest_kafka.alter_mirror_config(
            self.client_node, mirror_name, "mirror.topics.include=[orders-.*,payments]")

        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING",
                               topics={"payments": {"partitions": 1, "replication-factor": 2}})
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name,
                                  topics={"payments": {"partitions": 1, "replication-factor": 2}})

        self.consume_records(self.dest_kafka, "payments", self.client_node, max_messages=2,
                             expected_count=2)

        self.logger.info("Verify orders-internal still doesn't exist on destination after all operations")
        dest_topics = list(self.dest_kafka.list_topics(self.client_node))
        assert "orders-internal" not in dest_topics, \
            "Expected orders-internal to still not exist on destination (excluded)"

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_props_exclude(self, metadata_quorum):
        """Verify that excluded topic properties are not synced to destination."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 1})

        self.logger.info("Starting cluster mirror with properties exclude")
        mirror_cfg = MirrorConfig(
            self.source_kafka.bootstrap_servers(),
            mirror_topic_properties_exclude="retention.bytes,min.insync.replicas")
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])

        self.logger.info("Altering topic config on source")
        self.source_kafka.alter_topic_config(
            topic, "retention.ms=100002,retention.bytes=10000000002,min.insync.replicas=2",
            node=self.client_node)

        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        def parse_topic_configs(desc):
            """Extract topic configs from kafka-topics describe output."""
            for line in desc.splitlines():
                if "Configs:" in line:
                    configs_str = line.split("Configs:")[1].strip()
                    if not configs_str:
                        return {}
                    result = {}
                    for pair in configs_str.split(","):
                        k, v = pair.strip().split("=", 1)
                        result[k] = v
                    return result
            return {}

        self.logger.info("Verifying excluded properties were not synced to destination")
        dest_configs = parse_topic_configs(
            self.dest_kafka.describe_topic(topic, node=self.client_node))
        self.logger.info("Dest topic configs: %s", dest_configs)

        assert dest_configs.get("retention.ms") == "100002", \
            "Expected dest retention.ms=100002 (synced), got %s" % dest_configs.get("retention.ms")
        assert dest_configs.get("retention.bytes") != "10000000002", \
            "Expected dest retention.bytes to not be synced (excluded), got %s" % dest_configs.get("retention.bytes")
        assert dest_configs.get("min.insync.replicas") != "2", \
            "Expected dest min.insync.replicas to not be synced (excluded), got %s" % dest_configs.get("min.insync.replicas")

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_groups_filtering(self, metadata_quorum):
        """Verify consumer group offset mirroring with include/exclude filtering."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 2})

        self.logger.info("Produce records to source")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)

        self.logger.info("Create consumer groups on source by consuming all records")
        for group in ["app-group1", "app-group2", "internal-group1"]:
            self.consume_records(self.source_kafka, topic, self.client_node, max_messages=3, group=group)

        self.logger.info("Start mirror with groups include (app-.*) and exclude (app-group2)")
        mirror_cfg = MirrorConfig(
            self.source_kafka.bootstrap_servers(),
            mirror_groups_include="app-.*",
            mirror_groups_exclude="app-group2",
        )
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.logger.info("Verify only app-group1 is synced on destination")
        groups_output = self.dest_kafka.list_consumer_groups(self.client_node)
        assert "app-group1" in groups_output, \
            "Expected app-group1 to be synced on destination"
        assert "app-group2" not in groups_output, \
            "Expected app-group2 to be excluded from destination"
        assert "internal-group1" not in groups_output, \
            "Expected internal-group1 to not be included on destination"

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_log_convergence(self, metadata_quorum):
        """Verify that mirrored log segments are byte identical to source after bouncing both clusters."""
        self.logger.info("Create source topics and produce initial data")
        topics = {
            "my-topic-a": {"partitions": 3, "replication-factor": 2},
            "my-topic-b": {"partitions": 2, "replication-factor": 2},
            "new-topic": {"partitions": 1, "replication-factor": 2},
        }
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        for t in topics:
            self.produce_records(self.source_kafka, t, 100, self.client_node)

        self.logger.info("Bounce source cluster to trigger leader elections before mirroring")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Create and start cluster mirrors")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        # mirrors map mirror name to (topics_regex, topic_list)
        mirrors = {
            "my-mirror": ("my-topic.*", ["my-topic-a", "my-topic-b"]),
            "new-mirror": ("new-topic", ["new-topic"]),
        }

        for mirror_name in mirrors:
            self.logger.info("Creating mirror %s", mirror_name)
            wait_until(
                lambda mn=mirror_name: self.dest_kafka.create_cluster_mirror(
                    self.client_node, mn, mirror_cfg
                ),
                timeout_sec=300,
                backoff_sec=2,
                err_msg="Failed to create cluster mirror %s" % mirror_name,
            )
        for mirror_name, (topics_regex, _) in mirrors.items():
            wait_until(
                lambda mn=mirror_name, tr=topics_regex: (
                    "Started"
                    in self.dest_kafka.start_cluster_mirror_topics(self.client_node, mn, tr)
                ),
                timeout_sec=300,
                backoff_sec=2,
                err_msg="Failed to start mirror topics for %s" % mirror_name,
            )
        for mirror_name, (_, topic_list) in mirrors.items():
            self.wait_mirror_state(
                self.dest_kafka, mirror_name, "MIRRORING", topics=topic_list,
                err_msg="Failed to start mirror %s" % mirror_name,
            )

        self.logger.info("Bounce source and dest brokers in interleaved order to trigger leader elections")
        src0, src1 = self.source_kafka.nodes[0], self.source_kafka.nodes[1]
        dst0, dst1 = self.dest_kafka.nodes[0], self.dest_kafka.nodes[1]
        for kafka, node in [
            (self.dest_kafka, dst0), (self.dest_kafka, dst1),
            (self.source_kafka, src1), (self.dest_kafka, dst0),
            (self.source_kafka, src0), (self.dest_kafka, dst1),
        ]:
            kafka.restart_node(node)

        self.logger.info("Send more data and wait for all mirrors to catch up")
        for t in topics:
            self.produce_records(self.source_kafka, t, 100, self.client_node)

        for mirror_name, (_, topic_list) in mirrors.items():
            self.wait_mirror_lag_zero(
                self.dest_kafka, mirror_name, topics=topic_list,
                err_msg="Mirror %s did not catch up" % mirror_name)

        self.logger.info("Poll until source and destination log segment hashes converge")
        self.wait_for_log_convergence(self.source_kafka, self.dest_kafka, topics)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_log_convergence_ule(self, metadata_quorum):
        """Verify log convergence after unclean leader elections and failover/failback."""
        self.logger.info("Create source topic with ULE support enabled")
        topic = "my-topic"
        mirror_name = "new-mirror"
        topics = {topic: {"partitions": 1, "replication-factor": 2}}

        self.source_kafka.create_topic({
            "topic": topic, **topics[topic],
            "configs": {"mirror.support.unclean.leader.election": "true"},
        })

        src_broker0 = self.source_kafka.nodes[0]
        src_broker1 = self.source_kafka.nodes[1]

        def broker_bootstrap(node):
            """Return bootstrap server address for a single broker node."""
            return "%s:9092" % node.account.hostname

        def trigger_ule(node):
            """Trigger unclean leader election on the given node."""
            cmd = "%s --bootstrap-server %s --topic %s --partition 0 --election-type UNCLEAN" % (
                self.source_kafka.path.script("kafka-leader-election.sh", self.client_node),
                broker_bootstrap(node), topic)
            self.client_node.account.ssh(cmd, allow_fail=False)

        def log_hashes(label):
            """Log MD5 hashes of partition log segments for all brokers across both clusters."""
            self.logger.info("#### %s-0 %s ####", topic, label)
            for name, kafka in [("source", self.source_kafka), ("dest", self.dest_kafka)]:
                for node in kafka.nodes:
                    cmd = "md5sum %s*/%s-0/*.log 2>/dev/null" % (
                        KafkaService.DATA_LOG_DIR_PREFIX, topic)
                    lines = list(node.account.ssh_capture(cmd, allow_fail=True))
                    if lines:
                        for line in lines:
                            self.logger.info("%s %s: %s", name, node.name, line.strip())
                    else:
                        self.logger.info("%s %s: n/a", name, node.name)

        self.logger.info("Bounce source brokers to trigger leader elections")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Send 1 message via source broker 0")
        self.produce_records(self.source_kafka, topic, 1, self.client_node,
                             bootstrap_servers=broker_bootstrap(src_broker0))

        self.logger.info("Start cluster mirror on destination")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(
            self.dest_kafka, mirror_name, "MIRRORING", [topic],
            err_msg="Mirror did not reach MIRRORING state",
        )

        self.logger.info("Stop source broker 0 (broker 0 becomes stale)")
        self.source_kafka.stop_node(src_broker0)

        self.logger.info("Send 1 message via source broker 1")
        self.produce_records(self.source_kafka, topic, 1, self.client_node,
                             bootstrap_servers=broker_bootstrap(src_broker1))
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic],
                                  err_msg="Mirror did not catch up after broker 0 stopped")
        log_hashes("after source broker 0 stopped (broker 0 should be out of sync)")

        self.logger.info("ULE 1: stop broker 1, start broker 0 (stale), elect it as leader")
        self.source_kafka.stop_node(src_broker1)
        self.source_kafka.start_node(src_broker0)
        time.sleep(5)
        trigger_ule(src_broker0)

        self.logger.info("Send 2 messages via source broker 0")
        self.produce_records(self.source_kafka, topic, 2, self.client_node,
                             bootstrap_servers=broker_bootstrap(src_broker0))
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic],
                                  err_msg="Mirror did not catch up after ULE 1")
        log_hashes("after ULE 1 (broker 1 should be out of sync)")

        self.logger.info("Failover: stop mirror so destination topic becomes writable")
        self.dest_kafka.stop_cluster_mirror_topics(self.client_node, mirror_name, topic)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])
        self.logger.info("describe_cluster_mirror: %s",
                         self.dest_kafka.describe_cluster_mirror(self.client_node))

        self.logger.info("Send 2 messages via destination broker 0")
        self.produce_records(self.dest_kafka, topic, 2, self.client_node)

        self.logger.info("ULE 2: stop broker 0, start broker 1 (stale), elect it as leader")
        self.source_kafka.stop_node(src_broker0)
        self.source_kafka.start_node(src_broker1)
        time.sleep(5)
        trigger_ule(src_broker1)

        self.logger.info("Send 6 messages via source broker 1")
        self.produce_records(self.source_kafka, topic, 6, self.client_node,
                             bootstrap_servers=broker_bootstrap(src_broker1))
        log_hashes("after ULE 2 (broker 1 should have the most up to date data)")

        self.logger.info("Failback: source now mirrors from destination")
        # Mirror stays in LOG_TRUNCATION until all source replicas rejoin ISR for LME truncation
        mirror_cfg = MirrorConfig(self.dest_kafka.bootstrap_servers())

        wait_until(
            lambda: self.source_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create reverse cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.source_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start reverse mirror topics",
        )
        self.wait_mirror_state(self.source_kafka, mirror_name, "LOG_TRUNCATION", [topic])

        self.logger.info("Start the stopped source broker so all replicas rejoin ISR for LME truncation")
        self.source_kafka.start_node(src_broker0)
        self.wait_mirror_state(self.source_kafka, mirror_name, "MIRRORING", [topic],
                               err_msg="Reverse mirror did not reach MIRRORING state")
        self.logger.info("describe_cluster_mirror: %s",
                         self.source_kafka.describe_cluster_mirror(self.client_node))

        self.logger.info("Wait for reverse mirror to catch up")
        self.wait_mirror_lag_zero(self.source_kafka, mirror_name, [topic],
                                  err_msg="Reverse mirror did not catch up")

        self.logger.info("Poll until log segment hashes converge (dest is source of truth after failback)")
        self.wait_for_log_convergence(self.dest_kafka, self.source_kafka, topics)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_stop_with_txns(self, metadata_quorum):
        """Verify that stop operation aborts pending transactions and allows committed reads."""
        self.logger.info("Create source topic")
        topic = "my-topic"
        topics = {topic: {"partitions": 1, "replication-factor": 2}}
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Run initial committed transaction and bounce source to bump leader epoch")
        def run_producer(kafka, topic, transactional_id=None, mode="commit",
                         num_records=1, waiting_ms=0, background=False):
            """Run TransactionalTestProducer."""
            cmd = kafka.path.script("kafka-run-class.sh", self.client_node)
            cmd += " org.apache.kafka.tools.TransactionalTestProducer"
            cmd += " --bootstrap-server %s" % kafka.bootstrap_servers(kafka.security_protocol)
            cmd += " --topic %s" % topic
            if transactional_id is not None:
                cmd += " --transactional-id %s" % transactional_id
                cmd += " --mode %s" % mode
                if waiting_ms > 0:
                    cmd += " --waiting-ms %s" % str(waiting_ms)
            cmd += " --num-records %s" % str(num_records)
            if background:
                cmd += " &"
            self.client_node.account.ssh(cmd, allow_fail=False)

        run_producer(self.source_kafka, topic, topic + "-a", "commit")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        self.logger.info("Send interleaving transactions, leaving 2 pending")
        def send_interleaving_txns(topic):
            """Send interleaved transactional and non-transactional records, leaving 2 txns pending."""
            run_producer(self.source_kafka, topic, topic + "-a", "commit", waiting_ms=5000, background=True)
            run_producer(self.source_kafka, topic)
            run_producer(self.source_kafka, topic, topic + "-b", "pending")
            run_producer(self.source_kafka, topic, topic + "-d", "pending")
            run_producer(self.source_kafka, topic, topic + "-c", "abort", waiting_ms=5000, background=True)
            run_producer(self.source_kafka, topic, topic + "-d", "pending")
            run_producer(self.source_kafka, topic)

        def count_ongoing_txns(kafka, topic):
            """Count ongoing transactions across all partitions of a topic."""
            count = 0
            for partition in range(topics[topic]["partitions"]):
                cmd = kafka.path.script("kafka-transactions.sh", self.client_node)
                cmd += " --bootstrap-server %s" % kafka.bootstrap_servers(kafka.security_protocol)
                cmd += " describe-producers --topic %s --partition %d" % (topic, partition)
                for line in self.client_node.account.ssh_capture(cmd, allow_fail=True):
                    fields = line.strip().split()
                    if fields and fields[-1] != "None" and fields[-1] != "CurrentTransactionStartOffset":
                        count += 1
            return count

        send_interleaving_txns(topic)
        ongoing = count_ongoing_txns(self.source_kafka, topic)
        assert ongoing == 2, "Expected 2 ongoing transactions, got %d" % ongoing

        self.logger.info("Create and start cluster mirror, wait for it to catch up")
        mirror_name = "my-mirror"

        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        self.logger.info(
            "Creating mirror %s with settings %s", mirror_name, mirror_cfg
        )

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg
            ),
            timeout_sec=300,
            backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: (
                "Started"
                in self.dest_kafka.start_cluster_mirror_topics(
                    self.client_node, mirror_name, topic
                )
            ),
            timeout_sec=300,
            backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.logger.info("Stop cluster mirror (aborts pending transactions and writes PID reset barriers)")
        self.dest_kafka.stop_cluster_mirror_topics(
            self.client_node, mirror_name, topic
        )

        self.wait_mirror_state(self.dest_kafka, mirror_name, "STOPPED", [topic])

        self.logger.info("Verify extra records on destination from abort markers and PID reset barriers")
        def parse_end_offsets(output):
            """Parse get_offset_shell output into a {partition: offset} dict."""
            offsets = {}
            for line in output.strip().splitlines():
                parts = line.strip().split(":")
                if len(parts) == 3:
                    offsets[int(parts[1])] = int(parts[2])
            return offsets

        source_offsets = parse_end_offsets(
            self.get_offset_shell(self.source_kafka, topic=topic, time=-1))
        dest_offsets = parse_end_offsets(
            self.get_offset_shell(self.dest_kafka, topic=topic, time=-1))
        extra = sum(dest_offsets.values()) - sum(source_offsets.values())
        # 2 abort markers (one per pending txn) + 1 PID reset barrier per partition
        num_partitions = topics[topic]["partitions"]
        expected_extra = 2 + num_partitions
        assert extra == expected_extra, \
            "Expected %d extra records (2 abort + %d PID reset) on destination, got %d. " \
            "source=%s, dest=%s" % (expected_extra, num_partitions, extra, source_offsets, dest_offsets)

        self.logger.info("Commit pending transactions on destination and verify consumer counts")
        run_producer(self.dest_kafka, topic, topic + "-b", "commit")
        run_producer(self.dest_kafka, topic, topic + "-d", "commit")

        self.logger.info("Checking raw number of records")
        source_count = self.consume_records(self.source_kafka, topic, self.client_node)
        dest_count = self.consume_records(self.dest_kafka, topic, self.client_node,
                                          expected_count=source_count + 2)

        self.logger.info("Checking read_committed consumer can make progress")
        # 4 aborted data records are filtered out: txn-b, txn-c, txn-d fenced, txn-d pending
        self.consume_records(self.dest_kafka, topic, self.client_node,
                             isolation_level="read_committed",
                             expected_count=dest_count - 4)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_consumer_groups_commit(self, metadata_quorum):
        """Verify consumer group offset sync for mirror topics and active consumer protection."""
        mirror_name = "my-mirror"

        self.logger.info("Create two topics on source")
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})
        self.source_kafka.create_topic({"topic": "other-topic", "partitions": 1, "replication-factor": 2})
        self.produce_records(self.source_kafka, "my-topic", 3, self.client_node)
        self.produce_records(self.source_kafka, "other-topic", 3, self.client_node)

        self.logger.info("Create consumer group my-group on source by consuming both topics")
        self.consume_records(self.source_kafka, "my-topic", self.client_node, max_messages=3, group="my-group")
        self.consume_records(self.source_kafka, "other-topic", self.client_node, max_messages=3, group="my-group")

        self.logger.info("Start mirror with only my-topic (not other-topic)")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, "my-topic"),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, ["my-topic"])
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.logger.info("Verify my-group on dest has my-topic offset but not other-topic")
        group_desc = self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node)
        assert "my-topic" in group_desc, \
            "Expected my-topic offset to be synced on destination"
        assert "other-topic" not in group_desc, \
            "Expected other-topic offset to not appear on destination"

        def parse_current_offset(desc, topic):
            """Parse describe_consumer_group output and return current offset for a topic."""
            for line in desc.strip().splitlines():
                fields = line.split()
                if len(fields) >= 4 and fields[1] == topic:
                    return int(fields[3])
            return None

        self.logger.info("Produce 2 more records to my-topic and verify synced offset is exactly 3")
        self.produce_records(self.source_kafka, "my-topic", 2, self.client_node)
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, ["my-topic"])
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        dest_desc = self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node)
        dest_offset = parse_current_offset(dest_desc, "my-topic")
        assert dest_offset == 3, "Expected dest offset 3 (synced from source), got %s" % dest_offset

        self.logger.info("Start active consumer on destination (background)")
        self.run_background_consumer(self.dest_kafka, "my-topic", "my-group")
        time.sleep(5)

        self.logger.info("Reset source offset to 2 to prove sync skips active consumer")
        self.source_kafka.reset_consumer_group_offsets(self.client_node, "my-group", "my-topic", 2)
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.logger.info("Verify source offset is 2 but dest offset is still 5 (sync skipped)")
        src_desc = self.describe_consumer_group(self.source_kafka, "my-group", self.client_node)
        dest_desc = self.describe_consumer_group(self.dest_kafka, "my-group", self.client_node)
        self.logger.info("Source group describe: %s", src_desc)
        self.logger.info("Dest group describe: %s", dest_desc)

        src_offset = parse_current_offset(src_desc, "my-topic")
        dest_offset = parse_current_offset(dest_desc, "my-topic")
        assert src_offset == 2, "Expected source offset 2, got %s" % src_offset
        assert dest_offset == 5, "Expected dest offset 5 (sync skipped), got %s" % dest_offset

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_share_groups_commit(self, metadata_quorum):
        """Verify share group offset sync for mirror topics and active consumer protection."""
        mirror_name = "my-mirror"
        share_group = "my-share-group"

        self.logger.info("Create two topics on source")
        self.source_kafka.create_topic({"topic": "my-topic", "partitions": 1, "replication-factor": 2})
        self.source_kafka.create_topic({"topic": "other-topic", "partitions": 1, "replication-factor": 2})
        self.produce_records(self.source_kafka, "my-topic", 3, self.client_node)
        self.produce_records(self.source_kafka, "other-topic", 3, self.client_node)

        self.logger.info("Set share.auto.offset.reset=earliest and consume both topics")
        self.source_kafka.set_share_group_offset_reset_strategy(share_group, "earliest", node=self.client_node)
        self.consume_share_records("my-topic", self.source_kafka, share_group, max_messages=3)
        self.consume_share_records("other-topic", self.source_kafka, share_group, max_messages=3)

        self.logger.info("Run background share consumer + produce more data to commit SPSO")
        self.run_background_share_consumer(self.source_kafka, "my-topic", share_group)
        self.produce_records(self.source_kafka, "my-topic", 3, self.client_node)
        time.sleep(5)

        self.logger.info("Source share group describe: %s",
                         self.source_kafka.describe_share_group(share_group, self.client_node))

        self.logger.info("Kill background share consumer")
        self.kill_background_consumer(self.client_node, "ConsoleShareConsumer")

        self.logger.info("Start mirror with only my-topic")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, "my-topic"),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, ["my-topic"])
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.logger.info("Verify my-share-group on dest has my-topic but not other-topic")
        src_desc = self.source_kafka.describe_share_group(share_group, self.client_node)
        dest_desc = self.dest_kafka.describe_share_group(share_group, self.client_node)
        self.logger.info("Source share group describe: %s", src_desc)
        self.logger.info("Dest share group describe: %s", dest_desc)
        assert "my-topic" in dest_desc, \
            "Expected my-topic offset to be synced on destination"
        assert "other-topic" not in dest_desc, \
            "Expected other-topic offset to not appear on destination"

        self.logger.info("Produce 2 more and consume delta using synced offsets")
        self.produce_records(self.source_kafka, "my-topic", 2, self.client_node)
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, ["my-topic"])
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.consume_share_records("my-topic", self.dest_kafka, share_group, max_messages=2,
                                   expected_count=2)

        self.logger.info("Start active share consumer on destination (background)")
        self.run_background_share_consumer(self.dest_kafka, "my-topic", share_group)
        time.sleep(5)

        self.logger.info("Reset source offset to earliest to prove sync skips active consumer")
        self.source_kafka.reset_share_group_offsets(self.client_node, share_group, "my-topic")
        self.wait_for_metadata_sync(self.dest_kafka, mirror_name, num_cycles=2)

        self.logger.info("Verify source offset reset but dest offset unchanged (sync skipped)")
        src_desc = self.source_kafka.describe_share_group(share_group, self.client_node)
        dest_desc = self.dest_kafka.describe_share_group(share_group, self.client_node)
        self.logger.info("Source share group describe after reset: %s", src_desc)
        self.logger.info("Dest share group describe after reset: %s", dest_desc)

        def parse_share_start_offset(desc, topic):
            """Parse describe_share_group output and return start offset for a topic."""
            for line in desc.strip().splitlines():
                fields = line.split()
                if len(fields) >= 4 and fields[1] == topic:
                    return int(fields[3])
            return None

        src_offset = parse_share_start_offset(src_desc, "my-topic")
        dest_offset = parse_share_start_offset(dest_desc, "my-topic")
        assert src_offset == 0, "Expected source offset 0 (reset to earliest), got %s" % src_offset
        assert dest_offset == 5, "Expected dest offset 5 (sync skipped), got %s" % dest_offset

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_epoch_bump(self, metadata_quorum):
        """Verify that consumers on mirror topics with source leader epochs don't hang on restart."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        group = "my-group"
        self.source_kafka.create_topic({"topic": topic, "partitions": 1, "replication-factor": 1})

        self.logger.info("Produce 3 records and bounce source brokers to bump leader epoch")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)
        for node in self.source_kafka.nodes:
            self.source_kafka.restart_node(node)

        self.logger.info("Produce 2 more records after leader elections")
        self.produce_records(self.source_kafka, topic, 2, self.client_node)

        self.logger.info("Start cluster mirror")
        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.logger.info("Consume 2 records from destination with a consumer group (commits offsets with source LE)")
        self.consume_records(self.dest_kafka, topic, self.client_node,
                             max_messages=2, group=group, expected_count=2)

        self.logger.info("Restart consumer (triggers OffsetFetch and refreshCommittedOffsets)")
        # Without the epoch bump fix, this would hang because source LE > local LE
        self.consume_records(self.dest_kafka, topic, self.client_node, max_messages=2,
                             timeout_ms=20000, group=group, from_beginning=False,
                             expected_count=2)

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_failed_retry(self, metadata_quorum):
        """Verify that mirror recovers from FAILED state after source cluster comes back."""
        topic = "my-topic"
        mirror_name = "my-mirror"
        self.source_kafka.create_topic({"topic": topic, "partitions": 3, "replication-factor": 1})

        self.logger.info("Produce initial records and start cluster mirror")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)

        mirror_cfg = MirrorConfig(self.source_kafka.bootstrap_servers())
        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        wait_until(
            lambda: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                self.client_node, mirror_name, topic),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic])

        self.logger.info("Stop all source brokers to trigger FAILED state")
        for node in self.source_kafka.nodes:
            self.source_kafka.stop_node(node)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "FAILED", [topic],
                               err_msg="Mirror did not reach FAILED state after source shutdown")

        self.logger.info("Restart source brokers so scheduled retry can recover")
        for node in self.source_kafka.nodes:
            self.source_kafka.start_node(node)
        self.wait_mirror_state(self.dest_kafka, mirror_name, "MIRRORING", [topic],
                               err_msg="Mirror did not recover to MIRRORING after source restart")

        self.logger.info("Verify data still flows after recovery")
        self.produce_records(self.source_kafka, topic, 3, self.client_node)
        self.wait_mirror_lag_zero(self.dest_kafka, mirror_name, [topic])

        self.consume_records(self.dest_kafka, topic, self.client_node, max_messages=6,
                             expected_count=6)
