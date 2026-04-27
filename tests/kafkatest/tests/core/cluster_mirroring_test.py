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

import os

from ducktape.mark.resource import cluster
from ducktape.mark import defaults
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
)
import itertools

class ClusterMirrorConfig:
    def __init__(
        self,
        bootstrap_servers: str,
        mirror_topic_properties_exclude: str = None,
        mirror_groups_include: str = None,
        mirror_acl_include: str = None,
        security_config: SecurityConfig = None,
    ):
        self.properties = {
            "bootstrap.servers": bootstrap_servers,
        }
        if mirror_topic_properties_exclude is not None:
            self.properties["mirror.topic.properties.exclude"] = (
                mirror_topic_properties_exclude
            )
        if mirror_groups_include is not None:
            self.properties["mirror.groups.include"] = mirror_groups_include
        if mirror_acl_include is not None:
            self.properties["mirror.acl.include"] = (mirror_acl_include,)

        if (
            security_config is not None
            and security_config.security_protocol != SecurityConfig.PLAINTEXT
        ):
            self.properties |= security_config.properties

    def props(self, prefix=""):
        config_lines = (
            prefix + key + "=" + value for key, value in self.properties.items()
        )
        return "\n".join(itertools.chain([""], config_lines, [""]))

    def __str__(self):
        """
        Return properties as a string with line separators.
        """
        return self.props()

class ClusterMirroringTest(Test):
    PERSISTENT_ROOT = "/mnt/cluster_mirroring"
    MIRROR_CONFIG_FILE = os.path.join(PERSISTENT_ROOT, "cluster_mirror.properties")

    def __init__(self, test_context):
        """:type test_context: ducktape.tests.test.TestContext"""
        super(ClusterMirroringTest, self).__init__(test_context)
        topic = "my-topic"
        self.topics = {topic: {"partitions": 3, "replication-factor": 3}}
        self.source_kafka = KafkaService(
            test_context,
            num_nodes=3,
            zk=None,
            topics=self.topics,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
        )
        self.dest_kafka = KafkaService(
            test_context, num_nodes=3, zk=None, use_cluster_mirroring=True,
            controller_num_nodes_override=1,
        )
        self.producer = VerifiableProducer(
            self.test_context,
            num_nodes=1,
            kafka=self.source_kafka,
            topic=topic,
            throughput=100,
        )

    def setup(self):
        self.source_kafka.start()
        self.dest_kafka.start()

        self.logger.info(
            "Changing metadata.version in destination to %s"
            % CLUSTER_MIRRORING_METADATA_VERSION
        )
        self.dest_kafka.upgrade_metadata_version(CLUSTER_MIRRORING_METADATA_VERSION)
        self.logger.info(
            "Changing mirror.version in destination to %s" % CLUSTER_MIRRORING_VERSION
        )
        self.dest_kafka.run_features_command(
            "upgrade", "mirror.version", CLUSTER_MIRRORING_VERSION
        )

        self.producer.start()

    def teardown(self):
        if any(self.producer.alive(node) for node in self.producer.nodes):
            try:
                self.producer.stop()
            except Exception as e:
                self.logger.warning("Error stopping producer: %s" % str(e))
        for kafka in [self.dest_kafka, self.source_kafka]:
            try:
                kafka.stop()
            except Exception:
                self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                for node in kafka.nodes:
                    kafka.stop_node(node, clean_shutdown=False)

    def all_satisfy_in_mirror(self, kafka_node, mirror_name, per_partition_condition):
        """Check that all partitions of all mirrored topics satisfy the given condition."""
        output = self.dest_kafka.describe_cluster_mirror(kafka_node)
        if output == "":
            return False

        mirrors = self.dest_kafka.parse_describe_cluster_mirror(output)
        if mirror_name not in mirrors:
            return False
        mirror = mirrors[mirror_name]

        for topic in self.topics:
            if topic not in mirror:
                return False

            for partition in mirror[topic].values():
                if not per_partition_condition(partition):
                    return False
        return True

    @cluster(num_nodes=9)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_log_convergence(self, metadata_quorum):
        """Verify that mirrored log segments are byte identical to source after bouncing both clusters."""
        mirror_cfg = {
            "name": "my-mirror",
            "cfg": ClusterMirrorConfig(self.source_kafka.bootstrap_servers()),
        }
        self.logger.info(
            "Creating mirror %s with settings %s", mirror_cfg["name"], mirror_cfg["cfg"]
        )
        kafka_node = self.dest_kafka.nodes[0]

        prop_file = str(mirror_cfg["cfg"])
        self.logger.info(prop_file)
        kafka_node.account.ssh(
            "mkdir -p %s" % ClusterMirroringTest.PERSISTENT_ROOT, allow_fail=False
        )
        kafka_node.account.create_file(
            ClusterMirroringTest.MIRROR_CONFIG_FILE, prop_file
        )

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                kafka_node, mirror_cfg["name"], ClusterMirroringTest.MIRROR_CONFIG_FILE
            ),
            timeout_sec=20,
            backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )

        wait_until(
            lambda: (
                "Started"
                in self.dest_kafka.start_cluster_mirror_topics(
                    kafka_node, mirror_cfg["name"], self.topics.keys()
                )
            ),
            timeout_sec=20,
            backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )

        wait_until(
            lambda: self.all_satisfy_in_mirror(
                kafka_node, mirror_cfg["name"], lambda p: p["state"] == "MIRRORING"
            ),
            timeout_sec=20,
            backoff_sec=2,
            err_msg="Failed to start mirrors",
        )

        wait_until(
            lambda: self.producer.num_acked >= 5,
            timeout_sec=10,
            backoff_sec=2,
            err_msg="Failed to produce to source cluster",
        )

        acked = self.producer.num_acked
        self.source_kafka.restart_cluster(clean_shutdown=True)
        wait_until(
            lambda: self.producer.num_acked - acked > 1000,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Failed to produce since source cluster bounced",
        )

        acked = self.producer.num_acked
        self.dest_kafka.restart_cluster(clean_shutdown=True)

        wait_until(
            lambda: self.producer.num_acked - acked > 1000,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Failed to produce since destination cluster bounced",
        )
        self.producer.stop()

        wait_until(
            lambda: self.all_satisfy_in_mirror(
                kafka_node, mirror_cfg["name"],
                lambda p: p["lag"] == 0 and p["state"] == "MIRRORING"
            ),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Failed to start mirrors",
        )

        def log_segment_hashes(kafka, topic, partition=0):
            """Return a dict mapping segment file names to their MD5 hashes."""
            leader_node = kafka.leader(topic, partition)
            cmd = "md5sum %s*/%s-%d/*.log 2>/dev/null" % (
                KafkaService.DATA_LOG_DIR_PREFIX, topic, partition)
            hashes = {}
            for line in leader_node.account.ssh_capture(cmd, allow_fail=True):
                parts = line.strip().split()
                if len(parts) == 2:
                    hashes[os.path.basename(parts[1])] = parts[0]
            return hashes

        # Check log segments convergence by comparing md5 hashes
        for topic, cfg in self.topics.items():
            for partition in range(cfg["partitions"]):
                source_hashes = log_segment_hashes(self.source_kafka, topic, partition)
                dest_hashes = log_segment_hashes(self.dest_kafka, topic, partition)
                assert source_hashes.keys() == dest_hashes.keys(), \
                    "Segment files differ for %s-%d: source=%s, dest=%s" % (
                        topic, partition, sorted(source_hashes.keys()), sorted(dest_hashes.keys()))
                mismatches = []
                for seg in source_hashes:
                    if source_hashes[seg] != dest_hashes[seg]:
                        mismatches.append("%s: source=%s, dest=%s" % (
                            seg, source_hashes[seg], dest_hashes[seg]))
                assert not mismatches, \
                    "Log segment mismatch for %s-%d:\n  %s" % (topic, partition, "\n  ".join(mismatches))

    @cluster(num_nodes=9)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_stop_with_txns(self, metadata_quorum):
        """Verify that STOPPING transition aborts pending transactions and allows committed reads."""
        mirror_name = "my-mirror"
        kafka_node = self.dest_kafka.nodes[0]
        topic = list(self.topics.keys())[0]

        def run_producer(kafka, topic, transactional_id=None, mode="commit",
                         num_records=1, waiting_ms=0, background=False):
            """Run TransactionalTestProducer via SSH on the first broker node."""
            node = kafka.nodes[0]
            cmd = kafka.path.script("kafka-run-class.sh", node)
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
            node.account.ssh(cmd, allow_fail=False)

        run_producer(self.source_kafka, topic, topic + "-a", "commit")

        self.logger.info("Restarting source brokers to bump leader epoch")
        self.source_kafka.restart_cluster(clean_shutdown=True)

        def count_ongoing_txns(kafka, topic):
            """Count ongoing transactions across all partitions using describe-producers."""
            node = kafka.nodes[0]
            count = 0
            for partition in range(self.topics[topic]["partitions"]):
                cmd = kafka.path.script("kafka-transactions.sh", node)
                cmd += " --bootstrap-server %s" % kafka.bootstrap_servers(kafka.security_protocol)
                cmd += " describe-producers --topic %s --partition %d" % (topic, partition)
                for line in node.account.ssh_capture(cmd, allow_fail=True):
                    fields = line.strip().split()
                    if fields and fields[-1] != "None" and fields[-1] != "CurrentTransactionStartOffset":
                        count += 1
            return count

        def send_interleaving_txns(topic):
            """Send interleaved transactional and non-transactional records, leaving 2 txns pending."""
            run_producer(self.source_kafka, topic, topic + "-a", "commit", waiting_ms=5000, background=True)
            run_producer(self.source_kafka, topic)
            run_producer(self.source_kafka, topic, topic + "-b", "pending")
            run_producer(self.source_kafka, topic, topic + "-d", "pending")
            run_producer(self.source_kafka, topic, topic + "-c", "abort", waiting_ms=5000, background=True)
            run_producer(self.source_kafka, topic, topic + "-d", "pending")
            run_producer(self.source_kafka, topic)

        send_interleaving_txns(topic)
        ongoing = count_ongoing_txns(self.source_kafka, topic)
        assert ongoing == 2, "Expected 2 ongoing transactions, got %d" % ongoing

        self.producer.stop()

        mirror_cfg = ClusterMirrorConfig(self.source_kafka.bootstrap_servers())
        self.logger.info(
            "Creating mirror %s with settings %s", mirror_name, mirror_cfg
        )
        prop_file = str(mirror_cfg)
        kafka_node.account.ssh(
            "mkdir -p %s" % ClusterMirroringTest.PERSISTENT_ROOT, allow_fail=False
        )
        kafka_node.account.create_file(
            ClusterMirroringTest.MIRROR_CONFIG_FILE, prop_file
        )

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                kafka_node, mirror_name, ClusterMirroringTest.MIRROR_CONFIG_FILE
            ),
            timeout_sec=20,
            backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )

        wait_until(
            lambda: (
                "Started"
                in self.dest_kafka.start_cluster_mirror_topics(
                    kafka_node, mirror_name, self.topics.keys()
                )
            ),
            timeout_sec=20,
            backoff_sec=2,
            err_msg="Failed to start mirror topics",
        )

        wait_until(
            lambda: self.all_satisfy_in_mirror(
                kafka_node, mirror_name,
                lambda p: p["lag"] == 0 and p["state"] == "MIRRORING"
            ),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Mirror did not catch up",
        )

        self.logger.info("Stopping mirror topics for %s", mirror_name)
        self.dest_kafka.stop_cluster_mirror_topics(
            kafka_node, mirror_name, self.topics.keys()
        )

        def check_stopped():
            output = self.dest_kafka.describe_cluster_mirror(kafka_node)
            self.logger.info("describe_cluster_mirror: %s", output)
            return self.all_satisfy_in_mirror(
                kafka_node, mirror_name, lambda p: p["state"] == "STOPPED"
            )

        wait_until(
            check_stopped,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Mirror topics did not reach STOPPED state",
        )

        def parse_end_offsets(output):
            """Parse get_offset_shell output into a {partition: offset} dict."""
            offsets = {}
            for line in output.strip().splitlines():
                parts = line.strip().split(":")
                if len(parts) == 3:
                    offsets[int(parts[1])] = int(parts[2])
            return offsets

        source_offsets = parse_end_offsets(
            self.source_kafka.get_offset_shell(topic=topic, time=-1))
        dest_offsets = parse_end_offsets(
            self.dest_kafka.get_offset_shell(topic=topic, time=-1))
        extra = sum(dest_offsets.values()) - sum(source_offsets.values())
        # 2 abort markers (one per pending txn) + 1 PID reset barrier per partition
        num_partitions = self.topics[topic]["partitions"]
        expected_extra = 2 + num_partitions
        assert extra == expected_extra, \
            "Expected %d extra records (2 abort + %d PID reset) on destination, got %d. " \
            "source=%s, dest=%s" % (expected_extra, num_partitions, extra, source_offsets, dest_offsets)

        run_producer(self.dest_kafka, topic, topic + "-b", "commit")
        run_producer(self.dest_kafka, topic, topic + "-d", "commit")

        def run_consumer(kafka, topic, isolation_level="read_uncommitted"):
            """Run console consumer and return the record count."""
            node = kafka.nodes[0]
            cmd = kafka.path.script("kafka-console-consumer.sh", node)
            cmd += " --bootstrap-server %s" % kafka.bootstrap_servers(kafka.security_protocol)
            cmd += " --topic %s --from-beginning" % topic
            cmd += " --isolation-level %s" % isolation_level
            cmd += " --timeout-ms 10000"
            count = 0
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                if line.strip():
                    count += 1
            return count

        # checking raw number of records
        source_count = run_consumer(self.source_kafka, topic)
        dest_count = run_consumer(self.dest_kafka, topic)
        assert dest_count == source_count + 2, \
            "Expected dest to have exactly 2 more records than source, " \
            "got source=%d, dest=%d" % (source_count, dest_count)

        # checking read_committed consumer can make progress
        # 4 aborted data records are filtered out: txn-b, txn-c, txn-d fenced, txn-d pending
        dest_committed = run_consumer(self.dest_kafka, topic, "read_committed")
        assert dest_committed == dest_count - 4, \
            "Expected dest_committed=%d, got %d" % (dest_count - 4, dest_committed)
