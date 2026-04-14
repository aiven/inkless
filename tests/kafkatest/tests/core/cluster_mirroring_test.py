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
        topic = "test"
        self.topics = {topic: {"partitions": 1, "replication-factor": 3}}
        self.source_kafka = KafkaService(
            test_context,
            num_nodes=3,
            zk=None,
            topics=self.topics,
            use_cluster_mirroring=True,
        )
        self.dest_kafka = KafkaService(
            test_context, num_nodes=3, zk=None, use_cluster_mirroring=True
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
        self.producer.stop()
        self.dest_kafka.stop()
        self.source_kafka.stop()

    @cluster(num_nodes=13)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_convergence(self, metadata_quorum):
        mirror_cfg = {
            "name": "test-mirror",
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

        def all_satisfy_in_mirror(mirror_name, per_partition_condition):
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

        wait_until(
            lambda: all_satisfy_in_mirror(
                mirror_cfg["name"], lambda p: p["state"] == "MIRRORING"
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
            lambda: all_satisfy_in_mirror(mirror_cfg["name"], lambda p: p["lag"] == 0 and p["state"] == "MIRRORING"),
            timeout_sec=60,
            backoff_sec=2,
            err_msg="Failed to start mirrors",
        )

        # TODO: invoke log segment comparision tooling
