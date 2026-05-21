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

import itertools
import json
import os
import time

from ducktape.services.service import Service
from ducktape.utils.util import wait_until
from kafkatest.directory_layout.kafka_path import KafkaPathResolverMixin
from kafkatest.services.kafka import KafkaService
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.version import DEV_BRANCH


class ClientService(KafkaPathResolverMixin, Service):
    """Provides a dedicated node for running Kafka CLI commands off broker nodes."""

    def __init__(self, context, version=DEV_BRANCH):
        super().__init__(context, 1)
        for node in self.nodes:
            node.version = version

    def setup_security(self, security_config):
        security_config.setup_node(self.nodes[0])

    def start_node(self, node):
        pass

    def stop_node(self, node):
        pass

    def clean_node(self, node):
        pass


class MirrorConfig:
    """Configuration for a cluster mirror connection properties file."""
    def __init__(
        self,
        bootstrap_servers: str,
        mirror_topic_properties_exclude: str = None,
        mirror_groups_include: str = None,
        mirror_groups_exclude: str = None,
        mirror_acl_include: str = None,
        security_config: SecurityConfig = None,
    ):
        self.properties = {
            "bootstrap.servers": bootstrap_servers,
            "mirror.metadata.refresh.interval.ms": "10000",
        }
        if mirror_topic_properties_exclude is not None:
            self.properties["mirror.topic.properties.exclude"] = (
                mirror_topic_properties_exclude
            )
        if mirror_groups_include is not None:
            self.properties["mirror.groups.include"] = mirror_groups_include
        if mirror_groups_exclude is not None:
            self.properties["mirror.groups.exclude"] = mirror_groups_exclude
        if mirror_acl_include is not None:
            self.properties["mirror.acl.include"] = mirror_acl_include

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


class MirrorUtils:
    """Shared helpers for cluster mirroring tests."""

    def produce_records(self, kafka, topic, num_records, client_node,
                        bootstrap_servers=None):
        """Produce records on a client node using kafka-producer-perf-test."""
        if bootstrap_servers is None:
            bootstrap_servers = kafka.bootstrap_servers(kafka.security_protocol)
        env_prefix, cmd_suffix = kafka._cmd_security_opts(client_node)
        cmd = "%s%s --topic %s --num-records %d --record-size 1 --throughput -1" \
              " --producer-props bootstrap.servers=%s%s" % (
                  env_prefix,
                  kafka.path.script("kafka-producer-perf-test.sh", client_node),
                  topic, num_records, bootstrap_servers,
                  cmd_suffix.replace("--command-config", "--producer.config")
                  if cmd_suffix else "")
        client_node.account.ssh(cmd, allow_fail=True)

    def consume_records(self, kafka, topic, client_node, max_messages=None,
                        timeout_ms=30000, isolation_level=None, group=None,
                        from_beginning=True):
        """Consume records on a client node and return count."""
        env_prefix, cmd_suffix = kafka._cmd_security_opts(client_node)
        cmd = "%s%s --bootstrap-server %s --topic %s --timeout-ms %d" % (
            env_prefix,
            kafka.path.script("kafka-console-consumer.sh", client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, timeout_ms)
        if from_beginning:
            cmd += " --from-beginning"
        if max_messages is not None:
            cmd += " --max-messages %d" % max_messages
        if isolation_level is not None:
            cmd += " --isolation-level %s" % isolation_level
        if group is not None:
            cmd += " --group %s" % group
        if cmd_suffix:
            cmd += cmd_suffix.replace("--command-config", "--consumer.config")
        cmd += " 2>/dev/null"
        count = 0
        for line in client_node.account.ssh_capture(cmd, allow_fail=True):
            if line.strip():
                count += 1
        return count

    def all_satisfy_in_mirror(self, kafka, mirror_name, per_partition_condition, topics):
        """Check that all partitions of the given mirror topics satisfy the condition."""
        output = kafka.describe_cluster_mirror(kafka.nodes[0])
        if output == "":
            return False

        try:
            mirrors = kafka.parse_describe_cluster_mirror(output)
        except (json.JSONDecodeError, KeyError):
            return False
        if mirror_name not in mirrors:
            return False
        mirror = mirrors[mirror_name]

        for topic in topics:
            if topic not in mirror:
                return False

            for partition in mirror[topic].values():
                if not per_partition_condition(partition):
                    return False
        return True

    def wait_mirror_state(self, kafka, mirror_name, state, topics,
                          err_msg=None):
        """Wait until all mirror partitions reach the given state."""
        def check():
            self.logger.debug("describe_cluster_mirror: %s",
                              kafka.describe_cluster_mirror(kafka.nodes[0]))
            return self.all_satisfy_in_mirror(
                kafka, mirror_name,
                lambda p: p["state"] == state, topics)
        if err_msg is None:
            err_msg = "Mirror did not reach %s state" % state
        wait_until(check, timeout_sec=300, backoff_sec=2, err_msg=err_msg)

    def wait_for_metadata_sync(self, kafka, mirror_name, num_cycles=1):
        """Wait for metadata sync by sleeping based on the configured refresh interval."""
        output = kafka.describe_mirror_config(kafka.nodes[0], mirror_name)
        interval_ms = 30000
        for line in output.splitlines():
            if "mirror.metadata.refresh.interval.ms=" in line:
                interval_ms = int(line.strip().split()[0].split("=")[1])
                break
        sleep_s = (interval_ms // 1000) * num_cycles + 2
        self.logger.info("Waiting %ds for %d metadata sync cycle(s) (interval=%dms)",
                         sleep_s, num_cycles, interval_ms)
        time.sleep(sleep_s)

    def wait_mirror_lag_zero(self, kafka, mirror_name, topics,
                             err_msg="Mirror did not catch up"):
        """Wait until all mirror partitions reach MIRRORING state with zero lag."""
        def check():
            self.logger.debug("describe_cluster_mirror: %s",
                              kafka.describe_cluster_mirror(kafka.nodes[0]))
            return self.all_satisfy_in_mirror(
                kafka, mirror_name,
                lambda p: p["lag"] == 0 and p["state"] == "MIRRORING", topics)
        wait_until(check, timeout_sec=300, backoff_sec=5, err_msg=err_msg)

    def describe_consumer_group(self, kafka, group, client_node):
        """Describe a consumer group on a client node with security support."""
        env_prefix, cmd_suffix = kafka._cmd_security_opts(client_node)
        cmd = "%s%s --bootstrap-server %s --group %s --describe%s" % (
            env_prefix,
            kafka.path.script("kafka-consumer-groups.sh", client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            group, cmd_suffix)
        output = ""
        for line in client_node.account.ssh_capture(cmd, allow_fail=True):
            if not (line.startswith("SLF4J") or line.startswith("GROUP")
                    or line.startswith("Could not fetch offset")):
                output += line
        return output

    def wait_for_log_convergence(self, source_kafka, dest_kafka, topics):
        """Poll until source leader and all dest replica log segment hashes match."""
        def log_segment_hashes(node, topic, partition):
            cmd = "md5sum %s*/%s-%d/*.log 2>/dev/null" % (
                KafkaService.DATA_LOG_DIR_PREFIX, topic, partition)
            hashes = {}
            for line in node.account.ssh_capture(cmd, allow_fail=True):
                parts = line.strip().split()
                if len(parts) == 2:
                    hashes[os.path.basename(parts[1])] = parts[0]
            return hashes

        def check():
            for topic, cfg in topics.items():
                for partition in range(cfg["partitions"]):
                    source = log_segment_hashes(
                        source_kafka.leader(topic, partition), topic, partition)
                    for node in dest_kafka.replicas(topic, partition):
                        dest = log_segment_hashes(node, topic, partition)
                        if source.keys() != dest.keys() or any(
                                source[seg] != dest[seg] for seg in source):
                            return False
                        self.logger.info("Hashes match for %s-%d dest %s: %d segments verified",
                                         topic, partition, node.name, len(source))
            return True

        wait_until(
            check,
            timeout_sec=300,
            backoff_sec=5,
            err_msg="Log segments did not converge between source and destination",
        )
