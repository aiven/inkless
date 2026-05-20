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

from ducktape.mark import defaults
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from kafkatest.services.kafka import KafkaService
from kafkatest.services.kafka import quorum
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.tests.core.cluster_mirroring_test import ClientService, MirrorConfig, MirrorHelpers
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
)


class ClusterMirroringSecureTest(MirrorHelpers, Test):
    """Tests for KIP-1279 Cluster Mirroring with SASL_SSL security and authorization."""

    def __init__(self, test_context):
        super(ClusterMirroringSecureTest, self).__init__(test_context)
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
            ["super.users", "User:kafka"],
        ]
        self.source_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            security_protocol=SecurityConfig.SASL_SSL,
            interbroker_security_protocol=SecurityConfig.SASL_SSL,
            client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
            interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
            authorizer_class_name=KafkaService.KRAFT_ACL_AUTHORIZER,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )
        self.dest_kafka = KafkaService(
            test_context, num_nodes=2, zk=None,
            security_protocol=SecurityConfig.SASL_SSL,
            interbroker_security_protocol=SecurityConfig.SASL_SSL,
            client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
            interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
            authorizer_class_name=KafkaService.KRAFT_ACL_AUTHORIZER,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=server_props,
        )

    def setup(self):
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

        self.client = ClientService(self.test_context, num_nodes=1)
        self.client_node = self.client.nodes[0]
        self.source_kafka.security_config.setup_node(self.client_node)

    def teardown(self):
        if hasattr(self, "client"):
            self.client.stop()
        for kafka in [self.dest_kafka, self.source_kafka]:
            try:
                kafka.stop()
            except Exception:
                self.logger.warning("Graceful stop failed for %s, forcing SIGKILL" % str(kafka))
                for node in kafka.nodes:
                    kafka.stop_node(node, clean_shutdown=False)

    ######### helpers #########

    def run_acl_cmd(self, kafka, args):
        cmd = "%s %s" % (kafka.kafka_acls_cmd_with_optional_security_settings(self.client_node), args)
        return kafka.run_cli_tool(self.client_node, cmd)

    def create_topic(self, kafka, topic, partitions=1):
        cmd = "%s --create --topic %s --partitions %d" % (
            kafka.kafka_topics_cmd_with_optional_security_settings(self.client_node, force_use_zk_connection=False),
            topic, partitions)
        kafka.run_cli_tool(self.client_node, cmd)

    def produce_records(self, kafka, topic, messages):
        payload = "\\n".join(messages)
        env_prefix, cmd_suffix = kafka._cmd_security_opts(self.client_node)
        cmd = "echo -e '%s' | %s%s --bootstrap-server %s --topic %s%s" % (
            payload,
            env_prefix,
            kafka.path.script("kafka-console-producer.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic,
            cmd_suffix.replace("--command-config", "--producer.config"))
        self.client_node.account.ssh(cmd)

    def consume_records(self, kafka, topic, max_messages, timeout_ms=30000, group=None):
        env_prefix, cmd_suffix = kafka._cmd_security_opts(self.client_node)
        cmd = "%s%s --bootstrap-server %s --topic %s --from-beginning --max-messages %d --timeout-ms %d" % (
            env_prefix,
            kafka.path.script("kafka-console-consumer.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            topic, max_messages, timeout_ms)
        if group is not None:
            cmd += " --group %s" % group
        cmd += "%s 2>/dev/null" % cmd_suffix.replace("--command-config", "--consumer.config")
        count = 0
        for line in self.client_node.account.ssh_capture(cmd, allow_fail=True):
            if line.strip():
                count += 1
        return count

    def create_mirror_config(self, source_kafka):
        mirror_cfg = MirrorConfig(
            source_kafka.bootstrap_servers(source_kafka.security_protocol),
            security_config=source_kafka.security_config,
        )
        mirror_cfg.properties["sasl.jaas.config"] = \
            'org.apache.kafka.common.security.plain.PlainLoginModule required ' \
            'username="kafka" password="kafka-secret";'
        return mirror_cfg

    def describe_cluster_mirror_as_client(self, kafka):
        client_env = "KAFKA_OPTS='-D%s -D%s' " % (
            KafkaService.JAAS_CONF_PROPERTY, KafkaService.KRB5_CONF)
        client_props = str(kafka.security_config.client_config(
            use_inter_broker_mechanism_for_client=False))
        cmd = "%s%s --bootstrap-server %s --describe --command-config <(echo '%s')" % (
            client_env,
            kafka.path.script("kafka-cluster-mirrors.sh", self.client_node),
            kafka.bootstrap_servers(kafka.security_protocol),
            client_props)
        return kafka.run_cli_tool(self.client_node, cmd)

    def list_acls(self, kafka):
        return self.run_acl_cmd(kafka, "--list")

    def wait_for_acl_condition(self, kafka, mirror_name, condition, err_msg):
        self.wait_for_metadata_sync(kafka, mirror_name)
        def check():
            dest_acls = self.list_acls(kafka)
            self.logger.debug("Destination ACLs:\n%s" % dest_acls)
            return condition(dest_acls)
        wait_until(check, timeout_sec=60, backoff_sec=5, err_msg=err_msg)

    ######### tests #########

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_secure_mirror(self, metadata_quorum):
        """Verify SASL_SSL mirroring with ACL sync and ACL removal propagation."""
        self.logger.info("Granting client producer and consumer permissions on my-topic in source cluster")
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:client --producer --topic my-topic")
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:client --consumer "
                         "--topic my-topic --group my-group")
        time.sleep(5)

        self.logger.info("Creating topic, producing, and consuming on source cluster")
        self.create_topic(self.source_kafka, "my-topic", partitions=1)
        self.produce_records(self.source_kafka, "my-topic", ["a", "b", "c"])
        count = self.consume_records(self.source_kafka, "my-topic", max_messages=3, group="my-group")
        assert count == 3, "Expected 3 records on source, got %d" % count

        self.logger.info("Starting cluster mirror on destination")
        mirror_cfg = self.create_mirror_config(self.source_kafka)
        self.dest_kafka.create_cluster_mirror(self.client_node, "my-mirror", mirror_cfg)
        self.dest_kafka.start_cluster_mirror_topics(self.client_node, "my-mirror", "my-topic")
        self.wait_mirror_state(self.dest_kafka, "my-mirror", "MIRRORING",
                               topics=["my-topic"])

        self.wait_for_metadata_sync(self.dest_kafka, "my-mirror")

        self.logger.info("Granting client DESCRIBE on cluster mirror resource")
        self.run_acl_cmd(self.dest_kafka,
                         "--add --allow-principal User:client --operation DESCRIBE "
                         "--cluster-mirror my-mirror")
        time.sleep(5)

        self.logger.info("Verifying client can describe mirrors on destination")
        output = self.describe_cluster_mirror_as_client(self.dest_kafka)
        assert "my-mirror" in output, "Client should see my-mirror in describe output"

        self.logger.info("Verifying client can consume mirrored data from destination (ACLs synced from source)")
        count = self.consume_records(self.dest_kafka, "my-topic", max_messages=3, group="my-group")
        assert count == 3, "Expected 3 mirrored records on destination, got %d" % count

        self.logger.info("Removing consumer ACL from source cluster")
        self.run_acl_cmd(self.source_kafka,
                         "--remove --allow-principal User:client --operation READ "
                         "--group my-group --force")

        self.logger.info("Verifying consumer group ACL removal propagated to destination")
        self.wait_for_acl_condition(
            self.dest_kafka, "my-mirror",
            lambda acls: "my-group" not in acls,
            "Group ACL for my-group should have been removed from destination")

    @cluster(num_nodes=7)
    @defaults(metadata_quorum=[quorum.isolated_kraft])
    def test_acl_include(self, metadata_quorum):
        """Verify that mirror.acl.include filters which ACLs are synced to destination."""
        self.logger.info("Creating topics on source cluster")
        self.create_topic(self.source_kafka, "orders-topic", partitions=1)
        self.create_topic(self.source_kafka, "internal-topic", partitions=1)

        self.logger.info("Granting topic ACLs on source cluster")
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:client --operation READ --topic orders-topic")
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:other-client --operation WRITE --topic internal-topic")
        time.sleep(5)

        src_acls = self.list_acls(self.source_kafka)
        self.logger.info("Source ACLs:\n%s" % src_acls)

        self.logger.info("Starting cluster mirror with mirror.acl.include=TOPIC;orders-.*")
        mirror_cfg = self.create_mirror_config(self.source_kafka)
        mirror_cfg.properties["mirror.acl.include"] = "TOPIC;orders-.*"
        self.dest_kafka.create_cluster_mirror(self.client_node, "my-mirror", mirror_cfg)
        self.dest_kafka.start_cluster_mirror_topics(self.client_node, "my-mirror", "orders-topic")
        self.dest_kafka.start_cluster_mirror_topics(self.client_node, "my-mirror", "internal-topic")
        self.wait_mirror_state(self.dest_kafka, "my-mirror", "MIRRORING",
                               topics=["orders-topic", "internal-topic"])

        self.logger.info("Verifying ACL filtering on destination")
        self.wait_for_acl_condition(
            self.dest_kafka, "my-mirror",
            lambda acls: "User:client" in acls and "orders-topic" in acls,
            "Client READ ACL on orders-topic should be synced (matches include rule)")

        dest_acls = self.list_acls(self.dest_kafka)
        assert "User:other-client" not in dest_acls and "internal-topic" not in dest_acls, \
            "Other-client WRITE ACL on internal-topic should not be synced (filtered out)"
