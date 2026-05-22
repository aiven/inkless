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
from kafkatest.services.security.security_config import SecurityConfig
from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.tests.core.cluster_mirroring_common import ClientService, MirrorConfig, MirrorUtils
from kafkatest.version import (
    CLUSTER_MIRRORING_METADATA_VERSION,
    CLUSTER_MIRRORING_VERSION,
    KafkaVersion,
    LATEST_2_1,
    LATEST_3_9,
    LATEST_4_0,
    V_3_0_0,
)

ZK_ACL_AUTHORIZER_OLD = "kafka.security.auth.SimpleAclAuthorizer"
ZK_ACL_AUTHORIZER_NEW = "kafka.security.authorizer.AclAuthorizer"


class ClusterMirroringCompAclsTest(MirrorUtils, Test):
    """Tests for KIP-1279 Cluster Mirroring ACLs sync across different Kafka versions."""

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
        ["super.users", "User:kafka"],
    ]

    SOURCE_SERVER_PROPS = [
        ["default.replication.factor", "2"],
        ["min.insync.replicas", "1"],
        ["offsets.topic.replication.factor", "2"],
        ["transaction.state.log.replication.factor", "2"],
        ["transaction.state.log.min.isr", "1"],
        ["super.users", "User:kafka"],
    ]

    def __init__(self, test_context):
        super(ClusterMirroringCompAclsTest, self).__init__(test_context)

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
        self.source_client = ClientService(self.test_context, version=source_version)
        self.source_client.start()
        self.source_client_node = self.source_client.nodes[0]
        if metadata_quorum == quorum.zk:
            self.zk = ZookeeperService(self.test_context, num_nodes=1)
            self.zk.start()
            self.source_kafka = KafkaService(
                self.test_context, num_nodes=2, zk=self.zk,
                version=source_version,
                security_protocol=SecurityConfig.SASL_SSL,
                interbroker_security_protocol=SecurityConfig.SASL_SSL,
                client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
                interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
                authorizer_class_name=ZK_ACL_AUTHORIZER_OLD if source_version < V_3_0_0 else ZK_ACL_AUTHORIZER_NEW,
                server_prop_overrides=self.SOURCE_SERVER_PROPS,
            )
            self._original_metadata_quorum = self.test_context.injected_args.get("metadata_quorum")
            self.test_context.injected_args["metadata_quorum"] = quorum.isolated_kraft
        else:
            self.source_kafka = KafkaService(
                self.test_context, num_nodes=2, zk=None,
                version=source_version,
                security_protocol=SecurityConfig.SASL_SSL,
                interbroker_security_protocol=SecurityConfig.SASL_SSL,
                client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
                interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
                authorizer_class_name=KafkaService.KRAFT_ACL_AUTHORIZER,
                controller_num_nodes_override=1,
                server_prop_overrides=self.SOURCE_SERVER_PROPS,
            )
        self.source_kafka.start()
        self.source_client.setup_security(self.source_kafka.security_config)

    def setup_dest(self):
        self.dest_client = ClientService(self.test_context)
        self.dest_client.start()
        self.dest_client_node = self.dest_client.nodes[0]
        self.dest_kafka = KafkaService(
            self.test_context, num_nodes=2, zk=None,
            security_protocol=SecurityConfig.SASL_SSL,
            interbroker_security_protocol=SecurityConfig.SASL_SSL,
            client_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
            interbroker_sasl_mechanism=SecurityConfig.SASL_MECHANISM_PLAIN,
            authorizer_class_name=KafkaService.KRAFT_ACL_AUTHORIZER,
            use_cluster_mirroring=True,
            controller_num_nodes_override=1,
            server_prop_overrides=self.DEST_SERVER_PROPS,
        )
        self.dest_kafka.start()
        self.dest_client.setup_security(self.dest_kafka.security_config)
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

    def create_mirror_config(self, source_kafka):
        mirror_cfg = MirrorConfig(
            source_kafka.bootstrap_servers(source_kafka.security_protocol),
            security_config=source_kafka.security_config,
        )
        mirror_cfg.properties["sasl.jaas.config"] = \
            'org.apache.kafka.common.security.plain.PlainLoginModule required ' \
            'username="kafka" password="kafka-secret";'
        return mirror_cfg

    def run_acl_cmd(self, kafka, args, client_node):
        if kafka.zk is not None:
            cmd = "%s --authorizer-properties zookeeper.connect=%s %s" % (
                kafka.path.script("kafka-acls.sh", client_node),
                kafka.zk.connect_setting(), args)
        else:
            cmd = "%s %s" % (kafka.kafka_acls_cmd_with_optional_security_settings(client_node), args)
        return kafka.run_cli_tool(client_node, cmd)

    def produce_as_client(self, kafka, topic, num_records, client_node):
        client_env = "KAFKA_OPTS='-D%s -D%s' " % (
            KafkaService.JAAS_CONF_PROPERTY, KafkaService.KRB5_CONF)
        client_props = str(kafka.security_config.client_config(
            use_inter_broker_mechanism_for_client=False))
        cmd = "%s%s --topic %s --num-records %d --record-size 1 --throughput -1" \
              " --producer-props bootstrap.servers=%s" \
              " --producer.config <(echo '%s')" % (
                  client_env,
                  kafka.path.script("kafka-producer-perf-test.sh", client_node),
                  topic, num_records,
                  kafka.bootstrap_servers(kafka.security_protocol),
                  client_props)
        client_node.account.ssh(cmd, allow_fail=False)

    def consume_as_client(self, kafka, topic, client_node, max_messages,
                          group=None, timeout_ms=30000):
        client_env = "KAFKA_OPTS='-D%s -D%s' " % (
            KafkaService.JAAS_CONF_PROPERTY, KafkaService.KRB5_CONF)
        client_props = str(kafka.security_config.client_config(
            use_inter_broker_mechanism_for_client=False))
        cmd = "%s%s --bootstrap-server %s --topic %s --from-beginning" \
              " --max-messages %d --timeout-ms %d" % (
                  client_env,
                  kafka.path.script("kafka-console-consumer.sh", client_node),
                  kafka.bootstrap_servers(kafka.security_protocol),
                  topic, max_messages, timeout_ms)
        if group is not None:
            cmd += " --group %s" % group
        cmd += " --consumer.config <(echo '%s') 2>/dev/null" % client_props
        count = 0
        for line in client_node.account.ssh_capture(cmd, allow_fail=True):
            if line.strip():
                count += 1
        return count

    def list_acls(self, kafka, client_node):
        return self.run_acl_cmd(kafka, "--list", client_node)

    def wait_for_acl_condition(self, kafka, mirror_name, condition, err_msg, client_node):
        self.wait_for_metadata_sync(kafka, mirror_name)
        def check():
            dest_acls = self.list_acls(kafka, client_node)
            self.logger.debug("Destination ACLs:\n%s" % dest_acls)
            return condition(dest_acls)
        wait_until(check, timeout_sec=300, backoff_sec=5, err_msg=err_msg)


    @cluster(num_nodes=8)
    @parametrize(source_version=str(LATEST_2_1), metadata_quorum=quorum.zk)
    @parametrize(source_version=str(LATEST_3_9), metadata_quorum=quorum.zk)
    @parametrize(source_version=str(LATEST_4_0), metadata_quorum=quorum.isolated_kraft)
    def test_mirroring(self, source_version, metadata_quorum):
        """Verify ACL sync and filtering across different Kafka versions with SASL_SSL."""
        self.setup_source(KafkaVersion(source_version), metadata_quorum)
        self.setup_dest()

        topics = {
            "my-topic-a": {"partitions": 3, "replication-factor": 2},
            "my-topic-b": {"partitions": 1, "replication-factor": 2},
            "new-topic": {"partitions": 2, "replication-factor": 2},
        }

        self.logger.info("Creating topics on source cluster")
        for t, cfg in topics.items():
            self.source_kafka.create_topic({"topic": t, **cfg})

        self.logger.info("Granting ACLs on source cluster")
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:client --operation READ --topic my-topic-a",
                         self.source_client_node)
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:client --operation WRITE --topic my-topic-a",
                         self.source_client_node)
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:client --operation READ --group my-group",
                         self.source_client_node)
        self.run_acl_cmd(self.source_kafka,
                         "--add --allow-principal User:other-client --operation READ --topic new-topic",
                         self.source_client_node)
        time.sleep(5)

        self.logger.info("Producing 10 records to my-topic-a as client user")
        self.produce_as_client(self.source_kafka, "my-topic-a",
                               10, self.source_client_node)

        self.logger.info("Creating and starting cluster mirror with ACL include filter")
        mirror_name = "my-mirror"
        mirror_cfg = self.create_mirror_config(self.source_kafka)
        mirror_cfg.properties["mirror.acl.include"] = "TOPIC;my-topic-.*,GROUP;my-group"

        wait_until(
            lambda: self.dest_kafka.create_cluster_mirror(
                self.dest_client_node, mirror_name, mirror_cfg),
            timeout_sec=300, backoff_sec=2,
            err_msg="Failed to create cluster mirror",
        )
        for regex in ["my-topic.*", "new-topic"]:
            wait_until(
                lambda r=regex: "Started" in self.dest_kafka.start_cluster_mirror_topics(
                    self.dest_client_node, mirror_name, r),
                timeout_sec=300, backoff_sec=2,
                err_msg="Failed to start mirror topics for %s" % regex,
            )
        self.logger.info("Waiting for all partitions to reach MIRRORING with zero lag")
        self.wait_mirror_lag_zero(
            self.dest_kafka, mirror_name, topics=list(topics.keys()))

        self.logger.info("Consuming 10 records from my-topic-a on destination as client user")
        count = self.consume_as_client(self.dest_kafka, "my-topic-a",
                                       self.dest_client_node, max_messages=10,
                                       group="my-group")
        assert count == 10, "Expected 10 records on destination for my-topic-a, got %d" % count

        self.logger.info("Verifying ACL filtering: my-topic-a ACL synced, new-topic ACL filtered out")
        self.wait_for_acl_condition(
            self.dest_kafka, mirror_name,
            lambda acls: "User:client" in acls and "my-topic-a" in acls,
            "Client READ ACL on my-topic-a should be synced (matches include rule)",
            self.dest_client_node)

        dest_acls = self.list_acls(self.dest_kafka, self.dest_client_node)
        assert "User:other-client" not in dest_acls and "new-topic" not in dest_acls, \
            "Other-client READ ACL on new-topic should not be synced (filtered out)"

        self.logger.info("Removing my-topic-a ACLs from source cluster")
        self.run_acl_cmd(self.source_kafka,
                         "--remove --allow-principal User:client --operation READ "
                         "--topic my-topic-a --force",
                         self.source_client_node)
        self.run_acl_cmd(self.source_kafka,
                         "--remove --allow-principal User:client --operation WRITE "
                         "--topic my-topic-a --force",
                         self.source_client_node)

        self.logger.info("Verifying ACL removal propagated to destination")
        self.wait_for_acl_condition(
            self.dest_kafka, mirror_name,
            lambda acls: "my-topic-a" not in acls,
            "Client READ ACL on my-topic-a should have been removed from destination",
            self.dest_client_node)
