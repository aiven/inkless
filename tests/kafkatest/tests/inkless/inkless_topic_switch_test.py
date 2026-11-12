# Inkless
# Copyright (C) 2026 Aiven OY
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

import os
import re
import uuid

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test, TestContext
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.inkless.consolidation_verifier import ConsolidationVerifier
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.trogdor.degraded_network_fault_spec import DegradedNetworkFaultSpec
from kafkatest.services.trogdor.network_partition_fault_spec import NetworkPartitionFaultSpec
from kafkatest.services.trogdor.process_stop_fault_spec import ProcessStopFaultSpec
from kafkatest.services.trogdor.trogdor import TrogdorService
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int_with_prefix


def _enable_tiered_storage_classpath(kafka):
    """Add the ``:storage`` test jar (containing ``LocalTieredStorage``)
    to the broker classpath. ``kafka-run-class.sh`` neither scans
    ``storage/build/libs/`` nor keeps ``*-test.jar`` files by default, so
    we set ``INCLUDE_TEST_JARS=true`` and prepend the jar explicitly.

    Patched on the instance rather than via a ``KafkaService`` subclass
    because ducktape's ``render()`` resolves Jinja2 templates relative to
    the class's module, which breaks for subclasses defined in test files.
    """
    original_start_cmd = kafka.start_cmd

    def _patched_start_cmd(node):
        kafka_home = kafka.path.home(node)
        storage_test_libs = os.path.join(kafka_home, "storage", "build", "libs", "*-test.jar")
        prefix = (
            "export INCLUDE_TEST_JARS=true; "
            "export CLASSPATH=\"$(echo %s 2>/dev/null | tr ' ' ':')${CLASSPATH:+:$CLASSPATH}\"; "
        ) % (storage_test_libs,)
        return prefix + original_start_cmd(node)

    kafka.start_cmd = _patched_start_cmd


class InklessClassicToDisklessSwitchTest(Test):
    """System tests for classic-to-diskless topic switch.

    Each test belongs to one of these categories:

    - Category A covers post-switch data availability.
    - Category B covers mid-switch fault tolerance, with faults injected during the switch.
    - Category C covers operational scenarios, such as a concurrent switch.
    - Category D covers faults between the seal commit and phase 2.

    Brokers consolidate by default, as in production. Every switch runs both
    phases. The completion wait requires phase 2, and each data check
    compares every consumed record with the records that the producers saw
    acknowledged.
    """

    SWITCH_TIMEOUT_SEC = 120
    # Phase 2 waits for the remote log manager to copy the classic prefix. Under the
    # degraded network, that copy shares the throttled link with replication.
    PHASE_2_TIMEOUT_SEC = 300
    PRODUCE_TIMEOUT_SEC = 120
    CONSUME_TIMEOUT_SEC = 120
    BROKER_STARTUP_TIMEOUT_SEC = 120

    # Minimum idle window after which kafka-console-consumer self-terminates when
    # no new record arrives. For exact-count reads this doubles as the "end of
    # stream" signal, so it must be comfortably longer than the worst-case
    # latency for a freshly-committed diskless batch to become readable from the
    # leader.
    # Otherwise a transiently-slow diskless tail (a record that is already acked
    # and committed but not yet served) looks like end-of-stream and the read
    # stops short, producing a false data-loss failure. The wait loop returns as
    # soon as the expected count is reached, so a generous value here costs
    # nothing on the happy path. Exact-count reads raise this to at least the
    # enclosing wait timeout so the wait loop owns the overall deadline.
    CONSUME_COMPLETION_IDLE_SEC = 60

    def __init__(self, test_context: TestContext) -> None:
        super(InklessClassicToDisklessSwitchTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.topic = "switch-test-topic"
        self.num_partitions = 6
        self.replication_factor = 3
        self._switched_topics = set()
        self._consolidation = False
        # topic -> {(partition, offset): (key, value)} for every acknowledged record.
        self._acked_records = {}
        # topic -> {partition: committed seal offset}, filled by the phase 2 wait.
        self._seals = {}
        self.verifier = None

    def teardown(self):
        # A failed assertion must not leave a broker cut off from a dependency.
        if self.verifier is not None:
            self.verifier.heal_all()

    # -----------------------------------------------------------------------
    # Cluster setup
    # -----------------------------------------------------------------------

    # Per-state JMX gauges that the Category B tests poll on every
    # broker to confirm that the injected fault actually overlapped a mid-
    # switch state (rather than landing after switch had already
    # completed). Each entry maps a short state name to ``(count_object_name,
    # oldest_age_ms_object_name)``: the count drives the assertion, the
    # oldest-age gauge is reported alongside as diagnostic context (so a
    # failure log shows e.g. ``max_count=2 max_age_ms=45123`` instead of just
    # ``max_count=2``).
    _IDLM_OBJ = "kafka.server:type=InitDisklessLogManager,name=%s"
    SWITCH_STATE_GAUGES = {
        "WaitingForReplication": (
            _IDLM_OBJ % "WaitingForReplicationPartitions",
            _IDLM_OBJ % "OldestWaitingForReplicationAgeMs",
        ),
        "SendingToController": (
            _IDLM_OBJ % "SendingToControllerPartitions",
            _IDLM_OBJ % "OldestSendingToControllerAgeMs",
        ),
        "AwaitingMetadata": (
            _IDLM_OBJ % "AwaitingMetadataPartitions",
            _IDLM_OBJ % "OldestAwaitingMetadataAgeMs",
        ),
        "WaitingForUpload": (
            _IDLM_OBJ % "WaitingForUploadPartitions",
            _IDLM_OBJ % "OldestWaitingForUploadAgeMs",
        ),
        "SendingFlagToController": (
            _IDLM_OBJ % "SendingFlagToControllerPartitions",
            _IDLM_OBJ % "OldestSendingFlagToControllerAgeMs",
        ),
    }
    SEALED_LEADER_PARTITIONS_JMX_OBJECT = "kafka.server:type=ReplicaManager,name=SealedPartitionsCount"
    UNDER_REPLICATED_PARTITIONS_JMX_OBJECT = "kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions"
    INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT = _IDLM_OBJ % "InFlightPartitions"
    SWITCH_COMPLETION_JMX_OBJECT_NAMES = [
        SEALED_LEADER_PARTITIONS_JMX_OBJECT,
        UNDER_REPLICATED_PARTITIONS_JMX_OBJECT,
        INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT,
    ]
    SWITCH_STATE_JMX_OBJECT_NAMES = [obj for pair in SWITCH_STATE_GAUGES.values() for obj in pair]
    SWITCH_JMX_OBJECT_NAMES = SWITCH_COMPLETION_JMX_OBJECT_NAMES + SWITCH_STATE_JMX_OBJECT_NAMES
    SWITCH_JMX_ATTRIBUTES = ["Value"]

    def _create_kafka(self, num_nodes=None, controller_num_nodes=1,
                      scrape_switch_state_jmx=False, consolidation=True):
        """Create the switch test cluster.

        With ``consolidation``, brokers upload the classic prefix of a switched
        partition to tiered storage on MinIO. The controller then sets
        ``classicPrefixInRemoteStorage``, which starts phase 2. Production
        brokers consolidate, so this is the default.
        """
        if num_nodes is None:
            num_nodes = self.num_brokers
        self._switched_topics = set()
        self._consolidation = consolidation
        jmx_object_names = self.SWITCH_JMX_OBJECT_NAMES if scrape_switch_state_jmx else \
            self.SWITCH_COMPLETION_JMX_OBJECT_NAMES
        server_prop_overrides = [
            ["diskless.managed.rf.enable", "true"],
            # `_degrade_network()` also throttles the broker's path to object storage, so a WAL
            # upload outlasts the 2 s default S3 call timeout. The producer then retries the
            # failed produce, which duplicates records and stalls production.
            ["inkless.storage.s3.api.call.timeout", "60000"],
            ["inkless.storage.s3.api.call.attempt.timeout", "30000"],
        ]
        if consolidation:
            # The consolidation template makes new topics diskless by default. The switch
            # tests create classic topics, so keep the classic default.
            server_prop_overrides.append(["log.diskless.enable", "false"])
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=num_nodes,
            zk=None,
            controller_num_nodes_override=controller_num_nodes,
            server_prop_overrides=server_prop_overrides,
            jmx_object_names=jmx_object_names,
            jmx_attributes=self.SWITCH_JMX_ATTRIBUTES,
            consolidation=consolidation,
        )
        # Switch configs (diskless.allow.from.classic.enable) require
        # diskless.remote.storage.consolidation.enable, which requires
        # remote.log.storage.system.enable, which needs RSM class names.
        # The NoOp classes are test-only and absent from the broker runtime
        # classpath, but the controller never instantiates RemoteLogManager
        # so referencing them there is safe. We must copy the list first
        # because both KafkaService instances share the same list object.
        controller_only_overrides = [
            ["diskless.remote.storage.consolidation.enable", "true"],
            ["diskless.allow.from.classic.enable", "true"],
            ["remote.log.storage.system.enable", "true"],
            ["remote.log.storage.manager.class.name",
             "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager"],
            ["remote.log.metadata.manager.class.name",
             "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager"],
        ]
        if hasattr(self.kafka, 'isolated_controller_quorum') and self.kafka.isolated_controller_quorum:
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + controller_only_overrides
            # KafkaService forwards jmx_object_names/jmx_attributes to the
            # isolated controller quorum too, but our broker-side MBeans
            # (ReplicaManager, InitDisklessLogManager) don't exist on a
            # controller-only node. Disable JMX scraping there so JmxTool
            # doesn't block start_jmx_tool() waiting for MBeans that never
            # show up.
            ctrl.jmx_object_names = None
            ctrl.jmx_attributes = []

        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

    def _create_classic_topic(self, topic=None, num_partitions=None,
                              replica_assignment=None, configs=None):
        if topic is None:
            topic = self.topic
        if num_partitions is None:
            num_partitions = self.num_partitions
        topic_cfg = {
            "topic": topic,
            "configs": {
                "min.insync.replicas": 2,
                "diskless.enable": "false",
            }
        }
        if replica_assignment is None:
            topic_cfg["partitions"] = num_partitions
            topic_cfg["replication-factor"] = self.replication_factor
        else:
            topic_cfg["replica-assignment"] = replica_assignment
        if configs:
            topic_cfg["configs"].update(configs)
        self.kafka.create_topic(topic_cfg)

    def _create_kafka_with_tiered_storage(self):
        """Single-broker cluster with real (LocalTieredStorage) tiered storage
        on the broker, plus the diskless-from-classic switch bridge.

        Single broker keeps the test self-contained: LocalTieredStorage uses
        a per-broker filesystem path, so multi-broker reads from remote would
        require shared storage or sticky leadership - neither is needed to
        exercise the classic-tiered to diskless switch path.
        """
        self.replication_factor = 1
        self.num_partitions = 1
        self._switched_topics = set()
        self._consolidation = True

        storage_dir = os.path.join(KafkaService.PERSISTENT_ROOT, "kafka-tiered-storage")

        common_overrides = [
            ["diskless.managed.rf.enable", "true"],
            ["remote.log.storage.system.enable", "true"],
            ["diskless.remote.storage.consolidation.enable", "true"],
            ["diskless.allow.from.classic.enable", "true"],
            ["remote.log.metadata.manager.class.name",
             "org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManager"],
            ["remote.log.metadata.manager.listener.name", "PLAINTEXT"],
            ["rlmm.config.remote.log.metadata.topic.replication.factor", "1"],
            ["rlmm.config.remote.log.metadata.topic.num.partitions", "1"],
            ["remote.log.manager.task.interval.ms", "1000"],
            ["log.retention.check.interval.ms", "1000"],
        ]

        broker_overrides = list(common_overrides) + [
            ["remote.log.storage.manager.class.name",
             "org.apache.kafka.server.log.remote.storage.LocalTieredStorage"],
            ["rsm.config.dir", storage_dir],
            ["rsm.config.delete.on.close", "true"],
        ]

        # Controllers only validate the class names; using NoOp avoids needing
        # LocalTieredStorage on the controller classpath.
        controller_overrides = list(common_overrides) + [
            ["remote.log.storage.manager.class.name",
             "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager"],
        ]

        self.kafka = KafkaService(
            self.test_context,
            num_nodes=1,
            zk=None,
            controller_num_nodes_override=1,
            server_prop_overrides=broker_overrides,
            jmx_object_names=self.SWITCH_COMPLETION_JMX_OBJECT_NAMES,
            jmx_attributes=self.SWITCH_JMX_ATTRIBUTES,
        )
        _enable_tiered_storage_classpath(self.kafka)
        if hasattr(self.kafka, 'isolated_controller_quorum') and self.kafka.isolated_controller_quorum:
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + controller_overrides
            ctrl.jmx_object_names = None
            ctrl.jmx_attributes = []

        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

    def _create_classic_tiered_topic(self, topic, num_partitions=1):
        """Classic topic with tiered storage enabled and very short local
        retention so closed segments are uploaded to remote and then
        deleted from the local log directory.
        ``segment.bytes`` has a hard floor of 1 MiB (LogConfig.validate),
        so we additionally force time-based rolling via ``segment.ms`` to
        produce closed segments quickly without needing a huge produce
        volume.
        """
        self.kafka.create_topic({
            "topic": topic,
            "partitions": num_partitions,
            "replication-factor": 1,
            "configs": {
                "remote.storage.enable": "true",
                "min.insync.replicas": 1,
                "segment.bytes": 1048576,
                # Force a roll every 2s regardless of segment fill, so that
                # produced records flow through closed segments quickly.
                "segment.ms": 2000,
                # Delete local segments ~immediately after the upload to remote.
                "local.retention.ms": 1000,
                # Long total retention so remote data remains during the test.
                "retention.ms": 3600000,
                "file.delete.delay.ms": 1000,
            }
        })

    # -----------------------------------------------------------------------
    # Helpers: switch
    # -----------------------------------------------------------------------

    def _switch_topic_to_diskless(self, topic=None, node=None, extra_configs=None):
        """Run the switch alter from ``node``, or from the first broker if unset.

        While a network partition is active, pass a node outside the isolated
        side: an admin client on the isolated broker can't reach its peers and
        times out describing the topic config.

        ``extra_configs`` go into the same alter as ``diskless.enable``. A switch
        to a consolidating topic needs ``remote.storage.enable`` in the same
        alter. The helper adds it when the brokers consolidate.
        """
        if topic is None:
            topic = self.topic
        self.logger.info("Switching topic %s to diskless", topic)
        self._switched_topics.add(topic)
        extra_configs = dict(extra_configs or {})
        if self._consolidation:
            extra_configs.setdefault("remote.storage.enable", "true")
        if extra_configs:
            configs = dict(extra_configs)
            configs["diskless.enable"] = "true"
            self.kafka.alter_topic_configs(topic, configs, node=node)
        else:
            self.kafka.alter_topic_config(topic, "diskless.enable", "true", node=node)

    def _wait_for_switch_config(self, topic=None, timeout_sec=None):
        """Wait until kafka-configs reports diskless.enable=true for the topic."""
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.SWITCH_TIMEOUT_SEC

        def check():
            try:
                config = self.kafka.describe_topic_config(topic)
                return config.get("diskless.enable") == "true"
            except Exception:
                return False

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Topic %s did not become diskless within %ds" % (topic, timeout_sec))

    def _wait_for_switch_complete(self, topic=None, timeout_sec=None, phase_2_timeout_sec=None):
        """Wait until the switch of ``topic`` finishes.

        The topic config becomes visible before leaders finish sealing their
        classic logs and before InitDisklessLogManager commits the diskless
        start offsets. Poll broker-side JMX so post-switch produce/read
        steps do not race the switch-pending state.

        ``InFlightPartitions`` counts only the steps up to control-plane init,
        so it reaches zero in phase 1. If the brokers consolidate, the wait
        requires phase 2 on every partition of ``topic`` instead. The leader
        requests the flag only after control-plane init, so phase 2 implies
        that phase 1 finished. The sealed-leader count doesn't work there: a
        leader that takes over a consolidating partition in phase 2, for
        example after a restart, doesn't seal it.
        """
        if self._consolidation:
            self._wait_for_switch_config(topic, timeout_sec)
            self._wait_for_classic_prefix_in_remote_storage(
                topic,
                timeout_sec=phase_2_timeout_sec or self.PHASE_2_TIMEOUT_SEC,
            )
        else:
            self._wait_for_phase_1_complete(topic, timeout_sec)

    def _wait_for_phase_1_complete(self, topic=None, timeout_sec=None):
        """Wait until leaders sealed every switched partition and the init work drained."""
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.SWITCH_TIMEOUT_SEC

        self._wait_for_switch_config(topic, timeout_sec)
        expected_sealed_leader_count = 0
        for switched_topic in self._switched_topics:
            description = self.kafka.describe_topic(switched_topic)
            expected_sealed_leader_count += len(self.kafka.parse_describe_topic(description)["partitions"])

        def check():
            try:
                self.kafka.read_jmx_output_all_nodes()
                sealed_key = "%s:Value" % self.SEALED_LEADER_PARTITIONS_JMX_OBJECT
                in_flight_key = "%s:Value" % self.INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT
                sealed_leader_count = 0.0
                in_flight_init_count = 0.0
                for time_to_stats in self.kafka.jmx_stats:
                    if time_to_stats:
                        latest = max(time_to_stats.keys())
                        sealed_leader_count += time_to_stats[latest].get(sealed_key, 0)
                        in_flight_init_count += time_to_stats[latest].get(in_flight_key, 0)
                self.logger.info(
                    "Switch state for %s: sealed leader partitions=%s/%s, in-flight init partitions=%s",
                    topic,
                    int(sealed_leader_count),
                    int(expected_sealed_leader_count),
                    int(in_flight_init_count)
                )
                return sealed_leader_count >= expected_sealed_leader_count and in_flight_init_count == 0
            except Exception as e:
                self.logger.debug("Failed to read switch JMX state for %s: %s", topic, e)
                return False

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=5,
            err_msg=("Topic %s did not finish classic-to-diskless switch within %ds "
                     "(expected at least %d sealed leader partitions and zero in-flight init partitions)" %
                     (topic, timeout_sec, expected_sealed_leader_count))
        )

    def _switch_state(self, topic=None, node=None):
        """Returns the switch state of each partition, read with ``kafka-topic-switch.sh state``.

        Maps each partition index to a dict with the committed seal offset
        (``seal``, negative before the controller commits it) and the
        ``classicPrefixInRemoteStorage`` flag (``classic_prefix_in_remote_storage``).
        """
        if topic is None:
            topic = self.topic
        if node is None:
            node = self.kafka.nodes[0]
        cmd = "%s state --bootstrap-server %s --topic %s" % (
            self.kafka.path.script("kafka-topic-switch.sh", node),
            self.kafka.bootstrap_servers(),
            topic,
        )
        states = {}
        partition = None
        for line in node.account.ssh_capture(cmd, combine_stderr=False):
            line = line.decode("utf-8") if isinstance(line, bytes) else line
            line = line.strip()
            if line.startswith("Partition ") and line.endswith(":"):
                partition = int(line[len("Partition "):-1])
                states[partition] = {}
            elif partition is None:
                continue
            elif line.startswith("classicToDisklessStartOffset="):
                states[partition]["seal"] = int(line.split("=", 1)[1].split()[0])
            elif line.startswith("classicPrefixInRemoteStorage="):
                states[partition]["classic_prefix_in_remote_storage"] = \
                    line.split("=", 1)[1].startswith("true")
        return states

    def _wait_for_classic_prefix_in_remote_storage(self, topic=None, timeout_sec=None):
        """Wait until every partition of the topic is in phase 2 of the switch.

        ``_wait_for_switch_complete`` returns once the controller commits the
        seal, in phase 1. Phase 2 starts when the controller sets
        ``classicPrefixInRemoteStorage``, after the leader uploads the classic
        prefix to remote storage. Returns the committed seal offset of each
        partition.
        """
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.SWITCH_TIMEOUT_SEC
        last_states = {}

        def check():
            try:
                states = self._switch_state(topic)
            except (Exception, RemoteCommandError) as e:
                self.logger.debug("Failed to read the switch state of %s: %s", topic, e)
                return False
            last_states.clear()
            last_states.update(states)
            self.logger.info("Switch state for %s: %s", topic, states)
            return bool(states) and all(
                state.get("seal", -1) >= 0 and state.get("classic_prefix_in_remote_storage", False)
                for state in states.values()
            )

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=lambda: "Topic %s did not reach phase 2 of the switch within %ds (last state: %s)" %
                    (topic, timeout_sec, last_states)
        )
        seals = {partition: state["seal"] for partition, state in last_states.items()}
        self._seals[topic] = seals
        return seals

    def _assert_in_phase_1(self, topic=None, node=None):
        """Assert that the controller committed the seal of every partition and didn't set the flag.

        Category D tests call this after the fault, to show that the fault
        landed after the seal commit and before phase 2.
        """
        if topic is None:
            topic = self.topic
        states = self._switch_state(topic, node=node)
        assert states, "kafka-topic-switch.sh state returned no partitions for %s" % topic
        not_sealed = sorted(p for p, s in states.items() if s.get("seal", -1) < 0)
        in_phase_2 = sorted(p for p, s in states.items() if s.get("classic_prefix_in_remote_storage", False))
        assert not not_sealed and not in_phase_2, \
            ("Expected every partition of %s in phase 1 with a committed seal, but partitions %s "
             "have no seal and partitions %s are in phase 2 (state: %s)" %
             (topic, not_sealed, in_phase_2, states))
        return {partition: state["seal"] for partition, state in states.items()}

    def _wait_for_seal_committed(self, topic=None, timeout_sec=None):
        """Wait until the controller commits the seal of every partition of ``topic``."""
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.SWITCH_TIMEOUT_SEC

        def check():
            try:
                states = self._switch_state(topic)
            except (Exception, RemoteCommandError) as e:
                self.logger.debug("Failed to read the switch state of %s: %s", topic, e)
                return False
            self.logger.info("Switch state for %s: %s", topic, states)
            return bool(states) and all(s.get("seal", -1) >= 0 for s in states.values())

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="The controller did not commit the seal of every partition of %s within %ds" %
                           (topic, timeout_sec))

    def _live_cluster_jmx_sum(self, obj_name):
        """Read and sum one JMX gauge across live broker nodes.

        Returns None unless every live broker reported the gauge. Leader-owned
        gauges such as UnderReplicatedPartitions cannot be treated as zero when
        the leader's scrape is missing.
        """
        key = "%s:Value" % obj_name
        total = 0.0
        observed_nodes = 0
        live_nodes = [node for node in self.kafka.nodes if self.kafka.pids(node)]
        if not live_nodes:
            return None
        for node in live_nodes:
            idx = self.kafka.idx(node)
            try:
                self.kafka.read_jmx_output(idx, node)
            except Exception as e:
                self.logger.debug("Failed to read JMX from live broker %s: %s",
                                  node.account.hostname, e)
                continue
            if idx - 1 >= len(self.kafka.jmx_stats):
                continue
            time_to_stats = self.kafka.jmx_stats[idx - 1]
            if time_to_stats:
                latest = max(time_to_stats.keys())
                latest_stats = time_to_stats[latest]
                if key not in latest_stats:
                    continue
                total += latest_stats[key]
                observed_nodes += 1
        return int(total) if observed_nodes == len(live_nodes) else None

    def _wait_for_under_replicated_partitions(self, expected_count, timeout_sec=120):
        def check():
            count = self._live_cluster_jmx_sum(self.UNDER_REPLICATED_PARTITIONS_JMX_OBJECT)
            self.logger.info("Cluster UnderReplicatedPartitions=%s, expected=%d",
                             count, expected_count)
            return count is not None and count == expected_count

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="UnderReplicatedPartitions did not become %d within %ds" %
                    (expected_count, timeout_sec)
        )

    def _wait_for_under_replicated_partitions_at_least(self, min_count, timeout_sec=120):
        def check():
            count = self._live_cluster_jmx_sum(self.UNDER_REPLICATED_PARTITIONS_JMX_OBJECT)
            self.logger.info("Cluster UnderReplicatedPartitions=%s, expected_at_least=%d",
                             count, min_count)
            return count is not None and count >= min_count

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="UnderReplicatedPartitions did not reach at least %d within %ds" %
                    (min_count, timeout_sec)
        )

    # -----------------------------------------------------------------------
    # Helpers: produce / consume
    # -----------------------------------------------------------------------

    def _start_producer(self, topic=None, num_nodes=1, max_messages=-1, enable_idempotence=True,
                        throughput=10000):
        if topic is None:
            topic = self.topic
        producer = VerifiableProducer(
            context=self.test_context,
            num_nodes=num_nodes,
            kafka=self.kafka,
            topic=topic,
            max_messages=max_messages,
            throughput=throughput,
            message_validator=is_int_with_prefix,
            enable_idempotence=enable_idempotence,
            repeating_keys=max_messages if max_messages > 0 else 10000,
        )
        producer.start()
        return producer

    def _produce_messages(self, topic=None, num_messages=10000):
        """Produce a fixed number of messages and wait for acks."""
        if topic is None:
            topic = self.topic
        producer = self._start_producer(topic=topic, max_messages=num_messages)
        wait_until(
            lambda: producer.num_acked + producer.num_not_acked >= num_messages or producer.worker_errors,
            timeout_sec=self.PRODUCE_TIMEOUT_SEC,
            err_msg="Producer failed to produce %d messages in %ds" % (num_messages, self.PRODUCE_TIMEOUT_SEC)
        )
        return self._stop_producer(producer)

    def _stop_producer(self, producer):
        """Stop ``producer``, check that it didn't see errors, and record what it acked.

        Returns the number of acknowledged records.
        """
        producer.stop()
        assert not producer.worker_errors, "Unexpected producer errors: %s" % producer.worker_errors
        assert producer.num_not_acked == 0, \
            "Producer failed to send %d records to %s, for example %s" % \
            (producer.num_not_acked, producer.topic, producer.not_acked[:5])
        self._record_acked(producer)
        acked = producer.num_acked
        producer.free()
        return acked

    def _record_acked(self, producer):
        """Add every record that ``producer`` saw acknowledged to the ledger of its topic.

        If an offset gets a second ack, the broker assigned that offset twice.
        The check rejects the second ack even when its content matches,
        because each producer run restarts the same key and value sequence.
        """
        ledger = self._acked_records.setdefault(producer.topic, {})
        for data in producer.acked_values_full:
            position = (int(data["partition"]), int(data["offset"]))
            record = (str(data.get("key")), str(data["value"]))
            previous = ledger.get(position)
            assert previous is None, \
                "Offset %d of %s-%d was acked twice, for %s and then %s" % \
                (position[1], producer.topic, position[0], previous, record)
            ledger[position] = record

    def _forget_acked(self, topic):
        self._acked_records.pop(topic, None)
        self._seals.pop(topic, None)

    def _assert_all_acked_records_readable(self, topic=None, context="", timeout_sec=None):
        """Read ``topic`` from the beginning and compare it with the acked records.

        Fails if a record is missing, duplicated, out of order, or differs in
        key or value, or if the topic contains a record that no producer saw
        acked. If the phase 2 wait recorded the seal, each problem states its
        side of the seal.
        """
        if topic is None:
            topic = self.topic
        expected = self._acked_records.get(topic, {})
        assert expected, "No acked records were recorded for %s" % topic
        records, unparsed = self._consume_records_from_beginning(
            len(expected), topic=topic, timeout_sec=timeout_sec)
        problems = self._diff_records(topic, expected, records)
        if unparsed:
            problems.append("%d unparsed consumer lines, for example %s" % (len(unparsed), unparsed[:3]))
        assert not problems, \
            "Topic %s %s: consumed %d records, expected %d. %s" % \
            (topic, context, len(records), len(expected), "; ".join(problems))
        self.logger.info("Topic %s %s: all %d acked records read in order", topic, context, len(expected))

    def _diff_records(self, topic, expected, records):
        """Returns a list of problems found comparing consumed ``records`` with ``expected``."""
        seals = self._seals.get(topic, {})

        def where(partition, offset):
            seal = seals.get(partition)
            if seal is None:
                return "%d@%d" % (partition, offset)
            return "%d@%d (%s seal %d)" % (partition, offset, "below" if offset < seal else "at or above", seal)

        missing, duplicated, out_of_order, unexpected, mismatched = [], [], [], [], []
        last_offset = {}
        seen = set()
        for partition, offset, key, value in records:
            position = (partition, offset)
            if position in seen:
                duplicated.append(where(partition, offset))
            elif partition in last_offset and offset < last_offset[partition]:
                out_of_order.append(where(partition, offset))
            seen.add(position)
            last_offset[partition] = max(offset, last_offset.get(partition, offset))
            acked = expected.get(position)
            if acked is None:
                unexpected.append(where(partition, offset))
            elif acked != (key, value):
                mismatched.append("%s acked %s but read %s" % (where(partition, offset), acked, (key, value)))
        for partition, offset in sorted(expected):
            if (partition, offset) not in seen:
                missing.append(where(partition, offset))

        problems = []
        for name, items in (("missing", missing), ("duplicated", duplicated), ("out of order", out_of_order),
                            ("never acked", unexpected), ("different content", mismatched)):
            if items:
                problems.append("%d %s, for example %s" % (len(items), name, items[:10]))
        return problems

    # The console consumer prints each record as partition, offset, key, and value.
    CONSUMED_RECORD_PATTERN = re.compile(r"^Partition:(\d+)\tOffset:(\d+)\t(.*)\t(.*)$")

    @staticmethod
    def _print_offsets(consumer):
        """Make ``consumer`` print each record's offset.

        ``ConsoleConsumer`` has no option for ``print.offset``. The helper
        patches the start command on the instance, as
        ``_enable_tiered_storage_classpath`` does.
        """
        original_start_cmd = consumer.start_cmd

        def _patched_start_cmd(node):
            cmd = original_start_cmd(node)
            return cmd.replace(" --formatter ", " --formatter-property print.offset=true --formatter ", 1)

        consumer.start_cmd = _patched_start_cmd

    def _consume_records_from_beginning(self, expected_count, topic=None, timeout_sec=None):
        """Read ``topic`` from the beginning with a fresh consumer.

        Returns ``(records, unparsed_lines)``. Each record is a
        ``(partition, offset, key, value)`` tuple in the order the consumer
        received it. The read stops as described in ``_consume_all_from_beginning``
        with ``wait_for_completion``.
        """
        consumer = self._consume_all_from_beginning(
            expected_count, topic=topic, timeout_sec=timeout_sec,
            wait_for_completion=True, print_offsets=True, return_consumer=True)
        records = []
        unparsed = []
        for line in consumer.messages_consumed[1]:
            match = self.CONSUMED_RECORD_PATTERN.match(line)
            if match:
                records.append((int(match.group(1)), int(match.group(2)), match.group(3), match.group(4)))
            else:
                unparsed.append(line)
        consumer.free()
        return records, unparsed

    def _consume_all_from_beginning(self, expected_count, topic=None, timeout_sec=None,
                                    wait_for_completion=False, print_offsets=False,
                                    return_consumer=False):
        """Start a fresh consumer from the beginning and collect all messages.

        Returns the number of messages consumed. With ``return_consumer``, it
        returns the stopped consumer instead. Set ``wait_for_completion`` when
        the caller needs an exact count rather than stopping as soon as the
        consumer reads the expected minimum.

        Completion is decided by record count, not by the console consumer's
        idle timeout. Minimum-count callers return as soon as ``expected_count``
        records have been delivered. Exact-count callers wait one additional
        poll after first seeing exactly ``expected_count`` records, so any
        immediately-readable duplicate or extra record is observed; if the count
        ever exceeds ``expected_count``, the wait returns immediately and the
        caller's exact-count assertion reports the over-count. Only if the
        consumer drains and exits on its own *before* reaching the expected
        count do we stop early and report the shortfall, which is the genuine
        data-loss signal. The console consumer's idle timeout is set generously
        (with ``CONSUME_COMPLETION_IDLE_SEC`` as a floor) so that a
        transiently-slow diskless tail does not look like end-of-stream and
        truncate the read before the wait deadline.
        """
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.CONSUME_TIMEOUT_SEC

        wait_backoff_sec = 2
        # For an exact-count read, keep the consumer alive across a slow diskless
        # tail until the outer wait_until controls the deadline. A short idle
        # timeout is fine when the caller only wants a minimum.
        if wait_for_completion:
            consumer_idle_sec = max(self.CONSUME_COMPLETION_IDLE_SEC, timeout_sec) + wait_backoff_sec
        else:
            consumer_idle_sec = 30
        consumer_idle_ms = int(consumer_idle_sec * 1000)

        group_id = "fresh-%s" % str(uuid.uuid4())[:8]
        consumer = ConsoleConsumer(
            context=self.test_context,
            num_nodes=1,
            kafka=self.kafka,
            topic=topic,
            group_id=group_id,
            from_beginning=True,
            consumer_timeout_ms=consumer_idle_ms,
            isolation_level="read_committed",
            print_key=True,
            print_partition=print_offsets,
        )
        if print_offsets:
            self._print_offsets(consumer)
        consumer.start()

        consumer_seen_alive = [False]
        expected_count_seen = [False]

        def _check_consumed():
            is_alive = consumer.alive(consumer.nodes[0])
            if is_alive:
                consumer_seen_alive[0] = True
            consumed = len(consumer.messages_consumed[1])
            if wait_for_completion:
                # Surface duplicates promptly, but confirm the exact count once
                # more so stopping the consumer does not hide already-readable
                # extra records.
                if consumed > expected_count:
                    return True
                if consumed == expected_count:
                    if expected_count_seen[0]:
                        return True
                    expected_count_seen[0] = True
                    return False
            elif consumed >= expected_count:
                return True
            # The consumer drained and exited on its own short of the expected
            # count: stop waiting so the caller sees the shortfall (genuine data
            # loss) instead of blocking until timeout_sec.
            return consumer_seen_alive[0] and not is_alive

        wait_until(
            _check_consumed,
            timeout_sec=timeout_sec,
            backoff_sec=wait_backoff_sec,
            err_msg=lambda: "Fresh consumer consumed only %d out of %d expected messages in %ds" %
                    (len(consumer.messages_consumed[1]), expected_count, timeout_sec)
        )

        consumer.stop()
        consumed = len(consumer.messages_consumed[1])

        expected_relation = "==" if wait_for_completion else ">="
        self.logger.info("Fresh consumer consumed %d messages from topic %s (expected %s %d)",
                         consumed, topic, expected_relation, expected_count)
        if return_consumer:
            return consumer
        consumer.free()
        return consumed

    def _earliest_local_offset(self, topic, partition=0):
        """Return the topic-partition's earliest-local offset via
        kafka-get-offsets.sh --time -4 (i.e. OffsetSpec.earliestLocal()).

        For a tiered topic this advances past 0 once segments have been
        uploaded to remote and deleted from the broker's local log dir.
        Returns -1 if the offset cannot be parsed yet."""
        node = self.kafka.nodes[0]
        cmd = "%s --bootstrap-server %s --topic %s --partitions %d --time -4" % (
            self.kafka.path.script("kafka-get-offsets.sh", node),
            self.kafka.bootstrap_servers(),
            topic,
            partition,
        )
        try:
            output = node.account.ssh_capture(cmd, allow_fail=True)
            for line in output:
                line = line.decode("utf-8") if isinstance(line, bytes) else line
                parts = line.strip().split(":")
                if len(parts) == 3 and parts[0] == topic and parts[1] == str(partition):
                    return int(parts[2])
        except Exception as e:
            self.logger.warn("Failed to read earliest-local offset for %s-%d: %s",
                             topic, partition, str(e))
        return -1

    def _wait_for_local_log_truncation(self, topic, partition=0, timeout_sec=120):
        """Wait until the topic's earliest-local offset advances past 0,
        which proves that some segments have been tiered to remote AND
        deleted locally (so reads from offset 0 must come from remote)."""
        def check():
            offset = self._earliest_local_offset(topic, partition)
            self.logger.info("Topic %s-%d earliest-local offset: %d",
                             topic, partition, offset)
            return offset > 0

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=("earliest-local offset for %s-%d did not advance past 0 within %ds; "
                     "tiering or local retention is not progressing") %
                    (topic, partition, timeout_sec)
        )

    def _wait_for_steady_production(self, producer, min_acked=5000, timeout_sec=60,
                                    err_msg=None, worker_err_msg=None):
        wait_until(
            lambda: producer.num_acked >= min_acked or producer.worker_errors,
            timeout_sec=timeout_sec,
            err_msg=err_msg or "Producer did not reach %d acks in %ds" % (min_acked, timeout_sec)
        )
        if worker_err_msg is None:
            worker_err_msg = "Unexpected producer errors: %s"
        assert not producer.worker_errors, worker_err_msg % producer.worker_errors

    # -----------------------------------------------------------------------
    # Helpers: broker operations
    # -----------------------------------------------------------------------

    def _get_leader_node(self, topic=None, partition=0):
        if topic is None:
            topic = self.topic
        leader = self.kafka.leader(topic, partition=partition)
        return leader

    def _get_follower_nodes(self, topic=None, partition=0):
        if topic is None:
            topic = self.topic
        leader = self._get_leader_node(topic, partition)
        replicas = self.kafka.replicas(topic, partition)
        return [r for r in replicas if r != leader]

    def _wait_for_partition_leader(self, expected_node, topic=None, partition=0, timeout_sec=120):
        if topic is None:
            topic = self.topic
        expected_broker_id = self.kafka.idx(expected_node)

        def leader_is_expected():
            try:
                return self.kafka.idx(self._get_leader_node(topic, partition)) == expected_broker_id
            except (Exception, RemoteCommandError):
                return False

        wait_until(
            leader_is_expected,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="Partition %s-%d did not move to expected leader broker %d within %ds" %
                    (topic, partition, expected_broker_id, timeout_sec)
        )

    def _partition_log_dir(self, node, topic=None, partition=0):
        if topic is None:
            topic = self.topic
        partition_dir = "%s-%d" % (topic, partition)
        cmd = "for d in %s-*; do if [ -d \"$d/%s\" ]; then echo \"$d\"; fi; done" % (
            KafkaService.DATA_LOG_DIR_PREFIX,
            partition_dir,
        )
        log_dirs = []
        for line in node.account.ssh_capture(cmd, allow_fail=True):
            line = line.decode("utf-8") if isinstance(line, bytes) else line
            stripped = line.strip()
            if stripped:
                log_dirs.append(stripped)
        assert len(log_dirs) == 1, \
            "Expected exactly one log dir for %s on %s, found %s" % \
            (partition_dir, node.account.hostname, log_dirs)
        return log_dirs[0]

    def _set_replication_checkpoint_offset(self, node, topic=None, partition=0, offset=0):
        """Rewrite the replica HW checkpoint for one topic-partition on a stopped broker.

        This is only for fault injection: it creates a replica that has the full
        local log on disk but reloads with a stale cached high-watermark.
        """
        if topic is None:
            topic = self.topic
        checkpoint = os.path.join(
            self._partition_log_dir(node, topic, partition),
            "replication-offset-checkpoint",
        )

        raw_checkpoint = ""
        for line in node.account.ssh_capture("cat %s" % checkpoint):
            line = line.decode("utf-8") if isinstance(line, bytes) else line
            raw_checkpoint += line

        lines = [line.strip() for line in raw_checkpoint.splitlines() if line.strip()]
        assert len(lines) >= 2, "Malformed checkpoint %s on %s: %r" % \
            (checkpoint, node.account.hostname, raw_checkpoint)

        version = lines[0]
        expected_entries = int(lines[1])
        entry_lines = lines[2:]
        assert expected_entries == len(entry_lines), \
            "Malformed checkpoint %s on %s: expected %d entries but found %d" % \
            (checkpoint, node.account.hostname, expected_entries, len(entry_lines))

        entries = []
        updated = False
        for entry_line in entry_lines:
            parts = entry_line.split()
            assert len(parts) == 3, "Malformed checkpoint entry in %s: %r" % \
                (checkpoint, entry_line)
            entry_topic, entry_partition, entry_offset = parts
            entry_partition = int(entry_partition)
            if entry_topic == topic and entry_partition == partition:
                entry_offset = offset
                updated = True
            else:
                entry_offset = int(entry_offset)
            entries.append((entry_topic, entry_partition, entry_offset))

        if not updated:
            entries.append((topic, partition, offset))

        rewritten_checkpoint = "%s\n%d\n%s\n" % (
            version,
            len(entries),
            "\n".join("%s %d %d" % entry for entry in entries),
        )
        self.logger.info("Setting %s-%d replication checkpoint on %s to %d",
                         topic, partition, node.account.hostname, offset)
        node.account.create_file(checkpoint, rewritten_checkpoint)

    def _restart_broker(self, node, clean_shutdown=True):
        self.logger.info("Restarting broker %s (clean=%s)", node.account.hostname, clean_shutdown)
        self.kafka.restart_node(node, clean_shutdown=clean_shutdown, timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)
        self._restart_broker_jmx_tool(node)

    def _stop_broker(self, node, clean_shutdown=True):
        self.logger.info("Stopping broker %s (clean=%s)", node.account.hostname, clean_shutdown)
        self.kafka.stop_node(node, clean_shutdown=clean_shutdown, timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)
        if not clean_shutdown:
            # Unclean shutdown recovery only needs replacement leaders before the test can continue.
            # Waiting for ISR shrinkage here adds replica.lag.time.max.ms latency and does not test
            # the post-crash behavior any more precisely.
            self._wait_for_all_partitions_have_leaders()

    def _start_broker(self, node):
        self.logger.info("Starting broker %s", node.account.hostname)
        self.kafka.start_node(node, timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)
        self._restart_broker_jmx_tool(node)

    def _restart_broker_jmx_tool(self, node):
        """Refresh JmxTool after a broker JVM restart.

        JmxMixin tracks whether it has ever started JmxTool on a node, but a
        broker restart also kills the tool process. Reset only the JMX tool
        state/log for this node so subsequent switch waits read live
        post-restart samples. Before removing the old log, preserve any samples
        not yet scraped into memory so the mid-switch assertions still see
        states observed before the broker bounce.
        """
        if self.kafka.jmx_object_names is None:
            return

        idx = self.kafka.idx(node)
        try:
            self.kafka.read_jmx_output(idx, node)
        except Exception as e:
            self.logger.debug("Could not preserve pre-restart JMX samples from %s: %s",
                              node.account.hostname, e)

        node.account.kill_java_processes(
            self.kafka.jmx_class_name(self.kafka.jmxtool_version(node)),
            clean_shutdown=False,
            allow_fail=True,
        )
        self.kafka.started[idx - 1] = False
        node.account.ssh(
            "rm -f -- %s %s" % (self.kafka.jmx_tool_log, self.kafka.jmx_tool_err_log),
            allow_fail=False,
        )
        self.kafka.start_jmx_tool(idx, node)

    def _rolling_restart(self, clean_shutdown=True, nodes=None):
        if nodes is None:
            nodes = self.kafka.nodes
        self.logger.info("Performing rolling restart of brokers %s", [self.kafka.idx(n) for n in nodes])
        partition_0_replicas = self.kafka.replicas(self.topic, partition=0)
        for node in nodes:
            self._restart_broker(node, clean_shutdown=clean_shutdown)
            self._wait_for_all_partitions_have_leaders()
            # Pace the next bounce on evidence that this broker is fetching again, without
            # forcing all partitions to fully heal between every restart under degraded network.
            # A broker outside the replicas of partition 0 can't join its ISR.
            if node in partition_0_replicas:
                self._wait_for_broker_in_isr(node, num_partitions=1)

    def _wait_for_all_partitions_have_leaders(self, topic=None, num_partitions=None, timeout_sec=120):
        if topic is None:
            topic = self.topic
        if num_partitions is None:
            num_partitions = self.num_partitions

        def all_partitions_have_leaders():
            try:
                query_node = None
                for candidate in self.kafka.nodes:
                    if self.kafka.pids(candidate):
                        query_node = candidate
                        break
                if query_node is None:
                    return False
                # Query once through any live broker; per-partition calls are very slow under
                # the network-degradation faults used by these tests.
                describe_output = self.kafka.describe_topic(topic, node=query_node)
                for p in range(num_partitions):
                    line = self.kafka._describe_topic_line_for_partition(p, describe_output)
                    if line is None:
                        return False
                    if int(line.split()[5]) < 0:
                        return False
                return True
            except (Exception, RemoteCommandError):
                return False

        wait_until(
            all_partitions_have_leaders,
            timeout_sec=timeout_sec, backoff_sec=2,
            err_msg="Not all %d partitions of %s had leaders within %ds" %
                    (num_partitions, topic, timeout_sec)
        )

    def _wait_for_broker_in_isr(self, node, topic=None, num_partitions=None, timeout_sec=120):
        if topic is None:
            topic = self.topic
        if num_partitions is None:
            num_partitions = self.num_partitions
        broker_idx = self.kafka.idx(node)

        def broker_in_isr():
            try:
                query_node = None
                for candidate in self.kafka.nodes:
                    if self.kafka.pids(candidate):
                        query_node = candidate
                        break
                if query_node is None:
                    return False
                for p in range(num_partitions):
                    if broker_idx not in self.kafka.isr_idx_list(topic, partition=p, node=query_node):
                        return False
                return True
            except (Exception, RemoteCommandError):
                return False

        wait_until(
            broker_in_isr,
            timeout_sec=timeout_sec, backoff_sec=2,
            err_msg="Broker %d did not rejoin ISR for all %d partitions of %s within %ds" %
                    (broker_idx, num_partitions, topic, timeout_sec)
        )

    def _wait_for_all_partitions_isr_full(self, topic=None, num_partitions=None, timeout_sec=180):
        """Wait until every partition of the topic has a full ISR.

        After a broker restart, partition leaders re-add the broker to ISR
        independently as each one processes the next follower fetch, so the
        per-partition timings are not identical. Tests that drive leadership
        movement (preferred-replica election) for *all* partitions need every
        partition's ISR healed first.
        """
        if topic is None:
            topic = self.topic
        if num_partitions is None:
            num_partitions = self.num_partitions
        expected_isr_size = self.replication_factor

        def all_isr_full():
            try:
                query_node = None
                for candidate in self.kafka.nodes:
                    if self.kafka.pids(candidate):
                        query_node = candidate
                        break
                if query_node is None:
                    return False

                # One describe call keeps this wait cheap even for 30 partitions over a slow link.
                describe_output = self.kafka.describe_topic(topic, node=query_node)
                for p in range(num_partitions):
                    line = self.kafka._describe_topic_line_for_partition(p, describe_output)
                    if line is None:
                        return False
                    if int(line.split()[5]) < 0:
                        return False
                    isr_csv = line.split()[9]
                    isr_size = 0 if isr_csv == "Elr:" else len(isr_csv.split(","))
                    if isr_size != expected_isr_size:
                        return False
                return True
            except (Exception, RemoteCommandError):
                return False

        wait_until(
            all_isr_full,
            timeout_sec=timeout_sec, backoff_sec=2,
            err_msg=("Not all %d partitions of %s reached full ISR size %d within %ds"
                     % (num_partitions, topic, expected_isr_size, timeout_sec))
        )

    def _run_preferred_leader_election(self, topic=None):
        """Run preferred-replica election for every partition of the topic.

        With round-robin replica assignment, this redistributes leadership
        back across all brokers (in particular, restoring leadership to a
        recently-restarted broker for the partitions where it is the
        preferred replica). Useful for regression tests that need a
        previously-failed broker to actually serve consumer fetches.
        """
        if topic is None:
            topic = self.topic
        node = self.kafka.nodes[0]
        cmd = "%s --bootstrap-server %s --election-type preferred --all-topic-partitions" % (
            self.kafka.path.script("kafka-leader-election.sh", node),
            self.kafka.bootstrap_servers(),
        )
        self.logger.info("Running preferred-leader election: %s", cmd)
        # The tool exits non-zero if there is no election to run for some
        # partition (e.g. already on the preferred leader); only the
        # successful redistribution matters here.
        node.account.ssh(cmd, allow_fail=True)

    # -----------------------------------------------------------------------
    # Helpers: Trogdor faults and mid-switch JMX assertions
    # -----------------------------------------------------------------------

    # Per-state mid-switch window used by the Category B tests. The two
    # RPC-bounded windows are the ones the injected faults (broker restart,
    # SIGKILL, SIGSTOP, rolling restart, network partition) most directly hit.
    # Names are short state keys into ``SWITCH_STATE_GAUGES``.
    DEFAULT_REQUIRED_SWITCH_STATES = (
        "SendingToController",
        "AwaitingMetadata",
    )

    # Two orthogonal floors, asserted together, that together establish
    # "the fault really overlapped this switch phase". Each catches a
    # different way the overlap could be illusory:
    #
    #   * ``partition_seconds`` - integral of the cluster-wide per-state
    #     gauge over the 1 s JMX polls. Catches "fault never landed at
    #     all" (value = 0) and "single non-zero sample" flukes (a value
    #     of 2 requires at least two 1-partition-poll units of mass,
    #     however distributed in time or across partitions).
    #
    #   * ``oldest_age_ms`` - max over brokers and polls of the
    #     per-state oldest-age gauge. Catches sub-second state
    #     traversals that all happened between polls: a healthy
    #     classic->diskless transition is sub-millisecond, so a sample
    #     showing >= 200 ms is two orders of magnitude above the
    #     happy-path noise floor and direct evidence that the state
    #     machine was actually blocked, not just briefly traversed.
    #
    # ``duration_sec`` (number of distinct 1 s polls with cluster-wide
    # count >= 1) is computed and logged as diagnostic context but not
    # asserted by default: with 1 s polling it is heavily quantised and
    # nearly redundant with ``partition_seconds`` once that floor is
    # >= 2. Tests that specifically want temporal-extent evidence can
    # opt in via the ``min_duration_sec`` kwarg.
    #
    # Defaults are chosen well above the noise floor (single-sample
    # flukes, JMX poll jitter, healthy-path transition times) and well
    # below what every Category B fault that actually bites produces:
    # even a vanilla broker restart with no ``tc`` shaping reliably
    # produces partition_seconds ~20 / oldest_age_ms ~400-1000; a 30 s
    # network partition with 6 partitions yields partition_seconds ~180
    # / oldest_age_ms ~30000; a 2 Mbit/s + 200 ms RTT degraded network
    # with 30 partitions yields partition_seconds in the tens /
    # oldest_age_ms in the thousands.
    DEFAULT_MIN_MID_SWITCH_PARTITION_SECONDS = 2
    DEFAULT_MIN_MID_SWITCH_OLDEST_AGE_MS = 200

    def _start_trogdor(self) -> None:
        """Idempotently start a TrogdorService scoped to the Kafka brokers."""
        if getattr(self, "trogdor", None) is None:
            self.trogdor = TrogdorService(
                context=self.test_context,
                client_services=[self.kafka],
            )
            self.trogdor.start()

    def _stop_trogdor(self) -> None:
        if getattr(self, "trogdor", None) is not None:
            self.trogdor.stop()
            self.trogdor.free()
            self.trogdor = None

    def _degrade_network(self, latency_ms=200, rate_kbit=2000, duration_ms=5 * 60 * 1000,
                         nodes=None, network_device="eth0", task_name="degrade-network"):
        """Apply tc-based latency + rate limit to every broker NIC. Returns the
        Trogdor task so the caller can ``.stop()`` it once switch finishes."""
        if nodes is None:
            nodes = list(self.kafka.nodes)
        spec = DegradedNetworkFaultSpec(0, duration_ms)
        for node in nodes:
            spec.add_node_spec(
                node.name, network_device,
                latencyMs=latency_ms, rateLimitKbit=rate_kbit,
            )
        return self.trogdor.create_task(task_name, spec)

    # Trogdor's ProcessStopFaultWorker selects the target JVM by a *literal*
    # substring match against each `jcmd -l` line (String.contains), so it must
    # be given the plain main-class name as it appears there. Note this differs
    # from KafkaService.java_class_name(), which returns the regex form
    # "kafka\.Kafka" (escaped dot) intended for pgrep/jps regex matching and
    # would not match literally here.
    BROKER_JCMD_PROCESS_NAME = "kafka.Kafka"

    def _pause_broker_process(self, node, duration_ms, task_name="pause-broker"):
        """SIGSTOP the broker JVM on ``node`` for ``duration_ms`` via Trogdor.
        Returns the Trogdor task; Trogdor will SIGCONT automatically when
        the task duration elapses or ``.stop()`` is called."""
        spec = ProcessStopFaultSpec(
            0, duration_ms, [node], self.BROKER_JCMD_PROCESS_NAME,
        )
        return self.trogdor.create_task(task_name, spec)

    def _broker_pid(self, node):
        """Return the broker JVM pid on ``node`` (the process Trogdor signals)."""
        pids = self.kafka.pids(node)
        assert pids, "No running broker JVM found on %s" % node.account.hostname
        return pids[0]

    def _broker_process_state(self, node, pid):
        """Return the primary ``ps`` state character for ``pid`` on ``node``.
        ``T`` means stopped by a job-control signal (i.e. SIGSTOP took effect);
        ``""`` means the process is no longer present."""
        for line in node.account.ssh_capture("ps -o stat= -p %s || true" % pid,
                                              allow_fail=True):
            line = line.decode("utf-8") if isinstance(line, bytes) else line
            stat = line.strip()
            if stat:
                return stat[0]
        return ""

    def _wait_for_broker_stopped(self, node, pid, timeout_sec=60):
        """Assert the broker JVM actually froze (``ps`` state ``T``). Fails
        loudly if the SIGSTOP fault is a no-op rather than silently exercising
        nothing."""
        wait_until(
            lambda: self._broker_process_state(node, pid) == "T",
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=lambda: "SIGSTOP fault was a no-op: broker pid %s on %s never "
                            "reached stopped (T) state (last state=%r)" %
                            (pid, node.account.hostname,
                             self._broker_process_state(node, pid)),
        )

    def _wait_for_broker_running(self, node, pid, timeout_sec=60):
        """Assert the broker JVM resumed after SIGCONT (still alive, not ``T``)."""
        wait_until(
            lambda: self._broker_process_state(node, pid) not in ("", "T"),
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=lambda: "Broker pid %s on %s did not resume after SIGCONT "
                            "(last state=%r)" %
                            (pid, node.account.hostname,
                             self._broker_process_state(node, pid)),
        )

    def _wait_for_mid_switch_state(
            self, state, min_partition_seconds=None, min_oldest_age_ms=None, timeout_sec=120) -> None:
        """Poll historical JMX until the injected fault is observed overlapping ``state``.

        This replaces fixed sleeps in network-partition tests with the same
        evidence that the final assertions use: partition-seconds plus the
        oldest-age gauge for the requested switch state.
        """
        if min_partition_seconds is None:
            min_partition_seconds = self.DEFAULT_MIN_MID_SWITCH_PARTITION_SECONDS
        if min_oldest_age_ms is None:
            min_oldest_age_ms = self.DEFAULT_MIN_MID_SWITCH_OLDEST_AGE_MS

        def check():
            try:
                self._assert_mid_switch_observed(
                    require_states=(state,),
                    min_partition_seconds=min_partition_seconds,
                    min_oldest_age_ms=min_oldest_age_ms,
                )
                return True
            except AssertionError as e:
                self.logger.debug("%s switch JMX state is not ready yet: %s", state, e)
                return False
            except Exception as e:
                self.logger.debug("Failed to read %s switch JMX state: %s", state, e)
                return False

        wait_until(
            check,
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg=("%s state did not reach partition_seconds >= %s and oldest_age_ms >= %s "
                     "while the fault was active within %ds" %
                     (state, min_partition_seconds, min_oldest_age_ms, timeout_sec))
        )

    def _assert_mid_switch_observed(
            self,
            require_states=None,
            min_partition_seconds=None,
            min_oldest_age_ms=None,
            min_duration_sec=None) -> None:
        """Assert that each state in ``require_states`` was occupied long
        enough during the test that the injected fault must have
        overlapped it, computed from the per-broker JmxTool logs scraped
        when the Kafka service was started with
        ``scrape_switch_state_jmx=True``.

        Two orthogonal signals are asserted together by default (see the
        class-level ``DEFAULT_MIN_MID_SWITCH_*`` thresholds for the
        rationale of each floor):

        - ``partition_seconds`` (mass): integral of the cluster-wide
          per-state gauge over the 1 s polls. Catches "fault never
          landed at all" and single non-zero-sample flukes.

        - ``oldest_age_ms`` (stuck-time evidence): max over brokers and
          polls of the per-state oldest-age gauge. Catches sub-second
          state traversals that all happened between polls.

        ``duration_sec`` (number of distinct 1 s polls with cluster-wide
        count >= 1) is always computed and logged as diagnostic context
        but only asserted when the caller passes ``min_duration_sec``.
        """
        if require_states is None:
            require_states = self.DEFAULT_REQUIRED_SWITCH_STATES
        if min_partition_seconds is None:
            min_partition_seconds = self.DEFAULT_MIN_MID_SWITCH_PARTITION_SECONDS
        if min_oldest_age_ms is None:
            min_oldest_age_ms = self.DEFAULT_MIN_MID_SWITCH_OLDEST_AGE_MS

        self.kafka.read_jmx_output_all_nodes()
        per_node_stats = self.kafka.jmx_stats

        all_times = set()
        for ts in per_node_stats:
            all_times.update(ts.keys())
        if all_times:
            start_sec = min(all_times)
            end_sec = max(all_times)
        else:
            start_sec, end_sec = 0, -1

        def _attr_key(obj_name):
            return "%s:Value" % obj_name

        def _cluster_sum_at(obj_name, t):
            key = _attr_key(obj_name)
            total = 0.0
            for ts in per_node_stats:
                bucket = ts.get(t)
                if bucket is not None:
                    total += bucket.get(key, 0)
            return total

        def _partition_seconds(obj_name):
            total = 0.0
            for t in range(start_sec, end_sec + 1):
                total += _cluster_sum_at(obj_name, t)
            return int(total)

        def _duration_sec(obj_name):
            count = 0
            for t in range(start_sec, end_sec + 1):
                if _cluster_sum_at(obj_name, t) >= 1:
                    count += 1
            return count

        def _max_per_broker(obj_name):
            # Max over (broker, time) of the gauge. Used for the oldest-age
            # gauges, where summing across brokers (as JmxMixin does for
            # ``maximum_jmx_value``) has no natural meaning - each
            # partition is led by one broker, so we want the largest
            # single sample anywhere in the cluster.
            key = _attr_key(obj_name)
            best = 0.0
            for ts in per_node_stats:
                for bucket in ts.values():
                    v = bucket.get(key, 0)
                    if v > best:
                        best = v
            return int(best)

        summary_parts = []
        for state, (count_obj, age_obj) in self.SWITCH_STATE_GAUGES.items():
            summary_parts.append(
                "%s: partition_seconds=%s duration_sec=%s oldest_age_ms=%s" %
                (state,
                 _partition_seconds(count_obj),
                 _duration_sec(count_obj),
                 _max_per_broker(age_obj))
            )
        summary = "; ".join(summary_parts)
        self.logger.info("Mid-switch JMX summary: %s", summary)

        failures = []
        for state in require_states:
            count_obj, age_obj = self.SWITCH_STATE_GAUGES[state]
            partition_seconds = _partition_seconds(count_obj)
            duration_sec = _duration_sec(count_obj)
            oldest_age_ms = _max_per_broker(age_obj)

            shortfalls = []
            if partition_seconds < min_partition_seconds:
                shortfalls.append("partition_seconds=%s < %s" %
                                  (partition_seconds, min_partition_seconds))
            if oldest_age_ms < min_oldest_age_ms:
                shortfalls.append("oldest_age_ms=%s < %s" %
                                  (oldest_age_ms, min_oldest_age_ms))
            if min_duration_sec is not None and duration_sec < min_duration_sec:
                shortfalls.append("duration_sec=%s < %s" %
                                  (duration_sec, min_duration_sec))

            if shortfalls:
                failures.append("%s: %s" % (state, ", ".join(shortfalls)))

        assert not failures, (
            "Fault did not meaningfully overlap required switch "
            "state(s): %s. Either the switch finished before the "
            "fault landed, the fault did not actually degrade the state "
            "machine, or the gauges are not being published. Full JMX "
            "summary: %s." % ("; ".join(failures), summary)
        )

    # -----------------------------------------------------------------------
    # Category A: Post-Switch Data Availability
    # -----------------------------------------------------------------------

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_available_after_restarts(self, metadata_quorum) -> None:
        """After switch completes, verify classic + diskless data remains
        readable after a single leader restart and after a full rolling restart.

        The classic prefix is deliberately ~3x the diskless tail so that a
        regression which only serves the diskless portion (or only the
        classic portion) shows up as an obviously short consume rather than
        a borderline count that could be mistaken for a flake."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        self._produce_messages(num_messages=15000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        self._produce_messages(num_messages=5000)

        leader_node = self._get_leader_node(partition=0)
        self._restart_broker(leader_node)
        self._wait_for_all_partitions_isr_full()

        self._assert_all_acked_records_readable(context="after a leader restart")

        self._rolling_restart()
        self._wait_for_all_partitions_isr_full()

        self._assert_all_acked_records_readable(context="after a rolling restart")

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switched_topic_urp_clears_after_replica_recovery(self, metadata_quorum) -> None:
        """A switched hybrid partition should report URP only while ISR is short.

        This guards the operational contract that the ReplicaManager aggregate
        gauge is live state, not a sticky artifact of switching to diskless:
        stopping one replica raises URP, and the same replica catching back up
        clears it.

        The test stays in phase 1. The URP gauge leaves out partitions that
        consolidate, so in phase 2 the switched partition doesn't count, and
        the replicated ``__remote_log_metadata`` topic could raise the gauge
        in its place.
        """
        self.num_partitions = 1
        self._create_kafka(consolidation=False)
        self.kafka.start()
        self._create_classic_topic(num_partitions=1)

        self._produce_messages(num_messages=5000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()
        self._wait_for_under_replicated_partitions(0)

        follower = self._get_follower_nodes(partition=0)[0]
        self._stop_broker(follower, clean_shutdown=False)
        self._wait_for_under_replicated_partitions_at_least(1)

        self._start_broker(follower)
        self._wait_for_all_partitions_isr_full(num_partitions=1)
        self._wait_for_under_replicated_partitions(0)

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_available_after_leader_failures(self, metadata_quorum) -> None:
        """After switch, verify classic + diskless data remains readable
        while the old leader is down and after an unclean broker restart."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        self._produce_messages(num_messages=10000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        self._produce_messages(num_messages=5000)

        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=True)
        self._wait_for_all_partitions_have_leaders()

        self._assert_all_acked_records_readable(context="while the old leader is down")

        self._start_broker(leader_node)
        self._wait_for_all_partitions_isr_full()
        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=False)

        self._start_broker(leader_node)
        self._wait_for_all_partitions_isr_full()

        self._assert_all_acked_records_readable(context="after an unclean leader restart")

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switched_leader_promotion_restores_stale_hw(self, metadata_quorum) -> None:
        """Promote a phase 2 replica whose checkpointed high watermark is below the seal.

        The replica restarts with a stale high watermark checkpoint while the
        other replicas are down. The controller elects it as the only live
        replica. In phase 2, remote storage holds the classic prefix: consumers
        read the prefix from there and cross the seal into the diskless tail. A
        leader that keeps the stale high watermark clamps consumers below the
        seal, and one that drops local records without rebuilding the prefix
        from remote storage loses records. The replica repairs the high
        watermark either when its consolidation fetcher starts or when it
        becomes leader. The test checks what consumers read, not which of those
        paths ran.

        The test doesn't need unclean leader election. The promoted replica
        stops last, so it's the last ISR member of this partition and of the
        internal topics. When it comes back alone, the controller elects it for
        all of them, including ``__remote_log_metadata``, which it needs to
        serve the prefix.
        """
        self.num_partitions = 1
        self._create_kafka(consolidation=True)
        self.kafka.start()

        switch_leader = self.kafka.nodes[0]
        promoted_replica = self.kafka.nodes[1]
        other_replica = self.kafka.nodes[2]
        replica_assignment = "%d:%d:%d" % (
            self.kafka.idx(switch_leader),
            self.kafka.idx(promoted_replica),
            self.kafka.idx(other_replica),
        )
        self._create_classic_topic(
            num_partitions=1,
            replica_assignment=replica_assignment,
        )
        self._wait_for_partition_leader(switch_leader, partition=0)

        self._produce_messages(num_messages=5000)

        self._switch_topic_to_diskless(extra_configs={"remote.storage.enable": "true"})
        self._wait_for_switch_complete()

        self._produce_messages(num_messages=1000)

        seal = self._wait_for_classic_prefix_in_remote_storage()[0]
        stale_hw = seal // 2
        assert 0 < stale_hw < seal, "Expected a seal above 1 but got %d" % seal

        self._stop_broker(other_replica)
        self._stop_broker(switch_leader)
        self._wait_for_partition_leader(promoted_replica, partition=0)

        self._stop_broker(promoted_replica)
        self._set_replication_checkpoint_offset(
            promoted_replica,
            partition=0,
            offset=stale_hw,
        )
        self._start_broker(promoted_replica)
        self._wait_for_partition_leader(promoted_replica, partition=0)

        self._assert_all_acked_records_readable(context="after promoting a replica with a stale high watermark")

    # -----------------------------------------------------------------------
    # Category B: Mid-Switch Fault Tolerance
    # -----------------------------------------------------------------------

    @cluster(num_nodes=7)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_succeeds_with_degraded_network(self, metadata_quorum) -> None:
        """Switch a classic topic to diskless under continuous load.
        Verify no data loss."""
        self.num_partitions = 30
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()
        degrade = self._degrade_network()

        producer = self._start_producer(max_messages=-1)
        consumer = VerifiableConsumer(
            context=self.test_context,
            num_nodes=1,
            kafka=self.kafka,
            topic=self.topic,
            group_id="continuous-%s" % str(uuid.uuid4())[:8],
            enable_autocommit=True,
        )
        consumer.start()

        self._wait_for_steady_production(producer, min_acked=5000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        post_switch_target = producer.num_acked + 5000
        self._wait_for_steady_production(
            producer,
            min_acked=post_switch_target,
            timeout_sec=180,
            err_msg="Producer did not reach %d acks within 180s after switch" % post_switch_target,
            worker_err_msg="Producer errors after switch under degraded network: %s",
        )

        total_produced = self._stop_producer(producer)

        wait_until(
            lambda: consumer.total_consumed() >= total_produced or consumer.worker_errors,
            timeout_sec=self.CONSUME_TIMEOUT_SEC,
            err_msg="Consumer consumed only %d out of %d produced" % (consumer.total_consumed(), total_produced)
        )
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()
        total_consumed = consumer.total_consumed()
        consumer.free()

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        assert total_consumed >= total_produced, \
            "Data loss: produced %d but consumed only %d" % (total_produced, total_consumed)
        self._assert_all_acked_records_readable(context="after a switch under a degraded network")

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_leader_restart(self, metadata_quorum) -> None:
        """Restart the leader broker during switch. Verify switch
        completes and all data is readable via fresh consumer.

        Runs under a degraded broker network (200 ms latency, 2 Mbit/s) and
        with 30 partitions so the restart reliably overlaps a partition still
        in SendingToController or AwaitingMetadata; the JMX assertion at the
        end fails the test if that overlap never happened."""
        self.num_partitions = 30
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()
        degrade = self._degrade_network()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._switch_topic_to_diskless()

        leader_node = self._get_leader_node(partition=0)
        self._restart_broker(leader_node)

        self._wait_for_switch_complete()
        self._wait_for_steady_production(producer, min_acked=producer.num_acked + 5000, timeout_sec=180)

        self._stop_producer(producer)

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        self._assert_all_acked_records_readable(context="after a leader restart during the switch")

    @cluster(num_nodes=6)
    @matrix(
        metadata_quorum=[quorum.isolated_kraft],
        leader_failure_mode=["sigkill", "sigstop"],
    )
    def test_switch_leader_crash(self, metadata_quorum, leader_failure_mode) -> None:
        """Take down the leader during switch via either SIGKILL or SIGSTOP
        and verify switch completes plus all data is recoverable.

        ``sigkill`` exercises hard-loss-of-in-memory-state recovery: the broker
        is killed, restarted, and must rebuild its InitDisklessLogManager state
        from disk and incoming metadata.

        ``sigstop`` (Trogdor ProcessStopFaultSpec) freezes the broker process
        while keeping in-memory state intact: the controller and other brokers
        observe it as unresponsive, leadership/switch progress on partitions
        led by it must continue, and after SIGCONT the broker should rejoin and
        catch up without re-issuing duplicate InitDisklessLog requests."""
        self.num_partitions = 30
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()
        degrade = self._degrade_network()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._switch_topic_to_diskless()

        leader_node = self._get_leader_node(partition=0)
        pause_duration_ms = 30_000
        if leader_failure_mode == "sigkill":
            self._stop_broker(leader_node, clean_shutdown=False)
            self._start_broker(leader_node)
        elif leader_failure_mode == "sigstop":
            leader_pid = self._broker_pid(leader_node)
            pause = self._pause_broker_process(leader_node, duration_ms=pause_duration_ms)
            # Confirm the fault actually took effect: a silent no-op here would
            # let the whole scenario pass without ever freezing the leader.
            self._wait_for_broker_stopped(leader_node, leader_pid)
            pause.wait_for_done(timeout_sec=pause_duration_ms // 1000 + 60)
            self._wait_for_broker_running(leader_node, leader_pid)
        else:
            raise AssertionError("Unknown leader_failure_mode: %s" % leader_failure_mode)

        self._wait_for_switch_complete()

        self._wait_for_steady_production(
            producer,
            min_acked=producer.num_acked + 5000,
            timeout_sec=240,
            err_msg="Producer did not recover after leader %s" % leader_failure_mode,
            worker_err_msg="Producer errors after leader %s during switch: %%s" % leader_failure_mode,
        )
        self._stop_producer(producer)

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        self._assert_all_acked_records_readable(context="after a leader %s during the switch" % leader_failure_mode)

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_follower_crash_with_lagged_hw(self, metadata_quorum) -> None:
        """SIGKILL a follower mid-switch while the leader's HW still lags
        its LEO, then verify a fresh consumer reads every produced record
        even after leadership moves back to the restarted broker.
        """
        self.num_partitions = 30
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()
        degrade = self._degrade_network()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._switch_topic_to_diskless()

        target_follower = self._get_follower_nodes(partition=0)[0]
        self.logger.info("Killing follower %s mid-switch", target_follower.account.hostname)
        self._stop_broker(target_follower, clean_shutdown=False)
        self._start_broker(target_follower)

        self._wait_for_switch_complete()

        self._wait_for_steady_production(
            producer,
            min_acked=producer.num_acked + 5000,
            timeout_sec=240,
            err_msg="Producer did not recover after follower SIGKILL",
            worker_err_msg="Producer errors after follower SIGKILL during switch: %s",
        )
        self._stop_producer(producer)

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        # Heal ISR on every partition so preferred-replica election can
        # actually move leadership (a replica out of ISR cannot take over).
        self._wait_for_all_partitions_isr_full()

        # Force the previously-killed broker back into a leader role for
        # the partitions where it is the preferred replica. Its local log
        # contains the pre-crash classic data with a possibly-stale HW;
        # only the fixed code path arms a replica fetcher to advance that
        # HW to the seal so reads below the seal are not clamped.
        self._run_preferred_leader_election()

        self._assert_all_acked_records_readable(context="after a follower SIGKILL during the switch")

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_rolling_restart(self, metadata_quorum) -> None:
        """Rolling restart all brokers during switch. Verify switch
        completes and data is fully readable.

        With a degraded broker network and 30 partitions, every broker bounce
        in the rolling sequence lands while at least one other broker still
        has partitions mid-switch. The JMX assertion at the end fails the
        test if no partition was ever observed in the RPC-bounded states."""
        self.num_partitions = 30
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()
        degrade = self._degrade_network()

        # Lower throughput than the default 10k/s: under the degraded network
        # (200ms latency, 2 Mbps cap) the cluster can only drain ~4.5k/s, so a
        # higher target makes the producer accumulator grow unbounded across
        # the rolling restart and OOMs the VerifiableProducer JVM.
        producer = self._start_producer(max_messages=-1, throughput=3000)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._switch_topic_to_diskless()

        self._rolling_restart()

        # After the final bounce, wait for cluster-wide ISR recovery before
        # using switch-complete JMX. This avoids racing leaders that still
        # cannot finish init work because their replicas have not caught up.
        self._wait_for_all_partitions_isr_full(timeout_sec=240)
        self._wait_for_switch_complete()

        self._wait_for_steady_production(
            producer,
            min_acked=producer.num_acked + 5000,
            timeout_sec=240,
            err_msg="Producer did not recover after rolling restart",
            worker_err_msg="Producer errors after rolling restart during switch: %s",
        )
        self._stop_producer(producer)

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        self._assert_all_acked_records_readable(context="after a rolling restart during the switch")

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_follower_network_partition(self, metadata_quorum) -> None:
        """Isolate a follower via Trogdor network partition during switch.
        Verify ISR shrinks, switch completes, ISR re-expands, no data loss.

        Switch is triggered while the partitioned follower is still
        listed in ISR: it cannot fetch while isolated, so HW cannot advance
        to LEO. Partitions therefore park in WaitingForReplication until
        ISR shrinks (after replica.lag.time.max.ms, ~30 s) and the network
        heals. The downstream states (SendingToController, AwaitingMetadata)
        are subsecond once unblocked and frequently fall between 1 s JMX
        polls, so we only assert the WaitingForReplication overlap here -
        same rationale as ``test_switch_leader_network_partition``.

        Note: triggering switch *after* waiting for ISR to shrink would
        defeat the purpose of the fault - once the slow follower is out of
        ISR, HW advances freely among the remaining replicas and
        WaitingForReplication is traversed sub-millisecond."""
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        follower_nodes = self._get_follower_nodes(partition=0)
        follower = follower_nodes[0]
        reachable_nodes = [n for n in self.kafka.nodes if n != follower]

        partition_spec = NetworkPartitionFaultSpec(
            0, 5 * 60 * 1000,
            [[follower], reachable_nodes]
        )
        fault = self.trogdor.create_task("follower_partition", partition_spec)

        self._switch_topic_to_diskless(node=reachable_nodes[0])

        # The partition fault should block switch in WaitingForReplication;
        # waiting on that JMX state is more precise than sleeping for ISR lag.
        self._wait_for_mid_switch_state("WaitingForReplication", timeout_sec=120)

        fault.stop()
        fault.wait_for_done(timeout_sec=120)

        self._wait_for_all_partitions_isr_full(num_partitions=1, timeout_sec=120)
        self._wait_for_switch_complete()

        self._wait_for_steady_production(
            producer,
            min_acked=producer.num_acked + 3000,
            timeout_sec=120,
            err_msg="Producer did not recover after follower partition",
            worker_err_msg="Producer errors after follower partition during switch: %s",
        )

        self._stop_producer(producer)

        self._assert_mid_switch_observed(require_states=("WaitingForReplication",))
        self._stop_trogdor()

        self._assert_all_acked_records_readable(context="after a follower network partition during the switch")

    @cluster(num_nodes=6)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_leader_network_partition(self, metadata_quorum) -> None:
        """Isolate the leader via Trogdor network partition during switch.
        Verify new leader elected, switch completes, ISR heals, no data loss.

        The new leader picks up switch once it is elected. It can reach
        the controller (only the old leader is isolated), but the old leader
        is still listed in ISR and cannot catch up while partitioned, so HW
        cannot advance to LEO. Partitions therefore park in
        WaitingForReplication until ISR shrinks (after
        replica.lag.time.max.ms, ~30 s) and the network heals. The downstream
        states (SendingToController, AwaitingMetadata) are subsecond once
        unblocked and frequently fall between 1 s JMX polls, so we only
        assert the WaitingForReplication overlap here."""
        self._create_kafka(scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()

        self._start_trogdor()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        leader_node = self._get_leader_node(partition=0)
        non_leader_nodes = [n for n in self.kafka.nodes if n != leader_node]

        partition_spec = NetworkPartitionFaultSpec(
            0, 5 * 60 * 1000,
            [[leader_node], non_leader_nodes]
        )
        fault = self.trogdor.create_task("leader_partition", partition_spec)

        self._switch_topic_to_diskless(node=non_leader_nodes[0])

        # The partition fault should block switch in WaitingForReplication;
        # waiting on that JMX state is more precise than sleeping for ISR lag.
        self._wait_for_mid_switch_state("WaitingForReplication", timeout_sec=120)

        fault.stop()
        fault.wait_for_done(timeout_sec=120)

        self._wait_for_all_partitions_isr_full(num_partitions=1, timeout_sec=180)
        self._wait_for_switch_complete()

        self._wait_for_steady_production(
            producer,
            min_acked=producer.num_acked + 3000,
            timeout_sec=120,
            err_msg="Producer did not recover after leader partition",
            worker_err_msg="Producer errors after leader partition during switch: %s",
        )
        self._stop_producer(producer)

        self._assert_mid_switch_observed(require_states=("WaitingForReplication",))
        self._stop_trogdor()

        self._assert_all_acked_records_readable(context="after a leader network partition during the switch")

    # -----------------------------------------------------------------------
    # Category C: Operational Scenarios
    # -----------------------------------------------------------------------

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_concurrent_topics(self, metadata_quorum) -> None:
        """Switch 3 classic topics simultaneously. Verify all switch
        successfully with no data loss."""
        self._create_kafka()
        self.kafka.start()

        topics = ["concurrent-topic-%d" % i for i in range(3)]

        for topic in topics:
            self._create_classic_topic(topic=topic, num_partitions=4)
            self._produce_messages(topic=topic, num_messages=5000)

        for topic in topics:
            self._switch_topic_to_diskless(topic=topic)

        for topic in topics:
            self._wait_for_switch_complete(topic=topic)

        for topic in topics:
            self._produce_messages(topic=topic, num_messages=2000)

        for topic in topics:
            self._assert_all_acked_records_readable(topic=topic, context="after a concurrent switch")

    @cluster(num_nodes=3)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_tiered_to_diskless_switch(self, metadata_quorum) -> None:
        """Switch a classic topic with real (LocalTieredStorage) tiered
        storage to diskless, then read across all three layers.

        Sequence:
          1. Create a classic topic with ``remote.storage.enable=true``,
             small ``segment.bytes``, and short ``local.retention.ms``.
             Produce a first batch large enough to roll several segments.
                                                          state: [empty]
          2. Wait until the topic's earliest-local offset advances past 0,
             confirming closed segments were uploaded to remote and then
             deleted from local disk.
                                                          state: [remote]
          3. Produce a second batch. The active segment is never eligible
             for upload, so the most recent records are local-only at this
             instant.
                                                          state: [remote][local]
          4. Switch the topic to diskless and produce a third batch which
             is written via the diskless write path.
                                                          state: [remote][local][diskless]
          5. A fresh consumer reads from offset 0 and must see every record,
             crossing the remote->local boundary, the classic->diskless
             boundary, and any internal classic-segment seam.
        """
        topic = "tiered-switch-topic"

        self._create_kafka_with_tiered_storage()
        self.kafka.start()
        self._create_classic_tiered_topic(topic=topic)

        self._produce_messages(topic=topic, num_messages=8000)
        self._wait_for_local_log_truncation(topic=topic)

        self._produce_messages(topic=topic, num_messages=2000)

        self._switch_topic_to_diskless(topic=topic)
        self._wait_for_switch_complete(topic=topic)

        self._produce_messages(topic=topic, num_messages=1000)

        self._assert_all_acked_records_readable(topic=topic, context="across remote, local, and diskless records")

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switch_idempotent_producer_state(self, metadata_quorum) -> None:
        """Verify idempotent producer state is preserved across the
        switch boundary. The same producer continues producing after
        switch without OutOfOrderSequence errors or duplicates."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        producer = self._start_producer(max_messages=50000, enable_idempotence=True)
        self._wait_for_steady_production(producer, min_acked=10000)

        pre_switch_acked = producer.num_acked

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        self._wait_for_steady_production(
            producer,
            min_acked=pre_switch_acked + 15000,
            timeout_sec=self.PRODUCE_TIMEOUT_SEC,
            err_msg="Idempotent producer stalled after switch at %d acks" % producer.num_acked,
            worker_err_msg="Idempotent producer errors after switch (possible OutOfOrderSequence): %s",
        )

        self._stop_producer(producer)

        self._assert_all_acked_records_readable(context="with an idempotent producer across the switch")

    # -----------------------------------------------------------------------
    # Category D: faults between the seal commit and phase 2
    # -----------------------------------------------------------------------

    # Category D injects each fault while every partition waits for the upload of
    # its classic prefix. The ``object_storage_outage`` fault is the hold alone.
    # The ``*_at_phase_2`` faults start when the hold ends. They overlap the flag
    # request and the phase 2 transition.
    PHASE_1_FAULTS = [
        "object_storage_outage",
        "leader_restart",
        "leader_sigkill",
        "leader_sigstop",
        "follower_sigkill",
        "rolling_restart",
        "leader_network_partition",
        "reassignment",
        "topic_deletion",
        "controller_failover_at_phase_2",
        "postgres_outage_at_phase_2",
        "rolling_restart_at_phase_2",
    ]
    PHASE_1_FAULT_DURATION_MS = 30_000
    FLAG_REQUEST_FAULT_DURATION_MS = 10_000

    def _live_broker(self, exclude=()):
        for node in self.kafka.nodes:
            if node not in exclude and self.kafka.pids(node):
                return node
        raise AssertionError("No live broker outside %s" % [n.account.hostname for n in exclude])

    def _active_controller_id(self, node=None):
        """Returns the node ID of the active KRaft controller, or -1 if there is none."""
        if node is None:
            node = self._live_broker()
        cmd = "%s describe --status" % self.kafka.kafka_metadata_quorum_cmd(node)
        output = "".join(
            line.decode("utf-8") if isinstance(line, bytes) else line
            for line in node.account.ssh_capture(cmd, allow_fail=True, combine_stderr=False)
        )
        match = re.search(r"LeaderId:\s+(-?\d+)", output)
        return int(match.group(1)) if match else -1

    def _active_controller_node(self):
        controllers = self.kafka.isolated_controller_quorum
        leader_id = self._active_controller_id()
        for node in controllers.nodes:
            if controllers.node_id_as_isolated_controller(node) == leader_id:
                return node
        raise AssertionError("Active controller %d isn't one of the isolated controllers" % leader_id)

    def _wait_for_new_active_controller(self, old_leader_id, timeout_sec=60):
        wait_until(
            lambda: self._active_controller_id() not in (-1, old_leader_id),
            timeout_sec=timeout_sec,
            backoff_sec=2,
            err_msg="No controller replaced controller %d within %ds" % (old_leader_id, timeout_sec),
        )

    def _reassignment_for_partition_0(self):
        """Returns a reassignment that moves one follower of partition 0 to the broker outside its replicas."""
        replicas = [self.kafka.idx(n) for n in self.kafka.replicas(self.topic, partition=0)]
        leader = self.kafka.idx(self._get_leader_node(partition=0))
        spare = [self.kafka.idx(n) for n in self.kafka.nodes if self.kafka.idx(n) not in replicas]
        assert spare, "Partition 0 of %s already uses every broker: %s" % (self.topic, replicas)
        follower = [r for r in replicas if r != leader][0]
        target = [spare[0] if r == follower else r for r in replicas]
        return {
            "version": 1,
            "partitions": [{"topic": self.topic, "partition": 0, "replicas": target}],
        }

    def _wait_for_reassignment(self, reassignment, timeout_sec=None):
        if timeout_sec is None:
            timeout_sec = self.PHASE_2_TIMEOUT_SEC
        target = reassignment["partitions"][0]["replicas"]

        def done():
            try:
                if not self.kafka.verify_reassign_partitions(reassignment):
                    return False
                replicas = [self.kafka.idx(n) for n in self.kafka.replicas(self.topic, partition=0)]
                return sorted(replicas) == sorted(target)
            except (Exception, RemoteCommandError) as e:
                self.logger.debug("Failed to verify the reassignment of %s-0: %s", self.topic, e)
                return False

        wait_until(done, timeout_sec=timeout_sec, backoff_sec=5,
                   err_msg="Reassignment of %s-0 to %s didn't finish within %ds" % (self.topic, target, timeout_sec))

    def _wait_for_no_upload_wait(self, timeout_sec=120):
        """Wait until no live broker tracks a partition in ``WaitingForUpload``."""
        waiting_obj = self.SWITCH_STATE_GAUGES["WaitingForUpload"][0]

        def check():
            count = self._live_cluster_jmx_sum(waiting_obj)
            self.logger.info("Cluster WaitingForUploadPartitions=%s", count)
            return count == 0

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Brokers still waited for the upload of a deleted topic after %ds" % timeout_sec)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft], fault=PHASE_1_FAULTS)
    def test_fault_between_seal_commit_and_phase_2(self, metadata_quorum, fault) -> None:
        """Inject ``fault`` after the controller commits the seal and before phase 2.

        Every broker drops its traffic to object storage before the switch.
        The controller still commits the seal, because steps 3 to 5 don't use
        object storage. The remote log manager then can't copy the classic
        prefix. Every partition waits in ``WaitingForUpload``, and the test
        asserts that before it injects the fault. That outage is the
        ``object_storage_outage`` fault on its own.

        For a fault that ends before the hold ends, the test asserts phase 1
        on every partition after the fault. The ``*_at_phase_2`` faults start
        as the hold ends. They overlap the ``SetClassicPrefixInRemoteStorage``
        request and the phase 2 transition:

        - ``controller_failover_at_phase_2`` stops the active controller and
          one more of three. The test waits until a flag request has been
          pending for 10 s, then starts the second controller. The flag
          requests retry until the two live controllers elect a new leader.
        - ``postgres_outage_at_phase_2`` keeps the control plane unreachable
          until the controller sets the flag on every partition. Consolidation
          from the seal then starts without Postgres.
        - ``rolling_restart_at_phase_2`` stops the leader of partition 0 before
          the hold ends and keeps it down until every partition is in phase 2.
          The leader then rejoins in phase 2, and the other brokers restart
          one at a time.

        After the hold, the test requires phase 2 on every partition, produces
        a diskless tail, and checks every record across the seal.
        """
        self.num_brokers = 4
        self.num_partitions = 6
        self._create_kafka(controller_num_nodes=3, scrape_switch_state_jmx=True)
        self.kafka.start()
        self._create_classic_topic()
        self.verifier = ConsolidationVerifier(self.kafka)

        self._produce_messages(num_messages=10000)

        self.verifier.block_dependency("object_storage")
        self._switch_topic_to_diskless()
        self._wait_for_phase_1_complete()
        self._assert_in_phase_1()
        self._wait_for_mid_switch_state("WaitingForUpload")

        stopped_controller = None
        reassignment = None
        leader_node = self._get_leader_node(partition=0)

        if fault in ("leader_sigstop", "leader_network_partition"):
            self._start_trogdor()

        if fault == "object_storage_outage":
            # Hold the outage across several upload retries of the remote log manager.
            self._wait_for_mid_switch_state(
                "WaitingForUpload", min_oldest_age_ms=self.PHASE_1_FAULT_DURATION_MS)
        elif fault == "leader_restart":
            self._restart_broker(leader_node)
            self._wait_for_all_partitions_have_leaders()
        elif fault == "leader_sigkill":
            self._stop_broker(leader_node, clean_shutdown=False)
            self._start_broker(leader_node)
        elif fault == "leader_sigstop":
            leader_pid = self._broker_pid(leader_node)
            pause = self._pause_broker_process(leader_node, duration_ms=self.PHASE_1_FAULT_DURATION_MS)
            self._wait_for_broker_stopped(leader_node, leader_pid)
            pause.wait_for_done(timeout_sec=self.PHASE_1_FAULT_DURATION_MS // 1000 + 60)
            self._wait_for_broker_running(leader_node, leader_pid)
        elif fault == "follower_sigkill":
            follower = self._get_follower_nodes(partition=0)[0]
            self._stop_broker(follower, clean_shutdown=False)
            self._start_broker(follower)
        elif fault == "rolling_restart":
            self._rolling_restart()
        elif fault == "leader_network_partition":
            others = [n for n in self.kafka.nodes if n != leader_node]
            partition_fault = self.trogdor.create_task(
                "leader_partition",
                NetworkPartitionFaultSpec(0, self.PHASE_1_FAULT_DURATION_MS, [[leader_node], others]),
            )
            partition_fault.wait_for_done(timeout_sec=self.PHASE_1_FAULT_DURATION_MS // 1000 + 60)
        elif fault == "reassignment":
            reassignment = self._reassignment_for_partition_0()
            self.logger.info("Reassigning %s-0 in phase 1: %s", self.topic, reassignment)
            self.kafka.execute_reassign_partitions(reassignment)
        elif fault == "topic_deletion":
            self.kafka.delete_topic(self.topic)
            self._switched_topics.discard(self.topic)
            self._forget_acked(self.topic)
            self._wait_for_no_upload_wait()
        elif fault in ("controller_failover_at_phase_2", "postgres_outage_at_phase_2",
                       "rolling_restart_at_phase_2"):
            pass
        else:
            raise AssertionError("Unknown fault: %s" % fault)

        if fault != "topic_deletion":
            self._wait_for_all_partitions_have_leaders()
            # The hold is still on. The controller can't have set the flag on any partition.
            self._assert_in_phase_1()

        if fault == "controller_failover_at_phase_2":
            controllers = self.kafka.isolated_controller_quorum
            stopped_controller = self._active_controller_node()
            old_leader_id = controllers.node_id_as_isolated_controller(stopped_controller)
            # Stopping a second controller takes the quorum down. The flag requests fail until
            # a new leader exists. Without that, the two remaining controllers can elect a
            # leader before any broker finishes its upload.
            standby = [n for n in controllers.nodes if n != stopped_controller][0]
            self.logger.info("Stopping active controller %d and controller %d as the hold ends",
                             old_leader_id, controllers.node_id_as_isolated_controller(standby))
            controllers.stop_node(stopped_controller, clean_shutdown=False)
            controllers.stop_node(standby, clean_shutdown=False)
            self.verifier.heal_dependency("object_storage")
            self._wait_for_mid_switch_state(
                "SendingFlagToController", min_oldest_age_ms=self.FLAG_REQUEST_FAULT_DURATION_MS,
                timeout_sec=self.PHASE_2_TIMEOUT_SEC)
            controllers.start_node(standby)
            self._wait_for_new_active_controller(old_leader_id)
        elif fault == "postgres_outage_at_phase_2":
            self.verifier.block_dependency("control_plane")
            self.verifier.heal_dependency("object_storage")
        elif fault == "rolling_restart_at_phase_2":
            # The first broker stays down until every partition is in phase 2, so the
            # transition runs while a broker is restarting.
            first = leader_node
            self._stop_broker(first, clean_shutdown=True)
            self._wait_for_all_partitions_have_leaders()
            self.verifier.heal_dependency("object_storage")
            self._wait_for_classic_prefix_in_remote_storage(timeout_sec=self.PHASE_2_TIMEOUT_SEC)
            self._start_broker(first)
            self._wait_for_all_partitions_have_leaders()
            self._wait_for_broker_in_isr(first, num_partitions=1)
            self._rolling_restart(nodes=[n for n in self.kafka.nodes if n != first])
        else:
            self.verifier.heal_dependency("object_storage")

        if fault == "topic_deletion":
            # A recreated topic with the same name must switch from scratch.
            self._create_classic_topic()
            self._produce_messages(num_messages=10000)
            self._switch_topic_to_diskless()

        self._wait_for_switch_complete()

        if fault == "postgres_outage_at_phase_2":
            self.verifier.heal_dependency("control_plane")
        if stopped_controller is not None:
            self.kafka.isolated_controller_quorum.start_node(stopped_controller)
        if reassignment is not None:
            self._wait_for_reassignment(reassignment)
        self._stop_trogdor()

        self._wait_for_all_partitions_isr_full(timeout_sec=240)
        self._produce_messages(num_messages=5000)
        self._assert_all_acked_records_readable(context="after %s between the seal commit and phase 2" % fault)
