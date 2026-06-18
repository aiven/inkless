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
import uuid

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test, TestContext
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
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

    Tests are organized into three categories:
      A) Post-switch data availability 
      B) Mid-switch fault tolerance (faults injected during switch)
      C) Operational scenarios (concurrent switch, producer state)
    """

    SWITCH_TIMEOUT_SEC = 120
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
    }
    SEALED_LEADER_PARTITIONS_JMX_OBJECT = "kafka.server:type=ReplicaManager,name=SealedPartitionsCount"
    INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT = _IDLM_OBJ % "InFlightPartitions"
    SWITCH_COMPLETION_JMX_OBJECT_NAMES = [
        SEALED_LEADER_PARTITIONS_JMX_OBJECT,
        INIT_DISKLESS_IN_FLIGHT_PARTITIONS_JMX_OBJECT,
    ]
    SWITCH_STATE_JMX_OBJECT_NAMES = [obj for pair in SWITCH_STATE_GAUGES.values() for obj in pair]
    SWITCH_JMX_OBJECT_NAMES = SWITCH_COMPLETION_JMX_OBJECT_NAMES + SWITCH_STATE_JMX_OBJECT_NAMES
    SWITCH_JMX_ATTRIBUTES = ["Value"]

    def _create_kafka(self, num_nodes=None, controller_num_nodes=1,
                      scrape_switch_state_jmx=False):
        if num_nodes is None:
            num_nodes = self.num_brokers
        self._switched_topics = set()
        jmx_object_names = self.SWITCH_JMX_OBJECT_NAMES if scrape_switch_state_jmx else \
            self.SWITCH_COMPLETION_JMX_OBJECT_NAMES
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=num_nodes,
            zk=None,
            controller_num_nodes_override=controller_num_nodes,
            server_prop_overrides=[
                ["diskless.managed.rf.enable", "true"],
            ],
            jmx_object_names=jmx_object_names,
            jmx_attributes=self.SWITCH_JMX_ATTRIBUTES,
        )
        # Switch configs (diskless.allow.from.classic.enable) require
        # remote.log.storage.system.enable, which needs RSM class names.
        # The NoOp classes are test-only and absent from the broker runtime
        # classpath, but the controller never instantiates RemoteLogManager
        # so referencing them there is safe. We must copy the list first
        # because both KafkaService instances share the same list object.
        controller_only_overrides = [
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

        storage_dir = os.path.join(KafkaService.PERSISTENT_ROOT, "kafka-tiered-storage")

        common_overrides = [
            ["diskless.managed.rf.enable", "true"],
            ["remote.log.storage.system.enable", "true"],
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
                # 1 MiB is the enforced minimum for segment.bytes.
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

    def _switch_topic_to_diskless(self, topic=None):
        if topic is None:
            topic = self.topic
        self.logger.info("Switching topic %s to diskless", topic)
        self._switched_topics.add(topic)
        self.kafka.alter_topic_config(topic, "diskless.enable", "true")

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

    def _wait_for_switch_complete(self, topic=None, timeout_sec=None):
        """Wait until the classic-to-diskless init work has drained.

        The topic config becomes visible before leaders finish sealing their
        classic logs and before InitDisklessLogManager commits the diskless
        start offsets. Poll broker-side JMX so post-switch produce/read
        steps do not race the switch-pending state.
        """
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
            lambda: producer.num_acked >= num_messages or producer.worker_errors,
            timeout_sec=self.PRODUCE_TIMEOUT_SEC,
            err_msg="Producer failed to produce %d messages in %ds" % (num_messages, self.PRODUCE_TIMEOUT_SEC)
        )
        assert not producer.worker_errors, "Unexpected producer errors: %s" % producer.worker_errors
        acked = producer.num_acked
        producer.stop()
        producer.free()
        return acked

    def _consume_all_from_beginning(self, expected_count, topic=None, timeout_sec=None,
                                    wait_for_completion=False):
        """Start a fresh consumer from the beginning and collect all messages.

        Returns the number of messages consumed. This is the Phase 2
        validation that catches post-restart classic data availability bugs.
        Set wait_for_completion when the caller needs an exact count rather
        than stopping as soon as the expected minimum is observed.

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
        )
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

    def _rolling_restart(self, clean_shutdown=True):
        self.logger.info("Performing rolling restart of all brokers")
        for node in self.kafka.nodes:
            self._restart_broker(node, clean_shutdown=clean_shutdown)
            self._wait_for_all_partitions_have_leaders()
            # Pace the next bounce on evidence that this broker is fetching again, without
            # forcing all partitions to fully heal between every restart under degraded network.
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

    def _run_unclean_leader_election(self, topic=None, partition=0):
        if topic is None:
            topic = self.topic
        node = self.kafka.nodes[0]
        cmd = "%s --bootstrap-server %s --election-type unclean --topic %s --partition %d" % (
            self.kafka.path.script("kafka-leader-election.sh", node),
            self.kafka.bootstrap_servers(),
            topic,
            partition,
        )
        self.logger.info("Running unclean leader election: %s", cmd)
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

        classic_count = self._produce_messages(num_messages=15000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        leader_node = self._get_leader_node(partition=0)
        self._restart_broker(leader_node)
        self._wait_for_all_partitions_isr_full()

        consumed = self._consume_all_from_beginning(expected_count=total, timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total, "Expected exactly %d messages after restart but got %d" % (total, consumed)

        self._rolling_restart()
        self._wait_for_all_partitions_isr_full()

        consumed = self._consume_all_from_beginning(expected_count=total, timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total, "Expected exactly %d messages after rolling restart but got %d" % (total, consumed)

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_available_after_leader_failures(self, metadata_quorum) -> None:
        """After switch, verify classic + diskless data remains readable
        while the old leader is down and after an unclean broker restart."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        classic_count = self._produce_messages(num_messages=10000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=True)
        self._wait_for_all_partitions_have_leaders()

        consumed = self._consume_all_from_beginning(expected_count=total, timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)

        self._start_broker(leader_node)

        assert consumed == total, "Classic data unavailable after leader change: expected exactly %d but got %d" % (total, consumed)

        self._wait_for_all_partitions_isr_full()
        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=False)

        self._start_broker(leader_node)
        self._wait_for_all_partitions_isr_full()

        consumed = self._consume_all_from_beginning(expected_count=total, timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)

        assert consumed == total, "Classic data lost after unclean shutdown: expected exactly %d but got %d" % (total, consumed)

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_switched_leader_promotion_restores_stale_hw(self, metadata_quorum) -> None:
        """Promote an already-switched replica whose local HW is stale.

        The CI failure this guards against is not missing log data: the promoted
        replica has the whole classic prefix up to the committed seal, but its
        cached high-watermark is below the seal. Without the leader-promotion
        reconcile, consumers are clamped at that stale HW and never reach the
        diskless tail. This test injects that state directly by rewriting the
        preferred replica's HW checkpoint while it is stopped, then promoting it
        after it has come back as an existing follower partition.
        """
        self.num_partitions = 1
        self._create_kafka()
        self.kafka.start()

        stale_preferred = self.kafka.nodes[0]
        switch_leader = self.kafka.nodes[1]
        other_replica = self.kafka.nodes[2]
        replica_assignment = "%d:%d:%d" % (
            self.kafka.idx(stale_preferred),
            self.kafka.idx(switch_leader),
            self.kafka.idx(other_replica),
        )
        self._create_classic_topic(
            num_partitions=1,
            replica_assignment=replica_assignment,
        )

        # The preferred replica starts as leader. Move leadership away before
        # producing/switching so it is a follower with a complete classic prefix.
        self._wait_for_partition_leader(stale_preferred, partition=0)
        self._stop_broker(stale_preferred, clean_shutdown=True)
        self._wait_for_partition_leader(switch_leader, partition=0)
        self._start_broker(stale_preferred)
        self._wait_for_all_partitions_isr_full(num_partitions=1)
        self._wait_for_partition_leader(switch_leader, partition=0)

        classic_count = self._produce_messages(num_messages=5000)

        self._switch_topic_to_diskless()
        self._wait_for_switch_complete()

        diskless_count = self._produce_messages(num_messages=1000)
        total = classic_count + diskless_count

        stale_hw = classic_count // 2
        assert stale_hw > 0 and stale_hw < classic_count

        # Enable unclean election only after the switch has committed. The
        # switch precondition rejects topics with unclean election enabled, but
        # the forced promotion below needs it to elect the isolated replica.
        self.kafka.alter_topic_config(
            self.topic,
            "unclean.leader.election.enable",
            "true",
        )

        # Restart the preferred follower with a stale HW checkpoint. Keep it
        # partitioned from the current ISR so its follower fetcher cannot heal
        # the HW before we promote it.
        self._stop_broker(stale_preferred, clean_shutdown=True)
        self._set_replication_checkpoint_offset(
            stale_preferred,
            partition=0,
            offset=stale_hw,
        )

        self._start_trogdor()
        partition_fault = None
        try:
            partition_spec = NetworkPartitionFaultSpec(
                0,
                5 * 60 * 1000,
                [[stale_preferred], [switch_leader, other_replica]],
            )
            partition_fault = self.trogdor.create_task(
                "isolate-stale-hw-preferred",
                partition_spec,
            )
            wait_until(
                lambda: partition_fault.running(),
                timeout_sec=30,
                backoff_sec=1,
                err_msg="Network partition fault did not start before stale broker restart",
            )

            self._start_broker(stale_preferred)

            # Take down the in-sync replicas so the isolated preferred replica
            # is the only live replica and the unclean election picks it. This
            # is deliberately a harness shortcut; the data-loss concern is
            # avoided because the edited replica's LEO is the committed seal.
            self.logger.info("Stopping ISR brokers %s and %s to force stale-HW leader promotion",
                             switch_leader.account.hostname, other_replica.account.hostname)
            self.kafka.stop_node(switch_leader, clean_shutdown=False,
                                 timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)
            self.kafka.stop_node(other_replica, clean_shutdown=False,
                                 timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)
            self._run_unclean_leader_election(partition=0)
            self._wait_for_partition_leader(stale_preferred, partition=0)

            partition_fault.stop()
            partition_fault.wait_for_done(timeout_sec=60)
            partition_fault = None
        finally:
            if partition_fault is not None:
                partition_fault.stop()
                partition_fault.wait_for_done(timeout_sec=60)
            self._stop_trogdor()

        consumed = self._consume_all_from_beginning(
            expected_count=total,
            timeout_sec=self.CONSUME_TIMEOUT_SEC,
            wait_for_completion=True,
        )
        assert consumed == total, \
            ("Stale-HW switched leader served only %d of %d records "
             "(classic=%d, diskless=%d, injected stale HW=%d)") % \
            (consumed, total, classic_count, diskless_count, stale_hw)

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

        producer.stop()
        total_produced = producer.num_acked
        producer.free()

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

        producer.stop()
        total_produced = producer.num_acked
        producer.free()

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count after leader restart during switch: expected exactly %d but got %d" % (total_produced, consumed)

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
        producer.stop()
        total_produced = producer.num_acked
        producer.free()

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count after leader %s during switch: expected exactly %d but got %d" % \
            (leader_failure_mode, total_produced, consumed)

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
        producer.stop()
        total_produced = producer.num_acked
        producer.free()

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

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count after follower SIGKILL during switch: expected exactly %d but got %d" % \
            (total_produced, consumed)

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
        producer.stop()
        total_produced = producer.num_acked
        producer.free()

        self._assert_mid_switch_observed()

        degrade.stop()
        degrade.wait_for_done(timeout_sec=60)
        self._stop_trogdor()

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count after rolling restart during switch: expected exactly %d but got %d" % (total_produced, consumed)

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

        partition_spec = NetworkPartitionFaultSpec(
            0, 5 * 60 * 1000,
            [[follower], [n for n in self.kafka.nodes if n != follower]]
        )
        fault = self.trogdor.create_task("follower_partition", partition_spec)

        self._switch_topic_to_diskless()

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

        producer.stop()
        total_produced = producer.num_acked
        producer.free()

        self._assert_mid_switch_observed(require_states=("WaitingForReplication",))
        self._stop_trogdor()

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count with follower partition during switch: expected exactly %d but got %d" % (total_produced, consumed)

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

        self._switch_topic_to_diskless()

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
        producer.stop()
        total_produced = producer.num_acked
        producer.free()

        self._assert_mid_switch_observed(require_states=("WaitingForReplication",))
        self._stop_trogdor()

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count with leader partition during switch: expected exactly %d but got %d" % (total_produced, consumed)

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
        produced_counts = {}

        for topic in topics:
            self._create_classic_topic(topic=topic, num_partitions=4)
            produced_counts[topic] = self._produce_messages(topic=topic, num_messages=5000)

        for topic in topics:
            self._switch_topic_to_diskless(topic=topic)

        for topic in topics:
            self._wait_for_switch_complete(topic=topic)

        for topic in topics:
            post_count = self._produce_messages(topic=topic, num_messages=2000)
            produced_counts[topic] += post_count

        for topic in topics:
            consumed = self._consume_all_from_beginning(
                topic=topic,
                expected_count=produced_counts[topic],
                timeout_sec=self.CONSUME_TIMEOUT_SEC,
                wait_for_completion=True
            )
            assert consumed == produced_counts[topic], \
                "Unexpected message count on topic %s: expected exactly %d but got %d" % \
                (topic, produced_counts[topic], consumed)

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

        remote_count = self._produce_messages(topic=topic, num_messages=8000)
        self._wait_for_local_log_truncation(topic=topic)

        local_count = self._produce_messages(topic=topic, num_messages=2000)

        self._switch_topic_to_diskless(topic=topic)
        self._wait_for_switch_complete(topic=topic)

        diskless_count = self._produce_messages(topic=topic, num_messages=1000)

        total = remote_count + local_count + diskless_count

        consumed = self._consume_all_from_beginning(
            topic=topic,
            expected_count=total,
            timeout_sec=self.CONSUME_TIMEOUT_SEC,
            wait_for_completion=True,
        )
        assert consumed == total, \
            "Cross-tier consumption (remote/local/diskless) failed: expected exactly %d but got %d" % \
            (total, consumed)

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

        producer.stop()
        total_produced = producer.num_acked
        producer.free()

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC,
                                                    wait_for_completion=True)
        assert consumed == total_produced, \
            "Unexpected message count with idempotent producer: expected exactly %d but got %d" % \
            (total_produced, consumed)
