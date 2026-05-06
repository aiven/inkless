# Inkless
# Copyright (C) 2024 - 2025 Aiven OY
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
import time
import uuid

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test, TestContext
from ducktape.utils.util import wait_until

from kafkatest.services.console_consumer import ConsoleConsumer
from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.trogdor.network_partition_fault_spec import NetworkPartitionFaultSpec
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


class InklessTopicMigrationTest(Test):
    """System tests for classic-to-diskless topic migration.

    Tests are organized into three categories:
      A) Post-migration data availability (classic data after broker restarts)
      B) Mid-migration fault tolerance (faults injected during migration)
      C) Operational scenarios (concurrent migration, producer state)
    """

    MIGRATION_TIMEOUT_SEC = 120
    PRODUCE_TIMEOUT_SEC = 120
    CONSUME_TIMEOUT_SEC = 120
    BROKER_STARTUP_TIMEOUT_SEC = 120

    def __init__(self, test_context: TestContext) -> None:
        super(InklessTopicMigrationTest, self).__init__(test_context=test_context)
        self.num_brokers = 3
        self.topic = "migration-test-topic"
        self.num_partitions = 6
        self.replication_factor = 3

    # -----------------------------------------------------------------------
    # Cluster setup
    # -----------------------------------------------------------------------

    def _create_kafka(self, num_nodes=None, controller_num_nodes=1):
        if num_nodes is None:
            num_nodes = self.num_brokers
        self.kafka = KafkaService(
            self.test_context,
            num_nodes=num_nodes,
            zk=None,
            controller_num_nodes_override=controller_num_nodes,
            server_prop_overrides=[
                ["diskless.managed.rf.enable", "true"],
            ],
        )
        # Migration configs (diskless.allow.from.classic.enable) require
        # remote.log.storage.system.enable, which needs RSM class names.
        # The NoOp classes are test-only and absent from the broker runtime
        # classpath, but the controller never instantiates RemoteLogManager
        # so referencing them there is safe. We must copy the list first
        # because both KafkaService instances share the same list object.
        controller_only_overrides = [
            ["diskless.allow.from.classic.enable", "true"],
            ["remote.log.storage.system.enable", "true"],
            ["remote.storage.manager.class.name",
             "org.apache.kafka.server.log.remote.storage.NoOpRemoteStorageManager"],
            ["remote.log.metadata.manager.class.name",
             "org.apache.kafka.server.log.remote.storage.NoOpRemoteLogMetadataManager"],
        ]
        if hasattr(self.kafka, 'isolated_controller_quorum') and self.kafka.isolated_controller_quorum:
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + controller_only_overrides

        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

    def _create_classic_topic(self, topic=None, num_partitions=None):
        if topic is None:
            topic = self.topic
        if num_partitions is None:
            num_partitions = self.num_partitions
        self.kafka.create_topic({
            "topic": topic,
            "partitions": num_partitions,
            "replication-factor": self.replication_factor,
            "configs": {
                "min.insync.replicas": 2,
                "diskless.enable": "false",
            }
        })

    def _create_kafka_with_tiered_storage(self):
        """Single-broker cluster with real (LocalTieredStorage) tiered storage
        on the broker, plus the diskless-from-classic migration bridge.

        Single broker keeps the test self-contained: LocalTieredStorage uses
        a per-broker filesystem path, so multi-broker reads from remote would
        require shared storage or sticky leadership - neither is needed to
        exercise the classic-tiered to diskless migration path.
        """
        self.replication_factor = 1
        self.num_partitions = 1

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
        )
        _enable_tiered_storage_classpath(self.kafka)
        if hasattr(self.kafka, 'isolated_controller_quorum') and self.kafka.isolated_controller_quorum:
            ctrl = self.kafka.isolated_controller_quorum
            ctrl.server_prop_overrides = list(ctrl.server_prop_overrides) + controller_overrides

        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

    def _create_classic_tiered_topic(self, topic, num_partitions=1):
        """Classic (non-diskless) topic with tiered storage enabled and very
        short local retention so closed segments are uploaded to remote and
        then deleted from the local log directory.

        ``segment.bytes`` has a hard floor of 1 MiB (LogConfig.validate),
        so we additionally force time-based rolling via ``segment.ms`` to
        produce closed segments quickly without needing a huge produce
        volume.
        """
        # NOTE: We can't pass both ``diskless.enable=false`` and
        # ``remote.storage.enable=true`` at create time - the broker rejects
        # that combination outside of an active migration. Diskless defaults
        # to false (cluster default ``log.diskless.enable=false``), so simply
        # omitting ``diskless.enable`` produces a classic+tiered topic.
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
    # Helpers: migration
    # -----------------------------------------------------------------------

    def _migrate_topic_to_diskless(self, topic=None):
        if topic is None:
            topic = self.topic
        self.logger.info("Migrating topic %s to diskless", topic)
        self.kafka.alter_topic_config(topic, "diskless.enable", "true")

    def _wait_for_migration_config(self, topic=None, timeout_sec=None):
        """Wait until kafka-configs reports diskless.enable=true for the topic."""
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.MIGRATION_TIMEOUT_SEC

        def check():
            try:
                config = self.kafka.describe_topic_config(topic)
                return config.get("diskless.enable") == "true"
            except Exception:
                return False

        wait_until(check, timeout_sec=timeout_sec, backoff_sec=2,
                   err_msg="Topic %s did not become diskless within %ds" % (topic, timeout_sec))

    def _wait_for_migration_complete(self, topic=None, timeout_sec=None):
        """Wait for the migration config to be applied.

        This polls the topic config to confirm diskless.enable=true.
        After the config is applied, a brief settle period allows the
        init state machine to complete on all partitions.
        """
        if topic is None:
            topic = self.topic
        self._wait_for_migration_config(topic, timeout_sec)
        time.sleep(10)

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

    def _start_continuous_consumer(self, topic=None, group_id=None, num_nodes=1):
        if topic is None:
            topic = self.topic
        if group_id is None:
            group_id = "continuous-%s" % str(uuid.uuid4())[:8]
        consumer = VerifiableConsumer(
            context=self.test_context,
            num_nodes=num_nodes,
            kafka=self.kafka,
            topic=topic,
            group_id=group_id,
            enable_autocommit=True,
        )
        consumer.start()
        return consumer

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

    def _consume_all_from_beginning(self, topic=None, expected_count=0, timeout_sec=None):
        """Start a fresh consumer from the beginning and collect all messages.

        Returns the number of messages consumed. This is the Phase 2
        validation that catches post-restart classic data availability bugs.
        """
        if topic is None:
            topic = self.topic
        if timeout_sec is None:
            timeout_sec = self.CONSUME_TIMEOUT_SEC

        group_id = "fresh-%s" % str(uuid.uuid4())[:8]
        consumer = ConsoleConsumer(
            context=self.test_context,
            num_nodes=1,
            kafka=self.kafka,
            topic=topic,
            group_id=group_id,
            from_beginning=True,
            consumer_timeout_ms=30000,
            isolation_level="read_committed",
            print_key=True,
        )
        consumer.start()

        if expected_count > 0:
            consumer_seen_alive = [False]

            def _check_consumed():
                if len(consumer.messages_consumed[1]) >= expected_count:
                    return True
                is_alive = consumer.alive(consumer.nodes[0])
                if is_alive:
                    consumer_seen_alive[0] = True
                return consumer_seen_alive[0] and not is_alive

            wait_until(
                _check_consumed,
                timeout_sec=timeout_sec,
                backoff_sec=2,
                err_msg="Fresh consumer consumed only %d out of %d expected messages in %ds" %
                        (len(consumer.messages_consumed[1]), expected_count, timeout_sec)
            )
        else:
            time.sleep(15)

        consumer.stop()
        consumed = len(consumer.messages_consumed[1])

        if consumed == 0 and expected_count > 0:
            node = consumer.nodes[0]
            try:
                count_output = node.account.ssh_output(
                    "wc -l < %s" % ConsoleConsumer.STDOUT_CAPTURE
                )
                if isinstance(count_output, bytes):
                    count_output = count_output.decode('utf-8')
                file_count = int(count_output.strip())
                if file_count > 0:
                    self.logger.info(
                        "messages_consumed was empty but stdout file has %d lines; "
                        "using file count (ssh_capture may not have delivered output)",
                        file_count)
                    consumed = file_count
            except Exception as e:
                self.logger.warn("Failed to read stdout capture file: %s", str(e))

        self.logger.info("Fresh consumer consumed %d messages from topic %s (expected >= %d)",
                         consumed, topic, expected_count)
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

    def _wait_for_steady_production(self, producer, min_acked=5000, timeout_sec=60):
        wait_until(
            lambda: producer.num_acked >= min_acked or producer.worker_errors,
            timeout_sec=timeout_sec,
            err_msg="Producer did not reach %d acks in %ds" % (min_acked, timeout_sec)
        )
        assert not producer.worker_errors, "Unexpected producer errors: %s" % producer.worker_errors

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

    def _restart_broker(self, node, clean_shutdown=True):
        self.logger.info("Restarting broker %s (clean=%s)", node.account.hostname, clean_shutdown)
        self.kafka.restart_node(node, clean_shutdown=clean_shutdown,
                                timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)

    def _stop_broker(self, node, clean_shutdown=True):
        self.logger.info("Stopping broker %s (clean=%s)", node.account.hostname, clean_shutdown)
        self.kafka.stop_node(node, clean_shutdown=clean_shutdown,
                             timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)
        if not clean_shutdown:
            time.sleep(5)

    def _start_broker(self, node):
        self.logger.info("Starting broker %s", node.account.hostname)
        self.kafka.start_node(node, timeout_sec=self.BROKER_STARTUP_TIMEOUT_SEC)

    def _rolling_restart(self, clean_shutdown=True):
        self.logger.info("Performing rolling restart of all brokers")
        for node in self.kafka.nodes:
            self._restart_broker(node, clean_shutdown=clean_shutdown)
            time.sleep(5)

    def _wait_for_isr_full(self, topic=None, partition=0, timeout_sec=120):
        if topic is None:
            topic = self.topic
        expected_isr_size = self.replication_factor
        wait_until(
            lambda: len(self.kafka.isr_idx_list(topic, partition=partition)) == expected_isr_size,
            timeout_sec=timeout_sec, backoff_sec=2,
            err_msg="ISR did not return to full size %d for %s-%d" % (expected_isr_size, topic, partition)
        )

    # -----------------------------------------------------------------------
    # Category A: Post-Migration Data Availability
    # -----------------------------------------------------------------------

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_available_after_broker_restart(self, metadata_quorum) -> None:
        """A1: After migration completes, restart one broker and verify that a
        fresh consumer can read ALL data (classic + diskless) from the beginning.
        Directly targets the applyLocalLeadersDelta Partition creation bug."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        classic_count = self._produce_messages(num_messages=10000)

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        leader_node = self._get_leader_node(partition=0)
        self._restart_broker(leader_node)
        time.sleep(10)

        consumed = self._consume_all_from_beginning(expected_count=total,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total, \
            "Expected at least %d messages after restart but got %d" % (total, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_available_after_rolling_restart(self, metadata_quorum) -> None:
        """A2: After migration completes, rolling restart ALL brokers and verify
        classic + diskless data is fully readable."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        classic_count = self._produce_messages(num_messages=10000)

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        self._rolling_restart()
        time.sleep(10)

        consumed = self._consume_all_from_beginning(expected_count=total,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total, \
            "Expected at least %d messages after rolling restart but got %d" % (total, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_cross_boundary_consumption(self, metadata_quorum) -> None:
        """A3: After migration + restart, a single consumer session reads
        seamlessly across the classic-to-diskless offset boundary."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        classic_count = self._produce_messages(num_messages=5000)

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        leader_node = self._get_leader_node(partition=0)
        self._restart_broker(leader_node)
        time.sleep(10)

        consumed = self._consume_all_from_beginning(expected_count=total,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total, \
            "Cross-boundary consumption failed: expected >= %d but got %d" % (total, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_after_leader_change_post_migration(self, metadata_quorum) -> None:
        """A4: After migration, force leadership change by killing the leader.
        Fresh consumer on new leader must serve classic data."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        classic_count = self._produce_messages(num_messages=10000)

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=True)
        time.sleep(10)

        consumed = self._consume_all_from_beginning(expected_count=total,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)

        self._start_broker(leader_node)

        assert consumed >= total, \
            "Classic data unavailable after leader change: expected >= %d but got %d" % (total, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_data_after_unclean_shutdown_post_migration(self, metadata_quorum) -> None:
        """A5: After migration, hard-kill a broker and verify classic data
        survives unclean shutdown."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        classic_count = self._produce_messages(num_messages=10000)

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        diskless_count = self._produce_messages(num_messages=5000)
        total = classic_count + diskless_count

        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=False)

        self._start_broker(leader_node)
        time.sleep(10)

        consumed = self._consume_all_from_beginning(expected_count=total,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total, \
            "Classic data lost after unclean shutdown: expected >= %d but got %d" % (total, consumed)

    # -----------------------------------------------------------------------
    # Category B: Mid-Migration Fault Tolerance
    # -----------------------------------------------------------------------

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_happy_path(self, metadata_quorum) -> None:
        """B1: Migrate a classic topic to diskless under continuous load.
        Verify no data loss."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        producer = self._start_producer(max_messages=50000)
        consumer = self._start_continuous_consumer()

        self._wait_for_steady_production(producer, min_acked=10000)

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        self._wait_for_steady_production(producer, min_acked=30000)

        producer.stop()
        total_produced = producer.num_acked

        wait_until(
            lambda: consumer.total_consumed() >= total_produced or consumer.worker_errors,
            timeout_sec=self.CONSUME_TIMEOUT_SEC,
            err_msg="Consumer consumed only %d out of %d produced" %
                    (consumer.total_consumed(), total_produced)
        )
        assert not consumer.worker_errors, "Consumer errors: %s" % consumer.worker_errors
        consumer.stop()

        assert consumer.total_consumed() >= total_produced, \
            "Data loss: produced %d but consumed only %d" % (total_produced, consumer.total_consumed())

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_leader_restart(self, metadata_quorum) -> None:
        """B2: Restart the leader broker during migration. Verify migration
        completes and all data is readable via fresh consumer."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._migrate_topic_to_diskless()
        time.sleep(2)

        leader_node = self._get_leader_node(partition=0)
        self._restart_broker(leader_node)

        self._wait_for_migration_complete()
        self._wait_for_steady_production(producer, min_acked=producer.num_acked + 5000)

        producer.stop()
        total_produced = producer.num_acked

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total_produced, \
            "Data loss after leader restart during migration: expected >= %d but got %d" % (total_produced, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_leader_crash(self, metadata_quorum) -> None:
        """B3: Hard-kill the leader during migration. Verify new leader
        completes migration, then fresh consumer reads all data."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._migrate_topic_to_diskless()
        time.sleep(2)

        leader_node = self._get_leader_node(partition=0)
        self._stop_broker(leader_node, clean_shutdown=False)

        time.sleep(10)
        self._start_broker(leader_node)

        self._wait_for_migration_complete()

        target_acked = producer.num_acked + 5000
        wait_until(
            lambda: producer.num_acked >= target_acked or producer.worker_errors,
            timeout_sec=60,
            err_msg="Producer did not recover after leader crash"
        )
        producer.stop()
        total_produced = producer.num_acked

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total_produced, \
            "Data loss after leader crash during migration: expected >= %d but got %d" % (total_produced, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_rolling_restart(self, metadata_quorum) -> None:
        """B4: Rolling restart all brokers during migration. Verify migration
        completes and data is fully readable."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        self._migrate_topic_to_diskless()
        time.sleep(2)

        self._rolling_restart()

        self._wait_for_migration_complete()

        target_acked = producer.num_acked + 5000
        wait_until(
            lambda: producer.num_acked >= target_acked or producer.worker_errors,
            timeout_sec=60,
            err_msg="Producer did not recover after rolling restart"
        )
        producer.stop()
        total_produced = producer.num_acked

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total_produced, \
            "Data loss after rolling restart during migration: expected >= %d but got %d" % (total_produced, consumed)

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_follower_network_partition(self, metadata_quorum) -> None:
        """B5: Isolate a follower via Trogdor network partition during migration.
        Verify ISR shrinks, migration completes, ISR re-expands, no data loss."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka])
        self.trogdor.start()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        leader_node = self._get_leader_node(partition=0)
        follower_nodes = self._get_follower_nodes(partition=0)
        follower = follower_nodes[0]

        partition_spec = NetworkPartitionFaultSpec(
            0, 5 * 60 * 1000,
            [[follower], [n for n in self.kafka.nodes if n != follower]]
        )
        fault = self.trogdor.create_task("follower_partition", partition_spec)

        def isr_shrunk():
            try:
                return len(self.kafka.isr_idx_list(
                    self.topic, partition=0,
                    node=leader_node, offline_nodes=[follower]
                )) < self.replication_factor
            except (Exception, RemoteCommandError):
                return False

        wait_until(isr_shrunk, timeout_sec=120, backoff_sec=2,
                   err_msg="ISR did not shrink after follower partition")

        self._migrate_topic_to_diskless()

        fault.stop()
        fault.wait_for_done(timeout_sec=120)

        self._wait_for_isr_full(partition=0)
        self._wait_for_migration_complete()

        producer.stop()
        total_produced = producer.num_acked

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total_produced, \
            "Data loss with follower partition during migration: expected >= %d but got %d" % (total_produced, consumed)

        self.trogdor.stop()

    @cluster(num_nodes=10)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_leader_network_partition(self, metadata_quorum) -> None:
        """B6: Isolate the leader via Trogdor network partition during migration.
        Verify new leader elected, migration completes, ISR heals, no data loss."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        self.trogdor = TrogdorService(context=self.test_context,
                                      client_services=[self.kafka])
        self.trogdor.start()

        producer = self._start_producer(max_messages=-1)
        self._wait_for_steady_production(producer, min_acked=5000)

        leader_node = self._get_leader_node(partition=0)
        non_leader_nodes = [n for n in self.kafka.nodes if n != leader_node]

        partition_spec = NetworkPartitionFaultSpec(
            0, 5 * 60 * 1000,
            [[leader_node], non_leader_nodes]
        )
        fault = self.trogdor.create_task("leader_partition", partition_spec)

        self._migrate_topic_to_diskless()

        time.sleep(30)

        fault.stop()
        fault.wait_for_done(timeout_sec=120)

        self._wait_for_isr_full(partition=0, timeout_sec=180)
        self._wait_for_migration_complete()

        target_acked = producer.num_acked + 3000
        wait_until(
            lambda: producer.num_acked >= target_acked or producer.worker_errors,
            timeout_sec=120,
            err_msg="Producer did not recover after leader partition"
        )
        producer.stop()
        total_produced = producer.num_acked

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total_produced, \
            "Data loss with leader partition during migration: expected >= %d but got %d" % (total_produced, consumed)

        self.trogdor.stop()

    # -----------------------------------------------------------------------
    # Category C: Operational Scenarios
    # -----------------------------------------------------------------------

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_concurrent_topics(self, metadata_quorum) -> None:
        """C1: Migrate 3 classic topics simultaneously. Verify all migrate
        successfully with no data loss."""
        self._create_kafka()
        self.kafka.start()

        topics = ["concurrent-topic-%d" % i for i in range(3)]
        produced_counts = {}

        for topic in topics:
            self._create_classic_topic(topic=topic, num_partitions=4)
            produced_counts[topic] = self._produce_messages(topic=topic, num_messages=5000)

        for topic in topics:
            self._migrate_topic_to_diskless(topic=topic)

        for topic in topics:
            self._wait_for_migration_complete(topic=topic)

        for topic in topics:
            post_count = self._produce_messages(topic=topic, num_messages=2000)
            produced_counts[topic] += post_count

        for topic in topics:
            consumed = self._consume_all_from_beginning(
                topic=topic,
                expected_count=produced_counts[topic],
                timeout_sec=self.CONSUME_TIMEOUT_SEC
            )
            assert consumed >= produced_counts[topic], \
                "Data loss on topic %s: expected >= %d but got %d" % (topic, produced_counts[topic], consumed)

    @cluster(num_nodes=5)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_classic_tiered_to_diskless_migration(self, metadata_quorum) -> None:
        """C3: Migrate a classic topic with real (LocalTieredStorage) tiered
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
        topic = "tiered-migration-topic"

        self._create_kafka_with_tiered_storage()
        self.kafka.start()
        self._create_classic_tiered_topic(topic=topic)

        remote_count = self._produce_messages(topic=topic, num_messages=8000)
        self._wait_for_local_log_truncation(topic=topic)

        local_count = self._produce_messages(topic=topic, num_messages=2000)

        self._migrate_topic_to_diskless(topic=topic)
        self._wait_for_migration_complete(topic=topic)

        diskless_count = self._produce_messages(topic=topic, num_messages=1000)

        total = remote_count + local_count + diskless_count

        consumed = self._consume_all_from_beginning(
            topic=topic,
            expected_count=total,
            timeout_sec=self.CONSUME_TIMEOUT_SEC,
        )
        assert consumed >= total, \
            "Cross-tier consumption (remote/local/diskless) failed: expected >= %d but got %d" % \
            (total, consumed)

    @cluster(num_nodes=9)
    @matrix(metadata_quorum=[quorum.isolated_kraft])
    def test_migration_idempotent_producer_state(self, metadata_quorum) -> None:
        """C2: Verify idempotent producer state is preserved across the
        migration boundary. The same producer continues producing after
        migration without OutOfOrderSequence errors or duplicates."""
        self._create_kafka()
        self.kafka.start()
        self._create_classic_topic()

        producer = self._start_producer(max_messages=50000, enable_idempotence=True)
        self._wait_for_steady_production(producer, min_acked=10000)

        pre_migration_acked = producer.num_acked

        self._migrate_topic_to_diskless()
        self._wait_for_migration_complete()

        target_acked = pre_migration_acked + 15000
        wait_until(
            lambda: producer.num_acked >= target_acked or producer.worker_errors,
            timeout_sec=self.PRODUCE_TIMEOUT_SEC,
            err_msg="Idempotent producer stalled after migration at %d acks" % producer.num_acked
        )
        assert not producer.worker_errors, \
            "Idempotent producer errors after migration (possible OutOfOrderSequence): %s" % producer.worker_errors

        producer.stop()
        total_produced = producer.num_acked

        consumed = self._consume_all_from_beginning(expected_count=total_produced,
                                                    timeout_sec=self.CONSUME_TIMEOUT_SEC)
        assert consumed >= total_produced, \
            "Data loss with idempotent producer: expected >= %d but got %d" % (total_produced, consumed)
