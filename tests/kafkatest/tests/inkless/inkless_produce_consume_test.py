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
from typing import Dict, Any

from antithesis.assertions import always_or_unreachable, unreachable, reachable
from ducktape.mark import matrix
from ducktape.mark.resource import cluster
from ducktape.tests.test import Test, TestContext
from ducktape.utils.util import wait_until

from kafkatest.services.kafka import KafkaService, quorum
from kafkatest.services.verifiable_consumer import VerifiableConsumer
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.utils import is_int_with_prefix


class InklessProduceConsumeTest(Test):
    def __init__(self, test_context: TestContext) -> None:
        super(InklessProduceConsumeTest, self).__init__(test_context=test_context)

        self.num_brokers = 3

        self.topic1 = "topic1"
        self.topic1_partitions = 10

    @cluster(num_nodes=9)
    @matrix(
        metadata_quorum=quorum.all_kraft,
    )
    def test_inkless(self, metadata_quorum) -> None:
        self.kafka = KafkaService(self.test_context,
                                  num_nodes=self.num_brokers,
                                  zk=None,
                                  controller_num_nodes_override=1)
        security_protocol = 'PLAINTEXT'
        self.kafka.security_protocol = security_protocol
        self.kafka.interbroker_security_protocol = security_protocol
        self.kafka.logs["kafka_data_1"]["collect_default"] = True
        self.kafka.logs["kafka_data_2"]["collect_default"] = True
        self.kafka.logs["kafka_operational_logs_debug"]["collect_default"] = True

        self.setup_topics()
        self.kafka.start()

        produce_timeout_sec = 60_000
        consume_timeout_sec = 60_000
        num_messages = 100_000
        num_producers = 3
        num_consumers = 2

        expected_messages = num_messages * num_producers
        producers = VerifiableProducer(context=self.test_context,
                                       num_nodes=num_producers,
                                       kafka=self.kafka,
                                       topic=self.topic1,
                                       max_messages=num_messages,
                                       message_validator=is_int_with_prefix,
                                       enable_idempotence=True,
                                       repeating_keys=num_messages)
        producers.start()

        consumed: Dict[int, Dict[int, Dict[str, Any]]] = {}
        def on_record_consumed(event: Dict[str, Any], node):
            partition = event["partition"]
            offset = event["offset"]
            if partition not in consumed:
                consumed[partition] = {}
            if offset not in consumed[partition]:
                consumed[partition][offset] = event
            else:
                # There may be reconsumption during retries, avoid failing in such cases.
                prev_event = consumed[partition][offset]
                if prev_event["key"] != event["key"] or prev_event["value"] != event["value"]:
                    unreachable("Only one record per offset can be consumed", {
                        "partition": partition,
                        "offset": offset,
                        "records": [prev_event, event]
                    })
                    assert False, f"Only one record per offset can be consumed, but found duplicate in partition {partition}, offset {offset}"
        consumer = VerifiableConsumer(context=self.test_context,
                                      num_nodes=num_consumers,
                                      kafka=self.kafka,
                                      topic=self.topic1,
                                      group_id="test-group",
                                      on_record_consumed=on_record_consumed)
        consumer.start()

        wait_until(lambda: producers.num_acked >= expected_messages or producers.worker_errors,
                   timeout_sec=produce_timeout_sec,
                   err_msg="Producer failed to produce messages %d in %ds." %\
                   (num_messages, produce_timeout_sec))
        wait_until(lambda: consumer.total_consumed() >= expected_messages or consumer.worker_errors,
                   timeout_sec=consume_timeout_sec,
                   err_msg="Concurrent consumer failed to consume messages %d in %ds." %\
                   (num_messages, consume_timeout_sec))

        # If there were errors in either producer or consumer, just abort the test, there's no point checking further.
        assert not producers.worker_errors, f"Unexpected producer errors: {producers.worker_errors}"
        assert not consumer.worker_errors, f"Unexpected consumer errors: {consumer.worker_errors}"

        # Rearrange produced events to compare conveniently later.
        produced: Dict[int, Dict[int, Dict[str, Any]]] = {}
        for event in producers.acked_values_full:
            partition = event["partition"]
            offset = event["offset"]
            if partition not in produced:
                produced[partition] = {}
            if offset not in produced[partition]:
                produced[partition][offset] = event
            else:
                unreachable("Only one record per offset can be produced", {
                    "partition": partition,
                    "offset": offset,
                    "records": [consumed[partition][offset], event]
                })
                assert False, f"Only one record per offset can be produced, but found duplicate in partition {partition}, offset {offset}"

        consumed_partitions = set(consumed.keys())
        produced_partitions = set(produced.keys())
        condition = consumed_partitions == produced_partitions
        always_or_unreachable(condition, "Consumers and producers must have same partition set",
                              {
                                  "consumed_partitions": list(consumed_partitions),
                                  "produced_partitions": list(produced_partitions),
                              })
        assert condition, f"Consumers and producers must have same partition set, was consumer: {consumed_partitions}, producer: {produced_partitions}"

        for partition in produced_partitions:
            produced_records = produced[partition]
            consumed_records = consumed[partition]

            # At least what's produced, must be consumed.
            produced_offsets = set(produced_records.keys())
            consumed_offsets = set(consumed_records.keys())
            condition = produced_offsets.issubset(consumed_offsets)
            in_producer_not_in_consumer = produced_offsets - consumed_offsets
            in_consumer_not_in_producer = consumed_offsets - produced_offsets
            always_or_unreachable(condition, "Produced offsets must be subset of consumed offsets",
                                  {
                                      "partition": partition,
                                      "in_producer_not_in_consumer": in_producer_not_in_consumer,
                                      "in_consumer_not_in_producer": in_consumer_not_in_producer,
                                  })
            assert condition, (f"Produced offsets must be subset of consumed offsets. "
                               f"In partition {partition}: "
                               f"In producer not in consumer: {in_producer_not_in_consumer}, "
                               f"in consumer not in producer: {in_consumer_not_in_producer}")

            for offset in produced_offsets:
                produced_record = produced_records[offset]
                del produced_record["name"]
                del produced_record["timestamp"]
                consumed_record = consumed_records[offset]
                del consumed_record["name"]
                del consumed_record["timestamp"]
                condition = produced_record == consumed_record
                always_or_unreachable(condition, "Records must match", {
                    "produced_record": produced_record,
                    "consumed_record": consumed_record,
                })
                assert condition, (f"Records must match, but don't in partition {partition} at offset {offset}: "
                                   f"produced {produced_record}, consumed {consumed_record}")

            # TODO do something with extra consumed

        reachable("Test finished", {})

    def setup_topics(self) -> None:
        # TODO mix Inkless and classic partitions
        self.kafka.topics = {
            self.topic1: {
                "partitions": self.topic1_partitions,
                "replication-factor": 1,
                "configs": {"diskless.enable": "true"}
            },
        }
