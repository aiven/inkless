use std::collections::BTreeMap;

use crate::domain::{
    ConsumerGroupID, TestEvent, TestLogLine, TestLogLocation, TestValidator, TopicName,
    TopicPartitionIndex, TopicPartitionOffset,
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json};


/// ConsumerMessageOrderingValidator verifies that the consumer always reads messages with offsets that are greater than the last committed offset
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ConsumerMessageOrderingValidator {
    topic_partition_offsets: BTreeMap<
        ConsumerGroupID,
        BTreeMap<TopicName, BTreeMap<TopicPartitionIndex, TestLogLocation<TopicPartitionOffset>>>,
    >,
}

impl TestValidator for ConsumerMessageOrderingValidator {
    fn validator_name(&self) -> &'static str {
        "consumer-message-ordering"
    }

    fn validate_event(&mut self, log: &TestLogLine){
        match &log.data.fields {
            TestEvent::ConsumerMessagesCommitted(event) => {
                for (topic_name, partitions) in event.topic_partition_list.0.iter() {
                    for (partition_index, committed_offsets) in partitions {
                        // Ignore invalid offset values
                        if committed_offsets[0] >= 0 {
                            self.topic_partition_offsets
                                .entry(event.consumer.group_id.clone())
                                .or_default()
                                .entry(topic_name.clone())
                                .or_default()
                                .entry(*partition_index)
                                .and_modify(|e| {
                                    *e = log.capture(committed_offsets[0]);
                                })
                                .or_insert(log.capture(committed_offsets[0]));
                        }
                    }
                }
            }
            TestEvent::ConsumerMessageCommitted(event) => {
                // Ignore invalid offset values
                self.topic_partition_offsets
                    .entry(event.consumer.group_id.clone())
                    .or_default()
                    .entry(event.metadata.topic_name.clone())
                    .or_default()
                    .entry(event.metadata.topic_partition)
                    .and_modify(|e| {
                        *e = log.capture(event.metadata.topic_partition_offset);
                    })
                    .or_insert(log.capture(event.metadata.topic_partition_offset));
            }
            TestEvent::MessageReadSucceeded(event) => {
                if let Some(last_committed_offset) = self
                    .topic_partition_offsets
                    .get(&event.consumer.group_id)
                    .and_then(|topic_partition_offsets| {
                        topic_partition_offsets.get(&event.message.metadata.topic_name)
                    })
                    .and_then(|partitions| partitions.get(&event.message.metadata.topic_partition))
                {
                    if event.message.metadata.topic_partition_offset < last_committed_offset.data {
                        let details = json!({"consumer": event.consumer, "message": event.message, "last_committed_offset_data": last_committed_offset.data, "last_committed_offset_location": last_committed_offset.location()});
                        antithesis_sdk::assert_unreachable!("Message offset is less than the last committed offset for this consumer group", &details);
                    }
                }
            }
            _ => {}
        }
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: ConsumerMessageOrderingValidator = serde_json::from_str(data)?;
        self.topic_partition_offsets = instance.topic_partition_offsets;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
