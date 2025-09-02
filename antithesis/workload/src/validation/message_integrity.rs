use crate::domain::{
    TestEvent, TestLogLine, TestLogLocation, TestValidator, TopicName, TopicPartitionIndex,
    TopicPartitionOffset
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{
    collections::{btree_map::Entry, BTreeMap, hash_map::DefaultHasher},
    hash::{Hasher, Hash}
};
use serde_json::{json};

/// MessageDurabilityValidatorMessage verifies that no message to modified between read and write, and that all messages are seen
/// at least once at the end of the test
/// TODO:
/// - without acks all some messages are lost
/// - without idempotent producers some messages are duplicated
type MessageReadHash = u64;
type MessageWriteHash = u64;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MessageIntegrityValidator {
    messages: BTreeMap<
        TopicName,
        BTreeMap<
            TopicPartitionIndex,
            BTreeMap<
                TopicPartitionOffset,
                (
                    Option<TestLogLocation<MessageReadHash>>,
                    Option<TestLogLocation<MessageWriteHash>>,
                ),
            >,
        >,
    >,
}

impl TestValidator for MessageIntegrityValidator {
    fn validator_name(&self) -> &'static str {
        "message-integrity"
    }

    fn validate_event(&mut self, log: &TestLogLine) {
        match &log.data.fields {
            TestEvent::MessageReadSucceeded(event) => {
                let mut hasher = DefaultHasher::new();
                event.message.data.hash(&mut hasher);
                let message_read_hash = hasher.finish();
                let existing_message_hashes = self
                    .messages
                    .entry(event.message.metadata.topic_name.clone())
                    .or_default()
                    .entry(event.message.metadata.topic_partition)
                    .or_default()
                    .entry(event.message.metadata.topic_partition_offset);

                match existing_message_hashes {
                    Entry::Vacant(entry) => {
                        entry.insert((Some(log.capture(message_read_hash)), None));
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        match &entry.0 {
                            Some(old_read_hash) => {
                                if old_read_hash.data != message_read_hash {
                                    let details = json!({"consumer": event.consumer, "message": event.message, "old_read_hash_location": old_read_hash.location()});
                                    antithesis_sdk::assert_unreachable!("Message data differs from previous read at location", &details);
                                }
                            }
                            None => {
                                entry.0 = Some(log.capture(message_read_hash));
                            }
                        }
                    }
                }
                if let Some((Some(final_read_hash), Some(final_write_hash))) = self
                    .messages
                    .get(&event.message.metadata.topic_name)
                    .and_then(|partition_offsets| {
                        partition_offsets.get(&event.message.metadata.topic_partition)
                    })
                    .and_then(|offsets| offsets.get(&event.message.metadata.topic_partition_offset))
                {
                    if final_read_hash.data != final_write_hash.data {
                        let details = json!({"consumer": event.consumer, "message": event.message, "final_write_hash_location": final_write_hash.location()});
                        antithesis_sdk::assert_unreachable!("Message data differs from previous write by producer at location", &details); 
                    }
                }
            }
            TestEvent::MessageWriteSucceeded(event) => {
                let mut hasher = DefaultHasher::new();
                event.message.data.hash(&mut hasher);
                let message_write_hash = hasher.finish();
                let existing_message_hashes = self
                    .messages
                    .entry(event.message.metadata.topic_name.clone())
                    .or_default()
                    .entry(event.message.metadata.topic_partition)
                    .or_default()
                    .entry(event.message.metadata.topic_partition_offset);

                match existing_message_hashes {
                    Entry::Vacant(entry) => {
                        entry.insert((None, Some(log.capture(message_write_hash))));
                    }
                    Entry::Occupied(entry) => {
                        let entry = entry.into_mut();
                        match &entry.1 {
                            Some(old_write_hash) => {
                                if old_write_hash.data != message_write_hash {
                                    let details = json!({"consumer": event.producer, "message": event.message, "old_write_hash_location": old_write_hash.location()});
                                    antithesis_sdk::assert_unreachable!("Message data differs from previous write at location", &details);
                                }
                            }
                            None => {
                                entry.1 = Some(log.capture(message_write_hash));
                            }
                        }
                    }
                }
                if let Some((Some(final_read_hash), Some(final_write_hash))) = self
                    .messages
                    .get(&event.message.metadata.topic_name)
                    .and_then(|partition_offsets| {
                        partition_offsets.get(&event.message.metadata.topic_partition)
                    })
                    .and_then(|offsets| offsets.get(&event.message.metadata.topic_partition_offset))
                {
                    if final_read_hash.data != final_write_hash.data {
                        let details = json!({"producer": event.producer, "message": event.message, "final_read_hash_location": final_read_hash.location()});
                        antithesis_sdk::assert_unreachable!("Message data differs from previous write at location", &details);
                    }
                }
            }
            TestEvent::WorkloadEnded => {
                for (topic_name, partition_offsets) in self.messages.iter() {
                    for (topic_partition, topic_partition_offset, read_write_state) in
                        partition_offsets
                            .iter()
                            .flat_map(|(topic_partition, offsets)| {
                                offsets
                                    .iter()
                                    .map(move |(offset, state)| (topic_partition, offset, state))
                            })
                    {
                        match read_write_state {
                            (Some(_), Some(_)) => {
                                // nothing to do here, since we already incrementally validated this
                            }
                            (Some(message_read_hash), None) => {
                                let details = json!({"topic_name": topic_name, "topic_partition": topic_partition, "topic_partition_offset": topic_partition_offset, "message_read_hash_location": message_read_hash.location()});
                                antithesis_sdk::assert_unreachable!("Read message was never written", &details);
                            }
                            (None, Some(message_write_hash)) => {
                                let details = json!({"topic_name": topic_name, "topic_partition": topic_partition, "topic_partition_offset": topic_partition_offset, "message_write_hash_location": message_write_hash.location()});
                                antithesis_sdk::assert_unreachable!("Written message never read", &details);
                            }
                            (None, None) => {
                                unreachable!()
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: MessageIntegrityValidator = serde_json::from_str(data)?;
        self.messages = instance.messages;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
