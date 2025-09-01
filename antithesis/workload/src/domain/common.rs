use anyhow::{Context, Result};
use core::fmt;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, sync::atomic::AtomicBool};

pub type TopicName = String;
pub type TopicPartitionIndex = i32;
pub type TopicPartitionOffset = i64;
pub type MessageKey = String;
pub type MessagePayload = String;
pub type ConsumerGroupID = String;
pub type ConsumerID = String;
pub type ProducerID = String;

#[derive(Debug, Default)]
pub struct GlobalState {
    pub all_producers_completed: AtomicBool,
    pub all_messages_read_by_observer: AtomicBool,
    pub topic_partition_offsets: GlobalTopicPartitionOffsets,
    pub consumer_groups_read_offsets: GlobalConsumerGroupCompletedPartitions,
}
pub type GlobalTopicPartitionOffsets =
    BTreeMap<TopicName, BTreeMap<TopicPartitionIndex, TopicPartitionOffset>>;
pub type GlobalConsumerGroupCompletedPartitions = BTreeMap<
    ConsumerGroupID,
    BTreeMap<TopicName, BTreeMap<TopicPartitionIndex, TopicPartitionOffset>>,
>;

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct MessageData {
    #[serde(rename = "message_key")]
    pub key: Option<MessageKey>,
    #[serde(rename = "message_payload")]
    pub payload: MessagePayload,
}

impl fmt::Display for MessageData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "payload = '{}'", self.payload)?;
        if let Some(key) = &self.key {
            write!(f, ", key = '{}'", key)?;
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct TopicPartitionList(
    pub BTreeMap<TopicName, BTreeMap<TopicPartitionIndex, Vec<TopicPartitionOffset>>>,
);

impl TryFrom<String> for TopicPartitionList {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let object: serde_json::Value = serde_json::from_str(&value)
            .context("failed to read topic partition list from json")?;
        let mut topic_partition_list = TopicPartitionList::default();
        for (key, value) in object.as_object().unwrap() {
            let partitions = topic_partition_list.0.entry(key.clone()).or_default();
            for (key, value) in value.as_object().unwrap() {
                let offsets = partitions.entry(key.parse().unwrap()).or_default();
                for item in value.as_array().unwrap() {
                    offsets.push(item.as_i64().unwrap());
                }
            }
        }
        Ok(topic_partition_list)
    }
}

impl From<&rdkafka::TopicPartitionList> for TopicPartitionList {
    fn from(value: &rdkafka::TopicPartitionList) -> Self {
        let mut result = TopicPartitionList::default();
        for item in value.elements() {
            result
                .0
                .entry(item.topic().to_string())
                .or_default()
                .entry(item.partition())
                .or_default()
                .push(item.offset().to_raw().unwrap());
        }
        result
    }
}

#[derive(Debug, Clone)]
pub struct TestTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Message {
    #[serde(flatten)]
    pub metadata: MessageMetadata,
    #[serde(flatten)]
    pub data: MessageData,
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}, {}", self.metadata, self.data)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, PartialEq, Eq, Hash, Clone)]
pub struct MessageMetadata {
    pub topic_name: TopicName,
    pub topic_partition: TopicPartitionIndex,
    pub topic_partition_offset: TopicPartitionOffset,
}

impl fmt::Display for MessageMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "topic = '{}', partition = {}, offset = {}",
            self.topic_name, self.topic_partition, self.topic_partition_offset
        )
    }
}

impl PartialOrd for MessageMetadata {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (
            self.topic_name.partial_cmp(&other.topic_name),
            self.topic_partition.partial_cmp(&other.topic_partition),
            self.topic_partition_offset
                .partial_cmp(&other.topic_partition_offset),
        ) {
            (Some(core::cmp::Ordering::Equal), Some(core::cmp::Ordering::Equal), Some(ord)) => {
                Some(ord)
            }
            _ => None,
        }
    }
}
