use core::fmt;

use super::{
    ConsumerGroupID, Message, MessageKey, MessageMetadata, MessagePayload, ProducerID, TopicName, TopicPartitionIndex, TopicPartitionList
};
use serde::{Deserialize, Serialize};

pub struct TestLogLine {
    pub line: TestLogLineIndex,
    pub data: TestLog,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestLogLocation<S> {
    pub line: TestLogLineIndex,
    pub timestamp: String,
    pub data: S,
}

impl<S> TestLogLocation<S> {
    pub fn location(&self) -> String {
        format!("line = {}, timestamp = '{}'", self.line, self.timestamp)
    }
}

impl TestLogLine {
    pub fn capture<S>(&self, data: S) -> TestLogLocation<S> {
        TestLogLocation {
            line: self.line,
            timestamp: self.data.timestamp.clone(),
            data,
        }
    }
}

pub type TestLogLineIndex = u64;

#[derive(Debug, Serialize, Deserialize)]
pub struct TestLog {
    pub timestamp: String,
    pub level: String,
    #[serde(flatten)]
    pub fields: TestEvent,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum TestEvent {
    ClusterStarted,
    WorkloadStarted,
    WorkloadEnded,
    AllProducersCompleted,
    TopicCreated(TopicCreated),
    TopicAlreadyExists,
    TopicCreationFailed,
    ProducerCreated,
    ProducerStarted(ProducerStarted),
    ProducerStopped(ProducerStopped),
    ConsumerCreated,
    ConsumerTopicsSubscribed,
    ConsumerStarted(ConsumerStarted),
    ConsumerWaiting(ConsumerWaiting),
    ConsumerStopping(ConsumerStopping),
    ConsumerStopped(ConsumerStopped),
    ConsumerMessageCommitted(ConsumerMessageCommitted),
    ConsumerMessagesCommitted(ConsumerMessagesCommitted),
    ConsumerMessageCommitFailure,
    ConsumerMessagesCommitFailure,
    ConsumerPostRebalanceAssign(ConsumerPostRebalanceAssign),
    ConsumerPostRebalanceRevoke(ConsumerPostRebalanceRevoke),
    ConsumerPostRebalanceError(ConsumerPostRebalanceError),
    MessageReadFailed(MessageReadFailed),
    MessageReadSucceeded(MessageReadSucceeded),
    MessageWriteFailed(MessageWriteFailed),
    MessageWriteSucceeded(MessageWriteSucceeded),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerMetadata {
    #[serde(rename = "consumer_group_id")]
    pub group_id: ConsumerGroupID,
    #[serde(rename = "consumer_id")]
    pub id: ConsumerGroupID,
}

impl fmt::Display for ConsumerMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "consumer_group_id = '{}', consumer_id = '{}'",
            self.group_id, self.id,
        )
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProducerMetadata {
    #[serde(rename = "producer_id")]
    pub id: ProducerID,
}

impl fmt::Display for ProducerMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "producer_id = '{}'", self.id,)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TopicCreated {
    pub topic_name: String,
    pub topic_num_partitions: TopicPartitionIndex,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProducerStarted {
    #[serde(flatten)]
    pub producer: ProducerMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ProducerStopped {
    #[serde(flatten)]
    pub producer: ProducerMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerStarted {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerWaiting {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub reason: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerStopping {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub reason: String
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerStopped {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerMessageCommitted {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    #[serde(flatten)]
    pub metadata: MessageMetadata
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerMessagesCommitted {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub topic_partition_list: TopicPartitionList,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerPreRebalanceAssign {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub topic_partition_list: TopicPartitionList,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerPreRebalanceRevoke {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub topic_partition_list: TopicPartitionList,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerPreRebalanceError {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerPostRebalanceAssign {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub topic_partition_list: TopicPartitionList,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerPostRebalanceRevoke {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub topic_partition_list: TopicPartitionList,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerPostRebalanceError {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageReadFailed {
    pub error: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageReadSucceeded {
    #[serde(flatten)]
    pub consumer: ConsumerMetadata,
    #[serde(flatten)]
    pub message: Message,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageWriteFailed {
    #[serde(flatten)]
    pub producer: ProducerMetadata,
    pub error: String,
    pub topic_name: TopicName,
    pub message_key: Option<MessageKey>,
    pub message_payload: MessagePayload,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MessageWriteSucceeded {
    #[serde(flatten)]
    pub producer: ProducerMetadata,
    #[serde(flatten)]
    pub message: Message,
}
