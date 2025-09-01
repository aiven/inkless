use crate::{
    config::WorkloadConfig,
    domain::{
        ConsumerConfig, GlobalState, TestTopic, TopicName, TopicPartitionIndex, TopicPartitionList,
        TopicPartitionOffset,
    },
};
use anyhow::{Context, Result};
use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer},
    ClientConfig, ClientContext, Message,
};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, RwLock},
    time::Duration,
};
use tokio::sync::oneshot;
use tracing::{error, info};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerState {
    NotReady,
    Started,
    Stopped,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConsumerPendingReadMetadata {
    topic_name: TopicName,
    topic_partition_index: TopicPartitionIndex,
    last_read_offset: Option<TopicPartitionOffset>,
    last_written_offset: TopicPartitionOffset,
}

pub enum ConsumerReceivedAllMessagesResult {
    ProducersNotCompleted,
    ConsumerNotStarted,
    PartitionsPendingRead(Vec<ConsumerPendingReadMetadata>),
    NoMessagesToBeReceived,
    AllMessagesReceived,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub enum LastReadOffset {
    Assigned(TopicPartitionOffset),
    Unassigned(TopicPartitionOffset),
}

pub struct TestConsumerContext {
    id: String,
    group_id: String,
    global_state: Arc<RwLock<GlobalState>>,
    topic_partition_offsets:
        Arc<RwLock<HashMap<TopicName, HashMap<TopicPartitionIndex, LastReadOffset>>>>,
    state: Arc<RwLock<ConsumerState>>,
}

impl TestConsumerContext {
    fn get_state(&self) -> ConsumerState {
        let state = self.state.read().unwrap();
        *state
    }
    fn set_state(&self, new_state: ConsumerState) {
        let mut state = self.state.write().unwrap();
        *state = new_state
    }

    fn all_messages_received(&self, is_observer: bool) -> ConsumerReceivedAllMessagesResult {
        let global_state = self.global_state.read().unwrap();
        if !global_state.all_producers_completed.load(Ordering::SeqCst) {
            return ConsumerReceivedAllMessagesResult::ProducersNotCompleted;
        }
        let state = self.state.read().unwrap();
        if *state == ConsumerState::NotReady {
            let topic_partition_offsets = self.topic_partition_offsets.read().unwrap();
            let mut all_subscribed_topics_are_empty = true;
            for topic_name in topic_partition_offsets.keys() {
                if global_state
                    .topic_partition_offsets
                    .get(topic_name)
                    .is_some()
                {
                    all_subscribed_topics_are_empty = false;
                }
            }
            if all_subscribed_topics_are_empty {
                // Since all producers are completed at this stage, this means the topic will always be empty
                // Hence we have received all messages
                return ConsumerReceivedAllMessagesResult::NoMessagesToBeReceived;
            } else {
                return ConsumerReceivedAllMessagesResult::ConsumerNotStarted;
            }
        }
        let topic_partition_offsets = self.topic_partition_offsets.read().unwrap();
        let mut remaining_topic_partitions = Vec::new();
        for (topic, partitions) in global_state.topic_partition_offsets.iter() {
            for (partition, last_written_offset) in partitions {
                if *last_written_offset < 0 {
                    continue;
                }

                // Skip partitions completed by the consumer group
                if global_state
                    .consumer_groups_read_offsets
                    .get(&self.group_id)
                    .and_then(|group| group.get(topic))
                    .and_then(|topic_partitions| topic_partitions.get(partition))
                    .map(|last_read_offset| *last_read_offset >= *last_written_offset)
                    .unwrap_or(false)
                {
                    continue;
                }

                if let Some(last_read_offset) = topic_partition_offsets
                    .get(topic)
                    .and_then(|topic_partitions| topic_partitions.get(partition))
                {
                    match last_read_offset {
                        LastReadOffset::Assigned(last_read_offset) => {
                            // If this partition is still assigned and has not read all written messages yet, it is not complete
                            if *last_read_offset < *last_written_offset {
                                remaining_topic_partitions.push(ConsumerPendingReadMetadata {
                                    topic_name: topic.clone(),
                                    topic_partition_index: *partition,
                                    last_read_offset: Some(*last_read_offset),
                                    last_written_offset: *last_written_offset,
                                });
                            }
                        }
                        LastReadOffset::Unassigned(last_read_offset) => {
                            // If this is the observer consumer it must wait till it is re-assigned and eventually reads all messages
                            if is_observer {
                                // This branch has been experimentally found to be quite a rare occurrence, so we assert we reach it within our session run
                                antithesis_sdk::assert_reachable!("observer consumer was un-assigned from topic partition before read completion", &json!({ "topic": topic, "partition": partition, "last_read_offset": last_read_offset }));
                                remaining_topic_partitions.push(ConsumerPendingReadMetadata {
                                    topic_name: topic.clone(),
                                    topic_partition_index: *partition,
                                    last_read_offset: Some(*last_read_offset),
                                    last_written_offset: *last_written_offset,
                                });
                            } else {
                                // Skip partitions that were un-assigned if we are not the observer consumer.
                                continue;
                            }
                        }
                    }
                } else if is_observer {
                    remaining_topic_partitions.push(ConsumerPendingReadMetadata {
                        topic_name: topic.clone(),
                        topic_partition_index: *partition,
                        last_read_offset: None,
                        last_written_offset: *last_written_offset,
                    });
                }
            }
        }
        if remaining_topic_partitions.is_empty() {
            if is_observer
                && !global_state
                    .all_messages_read_by_observer
                    .load(Ordering::SeqCst)
            {
                global_state
                    .all_messages_read_by_observer
                    .store(true, Ordering::SeqCst);
            }
            ConsumerReceivedAllMessagesResult::AllMessagesReceived
        } else {
            ConsumerReceivedAllMessagesResult::PartitionsPendingRead(remaining_topic_partitions)
        }
    }
}

impl ClientContext for TestConsumerContext {}

impl ConsumerContext for TestConsumerContext {
    fn post_rebalance(&self, rebalance: &Rebalance<'_>) {
        // Check and change  the consumer state if needed
        {
            let state = self.state.read().unwrap();
            if *state == ConsumerState::NotReady {
                drop(state);
                let mut state = self.state.write().unwrap();
                if *state == ConsumerState::NotReady {
                    *state = ConsumerState::Started;
                }
            }
        }
        match rebalance {
            Rebalance::Assign(assigned_topic_partitions) => {
                info!(
                    timestamp =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "consumer_post_rebalance_assign",
                    consumer_group_id = self.group_id,
                    consumer_id = self.id,
                    topic_partition_list = serde_json::to_string(&TopicPartitionList::from(
                        *assigned_topic_partitions
                    ))
                    .unwrap()
                    .as_str()
                );
                let mut topic_partition_offsets = self.topic_partition_offsets.write().unwrap();
                for assigned_topic_partition in assigned_topic_partitions.elements() {
                    topic_partition_offsets
                        .entry(assigned_topic_partition.topic().to_string())
                        .or_default()
                        .entry(assigned_topic_partition.partition())
                        .and_modify(|last_read_offset| {
                            *last_read_offset = match last_read_offset {
                                LastReadOffset::Assigned(offset) => {
                                    LastReadOffset::Assigned(*offset)
                                }
                                LastReadOffset::Unassigned(offset) => {
                                    LastReadOffset::Assigned(*offset)
                                }
                            };
                        })
                        .or_insert(LastReadOffset::Assigned(-1));
                }
            }
            Rebalance::Revoke(revoked_topic_partitions) => {
                info!(
                    timestamp =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "consumer_post_rebalance_revoke",
                    consumer_group_id = self.group_id,
                    consumer_id = self.id,
                    topic_partition_list =
                        serde_json::to_string(&TopicPartitionList::from(*revoked_topic_partitions))
                            .unwrap()
                            .as_str()
                );
                let mut topic_partition_offsets = self.topic_partition_offsets.write().unwrap();
                for revoked_topic_partition in revoked_topic_partitions.elements() {
                    topic_partition_offsets
                        .entry(revoked_topic_partition.topic().to_string())
                        .or_default()
                        .entry(revoked_topic_partition.partition())
                        .and_modify(|last_read_offset| {
                            *last_read_offset = match last_read_offset {
                                LastReadOffset::Assigned(offset) => {
                                    LastReadOffset::Unassigned(*offset)
                                }
                                LastReadOffset::Unassigned(offset) => {
                                    LastReadOffset::Unassigned(*offset)
                                }
                            };
                        })
                        .or_insert(LastReadOffset::Unassigned(-1));
                }
            }
            Rebalance::Error(err) => {
                error!(
                    timestamp =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "consumer_post_rebalance_error",
                    consumer_group_id = self.group_id,
                    consumer_id = self.id,
                    error = format!("{:#}", err).as_str(),
                    code = format!("{:?}", err.rdkafka_error_code().unwrap())
                );
            }
        }
    }

    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        committed_topic_partition_offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(_) => {
                info!(
                    timestamp =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "consumer_messages_committed",
                    consumer_group_id = self.group_id,
                    consumer_id = self.id,
                    topic_partition_list = serde_json::to_string(&TopicPartitionList::from(
                        committed_topic_partition_offsets
                    ))
                    .unwrap()
                    .as_str()
                );
                let mut global_state = self.global_state.write().unwrap();
                let mut topic_partition_offsets = self.topic_partition_offsets.write().unwrap();
                for committed_topic_partition_offset in committed_topic_partition_offsets.elements()
                {
                    if let rdkafka::Offset::Offset(committed_offset) =
                        committed_topic_partition_offset.offset()
                    {
                        // This kafka library seems to call the offset commit callback with committed offsets to partitions
                        // that were revoked from the consumer during shutdown. Because of this we cannot expect that the topic
                        // and partition which we are attempting to update offsets for is still available in `topic_partition_offsets`
                        if let Some(last_read_offset) = topic_partition_offsets
                            .get_mut(committed_topic_partition_offset.topic())
                            .and_then(|p| p.get_mut(&committed_topic_partition_offset.partition()))
                        {
                            match last_read_offset {
                                LastReadOffset::Assigned(_) => {
                                    *last_read_offset = LastReadOffset::Assigned(committed_offset);
                                }
                                LastReadOffset::Unassigned(_) => {
                                    *last_read_offset =
                                        LastReadOffset::Unassigned(committed_offset);
                                }
                            }
                        }
                        global_state
                            .consumer_groups_read_offsets
                            .entry(self.group_id.clone())
                            .or_default()
                            .entry(committed_topic_partition_offset.topic().to_string())
                            .or_default()
                            .insert(
                                committed_topic_partition_offset.partition(),
                                committed_offset,
                            );
                    }
                }
            }
            Err(err) => {
                error!(
                    timestamp =
                        chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                    event = "consumer_messages_commit_failure",
                    consumer_group_id = self.group_id,
                    consumer_id = self.id,
                    error = format!("{:#}", err).as_str(),
                    code = format!("{:?}", err.rdkafka_error_code().unwrap())
                )
            }
        }
    }
}

impl Drop for TestConsumerContext {
    fn drop(&mut self) {
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "consumer_stopped",
            consumer_group_id = self.group_id,
            consumer_id = self.id,
        );
    }
}

pub struct TestConsumer {
    pub id: String,
    pub group_id: String,
    enable_auto_commit: bool,
    inner_consumer: StreamConsumer<TestConsumerContext>,
    is_observer: bool,
    consumer_config: ConsumerConfig,
}

impl TestConsumer {
    pub fn new(
        id: &str,
        group_id: &str,
        config: &WorkloadConfig,
        global_state: Arc<RwLock<GlobalState>>,
        enable_auto_commit: bool,
        is_observer: bool,
    ) -> Result<TestConsumer> {
        let mut consumer_config = ConsumerConfig::new();
        consumer_config
            .set("client.id", format!("{}-{}", group_id, id).as_str())
            .set("group.id", group_id)
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("allow.auto.create.topics", "true")
            .set(
                "enable.auto.commit",
                if enable_auto_commit { "true" } else { "false" },
            )
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", &config.consumer_isolation_level)
            .set("partition.assignment.strategy", "range");
        if config.enable_debug {
            consumer_config.set("debug", "protocol");
        }
        let consumer = TestConsumer {
            id: String::from(id),
            group_id: String::from(group_id),
            enable_auto_commit,
            inner_consumer: ClientConfig::from(&consumer_config)
                .create_with_context(TestConsumerContext {
                    id: String::from(id),
                    group_id: String::from(group_id),
                    global_state,
                    topic_partition_offsets: Arc::new(RwLock::new(HashMap::default())),
                    state: Arc::new(RwLock::new(ConsumerState::NotReady)),
                })
                .context("failed to create kafka consumer")?,
            is_observer,
            consumer_config,
        };
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "consumer_created",
            consumer_group_id = consumer.group_id,
            consumer_id = consumer.id,
            config = serde_json::to_string(&consumer.consumer_config)
                .unwrap()
                .as_str()
        );
        Ok(consumer)
    }
    pub fn subscribe(&self, topics: &[TestTopic]) -> Result<()> {
        // If there are no topics the consumer is ready right away.
        if topics.is_empty() {
            self.inner_consumer
                .context()
                .set_state(ConsumerState::Started);
        } else {
            let topic_names: Vec<&str> = topics.iter().map(|t| &*t.name).collect();
            self.inner_consumer
                .subscribe(&topic_names)
                .context("failed to subscribe to kafka topic")?;
            // Create an entry for each subscribed topic in the topic partitions list.
            // This is useful to determine if a consumer should still be awaiting messages on a topic, if no messages were ever written to it.
            {
                let mut topic_partition_offsets = self
                    .inner_consumer
                    .context()
                    .topic_partition_offsets
                    .write().unwrap();
                for topic_name in topic_names.iter() {
                    topic_partition_offsets
                        .entry(topic_name.to_string())
                        .or_default();
                }
            }
            info!(
                timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                event = "consumer_topics_subscribed",
                consumer_group_id = self.group_id,
                consumer_id = self.id,
                topic_names = serde_json::to_string(&topic_names).unwrap().as_str()
            );
        }
        Ok(())
    }

    pub async fn poll(&self, config: WorkloadConfig) {
        if !self.is_observer {
            tokio::time::sleep(Duration::from_millis(
                config
                    .consumer_group_member_startup_delay_ms
                    .get_random_value(),
            ))
            .await;
        }
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "consumer_started",
            consumer_group_id = self.group_id,
            consumer_id = self.id,
        );
        let mut suspicious_timeouts = 0;
        loop {
            if self.inner_consumer.context().get_state() == ConsumerState::Stopped {
                break;
            }
            let (tx, rx) = oneshot::channel();
            let timeout = tokio::time::timeout(Duration::from_secs(5), rx);

            tokio::select! {
                result = self.inner_consumer.recv() => {
                    suspicious_timeouts = 0;
                    let _ = tx.send(());
                    match result {
                        Ok(borrowed_message) => {
                            let message = borrowed_message.detach();
                            info!(
                                timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "message_read_succeeded",
                                consumer_group_id = self.group_id,
                                consumer_id = self.id,
                                topic_name = message.topic(),
                                topic_partition = message.partition(),
                                topic_partition_offset = message.offset(),
                                message_key = message
                                    .key().map(|v| String::from_utf8_lossy(v).to_string()),
                                message_payload = message
                                    .payload().map(|v| String::from_utf8_lossy(v).to_string()),
                            );
                            if !self.is_observer {
                                tokio::time::sleep(Duration::from_millis(
                                    config
                                        .consumer_group_member_process_delay_ms
                                        .get_random_value(),
                                ))
                                .await;
                            };
                            if !self.enable_auto_commit {
                                match self.inner_consumer.commit_message(&borrowed_message, CommitMode::Sync) {
                                    Ok(_) => {
                                        info!(
                                            timestamp =
                                                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                                            event = "consumer_message_committed",
                                            consumer_group_id = self.group_id,
                                            consumer_id = self.id,
                                            topic_name = message.topic(),
                                            topic_partition = message.partition(),
                                            topic_partition_offset = message.offset(),
                                        );
                                        let mut global_state = self.inner_consumer.context().global_state.write().unwrap();
                                        let mut topic_partition_offsets = self.inner_consumer.context().topic_partition_offsets.write().unwrap();
                                        // This kafka library seems to call the offset commit callback with committed offsets to partitions
                                        // that were revoked from the consumer during shutdown. Because of this we cannot expect that the topic
                                        // and partition which we are attempting to update offsets for is still available in `topic_partition_offsets`
                                        if let Some(last_read_offset) = topic_partition_offsets
                                            .get_mut(message.topic())
                                            .and_then(|p| p.get_mut(&message.partition()))
                                        {
                                            *last_read_offset = LastReadOffset::Assigned(message.offset());
                                        }
                                        global_state
                                            .consumer_groups_read_offsets
                                            .entry(self.group_id.clone())
                                            .or_default()
                                            .entry(message.topic().to_string())
                                            .or_default()
                                            .insert(
                                                message.partition(),
                                                message.offset(),
                                            );
                                    }
                                    Err(err) => {
                                        error!(
                                            timestamp =
                                                chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                                            event = "consumer_message_commit_failure",
                                            consumer_group_id = self.group_id,
                                            consumer_id = self.id,
                                            topic_name = message.topic(),
                                            topic_partition = message.partition(),
                                            topic_partition_offset = message.offset(),
                                            error = format!("{:#}", err).as_str(),
                                            code = format!("{:?}", err.rdkafka_error_code().unwrap())
                                        )
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            error!(timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "message_read_failed", consumer_group_id = self.group_id, consumer_id = self.id, error = format!("{}", err), code = format!("{:?}", err.rdkafka_error_code().unwrap()));
                        }
                    }
                },
                Err(_) = timeout => {
                    suspicious_timeouts += 1;
                    if suspicious_timeouts > 24 && self.inner_consumer.context().global_state.read().unwrap().all_messages_read_by_observer.load(Ordering::SeqCst) {
                        info!(consumer_group_id = self.group_id, consumer_id = self.id, reason = "consumer_maybe_dead", "consumer appears to have died");
                    }
                    match self.inner_consumer.context().all_messages_received(self.is_observer) {
                        ConsumerReceivedAllMessagesResult::ProducersNotCompleted => {
                            info!(timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "consumer_waiting", consumer_group_id = self.group_id, consumer_id = self.id, reason = "producers_not_completed", "consumer awaiting more messages");
                        },
                        ConsumerReceivedAllMessagesResult::ConsumerNotStarted => {
                            info!(timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "consumer_waiting", consumer_group_id = self.group_id, consumer_id = self.id, reason = "consumer_not_started", "consumer awaiting more messages");
                        },
                        ConsumerReceivedAllMessagesResult::PartitionsPendingRead(remaining_partitions) => {
                            info!(timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "consumer_waiting", consumer_group_id = self.group_id, consumer_id = self.id, reason = "partitions_pending_read", partitions = serde_json::to_string(&remaining_partitions).unwrap().as_str(), "consumer awaiting more messages");
                        }
                        ConsumerReceivedAllMessagesResult::NoMessagesToBeReceived => {
                            info!(timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "consumer_stopping", consumer_group_id = self.group_id, consumer_id = self.id, reason = "no_messages_to_be_received");
                            self.stop();
                        }
                        ConsumerReceivedAllMessagesResult::AllMessagesReceived => {
                            info!(timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "consumer_stopping", consumer_group_id = self.group_id, consumer_id = self.id, reason = "all_messages_received");
                            self.stop();
                        }
                    }
                }
            }
        }
    }
    pub fn stop(&self) {
        self.inner_consumer
            .context()
            .set_state(ConsumerState::Stopped)
    }
}
