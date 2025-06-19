use crate::{
    config::WorkloadConfig,
    decisions::AddMessageKey,
    domain::{sequence::Sequence, GlobalState, ProducerConfig, TestTopic},
    rng,
};
use anyhow::{Context, Result};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
    ClientConfig,
};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};
use tracing::{error, info};

pub struct TestProducer {
    pub id: String,
    inner_producer: FutureProducer,
    global_state: Arc<RwLock<GlobalState>>,
    producer_config: ProducerConfig,
}

impl TestProducer {
    pub fn new(
        id: &str,
        config: &WorkloadConfig,
        global_state: Arc<RwLock<GlobalState>>,
    ) -> Result<TestProducer> {
        let mut producer_config = ProducerConfig::new();
        producer_config
            .set("client.id", id)
            .set("bootstrap.servers", &config.bootstrap_servers)
            .set("message.timeout.ms", "0")
            .set("request.required.acks", "all")
            .set("enable.idempotence", "true");

        if config.enable_debug {
            producer_config.set("debug", "protocol");
        }
        let producer = TestProducer {
            id: String::from(id),
            inner_producer: ClientConfig::from(&producer_config)
                .create()
                .context("failed to create kafka producer")?,
            global_state,
            producer_config,
        };
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "producer_created",
            producer_id = producer.id,
            config = serde_json::to_string(&producer.producer_config)
                .unwrap()
                .as_str()
        );
        Ok(producer)
    }
    pub async fn produce(self, config: WorkloadConfig, topics: Vec<TestTopic>) {
        tokio::time::sleep(Duration::from_millis(
            config.producer_startup_delay_ms.get_random_value(),
        ))
        .await;
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "producer_started",
            producer_id = self.id,
            config = serde_json::to_string(&self.producer_config)
                .unwrap()
                .as_str()
        );
        let unique_sequence_count = config.producer_unique_sequence_count.get_random_value();
        for _ in 0..unique_sequence_count {
            let sequence_length = config.producer_sequence_length.get_random_value();
            let sequence_name = uuid::Uuid::new_v4().to_string();

            let topic_name = topics[rng::u64_in(0, topics.len() as u64 - 1) as usize]
                .name
                .clone();
            let sequence_name = sequence_name.clone();
            let key = match antithesis_sdk::random::random_choice(&[
                AddMessageKey::Yes,
                AddMessageKey::No,
            ])
            .unwrap()
            {
                AddMessageKey::Yes => Some(uuid::Uuid::new_v4().to_string()),
                AddMessageKey::No => None,
            };
            self.produce_sequence(&config, &topic_name, key, &sequence_name, sequence_length)
                .await;
        }
        info!(
            timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            event = "producer_stopped",
            producer_id = self.id,
        );
    }

    async fn produce_sequence(
        &self,
        config: &WorkloadConfig,
        topic: &str,
        key: Option<String>,
        sequence_name: &str,
        sequence_length: u64,
    ) {
        let sequence = Sequence::new(sequence_name, sequence_length);
        for next_value in sequence {
            let mut version = 0;
            loop {
                version += 1;
                let payload = format!("{}:{}|{}", self.id, version, next_value);
                tokio::time::sleep(tokio::time::Duration::from_millis(
                    config.producer_sequence_delay_ms.get_random_value(),
                ))
                .await;
                let record = {
                    let record = FutureRecord::<str, String>::to(topic).payload(&payload);
                    if let Some(ref key) = key {
                        record.key(key)
                    } else {
                        record
                    }
                };
                match self
                    .inner_producer
                    .send(record, Timeout::After(Duration::from_secs(10)))
                    .await
                {
                    Ok((partition, offset)) => {
                        let mut global_state = self.global_state.write().unwrap();
                        global_state
                            .topic_partition_offsets
                            .entry(topic.to_string())
                            .or_default()
                            .insert(partition, offset);
                        info!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "message_write_succeeded",
                            producer_id = self.id,
                            topic_name = topic,
                            topic_partition = partition,
                            topic_partition_offset = offset,
                            message_key = key,
                            message_payload = payload
                        );
                        break;
                    }
                    Err((err, _)) => {
                        error!(
                            timestamp = chrono::Utc::now()
                                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                            event = "message_write_failed",
                            producer_id = self.id,
                            topic_name = topic,
                            message_key = key,
                            message_payload = payload,
                            error = format!("{:#}", err).as_str(),
                            code = format!("{:?}", err.rdkafka_error_code().unwrap())
                        );
                    }
                }
            }
        }
    }
}
