use crate::rng;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{fs::OpenOptions, path::Path};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct U64Range {
    min: u64,
    max: u64,
}

impl U64Range {
    pub fn get_random_value(&self) -> u64 {
        rng::u64_in(self.min, self.max)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerSystem {
    #[serde(rename = "kafka")]
    Kafka,
    #[serde(rename = "redpanda")]
    RedPanda,
}

impl Default for BrokerSystem {
    fn default() -> Self {
        BrokerSystem::Kafka
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkloadConfig {
    #[serde(default)]
    pub broker: BrokerSystem,
    #[serde(default)]
    pub broker_version: String,
    pub bootstrap_servers: String,
    pub log_dir: String,
    pub is_strict: bool,
    #[serde(default)]
    pub enable_debug: bool,
    pub topic_count: U64Range,
    pub topic_partition_count: U64Range,
    pub producer_count: U64Range,
    pub producer_startup_delay_ms: U64Range,
    pub producer_unique_sequence_count: U64Range,
    pub producer_sequence_length: U64Range,
    pub producer_sequence_delay_ms: U64Range,
    pub consumer_group_count: U64Range,
    pub consumer_group_members_count: U64Range,
    pub consumer_group_member_startup_delay_ms: U64Range,
    pub consumer_group_member_process_delay_ms: U64Range,
    pub consumer_isolation_level: String
}

impl WorkloadConfig {
    pub fn new(config_path: impl AsRef<Path>) -> Result<WorkloadConfig> {
        let config_file = OpenOptions::new()
            .read(true)
            .open(config_path)
            .expect("failed to read config file");
        let config: WorkloadConfig = serde_json::from_reader(config_file)?;
        Ok(config)
    }
}
