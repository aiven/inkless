use anyhow::Context;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ConsumerConfig(pub BTreeMap<String, String>);

impl ConsumerConfig {
    pub fn new() -> ConsumerConfig {
        ConsumerConfig(BTreeMap::default())
    }
    pub fn set(&mut self, key: &str, value: &str) -> &mut ConsumerConfig {
        self.0.insert(key.to_string(), value.to_string());
        self
    }
}

impl From<&ConsumerConfig> for ClientConfig {
    fn from(config: &ConsumerConfig) -> Self {
        let mut kafka_config = ClientConfig::new();
        for (key, value) in config.0.iter() {
            kafka_config.set(key, value);
        }
        kafka_config
    }
}

impl TryFrom<String> for ConsumerConfig {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let object: serde_json::Value =
            serde_json::from_str(&value).context("failed to read consumer config from json")?;
        let mut config = ConsumerConfig::default();
        for (key, value) in object.as_object().unwrap() {
            config
                .0
                .insert(key.clone(), value.as_str().unwrap().to_string());
        }
        Ok(config)
    }
}
