use anyhow::Context;
use rdkafka::ClientConfig;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct ProducerConfig(pub BTreeMap<String, String>);

impl ProducerConfig {
    pub fn new() -> ProducerConfig {
        ProducerConfig(BTreeMap::default())
    }
    pub fn set(&mut self, key: &str, value: &str) -> &mut ProducerConfig {
        self.0.insert(key.to_string(), value.to_string());
        self
    }
}

impl From<&ProducerConfig> for ClientConfig {
    fn from(config: &ProducerConfig) -> Self {
        let mut kafka_config = ClientConfig::new();
        for (key, value) in config.0.iter(){
            kafka_config.set(key, value);
        } 
        kafka_config
    }
}

impl TryFrom<String> for ProducerConfig {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let object: serde_json::Value =
            serde_json::from_str(&value).context("failed to read producer config from json")?;
        let mut config = ProducerConfig::default();
        for (key, value) in object.as_object().unwrap() {
            config
                .0
                .insert(key.clone(), value.as_str().unwrap().to_string());
        }
        Ok(config)
    }
}
