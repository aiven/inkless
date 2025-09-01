use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(try_from = "String")]
pub struct TopicConfig(pub BTreeMap<String, String>);

impl TopicConfig {
    pub fn new() -> TopicConfig {
        TopicConfig(BTreeMap::default())
    }
    pub fn set(&mut self, key: &str, value: &str) -> &mut TopicConfig {
        self.0.insert(key.to_string(), value.to_string());
        self
    }
    pub fn to_vec(&self) -> Vec<(&str, &str)> {
        self.0
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect()
    }
}

impl TryFrom<String> for TopicConfig {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let object: serde_json::Value =
            serde_json::from_str(&value).context("failed to read topic config from json")?;
        let mut config = TopicConfig::default();
        for (key, value) in object.as_object().unwrap() {
            config
                .0
                .insert(key.clone(), value.as_str().unwrap().to_string());
        }
        Ok(config)
    }
}
