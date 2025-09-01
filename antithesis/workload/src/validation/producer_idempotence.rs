use crate::domain::{
    MessageMetadata, TestEvent, TestLogLine, TestLogLocation, TestValidator
};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use serde_json::{json};

/// ProducerIdempotenceValidator verifies that a message produced by a producer will present at only one topic / partition / offset
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ProducerIdempotenceValidator {
    messages:
        BTreeMap<String, TestLogLocation<MessageMetadata>>,
}

impl TestValidator for ProducerIdempotenceValidator {
    fn validator_name(&self) -> &'static str {
        "producer-idempotence"
    }

    fn validate_event(&mut self, log: &TestLogLine) {
        match &log.data.fields {
            TestEvent::MessageReadSucceeded(event) => {
                if let Some(existing_metadata) = self.messages.get(&event.message.data.payload) {
                    if existing_metadata.data != event.message.metadata {
                        let details = json!({"consumer": event.consumer, "message": event.message, "existing_metadata_location": existing_metadata.location()});
                        antithesis_sdk::assert_unreachable!("Identical message previously seen (reader)_", &details);
                    }
                } else {
                    self.messages.insert(event.message.data.payload.clone(), log.capture(event.message.metadata.clone()));
                }
            }
            TestEvent::MessageWriteSucceeded(event) => {
                if let Some(existing_metadata) = self.messages.get(&event.message.data.payload) {
                    if existing_metadata.data != event.message.metadata {
                        let details = json!({"prodcuer": event.producer, "message": event.message, "existing_metadata_location": existing_metadata.location()});
                        antithesis_sdk::assert_unreachable!("Identical message previously seen (writer)", &details);
                    }
                } else {
                    self.messages.insert(event.message.data.payload.clone(), log.capture(event.message.metadata.clone()));
                }
            }
            _ => {}
        }
    }
    fn load_state(&mut self, data: &str) -> Result<()> {
        let instance: ProducerIdempotenceValidator = serde_json::from_str(data)?;
        self.messages = instance.messages;
        Ok(())
    }

    fn save_state(&self) -> Result<String> {
        Ok(serde_json::to_string(&self)?)
    }
}
