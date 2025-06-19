use anyhow::{Context, Result};
use std::collections::BTreeMap;

use crate::domain::{TestLogLine, TestValidator, ValidationFailure};

pub struct LogValidator {
    validators: Vec<Box<dyn TestValidator>>,
    failures: BTreeMap<&'static str, Vec<ValidationFailure>>,
}

impl LogValidator {
    pub fn new(validators: Vec<Box<dyn TestValidator>>) -> LogValidator {
        let mut failures: BTreeMap<&'static str, Vec<ValidationFailure>> = BTreeMap::default();
        for validator in validators.iter() {
            failures.entry(validator.validator_name()).or_default();
        }
        LogValidator {
            validators,
            failures,
        }
    }
    pub fn validate_event(&mut self, log: &TestLogLine) {
        for validator in self.validators.iter_mut() {
            validator.validate_event(log);
        }
    }
    pub fn load_state(&mut self, state: BTreeMap<String, String>) -> Result<()> {
        for validator in self.validators.iter_mut() {
            let validator_state = state.get(validator.validator_name()).with_context(|| {
                format!(
                    "failed to find saved state for validator '{}'",
                    validator.validator_name()
                )
            })?;
            validator.load_state(validator_state).with_context(|| {
                format!(
                    "failed to load state for validator '{}'",
                    validator.validator_name()
                )
            })?;
        }
        Ok(())
    }
    pub fn save_state(&self) -> Result<BTreeMap<String, String>> {
        let mut state = BTreeMap::default();
        for validator in self.validators.iter() {
            state.insert(
                validator.validator_name().to_string(),
                validator.save_state().with_context(|| {
                    format!(
                        "failed to save state for validator '{}'",
                        validator.validator_name()
                    )
                })?,
            );
        }
        Ok(state)
    }

    pub fn finalize(self) -> BTreeMap<&'static str, Vec<ValidationFailure>> {
        self.failures
    }
}
