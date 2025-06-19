use std::fmt::Display;
use super::sequence::SequenceItem;

pub struct ProducerMessage {
    pub id: String,
    pub message_version: u64,
    pub sequence_item: SequenceItem,
}

impl Display for ProducerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}:{}|{}",
            self.id, self.message_version, self.sequence_item
        )
    }
}

impl<S> From<S> for ProducerMessage
where
    S: AsRef<str>,
{
    fn from(value: S) -> Self {
        let (metadata, sequence) = value
            .as_ref()
            .split_once('|')
            .expect("invalid producer message");
        let mut parts = metadata.split(':');
        let id = parts.next().expect("producer id missing").to_string();
        let message_version = parts
            .next()
            .expect("producer message version missing")
            .parse()
            .expect("invalid producer message version");
        ProducerMessage {
            id,
            message_version,
            sequence_item: SequenceItem::from(sequence),
        }
    }
}
