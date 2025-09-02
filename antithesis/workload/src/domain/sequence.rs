use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Sequence {
    state: Option<SequenceItem>,
}

impl Sequence {
    pub fn new(name: &str, length: u64) -> Sequence {
        Sequence {
            state: Some(SequenceItem::NotStarted {
                name: name.to_string(),
                length,
            }),
        }
    }
}

impl Iterator for Sequence {
    type Item = SequenceItem;

    fn next(&mut self) -> Option<Self::Item> {
        match &self.state {
            Some(current_item) => {
                self.state = current_item.next();
                self.state.clone()
            }
            None => None,
        }
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum SequenceItem {
    NotStarted {
        name: String,
        length: u64,
    },
    Header {
        name: String,
        length: u64,
    },
    Body {
        name: String,
        length: u64,
        index: u64,
    },
    Footer {
        name: String,
        length: u64,
    },
}

impl<S> From<S> for SequenceItem
where
    S: AsRef<str>,
{
    fn from(value: S) -> Self {
        let mut parts = value.as_ref().split(':');
        let name = parts.next().expect("sequence name missing");
        let section = parts.next().expect("sequence section missing");
        let length = parts
            .next()
            .expect("sequence length missing")
            .parse()
            .expect("invalid sequence length");
        match section {
            "header" => SequenceItem::Header {
                name: name.to_string(),
                length,
            },
            "body" => SequenceItem::Body {
                name: name.to_string(),
                length,
                index: parts
                    .next()
                    .expect("missing sequence index")
                    .parse()
                    .expect("invalid sequence index"),
            },
            "footer" => SequenceItem::Footer {
                name: name.to_string(),
                length,
            },
            _ => {
                unreachable!("invalid sequence")
            }
        }
    }
}

impl SequenceItem {
    pub fn name(&self) -> &str {
        match self {
            Self::NotStarted { name, length: _ } => name,
            SequenceItem::Header { name, length: _ } => name,
            SequenceItem::Body {
                length: _,
                index: _,
                name,
            } => name,
            SequenceItem::Footer { name, length: _ } => name,
        }
    }
    pub fn next(&self) -> Option<SequenceItem> {
        match self {
            SequenceItem::NotStarted { name, length } => Some(SequenceItem::Header {
                name: name.clone(),
                length: *length,
            }),
            SequenceItem::Header { name, length } => {
                if *length > 0 {
                    Some(Self::Body {
                        name: name.clone(),
                        length: *length,
                        index: 0,
                    })
                } else {
                    Some(Self::Footer {
                        name: name.clone(),
                        length: *length,
                    })
                }
            }
            SequenceItem::Body {
                name,
                length,
                index,
            } => {
                if *index + 1 < *length {
                    Some(Self::Body {
                        name: name.clone(),
                        length: *length,
                        index: *index + 1,
                    })
                } else {
                    Some(Self::Footer {
                        name: name.clone(),
                        length: *length,
                    })
                }
            }
            SequenceItem::Footer { name: _, length: _ } => None,
        }
    }
}

impl Display for SequenceItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SequenceItem::NotStarted { name, length } => {
                write!(f, "{}:not-started:{}", name, length)
            }
            SequenceItem::Header { name, length } => write!(f, "{}:header:{}", name, length),
            SequenceItem::Body {
                name,
                length,
                index,
            } => write!(f, "{}:body:{}:{}", name, length, index),
            SequenceItem::Footer { name, length } => write!(f, "{}:footer:{}", name, length),
        }
    }
}
