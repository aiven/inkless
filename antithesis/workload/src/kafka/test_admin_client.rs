use crate::domain::{TestTopic, TopicConfig};
use anyhow::{anyhow, Context, Result};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, ResourceSpecifier, TopicReplication},
    client::DefaultClientContext,
    types::RDKafkaErrorCode,
    ClientConfig,
};
// use std::time::Duration;
use tracing::{error, info};
use tokio::time::{sleep, Duration};
use rdkafka::metadata::Metadata;

pub struct TestAdminClient {
    bootstrap_servers: String,
    inner_admin_client: Option<AdminClient<DefaultClientContext>>,
}

impl TestAdminClient {
    pub fn new(bootstrap_servers: &str) -> TestAdminClient {
        TestAdminClient {
            bootstrap_servers: bootstrap_servers.to_string(),
            inner_admin_client: None,
        }
    }
    pub async fn wait_on_cluster(&mut self) -> Result<()> {
        if self.inner_admin_client.is_some() {
            return Ok(())
        }

        loop {
            let client: AdminClient<DefaultClientContext> = ClientConfig::new()
                .set("bootstrap.servers", &self.bootstrap_servers)
                .create()
                .context("failed to create Kafka admin client")?;

            // Fetch cluster metadata to discover broker IDs
            let metadata: Metadata = client
                .inner()
                .fetch_metadata(None, Duration::from_secs(5))
                .context("failed to fetch cluster metadata")?;

            let mut all_brokers_up = true;

            for broker in metadata.brokers() {
                let broker_id = broker.id();
                let result = client
                    .describe_configs(
                        vec![&ResourceSpecifier::Broker(broker_id)],
                        &AdminOptions::default()
                            .request_timeout(Some(Duration::from_secs(5)))
                            .operation_timeout(Some(Duration::from_secs(5))),
                    )
                    .await;

                if result.is_err() {
                    all_brokers_up = false;
                    break;
                }
            }

            if all_brokers_up {
                self.inner_admin_client = Some(client);
                return Ok(());
            }

            // Avoid busy-looping if brokers aren't ready yet
            sleep(Duration::from_secs(5)).await;
        }
    }

    pub async fn check_config(&self) -> Result<()> {
        let consumer_offsets_config = self
            .inner_admin_client
            .as_ref()
            .unwrap()
            .describe_configs(
                vec![&ResourceSpecifier::Topic("__consumer_offsets")],
                &AdminOptions::default()
                    .request_timeout(Some(Duration::from_secs(5)))
                    .operation_timeout(Some(Duration::from_secs(5))),
            )
            .await;
        if let Ok(consumer_offsets_config) = consumer_offsets_config {
            if let Ok(topic_config) = &consumer_offsets_config[0] {
                if let Some(config_entry) = topic_config.get("min.insync.replicas") {
                    if let Some(ref config_value) = config_entry.value {
                        if config_value != "3" {
                            return Err(anyhow!("Please configure 'min.insync.replicas' for the topic '__consumer_offsets' to '3'"));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn create_topic(&self, topic: &TestTopic) -> Result<()> {
        loop {
            let mut topic_config = TopicConfig::new();
            topic_config
                .set("min.insync.replicas", "3")
                .set("flush.messages", "1");
            let create_topic_result = self
                .inner_admin_client
                .as_ref()
                .unwrap()
                .create_topics(
                    &[NewTopic {
                        name: &topic.name,
                        num_partitions: topic.num_partitions,
                        replication: TopicReplication::Fixed(topic.replication_factor),
                        config: topic_config.to_vec(),
                    }],
                    &AdminOptions::default(),
                )
                .await
                .context("failed to complete admin operation to create topics");
            match create_topic_result {
                Ok(topic_results) => {
                    for topic_result in topic_results {
                        match topic_result {
                            Ok(_) => {
                                info!(
                                    timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "topic_created",
                                    topic_name = topic.name,
                                    topic_num_partitions = topic.num_partitions,
                                    config = serde_json::to_string(&topic_config).unwrap().as_str()
                                );
                                return Ok(());
                            }
                            Err((topic_name, code)) => match code {
                                RDKafkaErrorCode::TopicAlreadyExists => {
                                    error!(
                                        timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "topic_already_exists",
                                        topic_name = topic_name.as_str(),
                                        error = format!("{}", code).as_str(),
                                        code = format!("{:?}", code)
                                    );
                                    return Ok(());
                                }
                                _ => {
                                    error!(
                                        timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true), event = "topic_creation_failed",
                                        topic_name = topic_name.as_str(),
                                        error = format!("{}", code).as_str(),
                                        code = format!("{:?}", code)
                                    );
                                    // wait for 5 secs before re-attempting
                                    tokio::time::sleep(Duration::from_secs(5)).await;
                                }
                            },
                        }
                    }
                }
                Err(err) => {
                    error!("failed to create topic: {:#}", err)
                }
            }
        }
    }
}
