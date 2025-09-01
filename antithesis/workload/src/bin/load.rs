use std::env;

use anyhow::{Context, Result};
use antithesis_kafka_workload::{config::WorkloadConfig, kafka::test_admin_client::TestAdminClient};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{fmt, layer::SubscriberExt, Layer, Registry};

use serde_json::{json};
use antithesis_sdk::lifecycle;

fn setup_logging() -> Result<()> {
    let global_log_subscriber =
        Registry::default().with(fmt::layer().json().with_filter(LevelFilter::INFO));

    // sets the global default log subscriber to the one we just created
    tracing::subscriber::set_global_default(global_log_subscriber)
        .context("failed to register global log subscriber")?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).expect("no config path provided");
    let config = WorkloadConfig::new(config_path).context("failed to read configuration file")?;

    setup_logging()?;

    let mut admin_client =
        TestAdminClient::new(&config.bootstrap_servers);

    admin_client.wait_on_cluster().await?;

    if config.is_strict {
        admin_client.check_config().await?;
    }

    // TODO: Maybe use antithesis send event
    info!(
        timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "cluster_started",
        is_strict_config = config.is_strict
    );
    
    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let startup_data = json!({"timestamp": timestamp});
    lifecycle::setup_complete(&startup_data);
    
    Ok(())
}
