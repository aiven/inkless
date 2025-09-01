use anyhow::{anyhow, Context, Result};
use antithesis_kafka_workload::{
    config::WorkloadConfig,
    decisions::{ConsumeTopic, CreateTopic},
    domain::{GlobalState, TestTopic},
    kafka::{
        test_admin_client::TestAdminClient, test_consumer::TestConsumer,
        test_producer::TestProducer,
    },
};
use std::{
    env,
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::{atomic::Ordering, Arc, RwLock},
};
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{
    fmt,
    layer::{Filter, SubscriberExt},
    Layer, Registry,
};
use uuid::Uuid;

use serde_json::{json};
use antithesis_sdk::lifecycle;

/// TestEventFilter filters out log events that indicate some meaningful workload test event.
/// In our case these include important events like the reading / writing of messages, partition reassignments, etc.
struct TestEventFilter;

impl<S> Filter<S> for TestEventFilter {
    fn enabled(
        &self,
        meta: &tracing::Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        meta.fields().field("event").is_some()
    }
}

/// setup_logging configures the logging system for the workload.
/// It establishes two logging layers:
/// 1. A JSON logging layer that filters log events containing the 'event' field and writes them to a dedicated workload log file.
/// 2. A JSON logging layer that outputs all log events to standard output.
fn setup_logging(workload_id: Uuid, config: &WorkloadConfig) -> Result<()> {
    if !fs::metadata(&config.log_dir).map(|meta| meta.is_dir())
        .with_context(|| format!("log directory path '{}' does not exist", config.log_dir))?
    {
        return Err(anyhow!(
            "log directory path '{}' must point to an existing directory",
            config.log_dir
        ));
    };

    // creates a workload log file with a random generated name to contain the output for this workload
    let workload_log_filename = format!("kafka-workload-{}.log", workload_id);
    let workload_log_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(PathBuf::from(&config.log_dir).join(workload_log_filename))
        .expect("failed to create output file");

    // creates a log subscriber with two layers
    let global_log_subscriber = Registry::default()
        .with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .without_time()
                .with_writer(workload_log_file) // write this layer's output to the workload log file
                .with_filter(TestEventFilter), // filter log events using the `TestEventFilter`
        )
        .with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .without_time()
                .with_filter(if config.enable_debug {
                    LevelFilter::DEBUG
                } else {
                    LevelFilter::INFO
                }),
        );

    // sets the global default log subscriber to the one we just created
    tracing::subscriber::set_global_default(global_log_subscriber)
        .context("failed to register global log subscriber")?;
    Ok(())
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<()> {
    antithesis_sdk::antithesis_init();

    let args: Vec<String> = env::args().collect();
    let config_path = args.get(1).expect("no config path provided");
    let config = WorkloadConfig::new(config_path).expect("failed to read configuration file");

    let workload_id = uuid::Uuid::new_v4();

    setup_logging(workload_id, &config)?;

    info!(
        config = serde_json::to_string(&config).expect("failed to serialize config"),
        "workload configuration loaded"
    );

    let mut producer_tasks = tokio::task::JoinSet::new();
    let mut consumer_tasks = tokio::task::JoinSet::new();

    let mut admin_client = TestAdminClient::new(&config.bootstrap_servers);

    admin_client.wait_on_cluster().await?;

    if config.is_strict {
        admin_client.check_config().await?;
    }

    let timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    let startup_data = json!({"timestamp": timestamp});
    lifecycle::setup_complete(&startup_data);
    // not necessary?
    // is_strict_config = config.is_strict;

    // info!(
    //     timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    //     event = "workload_started",
    //     is_strict_config = config.is_strict
    // );

    let mut topics = Vec::new();
    let mut producers = Vec::new();
    let mut consumer_groups = Vec::new();
    let global_state: Arc<RwLock<GlobalState>> = Arc::new(RwLock::new(GlobalState::default()));

    // create a random number of topics
    let topic_count = config.topic_count.get_random_value();
    for _ in 0..topic_count {
        let topic = TestTopic {
            name: uuid::Uuid::new_v4().to_string(),
            num_partitions: config.topic_partition_count.get_random_value() as i32,
            replication_factor: 3,
        };
        // If the config is strict we always create the topic prior to sending.
        // This is because auto-created topics often have a replication factor of 1
        if config.is_strict {
            admin_client
                .create_topic(&topic)
                .await
                .expect("failed to create topic");
        } else {
            match antithesis_sdk::random::random_choice(&[CreateTopic::Yes, CreateTopic::No])
                .unwrap()
            {
                CreateTopic::Yes => {
                    admin_client
                        .create_topic(&topic)
                        .await
                        .expect("failed to create topic");
                }
                CreateTopic::No => {}
            }
        }
        topics.push(topic);
    }

    // create a random number of consumer groups
    let consumer_group_count = config.consumer_group_count.get_random_value();
    for consumer_group_index in 0..consumer_group_count {
        let mut consumer_group = Vec::new();
        let group_id = format!("{}-cg{}", workload_id, consumer_group_index + 1);
        // create a random number of consumers
        let members_count = config.consumer_group_members_count.get_random_value();
        for consumer_index in 0..members_count {
            let member = TestConsumer::new(
                format!("c{}", consumer_index + 1).as_str(),
                &group_id,
                &config,
                global_state.clone(),
                true,
                false,
            )
            .expect("failed to create test consumer");
            consumer_group.push(member);
        }
        consumer_groups.push(consumer_group);
    }

    // create a random number of producers
    let producer_count = config.producer_count.get_random_value();
    for index in 0..producer_count {
        let producer = TestProducer::new(
            format!("{}-p{}", workload_id, index + 1).as_str(),
            &config,
            global_state.clone(),
        )
        .expect("failed to create test producer");
        producers.push(producer);
    }

    for producer in producers {
        producer_tasks.spawn({
            let config = config.clone();
            let topics = topics.clone();
            async move {
                producer.produce(config, topics).await;
            }
        });
    }

    for consumer_group in consumer_groups {
        for consumer in consumer_group {
            let mut consumer_topics = Vec::new();
            for topic in topics.iter() {
                match antithesis_sdk::random::random_choice(&[ConsumeTopic::Yes, ConsumeTopic::No])
                    .unwrap()
                {
                    ConsumeTopic::Yes => {
                        consumer_topics.push(topic.clone());
                    }
                    ConsumeTopic::No => {}
                }
            }
            consumer.subscribe(&consumer_topics).unwrap();
            consumer_tasks.spawn({
                let config = config.clone();
                async move {
                    consumer.poll(config).await;
                }
            });
        }
    }

    // Wait for all tasks to be completed
    while producer_tasks.join_next().await.is_some() {}
    global_state
        .write().unwrap()
        .all_producers_completed
        .store(true, Ordering::SeqCst);
    info!(
        timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "all_producers_completed"
    );

    // create a special consumer group and one consumer in it which observes all topics
    let observer = TestConsumer::new(
        "o",
        format!("{}-og", workload_id).as_str(),
        &config,
        global_state.clone(),
        true,
        true,
    )
    .expect("failed to create observer consumer");
    observer
        .subscribe(&topics)
        .context("failed to subscribe observer consumer to all topics")?;
    consumer_tasks.spawn({
        let config = config.clone();
        async move {
            observer.poll(config).await;
        }
    });

    while consumer_tasks.join_next().await.is_some() {}

    info!(
        timestamp = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        event = "workload_ended"
    );

    Ok(())
}
