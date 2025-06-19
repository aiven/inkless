use antithesis_kafka_workload::{
    domain::TestValidator,
    validation::{
        application_message_partitioning::ApplicationMessagePartitioningValidator,
        consumer_message_ordering::ConsumerMessageOrderingValidator, log_validator::LogValidator,
        message_integrity::MessageIntegrityValidator,
        producer_idempotence::ProducerIdempotenceValidator,
        producer_message_ordering::ProducerMessageOrderingValidator, workload_log::WorkloadLog,
    },
};
use std::{
    env,
    fs::{self},
    path::PathBuf,
};

fn validate_workload(workload_log_filename: PathBuf) {
    let validators: Vec<Box<dyn TestValidator>> = vec![
        Box::<ProducerMessageOrderingValidator>::default(),
        Box::<ProducerIdempotenceValidator>::default(),
        Box::<ConsumerMessageOrderingValidator>::default(),
        Box::<ApplicationMessagePartitioningValidator>::default(),
        Box::<MessageIntegrityValidator>::default(),
    ];
    let log_validator = LogValidator::new(validators);
    let workload_log = WorkloadLog::new(&workload_log_filename, log_validator).unwrap();
    let workload_start_cursor = workload_log.cursor();
    println!(
        "verifying workload log: {} from line {}",
        workload_log_filename.display(),
        workload_log.cursor()
    );
    let (workload_id, workload_end_cursor, workload_results, workload_ended) =
        workload_log.validate().expect("workload validation failed");

    // TODO: remove because we have in-line validation?
    for (validation, failures) in workload_results {
        for err in failures {
            println!(
                "{} | [FAILED] {} ({}) | Line {:>5}: {}",
                workload_id, validation, err.code, err.line, err.error
            );
        }
    }
    println!(
        "verified workload log: {}, checked {} lines up to line {}",
        workload_log_filename.display(),
        workload_end_cursor - workload_start_cursor,
        workload_end_cursor
    );
    if workload_ended {
        fs::rename(
            &workload_log_filename,
            format!("{}.verified", workload_log_filename.display()),
        )
        .expect("failed to rename verified log file");
    }
}

fn main() {
    antithesis_sdk::antithesis_init();
    let args: Vec<String> = env::args().collect();

    let workload_log_dir_path = args.get(1).expect("no log dir path provided");
    let entries = fs::read_dir(workload_log_dir_path).expect("failed to read log directory");
    for entry in entries.flatten() {
        if let Some(file_name) = entry.file_name().to_str() {
            if file_name.starts_with("kafka-workload") && file_name.ends_with(".log") {
                validate_workload(entry.path());
            }
        }
    }
}
