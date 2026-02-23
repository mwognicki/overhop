mod backend;
mod engine;
mod error;
mod facade;
mod path;
mod sled_backend;

pub use backend::StorageBackend;
pub use engine::{SledMode, StorageEngine};
pub use error::StorageError;
pub use facade::StorageFacade;
use path::expand_home_path;
pub use sled_backend::SledStorage;

#[cfg(test)]
mod tests {
    use crate::config::{
        AppConfig, HeartbeatConfig, LoggingConfig, PaginationConfig, ServerConfig, SledConfig,
        StorageConfig, WireConfig, WireSessionConfig,
    };
    use crate::logging::LoggerConfig;
    use crate::orchestrator::queues::Queue;
    use crate::storage::{StorageFacade, StorageEngine};

    use super::expand_home_path;

    fn test_storage(path: &str) -> StorageFacade {
        let app_config = AppConfig {
            logging: LoggingConfig {
                level: "debug".to_owned(),
                human_friendly: false,
            },
            heartbeat: HeartbeatConfig { interval_ms: 1000 },
            server: ServerConfig {
                host: "127.0.0.1".to_owned(),
                port: 9876,
                tls_enabled: false,
            },
            wire: WireConfig {
                max_envelope_size_bytes: 8_388_608,
                session: WireSessionConfig::default(),
            },
            pagination: PaginationConfig::default(),
            storage: StorageConfig {
                engine: "sled".to_owned(),
                path: path.to_owned(),
                self_debug_path: None,
                sled: SledConfig::default(),
            },
        };
        let logger = crate::logging::Logger::new(LoggerConfig {
            min_level: crate::logging::LogLevel::Error,
            human_friendly: false,
        });
        StorageFacade::initialize(&app_config, &logger).expect("storage init should work")
    }

    fn unique_temp_path(label: &str) -> String {
        let path = std::env::temp_dir().join(format!(
            "overhop-storage-test-{label}-{}-{}",
            std::process::id(),
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        path.to_string_lossy().to_string()
    }

    #[test]
    fn expands_tilde_prefix_to_home() {
        let home = std::env::var("HOME").expect("HOME should be available in tests");
        let expanded = expand_home_path("~/.overhop/data").expect("expansion should work");
        assert_eq!(expanded, std::path::Path::new(&home).join(".overhop/data"));
    }

    #[test]
    fn expands_home_env_prefix() {
        let home = std::env::var("HOME").expect("HOME should be available in tests");
        let expanded = expand_home_path("$HOME/.overhop/data").expect("expansion should work");
        assert_eq!(expanded, std::path::Path::new(&home).join(".overhop/data"));
    }

    #[test]
    fn keeps_absolute_paths_unchanged() {
        let expanded = expand_home_path("/var/lib/overhop/data").expect("expansion should work");
        assert_eq!(expanded, std::path::PathBuf::from("/var/lib/overhop/data"));
    }

    #[test]
    fn queue_replace_and_load_roundtrip() {
        let path = unique_temp_path("queues-roundtrip");
        let storage = test_storage(&path);
        assert_eq!(storage.engine(), StorageEngine::Sled);
        assert!(storage.data_path().to_string_lossy().contains("overhop-storage-test"));

        let queue = Queue::new("critical", None);
        storage
            .replace_queues(&[queue.clone()])
            .expect("queue persist should pass");

        let loaded = storage.load_queues().expect("queue load should pass");
        assert_eq!(loaded, vec![queue]);
        let missing_job = storage
            .get_job_payload_by_uuid(uuid::Uuid::new_v4())
            .expect("job lookup should work");
        assert!(missing_job.is_none());

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn job_status_index_roundtrip_and_updates() {
        let path = unique_temp_path("job-status-index");
        let storage = test_storage(&path);

        let job_uuid = uuid::Uuid::new_v4();
        let created_at = chrono::Utc::now();
        let record = serde_json::json!({
            "uuid": job_uuid.to_string(),
            "jid": format!("critical:{job_uuid}"),
            "queue_name": "critical",
            "status": "new",
            "execution_start_at": chrono::Utc::now().to_rfc3339(),
            "created_at": created_at.to_rfc3339(),
        });
        storage
            .upsert_job_record(
                job_uuid,
                &record,
                chrono::Utc::now().timestamp_millis(),
                created_at.timestamp_millis(),
                "critical",
                "new",
            )
            .expect("job upsert should work");

        let new_jobs = storage
            .list_job_uuids_by_status("new")
            .expect("new status query should work");
        assert!(new_jobs.contains(&job_uuid));

        let mut updated = record;
        updated["status"] = serde_json::Value::String("waiting".to_owned());
        storage
            .upsert_job_record(
                job_uuid,
                &updated,
                chrono::Utc::now().timestamp_millis(),
                created_at.timestamp_millis(),
                "critical",
                "waiting",
            )
            .expect("job status update should work");
        let waiting_jobs = storage
            .list_job_uuids_by_status("waiting")
            .expect("waiting status query should work");
        assert!(waiting_jobs.contains(&job_uuid));

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn waiting_status_fifo_index_orders_by_created_at_ascending() {
        let path = unique_temp_path("job-status-fifo");
        let storage = test_storage(&path);

        let created_a = chrono::Utc::now() - chrono::Duration::seconds(3);
        let created_b = chrono::Utc::now() - chrono::Duration::seconds(2);
        let created_c = chrono::Utc::now() - chrono::Duration::seconds(1);
        let uuid_a = uuid::Uuid::new_v4();
        let uuid_b = uuid::Uuid::new_v4();
        let uuid_c = uuid::Uuid::new_v4();

        let record_a = serde_json::json!({
            "uuid": uuid_a.to_string(),
            "jid": format!("alpha:{uuid_a}"),
            "queue_name": "alpha",
            "status": "waiting",
            "execution_start_at": created_a.to_rfc3339(),
            "created_at": created_a.to_rfc3339(),
        });
        let record_b = serde_json::json!({
            "uuid": uuid_b.to_string(),
            "jid": format!("alpha:{uuid_b}"),
            "queue_name": "alpha",
            "status": "waiting",
            "execution_start_at": created_b.to_rfc3339(),
            "created_at": created_b.to_rfc3339(),
        });
        let record_c = serde_json::json!({
            "uuid": uuid_c.to_string(),
            "jid": format!("alpha:{uuid_c}"),
            "queue_name": "alpha",
            "status": "waiting",
            "execution_start_at": created_c.to_rfc3339(),
            "created_at": created_c.to_rfc3339(),
        });

        storage
            .upsert_job_record(
                uuid_c,
                &record_c,
                created_c.timestamp_millis(),
                created_c.timestamp_millis(),
                "alpha",
                "waiting",
            )
            .expect("upsert c should work");
        storage
            .upsert_job_record(
                uuid_a,
                &record_a,
                created_a.timestamp_millis(),
                created_a.timestamp_millis(),
                "alpha",
                "waiting",
            )
            .expect("upsert a should work");
        storage
            .upsert_job_record(
                uuid_b,
                &record_b,
                created_b.timestamp_millis(),
                created_b.timestamp_millis(),
                "alpha",
                "waiting",
            )
            .expect("upsert b should work");

        let ordered = storage
            .list_job_uuids_by_status_fifo("waiting")
            .expect("fifo status query should work");
        assert_eq!(ordered, vec![uuid_a, uuid_b, uuid_c]);

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn remove_job_record_removes_primary_and_indexes() {
        let path = unique_temp_path("job-remove");
        let storage = test_storage(&path);

        let created_at = chrono::Utc::now() - chrono::Duration::seconds(1);
        let job_uuid = uuid::Uuid::new_v4();
        let record = serde_json::json!({
            "uuid": job_uuid.to_string(),
            "jid": format!("alpha:{job_uuid}"),
            "queue_name": "alpha",
            "status": "new",
            "execution_start_at": created_at.to_rfc3339(),
            "created_at": created_at.to_rfc3339(),
        });
        storage
            .upsert_job_record(
                job_uuid,
                &record,
                created_at.timestamp_millis(),
                created_at.timestamp_millis(),
                "alpha",
                "new",
            )
            .expect("job upsert should work");

        let removed = storage
            .remove_job_record(job_uuid)
            .expect("job remove should work");
        assert!(removed);
        assert!(
            storage
                .get_job_payload_by_uuid(job_uuid)
                .expect("job lookup should work")
                .is_none()
        );
        assert!(
            !storage
                .list_job_uuids_by_status("new")
                .expect("status lookup should work")
                .contains(&job_uuid)
        );
        assert!(
            !storage
                .list_job_uuids_by_status_fifo("new")
                .expect("status fifo lookup should work")
                .contains(&job_uuid)
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn list_job_records_by_queue_and_status_is_paginated_and_created_at_ordered() {
        let path = unique_temp_path("job-list-page");
        let storage = test_storage(&path);

        let queue_name = "critical";
        let status = "waiting";

        for offset in 0..5 {
            let created_at = chrono::Utc::now() + chrono::Duration::milliseconds(offset);
            let job_uuid = uuid::Uuid::new_v4();
            let record = serde_json::json!({
                "uuid": job_uuid.to_string(),
                "jid": format!("{queue_name}:{job_uuid}"),
                "queue_name": queue_name,
                "status": status,
                "execution_start_at": created_at.to_rfc3339(),
                "created_at": created_at.to_rfc3339(),
            });
            storage
                .upsert_job_record(
                    job_uuid,
                    &record,
                    created_at.timestamp_millis(),
                    created_at.timestamp_millis(),
                    queue_name,
                    status,
                )
                .expect("job upsert should work");
        }

        let first_page = storage
            .list_job_records_by_queue_and_status(queue_name, status, 1, 2)
            .expect("list should work");
        let second_page = storage
            .list_job_records_by_queue_and_status(queue_name, status, 2, 2)
            .expect("list should work");
        let third_page = storage
            .list_job_records_by_queue_and_status(queue_name, status, 3, 2)
            .expect("list should work");

        assert_eq!(first_page.len(), 2);
        assert_eq!(second_page.len(), 2);
        assert_eq!(third_page.len(), 1);
        let created_first = first_page[0]
            .get("created_at")
            .and_then(serde_json::Value::as_str)
            .expect("created_at should exist");
        let created_last = third_page[0]
            .get("created_at")
            .and_then(serde_json::Value::as_str)
            .expect("created_at should exist");
        assert!(created_first < created_last);

        let _ = std::fs::remove_dir_all(path);
    }
}
