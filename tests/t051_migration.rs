use std::{
    fs,
    path::{Path, PathBuf},
};

use guigu_agent_bridge::storage::{connect, migrate};
use sqlx::Row;
use uuid::Uuid;

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("t051-migration-{}", Uuid::now_v7()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn copy_legacy_migrations(target: &Path) {
    let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    fs::create_dir(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if name.to_string_lossy().starts_with("0015_") {
            continue;
        }
        fs::copy(entry.path(), target.join(name)).unwrap();
    }
}

async fn seed_legacy(pool: &sqlx::SqlitePool) -> (String, String) {
    let endpoint = Uuid::now_v7().to_string();
    let conversation = Uuid::now_v7().to_string();
    let task = Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO agents(endpoint_id,agent_id,transport,enabled,address_json,capabilities_json) VALUES (?,?,'acp',1,NULL,'[]')")
        .bind(&endpoint).bind(format!("agent-{endpoint}")).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO conversations(conversation_id,transport,external_id,thread_ref,participants_json) VALUES (?,NULL,NULL,NULL,'[]')")
        .bind(&conversation).execute(pool).await.unwrap();
    sqlx::query("INSERT INTO tasks(task_id,root_task_id,parent_task_id,from_agent,to_agent,conversation_id,reply_to,text,priority,depth,hops,deadline,version) VALUES (?,?,NULL,?,?,?,NULL,'legacy',7,0,0,NULL,0)")
        .bind(&task).bind(&task).bind(&endpoint).bind(&endpoint).bind(&conversation)
        .execute(pool).await.unwrap();
    (task, endpoint)
}

#[tokio::test]
async fn v0211_upgrade_preserves_rows_and_reopens_once() {
    let temp = Temp::new();
    let migrations = temp.0.join("legacy-migrations");
    copy_legacy_migrations(&migrations);
    let database = temp.0.join("state.db");
    let pool = connect(&database).await.unwrap();
    sqlx::migrate::Migrator::new(migrations)
        .await
        .unwrap()
        .run(&pool)
        .await
        .unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&pool)
            .await
            .unwrap(),
        14
    );
    let (task, endpoint) = seed_legacy(&pool).await;
    migrate(&pool).await.unwrap();
    let row = sqlx::query("SELECT text,to_agent FROM tasks WHERE task_id=?")
        .bind(&task)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("text"), "legacy");
    assert_eq!(row.get::<String, _>("to_agent"), endpoint);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM acp_output_quota")
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );
    pool.close().await;
    let reopened = connect(&database).await.unwrap();
    migrate(&reopened).await.unwrap();
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT MAX(version) FROM _sqlx_migrations")
            .fetch_one(&reopened)
            .await
            .unwrap(),
        15
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM tasks WHERE task_id=?")
            .bind(task)
            .fetch_one(&reopened)
            .await
            .unwrap(),
        1
    );
}
