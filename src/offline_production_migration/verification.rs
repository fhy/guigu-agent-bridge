use super::{Failure, evidence::digest};
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Table {
    pub columns: Vec<String>,
    pub digest: String,
    pub rows: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Facts {
    pub version: i64,
    pub schema_digest: String,
    pub tables: BTreeMap<String, Table>,
}

pub(super) fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

pub(super) async fn open(
    path: &std::path::Path,
    readonly: bool,
) -> Result<SqliteConnection, Failure> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(readonly)
        .create_if_missing(false)
        .foreign_keys(true)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Full)
        .busy_timeout(std::time::Duration::from_secs(5));
    SqliteConnection::connect_with(&options)
        .await
        .map_err(|_| Failure::Preflight)
}

pub(super) async fn facts(connection: &mut SqliteConnection) -> Result<Facts, Failure> {
    let integrity: String = sqlx::query_scalar("PRAGMA integrity_check")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    let foreign = sqlx::query("PRAGMA foreign_key_check")
        .fetch_all(&mut *connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    if integrity != "ok" || !foreign.is_empty() {
        return Err(Failure::ManualRequired);
    }
    let version = ledger(connection).await?;
    let schema = sqlx::query("SELECT type,name,tbl_name,sql FROM sqlite_schema WHERE name NOT LIKE 'sqlite_%' ORDER BY type,name")
        .fetch_all(&mut *connection).await.map_err(|_| Failure::Preflight)?;
    let mut normalized = Vec::new();
    let mut tables = BTreeMap::new();
    for row in schema {
        let kind: String = row.get(0);
        let name: String = row.get(1);
        normalized.push((
            kind.clone(),
            name.clone(),
            row.get::<String, _>(2),
            row.get::<Option<String>, _>(3),
        ));
        if kind == "table" && name != "_sqlx_migrations" {
            let columns = columns(connection, &name).await?;
            tables.insert(name.clone(), table(connection, &name, columns).await?);
        }
    }
    Ok(Facts {
        version,
        schema_digest: digest(&serde_json::to_vec(&normalized).map_err(|_| Failure::Preflight)?),
        tables,
    })
}

async fn ledger(connection: &mut SqliteConnection) -> Result<i64, Failure> {
    let ledger = sqlx::query(
        "SELECT version,description,success,checksum FROM _sqlx_migrations ORDER BY version",
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(|_| Failure::Preflight)?;
    let contract = crate::storage::migration_contract();
    if !(18..=20).contains(&ledger.len()) || ledger.len() > contract.len() {
        return Err(Failure::ManualRequired);
    }
    for (row, expected) in ledger.iter().zip(&contract) {
        if row
            .try_get::<i64, _>(0)
            .map_err(|_| Failure::ManualRequired)?
            != expected.0
            || row
                .try_get::<String, _>(1)
                .map_err(|_| Failure::ManualRequired)?
                != expected.1
            || !row
                .try_get::<bool, _>(2)
                .map_err(|_| Failure::ManualRequired)?
            || row
                .try_get::<Vec<u8>, _>(3)
                .map_err(|_| Failure::ManualRequired)?
                != expected.2
        {
            return Err(Failure::ManualRequired);
        }
    }
    Ok(ledger.len() as i64)
}

async fn columns(connection: &mut SqliteConnection, name: &str) -> Result<Vec<String>, Failure> {
    let query = AssertSqlSafe(format!("PRAGMA table_info({})", identifier(name)));
    let rows = sqlx::query(query)
        .fetch_all(connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    Ok(rows
        .into_iter()
        .map(|row| row.get::<String, _>(1))
        .collect())
}

async fn table(
    connection: &mut SqliteConnection,
    name: &str,
    columns: Vec<String>,
) -> Result<Table, Failure> {
    let expressions = columns
        .iter()
        .map(|column| format!("quote({})", identifier(column)))
        .collect::<Vec<_>>()
        .join(",");
    let query = AssertSqlSafe(format!("SELECT {expressions} FROM {}", identifier(name)));
    let rows = sqlx::query(query)
        .fetch_all(connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    let mut values = rows
        .iter()
        .map(|row| {
            (0..columns.len())
                .map(|index| row.get::<String, _>(index))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort();
    Ok(Table {
        rows: values.len(),
        columns,
        digest: digest(&serde_json::to_vec(&values).map_err(|_| Failure::Preflight)?),
    })
}

pub(super) async fn preserved(
    connection: &mut SqliteConnection,
    before: &Facts,
    version: i64,
) -> Result<(), Failure> {
    for (name, original) in &before.tables {
        if table(connection, name, original.columns.clone()).await? != *original {
            return Err(Failure::ManualRequired);
        }
    }
    if before.version < 19 && version >= 19 {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM session_owner_bindings")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| Failure::Migration)?;
        if count != 0 {
            return Err(Failure::ManualRequired);
        }
    }
    if version < 20 {
        return Ok(());
    }
    let bad: i64 = sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments WHERE session_id_digest IS NOT NULL OR before_state_digest IS NOT NULL OR post_state_digest IS NOT NULL OR abandonment_kind != 'manual_abandonment' OR termination_proven != 0")
        .fetch_one(connection).await.map_err(|_| Failure::Migration)?;
    if bad != 0 {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) async fn expected_schema(version: i64) -> Result<String, Failure> {
    let mut connection = SqliteConnection::connect("sqlite::memory:")
        .await
        .map_err(|_| Failure::Preflight)?;
    let migrator = embedded_through(version);
    migrator
        .run(&mut connection)
        .await
        .map_err(|_| Failure::Preflight)?;
    let result = facts(&mut connection).await;
    connection.close().await.map_err(|_| Failure::Preflight)?;
    Ok(result?.schema_digest)
}

pub(super) fn embedded_through(version: i64) -> sqlx::migrate::Migrator {
    static EMBEDDED: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
    sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            EMBEDDED
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    }
}
