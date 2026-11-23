use serde::Serialize;
use sqlx::{Connection, SqliteConnection, sqlite::SqliteConnectOptions};
use std::{path::Path, str::FromStr};

use super::{bootstrap::lineage::row_disposition, inventory};

#[derive(Serialize)]
struct RowReport {
    table: String,
    key: String,
    revision: String,
    digest: String,
    parents: Vec<inventory::RowIdentity>,
    disposition: String,
}

async fn collect(path: &Path) -> Vec<RowReport> {
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
        .unwrap()
        .read_only(true)
        .create_if_missing(false);
    let mut connection = SqliteConnection::connect_with(&options).await.unwrap();
    let mut reports = Vec::new();
    for identity in inventory::identities(&mut connection).await.unwrap() {
        let snapshot = inventory::snapshot(&mut connection, &identity)
            .await
            .unwrap();
        let (names, values) = inventory::values(&mut connection, &identity).await.unwrap();
        let disposition = match row_disposition(&mut connection, &identity, &names, &values).await {
            Ok(disposition) => disposition.to_owned(),
            Err(error) => format!("refused:{error}"),
        };
        reports.push(RowReport {
            table: identity.table,
            key: identity.key,
            revision: snapshot.revision,
            digest: snapshot.digest,
            parents: snapshot.parents,
            disposition,
        });
    }
    reports
}

#[tokio::test]
#[ignore = "explicit R070 retained-inventory diagnostic"]
async fn r070_collects_production_inventory_without_writes() {
    let retained = std::env::var_os("R070_RETAINED_DB").expect("R070_RETAINED_DB");
    let reports = collect(Path::new(&retained)).await;
    let output = std::env::var_os("R070_REPORT").expect("R070_REPORT");
    let bytes = serde_json::to_vec_pretty(&reports).unwrap();
    std::fs::write(output, bytes).unwrap();
}
