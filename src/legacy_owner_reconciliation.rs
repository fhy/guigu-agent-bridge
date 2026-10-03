use std::path::{Path, PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::{Connection, Row, SqliteConnection, sqlite::SqliteConnectOptions};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("rejected")]
    Rejected,
    #[error("database")]
    Database,
}
impl Error {
    pub const fn status(&self) -> u8 {
        3
    }
    pub const fn slug(&self) -> &'static str {
        match self {
            Self::Rejected => "rejected",
            Self::Database => "database",
        }
    }
}
#[derive(Debug, PartialEq, Eq)]
pub enum Outcome {
    Recovered,
    AlreadyStopped,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    operation_id: String,
    database: String,
    manifest_digest: String,
    pre_owner_state_digest: String,
    post_owner_state_digest: String,
    receipt_digest: String,
    receipt_sibling_digest: String,
    commit_heartbeat: String,
}

pub fn parse_args(args: &[std::ffi::OsString]) -> Result<PathBuf, Error> {
    if args.len() != 3 || args[0] != "reconcile-isolated-legacy-owner" || args[1] != "--manifest" {
        return Err(Error::Rejected);
    }
    Ok(PathBuf::from(&args[2]))
}
fn digest(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p.as_bytes());
    }
    format!("{:x}", h.finalize())
}
async fn owner_digest(c: &mut SqliteConnection) -> Result<String, Error> {
    let rows=sqlx::query("SELECT instance_token,started_at,heartbeat_at,state,process_fingerprint FROM runtime_instances ORDER BY instance_token").fetch_all(&mut *c).await.map_err(|_|Error::Database)?;
    let mut h = Sha256::new();
    for r in rows {
        for i in 0..5 {
            let v: String = r.try_get(i).map_err(|_| Error::Database)?;
            h.update((v.len() as u64).to_be_bytes());
            h.update(v);
        }
    }
    Ok(format!("{:x}", h.finalize()))
}
pub async fn reconcile(path: impl AsRef<Path>) -> Result<Outcome, Error> {
    let meta = std::fs::symlink_metadata(path.as_ref()).map_err(|_| Error::Rejected)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if !meta.is_file() || meta.mode() & 0o777 != 0o600 || meta.nlink() != 1 {
            return Err(Error::Rejected);
        }
    }
    let bytes = std::fs::read(path).map_err(|_| Error::Rejected)?;
    let m: Manifest = serde_json::from_slice(&bytes).map_err(|_| Error::Rejected)?;
    if digest(&[
        &m.operation_id,
        &m.database,
        &m.pre_owner_state_digest,
        &m.post_owner_state_digest,
        &m.commit_heartbeat,
    ]) != m.manifest_digest
    {
        return Err(Error::Rejected);
    }
    let opts = SqliteConnectOptions::new()
        .filename(&m.database)
        .create_if_missing(false)
        .foreign_keys(true);
    let mut c = SqliteConnection::connect_with(&opts)
        .await
        .map_err(|_| Error::Database)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut c)
        .await
        .map_err(|_| Error::Database)?;
    let result=async {
        if let Some(r)=sqlx::query("SELECT manifest_digest,pre_owner_state_digest,post_owner_state_digest,receipt_digest,receipt_sibling_digest,commit_heartbeat FROM legacy_owner_reconciliation_receipts WHERE operation_id=?").bind(&m.operation_id).fetch_optional(&mut c).await.map_err(|_|Error::Database)? {
            let values:(String,String,String,String,String,String)=(r.get(0),r.get(1),r.get(2),r.get(3),r.get(4),r.get(5));
            if values!=(m.manifest_digest.clone(),m.pre_owner_state_digest.clone(),m.post_owner_state_digest.clone(),m.receipt_digest.clone(),m.receipt_sibling_digest.clone(),m.commit_heartbeat.clone()) || owner_digest(&mut c).await?!=m.post_owner_state_digest { return Err(Error::Rejected); }
            return Ok(Outcome::AlreadyStopped)
        }
        if owner_digest(&mut c).await?!=m.pre_owner_state_digest { return Err(Error::Rejected); }
        let rows=sqlx::query("SELECT instance_token,process_fingerprint,state,started_at,heartbeat_at FROM runtime_instances WHERE state IN ('active','stopping')").fetch_all(&mut c).await.map_err(|_|Error::Database)?;
        if rows.len()!=1 { return Err(Error::Rejected); } let r=&rows[0]; let fp:String=r.get(1); if fp.starts_with("pid:"){return Err(Error::Rejected)}
        let changed=sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at=? WHERE instance_token=? AND process_fingerprint=? AND state=? AND started_at=? AND heartbeat_at=?").bind(&m.commit_heartbeat).bind(r.get::<String,_>(0)).bind(fp).bind(r.get::<String,_>(2)).bind(r.get::<String,_>(3)).bind(r.get::<String,_>(4)).execute(&mut c).await.map_err(|_|Error::Database)?;
        if changed.rows_affected()!=1 || owner_digest(&mut c).await?!=m.post_owner_state_digest {return Err(Error::Rejected)}
        let rd=digest(&[&m.operation_id,&m.manifest_digest,&m.pre_owner_state_digest,&m.post_owner_state_digest,&m.commit_heartbeat,"recovered"]); if rd!=m.receipt_digest{return Err(Error::Rejected)}
        sqlx::query("INSERT INTO legacy_owner_reconciliation_receipts VALUES(?,?,?,?,?,?,?,'recovered')").bind(&m.operation_id).bind(&m.manifest_digest).bind(&m.pre_owner_state_digest).bind(&m.post_owner_state_digest).bind(&m.receipt_digest).bind(&m.receipt_sibling_digest).bind(&m.commit_heartbeat).execute(&mut c).await.map_err(|_|Error::Database)?;
        Ok(Outcome::Recovered)
    }.await;
    match result {
        Ok(v) => {
            sqlx::query("COMMIT")
                .execute(&mut c)
                .await
                .map_err(|_| Error::Database)?;
            Ok(v)
        }
        Err(e) => {
            let _ = sqlx::query("ROLLBACK").execute(&mut c).await;
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{connect, migrate};
    use serde_json::json;

    #[tokio::test]
    async fn real_sqlite_legacy_owner_cas_and_repeat_are_atomic() {
        let root = std::env::temp_dir().join(format!("t050-legacy-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&root).unwrap();
        let db = root.join("state.db");
        let pool = connect(&db).await.unwrap();
        migrate(&pool).await.unwrap();
        sqlx::query(
            "INSERT INTO runtime_instances VALUES('legacy','start','heart','active','legacy-fp')",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;
        let mut c = SqliteConnection::connect_with(&SqliteConnectOptions::new().filename(&db))
            .await
            .unwrap();
        let pre = owner_digest(&mut c).await.unwrap();
        sqlx::query("UPDATE runtime_instances SET state='stopped',heartbeat_at='commit'")
            .execute(&mut c)
            .await
            .unwrap();
        let post = owner_digest(&mut c).await.unwrap();
        sqlx::query("UPDATE runtime_instances SET state='active',heartbeat_at='heart'")
            .execute(&mut c)
            .await
            .unwrap();
        drop(c);
        let op = "op";
        let dbs = db.to_string_lossy();
        let md = digest(&[op, &dbs, &pre, &post, "commit"]);
        let rd = digest(&[op, &md, &pre, &post, "commit", "recovered"]);
        let manifest = root.join("manifest.json");
        std::fs::write(&manifest,serde_json::to_vec(&json!({"operation_id":op,"database":dbs,"manifest_digest":md,"pre_owner_state_digest":pre,"post_owner_state_digest":post,"receipt_digest":rd,"receipt_sibling_digest":digest(&[]),"commit_heartbeat":"commit"})).unwrap()).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&manifest, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        assert_eq!(reconcile(&manifest).await.unwrap(), Outcome::Recovered);
        assert_eq!(reconcile(&manifest).await.unwrap(), Outcome::AlreadyStopped);
    }
}
