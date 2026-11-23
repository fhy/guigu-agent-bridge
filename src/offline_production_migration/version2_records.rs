use std::{fs, os::unix::fs::MetadataExt, path::Path};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
    Failure, Request,
    evidence::{self, Evidence},
    records::{self, Complete as LegacyComplete, Operation},
    verification::Facts,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Predecessor {
    pub root_digest: String,
    pub complete_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Root {
    pub protocol: u8,
    pub operation: Operation,
    pub db_generation: String,
    pub initial_ledger: i64,
    pub predecessor: Option<Predecessor>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Archive {
    pub root_digest: String,
    pub snapshot_digest: String,
    pub before: Facts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Intent {
    pub root_digest: String,
    pub archive_digest: String,
    pub target_schema: i64,
    pub migration_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Complete {
    pub intent_digest: String,
    pub actual_schema: i64,
    pub schema_digest: String,
    pub preserved_digest: String,
}

pub(super) fn load<Value>(path: &Path) -> Result<Value, Failure>
where
    Value: DeserializeOwned + Serialize,
{
    evidence::canonical(path).map_err(|_| Failure::ManualRequired)?;
    let metadata = fs::symlink_metadata(path).map_err(|_| Failure::ManualRequired)?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.mode() & 0o077 != 0
        || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(Failure::ManualRequired);
    }
    let bytes = fs::read(path).map_err(|_| Failure::ManualRequired)?;
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| Failure::ManualRequired)?;
    if serde_json::to_vec(&value).map_err(|_| Failure::ManualRequired)? != bytes {
        return Err(Failure::ManualRequired);
    }
    Ok(value)
}

pub(super) fn root(req: &Request, evidence: &Evidence, version: i64) -> Result<Root, Failure> {
    let anchor = records::anchor(&req.database);
    if anchor.exists() {
        if let Ok(root) = load::<Root>(&anchor) {
            validate_root(req, evidence, &root)?;
            validate_predecessor(&root)?;
            return Ok(root);
        }
        if version != 20 {
            return Err(Failure::ManualRequired);
        }
        records::startup_allowed(&req.database)?;
        let prior: Operation = records::load(&anchor)?;
        let complete_path = prior.manifest_dir.join("complete.json");
        let _: LegacyComplete = records::load(&complete_path)?;
        let expected = Predecessor {
            root_digest: evidence::hash_file(&anchor)?,
            complete_digest: evidence::hash_file(&complete_path)?,
        };
        let candidate = req.manifest_dir.join("root-v2.json");
        if candidate.exists() {
            let root: Root = load(&candidate)?;
            validate_root(req, evidence, &root)?;
            if root.predecessor.as_ref() != Some(&expected) || root.initial_ledger != 20 {
                return Err(Failure::ManualRequired);
            }
            validate_predecessor(&root)?;
            return Ok(root);
        }
        return Ok(new_root(req, evidence, version, Some(expected)));
    }
    if version != 18 {
        return Err(Failure::ManualRequired);
    }
    Ok(new_root(req, evidence, version, None))
}

fn new_root(req: &Request, evidence: &Evidence, version: i64, prior: Option<Predecessor>) -> Root {
    Root {
        protocol: 2,
        operation: records::operation(req, evidence),
        db_generation: uuid::Uuid::now_v7().to_string(),
        initial_ledger: version,
        predecessor: prior,
        created_at: chrono::Utc::now().to_rfc3339(),
    }
}

pub(super) fn validate_root(
    req: &Request,
    evidence: &Evidence,
    root: &Root,
) -> Result<(), Failure> {
    if root.protocol != 2
        || root.operation != records::operation(req, evidence)
        || !matches!(root.initial_ledger, 18 | 20)
        || uuid::Uuid::parse_str(&root.db_generation).is_err()
        || chrono::DateTime::parse_from_rfc3339(&root.created_at).is_err()
        || (root.initial_ledger == 20) != root.predecessor.is_some()
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

fn validate_predecessor(root: &Root) -> Result<(), Failure> {
    let Some(prior) = &root.predecessor else {
        return Ok(());
    };
    let path = root.operation.manifest_dir.join("predecessor-root-v1.json");
    if evidence::hash_file(&path)? != prior.root_digest {
        return Err(Failure::ManualRequired);
    }
    let operation: Operation = records::load(&path)?;
    if operation.database != root.operation.database
        || operation.database_identity != root.operation.database_identity
        || evidence::hash_file(&operation.manifest_dir.join("complete.json"))?
            != prior.complete_digest
    {
        return Err(Failure::ManualRequired);
    }
    Ok(())
}

pub(super) fn publish_root(req: &Request, root: &Root) -> Result<(), Failure> {
    let anchor = records::anchor(&req.database);
    if let Ok(existing) = load::<Root>(&anchor) {
        return if existing == *root {
            Ok(())
        } else {
            Err(Failure::ManualRequired)
        };
    }
    let Some(predecessor) = &root.predecessor else {
        return records::publish(&anchor, root);
    };
    if evidence::hash_file(&anchor)? != predecessor.root_digest {
        return Err(Failure::ManualRequired);
    }
    let old = fs::read(&anchor).map_err(|_| Failure::ManualRequired)?;
    let retained = req.manifest_dir.join("predecessor-root-v1.json");
    if retained.exists() {
        if fs::read(&retained).map_err(|_| Failure::ManualRequired)? != old {
            return Err(Failure::ManualRequired);
        }
    } else {
        publish_bytes(&retained, &old)?;
    }
    let candidate = req.manifest_dir.join("root-v2.json");
    if candidate.exists() {
        if load::<Root>(&candidate)? != *root {
            return Err(Failure::ManualRequired);
        }
    } else {
        records::publish(&candidate, root)?;
    }
    validate_predecessor(root)?;
    super::flow::fault("v2-before-root-replacement");
    let temporary = anchor.with_extension("successor");
    if temporary.exists() {
        if load::<Root>(&temporary)? != *root {
            return Err(Failure::ManualRequired);
        }
    } else {
        publish_bytes(
            &temporary,
            &serde_json::to_vec(root).map_err(|_| Failure::Preflight)?,
        )?;
    }
    fs::rename(&temporary, &anchor).map_err(|_| Failure::Preflight)?;
    records::sync_dir(anchor.parent().ok_or(Failure::Preflight)?)
}

fn publish_bytes(path: &Path, bytes: &[u8]) -> Result<(), Failure> {
    use std::{io::Write, os::unix::fs::OpenOptionsExt};
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(path)
        .map_err(|_| Failure::ManualRequired)?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|_| Failure::Preflight)?;
    records::sync_dir(path.parent().ok_or(Failure::Preflight)?)
}
