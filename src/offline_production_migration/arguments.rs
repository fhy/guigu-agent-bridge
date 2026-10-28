use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use super::Failure;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Preflight,
    Apply,
    Resume,
    Verify,
}

#[derive(Debug, Clone)]
pub struct Request {
    pub mode: Mode,
    pub target_schema: i64,
    pub config: PathBuf,
    pub database: PathBuf,
    pub operation_id: String,
    pub manifest_dir: PathBuf,
    pub release_provenance: PathBuf,
    pub executable: PathBuf,
}

pub fn parse_identity_args(args: &[OsString]) -> Result<Request, Failure> {
    if args.len() != 15 || args[0] != "offline-capture-matrix-identity" {
        return Err(Failure::InvalidArguments);
    }
    let mut translated = args.to_vec();
    translated[0] = "offline-migrate-production".into();
    for pair in translated[1..].chunks_exact_mut(2) {
        if pair[0] == "--mode" {
            if pair[1] == "capture" {
                pair[1] = "apply".into();
            } else if pair[1] != "verify" {
                return Err(Failure::InvalidArguments);
            }
        }
    }
    translated.extend(["--target-schema".into(), "25".into()]);
    parse_args(&translated)
}

pub fn parse_args(args: &[OsString]) -> Result<Request, Failure> {
    let args = args
        .iter()
        .map(|arg| arg.to_str().ok_or(Failure::InvalidArguments))
        .collect::<Result<Vec<_>, _>>()?;
    if !matches!(args.len(), 15 | 17) || args[0] != "offline-migrate-production" {
        return Err(Failure::InvalidArguments);
    }
    let mut values = BTreeMap::new();
    for pair in args[1..].as_chunks::<2>().0 {
        if values.insert(pair[0], pair[1]).is_some() {
            return Err(Failure::InvalidArguments);
        }
    }
    let target_schema = match values.remove("--target-schema") {
        None => 20,
        Some("25") => 25,
        Some(_) => return Err(Failure::InvalidArguments),
    };
    let mut take = |key| values.remove(key).ok_or(Failure::InvalidArguments);
    let mode = match take("--mode")? {
        "preflight" => Mode::Preflight,
        "apply" => Mode::Apply,
        "resume" => Mode::Resume,
        "verify" => Mode::Verify,
        _ => return Err(Failure::InvalidArguments),
    };
    let operation_id = take("--operation-id")?.to_owned();
    if operation_id.is_empty()
        || operation_id.len() > 100
        || !operation_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
    {
        return Err(Failure::InvalidArguments);
    }
    let request = Request {
        mode,
        target_schema,
        operation_id,
        config: take("--config")?.into(),
        database: take("--database")?.into(),
        manifest_dir: take("--manifest-dir")?.into(),
        release_provenance: take("--release-provenance")?.into(),
        executable: take("--executable")?.into(),
    };
    if !values.is_empty() {
        return Err(Failure::InvalidArguments);
    }
    Ok(request)
}
