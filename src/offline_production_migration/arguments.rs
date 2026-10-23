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
    pub config: PathBuf,
    pub database: PathBuf,
    pub operation_id: String,
    pub manifest_dir: PathBuf,
    pub release_provenance: PathBuf,
    pub executable: PathBuf,
}

pub fn parse_args(args: &[OsString]) -> Result<Request, Failure> {
    let args = args
        .iter()
        .map(|arg| arg.to_str().ok_or(Failure::InvalidArguments))
        .collect::<Result<Vec<_>, _>>()?;
    if args.len() != 15 || args[0] != "offline-migrate-production" {
        return Err(Failure::InvalidArguments);
    }
    let mut values = BTreeMap::new();
    for pair in args[1..].as_chunks::<2>().0 {
        if values.insert(pair[0], pair[1]).is_some() {
            return Err(Failure::InvalidArguments);
        }
    }
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
