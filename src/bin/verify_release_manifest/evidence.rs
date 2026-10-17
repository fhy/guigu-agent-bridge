use super::manifest::{Failure, Manifest};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    ffi::{CString, OsString},
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::ExitCode,
};

#[cfg(target_os = "linux")]
const AT_FDCWD: i32 = -100;
#[cfg(target_os = "linux")]
const RENAME_NOREPLACE: u32 = 1;
#[cfg(target_os = "linux")]
unsafe extern "C" {
    fn renameat2(
        olddirfd: i32,
        oldpath: *const std::ffi::c_char,
        newdirfd: i32,
        newpath: *const std::ffi::c_char,
        flags: u32,
    ) -> i32;
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    schema_version: u32,
    manifest_sha256: String,
    target_version: String,
    candidate_commit: String,
    source_tree_hash: String,
    phase: String,
    expected_tag: String,
    publication: Option<Publication>,
    installation: Option<Installation>,
    verifier_version: String,
    result: String,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Publication {
    observed_tag_object: String,
    peeled_commit: String,
    package_sha256: String,
    package_entries: u64,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Installation {
    candidate: BinaryFact,
    installed: BinaryFact,
}
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct BinaryFact {
    role: String,
    profile: String,
    expected_sha256: String,
    observed_sha256: String,
    provenance: String,
}
fn hex(v: &str) -> bool {
    v.len() == 64
        && v.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn git_oid(v: &str) -> bool {
    v.len() == 40
        && v.bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn valid(e: &Evidence) -> bool {
    if e.schema_version != 1
        || !hex(&e.manifest_sha256)
        || !git_oid(&e.candidate_commit)
        || !git_oid(&e.source_tree_hash)
        || e.target_version.is_empty()
        || e.expected_tag.is_empty()
        || e.verifier_version != "1"
        || e.result != "PASS"
    {
        return false;
    }
    match (e.phase.as_str(), &e.publication, &e.installation) {
        ("publication", Some(p), None) => {
            git_oid(&p.observed_tag_object)
                && p.peeled_commit == e.candidate_commit
                && hex(&p.package_sha256)
                && p.package_entries > 0
        }
        ("installation", None, Some(i)) => {
            [(&i.candidate, "candidate"), (&i.installed, "installed")]
                .into_iter()
                .all(|(f, r)| {
                    f.role == r
                        && matches!(f.profile.as_str(), "release" | "installed")
                        && hex(&f.expected_sha256)
                        && f.expected_sha256 == f.observed_sha256
                        && !f.provenance.is_empty()
                })
        }
        _ => false,
    }
}
fn bound(e: &Evidence, m: &Manifest) -> bool {
    let i = &m.identity;
    e.target_version == i.target_version
        && e.expected_tag == i.expected_tag
        && e.candidate_commit == i.candidate_commit
        && e.source_tree_hash == i.source_tree_hash
        && match (e.phase.as_str(), &e.publication, &e.installation) {
            ("publication", Some(p), None) => {
                p.peeled_commit == i.candidate_commit
                    && p.package_sha256 == i.package_sha256
                    && p.package_entries == i.package_entries
            }
            ("installation", None, Some(x)) => {
                x.candidate.expected_sha256 == i.binary_sha256
                    && x.installed.expected_sha256 == i.binary_sha256
            }
            _ => false,
        }
}
fn sync_parent(path: &Path) -> std::io::Result<()> {
    fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()
}
pub(crate) fn run(args: &[OsString]) -> Result<ExitCode, Failure> {
    if args.len() != 6 || args[0] != "--manifest" || args[2] != "--input" || args[4] != "--output" {
        return Err(Failure::new("invalid-arguments", 2));
    }
    let manifest = PathBuf::from(&args[1]);
    let input = PathBuf::from(&args[3]);
    let output = PathBuf::from(&args[5]);
    let raw = fs::read(input).map_err(|_| Failure::new("tool-failure", 8))?;
    if raw.len() > 65536 {
        return Err(Failure::new("manifest-invalid", 3));
    }
    let e: Evidence =
        serde_json::from_slice(&raw).map_err(|_| Failure::new("manifest-invalid", 3))?;
    if !valid(&e) {
        return Err(Failure::new("manifest-invalid", 3));
    }
    let manifest_bytes = fs::read(&manifest).map_err(|_| Failure::new("tool-failure", 8))?;
    if format!("{:x}", Sha256::digest(manifest_bytes)) != e.manifest_sha256 {
        return Err(Failure::new("identity-mismatch", 4));
    }
    let parsed: Manifest = toml::from_str(
        &fs::read_to_string(&manifest).map_err(|_| Failure::new("manifest-invalid", 3))?,
    )
    .map_err(|_| Failure::new("manifest-invalid", 3))?;
    if !bound(&e, &parsed) {
        return Err(Failure::new("identity-mismatch", 4));
    }
    let mut canonical = serde_json::to_vec(&e).map_err(|_| Failure::new("tool-failure", 8))?;
    canonical.push(b'\n');
    if let Ok(old) = fs::read(&output) {
        if old == canonical {
            println!(
                "verify-release-manifest result=verified status=0 manifest_sha256={} candidate={} phase={} capability=none",
                e.manifest_sha256,
                &e.candidate_commit[..12],
                e.phase
            );
            return Ok(ExitCode::SUCCESS);
        }
        return Err(Failure::new("evidence-conflict", 6));
    }
    let tmp = output.with_extension(format!("tmp-{}", std::process::id()));
    let mut f = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp)
        .map_err(|_| Failure::new("tool-failure", 8))?;
    f.write_all(&canonical)
        .and_then(|_| f.sync_all())
        .map_err(|_| Failure::new("tool-failure", 8))?;
    #[cfg(target_os = "linux")]
    let landed = {
        let old = CString::new(tmp.to_string_lossy().as_bytes())
            .map_err(|_| Failure::new("tool-failure", 8))?;
        let new = CString::new(output.to_string_lossy().as_bytes())
            .map_err(|_| Failure::new("tool-failure", 8))?;
        unsafe {
            renameat2(
                AT_FDCWD,
                old.as_ptr(),
                AT_FDCWD,
                new.as_ptr(),
                RENAME_NOREPLACE,
            ) == 0
        }
    };
    #[cfg(not(target_os = "linux"))]
    let landed = false;
    if landed {
    } else if output.exists() {
        let _ = fs::remove_file(&tmp);
        if fs::read(&output).ok().as_deref() == Some(canonical.as_slice()) {
            println!(
                "verify-release-manifest result=verified status=0 manifest_sha256={} candidate={} phase={} capability=none",
                e.manifest_sha256,
                &e.candidate_commit[..12],
                e.phase
            );
            return Ok(ExitCode::SUCCESS);
        }
        return Err(Failure::new("evidence-conflict", 6));
    } else {
        let _ = fs::remove_file(&tmp);
        return Err(Failure::new("tool-failure", 8));
    }
    sync_parent(&output).map_err(|_| Failure::new("tool-failure", 8))?;
    println!(
        "verify-release-manifest result=verified status=0 manifest_sha256={} candidate={} phase={} capability=none",
        e.manifest_sha256,
        &e.candidate_commit[..12],
        e.phase
    );
    Ok(ExitCode::SUCCESS)
}
