use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

#[derive(Debug)]
pub(crate) struct Failure {
    result: &'static str,
    status: u8,
    digest: String,
    candidate: String,
    phase: &'static str,
    capability: String,
}
impl Failure {
    pub(crate) fn new(result: &'static str, status: u8) -> Self {
        Self {
            result,
            status,
            digest: "none".into(),
            candidate: "none".into(),
            phase: "unknown",
            capability: "none".into(),
        }
    }
    fn context(mut self, digest: &str, candidate: &str, phase: &'static str) -> Self {
        self.digest = digest.into();
        self.candidate = candidate.into();
        self.phase = phase;
        self
    }
    pub(crate) fn capability(mut self, id: &str) -> Self {
        self.capability = id.into();
        self
    }
    pub(crate) fn emit(self) -> ExitCode {
        println!(
            "verify-release-manifest result={} status={} manifest_sha256={} candidate={} phase={} capability={}",
            self.result, self.status, self.digest, self.candidate, self.phase, self.capability
        );
        ExitCode::from(self.status)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    schema_version: u32,
    pub(crate) identity: Identity,
    capabilities: Vec<Capability>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub(crate) target_version: String,
    package_name: String,
    base_version: String,
    base_commit: String,
    pub(crate) candidate_commit: String,
    pub(crate) source_tree_hash: String,
    cargo_lock_sha256: String,
    pub(crate) expected_tag: String,
    package_filename: String,
    pub(crate) package_sha256: String,
    pub(crate) package_entries: u64,
    binary_filename: String,
    pub(crate) binary_sha256: String,
    binary_profile: String,
    pub(crate) rollback_version: String,
    rollback_commit: String,
    pub(crate) rollback_package_sha256: String,
    pub(crate) rollback_binary_sha256: String,
    freeze_state: String,
    freeze_authority: String,
    digest_algorithm: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Capability {
    pub(crate) id: String,
    pub(crate) kind: String,
    requiring_task: String,
    source_commit: String,
    source_path: String,
    gate: String,
    evidence: String,
    result: String,
    pub(crate) argv: Option<Vec<String>>,
    pub(crate) fixture: Option<String>,
    pub(crate) expected_status: Option<i32>,
    pub(crate) expected_stdout: Option<String>,
    pub(crate) expected_stderr: Option<String>,
}
struct Inputs {
    manifest: PathBuf,
    checkout: PathBuf,
    binary: PathBuf,
    package: PathBuf,
    fixture_root: PathBuf,
}

fn parse_args(args: &[OsString]) -> Result<Inputs, Failure> {
    if args.len() != 10 || args.iter().any(|v| v.to_str().is_none()) {
        return Err(Failure::new("invalid-arguments", 2));
    }
    let mut m = BTreeMap::new();
    for p in args.chunks_exact(2) {
        let k = p[0].to_str().unwrap();
        if !matches!(
            k,
            "--manifest" | "--checkout" | "--binary" | "--package" | "--fixture-root"
        ) || m.insert(k, PathBuf::from(&p[1])).is_some()
        {
            return Err(Failure::new("invalid-arguments", 2));
        }
    }
    let get = |k| {
        m.get(k)
            .cloned()
            .ok_or_else(|| Failure::new("invalid-arguments", 2))
    };
    Ok(Inputs {
        manifest: get("--manifest")?,
        checkout: get("--checkout")?,
        binary: get("--binary")?,
        package: get("--package")?,
        fixture_root: get("--fixture-root")?,
    })
}
fn hash(path: &Path) -> Result<String, Failure> {
    fs::read(path)
        .map(|v| format!("{:x}", Sha256::digest(v)))
        .map_err(|_| Failure::new("tool-failure", 8))
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
fn token(v: &str, max: usize) -> bool {
    !v.is_empty()
        && v.len() <= max
        && v.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
fn git(root: &Path, args: &[&str]) -> Result<String, Failure> {
    let o = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .map_err(|_| Failure::new("tool-failure", 8))?;
    if !o.status.success() {
        return Err(Failure::new("identity-mismatch", 4));
    }
    String::from_utf8(o.stdout)
        .map(|s| s.trim().into())
        .map_err(|_| Failure::new("identity-mismatch", 4))
}
fn archive_count(path: &Path) -> Result<u64, Failure> {
    let o = Command::new("tar")
        .arg("-tzf")
        .arg(path)
        .output()
        .map_err(|_| Failure::new("tool-failure", 8))?;
    if !o.status.success() {
        return Err(Failure::new("artifact-mismatch", 5));
    }
    let s = String::from_utf8(o.stdout).map_err(|_| Failure::new("artifact-mismatch", 5))?;
    let mut n = 0;
    for e in s.lines() {
        if e.starts_with('/') || e.split('/').any(|p| p == "..") {
            return Err(Failure::new("artifact-mismatch", 5));
        }
        n += 1;
    }
    Ok(n)
}
fn package_identity(path: &Path, name: &str, version: &str) -> Result<bool, Failure> {
    let out = Command::new("tar")
        .args(["-xOzf"])
        .arg(path)
        .args(["--wildcards", "*/Cargo.toml"])
        .output()
        .map_err(|_| Failure::new("tool-failure", 8))?;
    if !out.status.success() {
        return Err(Failure::new("artifact-mismatch", 5));
    }
    let cargo = String::from_utf8(out.stdout).map_err(|_| Failure::new("artifact-mismatch", 5))?;
    Ok(cargo.contains(&format!("name = \"{name}\""))
        && cargo.contains(&format!("version = \"{version}\"")))
}
fn shape(m: &Manifest) -> bool {
    let i = &m.identity;
    let mut ids = BTreeSet::new();
    let kinds = [
        "cli",
        "api",
        "migration",
        "configuration",
        "recovery",
        "operational",
    ];
    let ordered = m
        .capabilities
        .windows(2)
        .all(|pair| pair[0].id < pair[1].id);
    m.schema_version == 1
        && i.freeze_state == "frozen"
        && i.freeze_authority == "bridge-coordinator"
        && i.digest_algorithm == "sha256"
        && token(&i.target_version, 32)
        && token(&i.package_name, 64)
        && token(&i.base_version, 32)
        && token(&i.expected_tag, 64)
        && token(&i.package_filename, 128)
        && token(&i.binary_filename, 128)
        && matches!(i.binary_profile.as_str(), "dev" | "release" | "installed")
        && token(&i.rollback_version, 32)
        && [
            &i.base_commit,
            &i.candidate_commit,
            &i.source_tree_hash,
            &i.rollback_commit,
        ]
        .into_iter()
        .all(|v| git_oid(v))
        && [
            &i.cargo_lock_sha256,
            &i.package_sha256,
            &i.binary_sha256,
            &i.rollback_package_sha256,
            &i.rollback_binary_sha256,
        ]
        .into_iter()
        .all(|v| hex(v))
        && i.package_entries > 0
        && !m.capabilities.is_empty()
        && ordered
        && m.capabilities.iter().all(|c| {
            token(&c.id, 64)
                && ids.insert(&c.id)
                && kinds.contains(&c.kind.as_str())
                && token(&c.requiring_task, 64)
                && git_oid(&c.source_commit)
                && !c.source_path.is_empty()
                && !Path::new(&c.source_path).is_absolute()
                && !c.source_path.split('/').any(|p| p == "..")
                && token(&c.gate, 128)
                && matches!(
                    c.evidence.as_str(),
                    "exact-binary" | "exact-package" | "reviewed-staging"
                )
                && c.result == "PASS"
                && (c.kind != "cli" || c.evidence == "exact-binary")
        })
}
pub(crate) fn run(args: &[OsString]) -> Result<ExitCode, Failure> {
    let p = parse_args(args)?;
    let raw = fs::read(&p.manifest).map_err(|_| Failure::new("manifest-invalid", 3))?;
    let digest = format!("{:x}", Sha256::digest(&raw));
    let m: Manifest = toml::from_slice(&raw)
        .map_err(|_| Failure::new("manifest-invalid", 3).context(&digest, "none", "frozen"))?;
    if !shape(&m) {
        return Err(Failure::new("manifest-invalid", 3).context(&digest, "none", "frozen"));
    }
    let i = &m.identity;
    let candidate = &i.candidate_commit[..12];
    let ancestor = Command::new("git")
        .arg("-C")
        .arg(&p.checkout)
        .args(["merge-base", "--is-ancestor", &i.base_commit, "HEAD"])
        .status()
        .map_err(|_| Failure::new("tool-failure", 8))?
        .success();
    if git(&p.checkout, &["rev-parse", "HEAD"])? != i.candidate_commit
        || git(&p.checkout, &["rev-parse", "HEAD^{tree}"])? != i.source_tree_hash
        || !git(&p.checkout, &["status", "--porcelain"])?.is_empty()
        || !ancestor
    {
        return Err(Failure::new("identity-mismatch", 4).context(&digest, candidate, "frozen"));
    }
    let cargo = fs::read_to_string(p.checkout.join("Cargo.toml"))
        .map_err(|_| Failure::new("tool-failure", 8))?;
    if !cargo.contains(&format!("name = \"{}\"", i.package_name))
        || !cargo.contains(&format!(
            "version = \"{}\"",
            i.target_version.trim_start_matches('v')
        ))
        || hash(&p.checkout.join("Cargo.lock"))? != i.cargo_lock_sha256
    {
        return Err(Failure::new("identity-mismatch", 4).context(&digest, candidate, "frozen"));
    }
    if p.binary.file_name().and_then(|v| v.to_str()) != Some(&i.binary_filename)
        || p.package.file_name().and_then(|v| v.to_str()) != Some(&i.package_filename)
        || hash(&p.binary)? != i.binary_sha256
        || hash(&p.package)? != i.package_sha256
        || archive_count(&p.package)? != i.package_entries
        || !package_identity(
            &p.package,
            &i.package_name,
            i.target_version.trim_start_matches('v'),
        )?
    {
        return Err(Failure::new("artifact-mismatch", 5).context(&digest, candidate, "frozen"));
    }
    for c in &m.capabilities {
        let source_ancestor = Command::new("git")
            .arg("-C")
            .arg(&p.checkout)
            .args(["merge-base", "--is-ancestor", &c.source_commit, "HEAD"])
            .status()
            .map_err(|_| Failure::new("tool-failure", 8))?
            .success();
        if !source_ancestor || !p.checkout.join(&c.source_path).is_file() {
            return Err(Failure::new("capability-missing", 7)
                .context(&digest, candidate, "frozen")
                .capability(&c.id));
        }
        super::probe::run(&p.binary, &p.package, &p.fixture_root, c)
            .map_err(|e| e.context(&digest, candidate, "frozen"))?;
    }
    println!(
        "verify-release-manifest result=verified status=0 manifest_sha256={digest} candidate={candidate} phase=frozen capability=none"
    );
    Ok(ExitCode::SUCCESS)
}
