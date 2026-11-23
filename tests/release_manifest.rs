use sha2::{Digest, Sha256};
use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Command, Output},
};
use uuid::Uuid;

fn tool() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_verify-release-manifest"))
}
fn temp(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("g003-{name}-{}", Uuid::now_v7()));
    fs::create_dir_all(&p).unwrap();
    p
}
fn cmd(root: &Path, args: &[&str]) -> String {
    let o = Command::new(args[0])
        .args(&args[1..])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stderr));
    String::from_utf8(o.stdout).unwrap().trim().into()
}
fn hash(path: &Path) -> String {
    format!("{:x}", Sha256::digest(fs::read(path).unwrap()))
}
fn write(path: &Path, body: &str) {
    fs::write(path, body).unwrap()
}

struct Fixture {
    root: PathBuf,
    checkout: PathBuf,
    manifest: PathBuf,
    binary: PathBuf,
    package: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}
impl Fixture {
    fn new() -> Self {
        let root = temp("manifest");
        let checkout = root.join("checkout");
        fs::create_dir(&checkout).unwrap();
        write(
            &checkout.join("Cargo.toml"),
            "[package]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        );
        write(&checkout.join("Cargo.lock"), "# lock\n");
        write(&checkout.join("README"), "fixture\n");
        cmd(&checkout, &["git", "init", "-q"]);
        cmd(
            &checkout,
            &["git", "config", "user.email", "fixture@example.invalid"],
        );
        cmd(&checkout, &["git", "config", "user.name", "Fixture"]);
        cmd(
            &checkout,
            &["git", "add", "Cargo.toml", "Cargo.lock", "README"],
        );
        cmd(&checkout, &["git", "commit", "-qm", "fixture"]);
        let head = cmd(&checkout, &["git", "rev-parse", "HEAD"]);
        let tree = cmd(&checkout, &["git", "rev-parse", "HEAD^{tree}"]);
        let binary = root.join("candidate-bin");
        write(
            &binary,
            "#!/bin/sh\nif [ \"$1\" = --probe ]; then printf 'probe-ok\\n'; exit 0; fi\nexit 9\n",
        );
        let mut perms = fs::metadata(&binary).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&binary, perms).unwrap();
        let package_root = root.join("sample-1.2.3");
        fs::create_dir(&package_root).unwrap();
        write(
            &package_root.join("Cargo.toml"),
            "[package]\nname = \"sample\"\nversion = \"1.2.3\"\nedition = \"2024\"\n",
        );
        fs::create_dir(package_root.join("src")).unwrap();
        write(
            &package_root.join("src/lib.rs"),
            "pub fn answer() -> u8 { 42 }\n",
        );
        let consumer = root.join("consumer");
        fs::create_dir_all(consumer.join("src")).unwrap();
        write(
            &consumer.join("Cargo.toml"),
            "[package]\nname = \"consumer\"\nversion = \"0.1.0\"\nedition = \"2024\"\n[dependencies]\nsample = { path = \"{package}\" }\n",
        );
        write(
            &consumer.join("Cargo.lock"),
            "version = 4\n\n[[package]]\nname = \"consumer\"\nversion = \"0.1.0\"\ndependencies = [\"sample\"]\n\n[[package]]\nname = \"sample\"\nversion = \"1.2.3\"\n",
        );
        write(
            &consumer.join("src/lib.rs"),
            "#[test]\nfn packaged_api() { assert_eq!(sample::answer(), 42); }\n",
        );
        let package = root.join("sample-1.2.3.crate");
        let status = Command::new("tar")
            .args([
                "-czf",
                package.to_str().unwrap(),
                "sample-1.2.3/Cargo.toml",
                "sample-1.2.3/src/lib.rs",
            ])
            .current_dir(&root)
            .status()
            .unwrap();
        assert!(status.success());
        let manifest = root.join("manifest.toml");
        let zeros = "0".repeat(64);
        let zero_oid = "0".repeat(40);
        write(
            &manifest,
            &format!(
                r#"schema_version = 1
[identity]
target_version = "v1.2.3"
package_name = "sample"
base_version = "v1.2.2"
base_commit = "{head}"
candidate_commit = "{head}"
source_tree_hash = "{tree}"
cargo_lock_sha256 = "{}"
expected_tag = "v1.2.3"
package_filename = "sample-1.2.3.crate"
package_sha256 = "{}"
package_entries = 2
binary_filename = "candidate-bin"
binary_sha256 = "{}"
binary_profile = "release"
rollback_version = "v1.2.2"
rollback_commit = "{zero_oid}"
rollback_package_sha256 = "{zeros}"
rollback_binary_sha256 = "{zeros}"
freeze_state = "frozen"
freeze_authority = "bridge-coordinator"
digest_algorithm = "sha256"
[[capabilities]]
id = "api-probe"
kind = "api"
requiring_task = "T000"
source_commit = "{head}"
source_path = "Cargo.toml"
gate = "packaged_consumer"
evidence = "exact-package"
result = "PASS"
fixture = "consumer"
[[capabilities]]
id = "probe"
kind = "cli"
requiring_task = "T000"
source_commit = "{head}"
source_path = "Cargo.toml"
gate = "probe_process"
evidence = "exact-binary"
result = "PASS"
argv = ["--probe"]
expected_status = 0
expected_stdout = "probe-ok\n"
expected_stderr = ""
"#,
                hash(&checkout.join("Cargo.lock")),
                hash(&package),
                hash(&binary)
            ),
        );
        Self {
            root,
            checkout,
            manifest,
            binary,
            package,
        }
    }
    fn verify(&self) -> Output {
        Command::new(tool())
            .args([
                "--manifest",
                self.manifest.to_str().unwrap(),
                "--checkout",
                self.checkout.to_str().unwrap(),
                "--binary",
                self.binary.to_str().unwrap(),
                "--package",
                self.package.to_str().unwrap(),
                "--fixture-root",
                self.root.to_str().unwrap(),
            ])
            .output()
            .unwrap()
    }

    fn use_exact_verifier_binary(&self) {
        let old_hash = hash(&self.binary);
        fs::copy(tool(), &self.binary).unwrap();
        let raw = fs::read_to_string(&self.manifest).unwrap();
        let expected = "verify-release-manifest result=invalid-arguments status=2 manifest_sha256=none candidate=none phase=unknown capability=none\n";
        write(
            &self.manifest,
            &raw.replace(&old_hash, &hash(&self.binary))
                .replace("argv = [\"--probe\"]", "argv = []")
                .replace("expected_status = 0", "expected_status = 2")
                .replace(
                    "expected_stdout = \"probe-ok\\n\"",
                    &format!("expected_stdout = {expected:?}"),
                ),
        );
    }
}

#[test]
fn exact_candidate_binary_and_package_verify() {
    let f = Fixture::new();
    let o = f.verify();
    assert!(o.status.success(), "{}", String::from_utf8_lossy(&o.stdout));
    assert!(o.stderr.is_empty());
    assert!(
        String::from_utf8(o.stdout)
            .unwrap()
            .contains("result=verified status=0")
    );
}

#[test]
fn exact_verifier_binary_is_the_cli_probe_target() {
    let f = Fixture::new();
    f.use_exact_verifier_binary();
    let result = f.verify();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stdout)
    );
}

#[test]
fn cross_version_candidate_fails_before_capability_probe() {
    let f = Fixture::new();
    let raw = fs::read_to_string(&f.manifest).unwrap();
    write(
        &f.manifest,
        &raw.replace("target_version = \"v1.2.3\"", "target_version = \"v9.9.9\""),
    );
    let result = f.verify();
    assert_eq!(result.status.code(), Some(4));
    assert!(
        String::from_utf8(result.stdout)
            .unwrap()
            .contains("result=identity-mismatch")
    );
}

#[test]
fn artifact_mismatch_stops_before_probe() {
    let f = Fixture::new();
    write(&f.binary, "changed");
    let o = f.verify();
    assert_eq!(o.status.code(), Some(5));
    assert!(
        String::from_utf8(o.stdout)
            .unwrap()
            .contains("result=artifact-mismatch")
    );
    assert!(o.stderr.is_empty());
}

#[test]
fn dirty_checkout_and_source_only_capability_fail_closed() {
    let f = Fixture::new();
    write(&f.checkout.join("untracked"), "dirty\n");
    let dirty = f.verify();
    assert_eq!(dirty.status.code(), Some(4));
    fs::remove_file(f.checkout.join("untracked")).unwrap();

    let raw = fs::read_to_string(&f.manifest).unwrap();
    write(
        &f.manifest,
        &raw.replace(
            "source_path = \"Cargo.toml\"",
            "source_path = \"missing.rs\"",
        ),
    );
    let source_only = f.verify();
    assert_eq!(source_only.status.code(), Some(7));
    assert!(
        String::from_utf8(source_only.stdout)
            .unwrap()
            .contains("capability=api-probe")
    );
}

fn evidence(phase: &str, installed: bool, manifest_digest: &str) -> String {
    let a = "a".repeat(40);
    let tree = "b".repeat(40);
    let digest = "b".repeat(64);
    let phase_fields = if installed {
        format!(
            r#""publication":null,"installation":{{"candidate":{{"role":"candidate","profile":"release","expected_sha256":"{digest}","observed_sha256":"{digest}","provenance":"reviewed-package"}},"installed":{{"role":"installed","profile":"installed","expected_sha256":"{digest}","observed_sha256":"{digest}","provenance":"authorized-install"}}}}"#
        )
    } else {
        format!(
            r#""publication":{{"observed_tag_object":"{tree}","peeled_commit":"{a}","package_sha256":"{digest}","package_entries":1}},"installation":null"#
        )
    };
    format!(
        r#"{{"schema_version":1,"manifest_sha256":"{manifest_digest}","target_version":"v1.2.3","candidate_commit":"{a}","source_tree_hash":"{tree}","phase":"{phase}","expected_tag":"v1.2.3",{phase_fields},"verifier_version":"1","result":"PASS"}}"#
    )
}
fn fixture_evidence(f: &Fixture, phase: &str, installed: bool) -> String {
    let head = cmd(&f.checkout, &["git", "rev-parse", "HEAD"]);
    let tree = cmd(&f.checkout, &["git", "rev-parse", "HEAD^{tree}"]);
    let digest = hash(&f.binary);
    let phase_fields = if installed {
        format!(
            r#""publication":null,"installation":{{"candidate":{{"role":"candidate","profile":"release","expected_sha256":"{digest}","observed_sha256":"{digest}","provenance":"reviewed-package"}},"installed":{{"role":"installed","profile":"installed","expected_sha256":"{digest}","observed_sha256":"{digest}","provenance":"authorized-install"}}}}"#
        )
    } else {
        format!(
            r#""publication":{{"observed_tag_object":"{tree}","peeled_commit":"{head}","package_sha256":"{}","package_entries":2}},"installation":null"#,
            hash(&f.package)
        )
    };
    format!(
        r#"{{"schema_version":1,"manifest_sha256":"{}","target_version":"v1.2.3","candidate_commit":"{head}","source_tree_hash":"{tree}","phase":"{phase}","expected_tag":"v1.2.3",{phase_fields},"verifier_version":"1","result":"PASS"}}"#,
        hash(&f.manifest)
    )
}
fn append(manifest: &Path, input: &Path, output: &Path) -> Output {
    Command::new(tool())
        .args([
            "append-evidence",
            "--manifest",
            manifest.to_str().unwrap(),
            "--input",
            input.to_str().unwrap(),
            "--output",
            output.to_str().unwrap(),
        ])
        .output()
        .unwrap()
}

#[test]
fn installation_requires_both_roles_and_is_idempotent() {
    let f = Fixture::new();
    let root = &f.root;
    let manifest = &f.manifest;
    let input = root.join("input.json");
    let output = root.join("installation.json");
    write(&input, &fixture_evidence(&f, "installation", true));
    assert!(append(manifest, &input, &output).status.success());
    let bytes = fs::read(&output).unwrap();
    assert!(append(manifest, &input, &output).status.success());
    assert_eq!(fs::read(&output).unwrap(), bytes);
    let partial = fixture_evidence(&f, "installation", true)
        .replace("\"installation\":{", "\"installation\":null,");
    write(&input, &partial);
    assert_eq!(
        append(manifest, &input, &root.join("partial.json"))
            .status
            .code(),
        Some(3)
    );
}

#[test]
fn differing_evidence_never_overwrites() {
    let f = Fixture::new();
    let root = &f.root;
    let manifest = &f.manifest;
    let input = root.join("input.json");
    let output = root.join("publication.json");
    write(&input, &fixture_evidence(&f, "publication", false));
    assert!(append(manifest, &input, &output).status.success());
    let before = fs::read(&output).unwrap();
    write(&input, &fixture_evidence(&f, "installation", true));
    let o = append(manifest, &input, &output);
    assert_eq!(o.status.code(), Some(6));
    assert_eq!(fs::read(&output).unwrap(), before);
}

#[test]
fn evidence_requires_the_exact_frozen_manifest_digest() {
    let root = temp("manifest-digest");
    let manifest = root.join("manifest.toml");
    let other_manifest = root.join("other.toml");
    write(&manifest, "frozen manifest\n");
    write(&other_manifest, "changed manifest\n");
    let input = root.join("input.json");
    write(&input, &evidence("publication", false, &hash(&manifest)));
    let output = root.join("publication.json");
    let result = append(&other_manifest, &input, &output);
    assert_eq!(result.status.code(), Some(4));
    assert!(!output.exists());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn evidence_requires_manifest_identity_facts() {
    let f = Fixture::new();
    let input = f.root.join("input.json");
    let output = f.root.join("publication.json");
    let conflicting = fixture_evidence(&f, "publication", false).replace(
        "\"target_version\":\"v1.2.3\"",
        "\"target_version\":\"v9.9.9\"",
    );
    write(&input, &conflicting);
    let result = append(&f.manifest, &input, &output);
    assert_eq!(result.status.code(), Some(4));
    assert!(!output.exists());
}
