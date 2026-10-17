use super::manifest::{Capability, Failure};
use std::{env, fs, path::Path, process::Command};

fn safe(value: &str) -> bool {
    !value.is_empty() && !Path::new(value).is_absolute() && !value.split('/').any(|p| p == "..")
}

fn copy_tree(from: &Path, to: &Path) -> Result<(), Failure> {
    fs::create_dir_all(to).map_err(|_| Failure::new("tool-failure", 8))?;
    for entry in fs::read_dir(from).map_err(|_| Failure::new("tool-failure", 8))? {
        let entry = entry.map_err(|_| Failure::new("tool-failure", 8))?;
        let target = to.join(entry.file_name());
        if entry
            .file_type()
            .map_err(|_| Failure::new("tool-failure", 8))?
            .is_dir()
        {
            copy_tree(&entry.path(), &target)?;
        } else {
            fs::copy(entry.path(), target).map_err(|_| Failure::new("tool-failure", 8))?;
        }
    }
    Ok(())
}

fn cli(binary: &Path, fixture: &Path, c: &Capability) -> Result<(), Failure> {
    let argv = c
        .argv
        .as_ref()
        .ok_or_else(|| Failure::new("manifest-invalid", 3).capability(&c.id))?;
    if argv
        .iter()
        .any(|v| v.contains('$') || v.contains('`') || Path::new(v).is_absolute())
    {
        return Err(Failure::new("manifest-invalid", 3).capability(&c.id));
    }
    let mut cmd = Command::new(binary);
    cmd.env_clear().env("PATH", "/usr/bin:/bin");
    for a in argv {
        cmd.arg(a.replace("{fixture}", fixture.to_str().unwrap_or("")));
    }
    let out = cmd
        .output()
        .map_err(|_| Failure::new("tool-failure", 8).capability(&c.id))?;
    if out.status.code() != c.expected_status
        || out.stdout != c.expected_stdout.as_deref().unwrap_or("").as_bytes()
        || out.stderr != c.expected_stderr.as_deref().unwrap_or("").as_bytes()
    {
        return Err(Failure::new("capability-missing", 7).capability(&c.id));
    }
    Ok(())
}

fn api(package: &Path, fixture_root: &Path, c: &Capability) -> Result<(), Failure> {
    let fixture = c
        .fixture
        .as_deref()
        .filter(|v| safe(v))
        .ok_or_else(|| Failure::new("manifest-invalid", 3).capability(&c.id))?;
    let source = fixture_root.join(fixture);
    if !source.join("Cargo.toml").is_file() {
        return Err(Failure::new("manifest-invalid", 3).capability(&c.id));
    }
    let root = env::temp_dir().join(format!("g003-api-{}-{}", std::process::id(), c.id));
    if root.exists() {
        fs::remove_dir_all(&root).map_err(|_| Failure::new("tool-failure", 8))?;
    }
    let unpack = root.join("package");
    let consumer = root.join("consumer");
    fs::create_dir_all(&unpack).map_err(|_| Failure::new("tool-failure", 8))?;
    copy_tree(&source, &consumer)?;
    let extracted = Command::new("tar")
        .arg("-xzf")
        .arg(package)
        .arg("-C")
        .arg(&unpack)
        .status()
        .map_err(|_| Failure::new("tool-failure", 8))?;
    if !extracted.success() {
        let _ = fs::remove_dir_all(&root);
        return Err(Failure::new("artifact-mismatch", 5).capability(&c.id));
    }
    let package_dir = fs::read_dir(&unpack)
        .map_err(|_| Failure::new("tool-failure", 8))?
        .filter_map(Result::ok)
        .find(|e| e.path().is_dir())
        .ok_or_else(|| Failure::new("artifact-mismatch", 5).capability(&c.id))?
        .path();
    let cargo_path = consumer.join("Cargo.toml");
    let cargo = fs::read_to_string(&cargo_path)
        .map_err(|_| Failure::new("manifest-invalid", 3).capability(&c.id))?
        .replace("{package}", package_dir.to_str().unwrap_or(""));
    fs::write(&cargo_path, cargo).map_err(|_| Failure::new("tool-failure", 8))?;
    let cargo_bin = env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let out = Command::new(cargo_bin)
        .args(["test", "--offline", "--locked", "--manifest-path"])
        .arg(&cargo_path)
        .output()
        .map_err(|_| Failure::new("tool-failure", 8).capability(&c.id))?;
    let _ = fs::remove_dir_all(&root);
    if !out.status.success() {
        return Err(Failure::new("capability-missing", 7).capability(&c.id));
    }
    Ok(())
}

pub(crate) fn run(
    binary: &Path,
    package: &Path,
    fixture: &Path,
    c: &Capability,
) -> Result<(), Failure> {
    match c.kind.as_str() {
        "cli" => cli(binary, fixture, c),
        "api" => api(package, fixture, c),
        "migration" | "configuration" | "recovery" if c.argv.is_some() => cli(binary, fixture, c),
        "operational" if c.argv.is_none() => Ok(()),
        _ => Err(Failure::new("capability-missing", 7).capability(&c.id)),
    }
}
