//! Current scoped relevance after the old incarnation's boot has ended.
use super::{Result, contains, fs, inspect_preexisting, refuse};
use std::path::{Path, PathBuf};

pub(super) fn inspect(
    path: &Path,
    private_roots: &[PathBuf],
    device: &str,
    domain: &str,
) -> Result<()> {
    // Uptime ticks cannot establish irrelevance across boots. Public procfs
    // anchors must instead identify a distinct domain and nonempty command.
    let group =
        fs::read_to_string(path.join("cgroup")).map_err(|_| refuse("current-domain-unreadable"))?;
    let group = group.trim();
    if !group.starts_with("0::/") || group.contains('\n') {
        return Err(refuse("current-domain-ambiguous"));
    }
    if group == domain
        || group
            .strip_prefix(domain)
            .is_some_and(|suffix| suffix.starts_with('/'))
    {
        return Err(refuse("related-domain-member"));
    }
    let command =
        fs::read(path.join("cmdline")).map_err(|_| refuse("current-command-unreadable"))?;
    if command
        .split(|b| *b == 0)
        .next()
        .is_none_or(|arg| arg.is_empty())
    {
        return Err(refuse("current-command-ambiguous"));
    }
    if contains(&command, format!("MATRIX_DEVICE_ID={device}").as_bytes()) {
        return Err(refuse("related-escaped-process"));
    }
    // Check every accessible private resource/device selector, even for a
    // reused numeric PID. Shared workspaces are not private ownership evidence.
    // Protected processes with unrelated readable anchors need no universal
    // ptrace access; missing/ambiguous anchors above always refuse.
    inspect_preexisting(path, private_roots, device, domain)?;
    let after = fs::read_to_string(path.join("cgroup")).map_err(|_| refuse("process-raced"))?;
    let next = fs::read(path.join("cmdline")).map_err(|_| refuse("process-raced"))?;
    if after.trim() != group || next != command {
        return Err(refuse("process-raced"));
    }
    Ok(())
}
