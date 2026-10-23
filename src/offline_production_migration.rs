mod arguments;
pub(crate) mod cli_exclusion;
mod configuration;
#[cfg(test)]
mod crash_tests;
mod evidence;
#[cfg(test)]
mod exclusion_tests;
mod flow;
mod launcher;
mod records;
#[cfg(test)]
mod refusal_tests;
mod snapshot;
#[cfg(test)]
mod test_fixture;
#[cfg(test)]
mod tests;
mod verification;

pub use arguments::{Mode, Request, parse_args};
pub use flow::run;

#[derive(Debug, thiserror::Error)]
pub enum Failure {
    #[error("invalid arguments")]
    InvalidArguments,
    #[error("busy or launcher exclusion unavailable")]
    Busy,
    #[error("preflight or backup failed")]
    Preflight,
    #[error("manual disposition required")]
    ManualRequired,
    #[error("migration or verification failed")]
    Migration,
}

pub(crate) type RuntimeLock = crate::maintenance_lock::MaintenanceLock;

pub(crate) fn acquire_runtime_lock(database: &std::path::Path) -> std::io::Result<RuntimeLock> {
    let guard = RuntimeLock::acquire(database)?;
    let resolved = match std::fs::canonicalize(database) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => database.to_path_buf(),
        Err(error) => return Err(error),
    };
    records::startup_allowed(&resolved)
        .map_err(|_| std::io::Error::other("maintenance unresolved"))?;
    Ok(guard)
}
