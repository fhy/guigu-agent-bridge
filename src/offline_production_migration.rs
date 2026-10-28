mod arguments;
pub(crate) mod cli_exclusion;
mod configuration;
#[cfg(test)]
mod crash_tests;
mod discovery;
mod discovery_sqlite;
mod discovery_vfs;
mod evidence;
#[cfg(test)]
mod exclusion_tests;
mod flow;
mod identity_capture;
mod identity_records;
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
mod version2;
mod version2_archive;
mod version2_records;
#[cfg(test)]
mod version2_tests;

pub use arguments::{Mode, Request, parse_args, parse_identity_args};
pub use flow::run;
pub use identity_capture::run as capture_identity;

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

pub(crate) async fn dispatch_identity(args: &[std::ffi::OsString]) -> Result<(), crate::Error> {
    let result = match parse_identity_args(args) {
        Ok(request) => capture_identity(request).await,
        Err(error) => Err(error),
    };
    match result {
        Ok(message) => {
            println!("{message}");
            Ok(())
        }
        Err(error) => {
            let (category, status) = match error {
                Failure::InvalidArguments => ("invalid-arguments", 2),
                Failure::Busy => ("busy", 4),
                Failure::Preflight => ("identity-archive", 5),
                Failure::ManualRequired => ("decision-required", 6),
                Failure::Migration => ("verification", 7),
            };
            Err(crate::Error::Cli(crate::error::CliFailure {
                command: "offline-capture-matrix-identity",
                category,
                status,
            }))
        }
    }
}

pub(crate) fn check_runtime_discovery(database: &std::path::Path) -> std::io::Result<()> {
    let resolved = match std::fs::canonicalize(database) {
        Ok(path) => path,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => database.to_path_buf(),
        Err(error) => return Err(error),
    };
    records::startup_allowed(&resolved)
        .map_err(|_| std::io::Error::other("maintenance unresolved"))?;
    discovery::check(&resolved)
}

pub(crate) fn acquire_runtime_lock(database: &std::path::Path) -> std::io::Result<RuntimeLock> {
    let guard = RuntimeLock::acquire(database)?;
    check_runtime_discovery(database)?;
    Ok(guard)
}
