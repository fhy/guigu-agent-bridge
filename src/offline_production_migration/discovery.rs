use std::{io, os::unix::fs::MetadataExt, path::Path};

use super::discovery_sqlite::ReadOnly;

pub(super) fn check(database: &Path) -> io::Result<()> {
    let metadata = match std::fs::symlink_metadata(database) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_file() || metadata.nlink() != 1 || metadata.uid() != unsafe { libc::geteuid() }
    {
        return Err(refused());
    }
    let connection = ReadOnly::open(database).map_err(|_| refused())?;
    let tables = connection.rows(c"SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
        .map_err(|_| refused())?;
    if tables.is_empty() {
        return Ok(());
    }
    if !tables.iter().any(|table| table == &["_sqlx_migrations"]) {
        return Err(refused());
    }
    let rows = connection.rows(c"SELECT CAST(version AS TEXT),description,CAST(success AS TEXT),hex(checksum) FROM _sqlx_migrations ORDER BY version")
        .map_err(|_| refused())?;
    validate_ledger(&rows)?;
    if rows.len() == 25 {
        check_binding(&connection)?;
    }
    let after = std::fs::symlink_metadata(database)?;
    if metadata.dev() != after.dev() || metadata.ino() != after.ino() {
        return Err(refused());
    }
    Ok(())
}

fn validate_ledger(rows: &[Vec<String>]) -> io::Result<()> {
    if rows.is_empty() || rows.len() > 25 {
        return Err(refused());
    }
    let migrator = super::verification::embedded_through(rows.len() as i64);
    let expected = migrator.iter().collect::<Vec<_>>();
    if expected.len() != rows.len() {
        return Err(refused());
    }
    for (row, migration) in rows.iter().zip(expected) {
        let checksum = migration
            .checksum
            .iter()
            .map(|byte| format!("{byte:02X}"))
            .collect::<String>();
        let expected = [
            migration.version.to_string(),
            migration.description.to_string(),
            "1".to_owned(),
            checksum,
        ];
        if row != &expected {
            return Err(refused());
        }
    }
    Ok(())
}

fn check_binding(connection: &ReadOnly) -> io::Result<()> {
    if connection
        .rows(c"SELECT CAST(count(*) AS TEXT) FROM offline_recovery_binding")
        .map_err(|_| refused())?
        != vec![vec!["0".to_owned()]]
    {
        return Err(refused());
    }
    let bootstrap = connection.rows(c"SELECT state,bootstrap_nonce,coalesce(generation,''),coalesce(inventory_digest,'') FROM runtime_eligibility_bootstrap WHERE singleton=1")
        .map_err(|_| refused())?;
    let Some(row) = bootstrap.first() else {
        return Err(refused());
    };
    if bootstrap.len() != 1 || row.len() != 4 || row[1].is_empty() {
        return Err(refused());
    }
    match row[0].as_str() {
        "pending" if row[2].is_empty() && row[3].is_empty() => {
            if connection.rows(c"SELECT CAST((SELECT count(*) FROM runtime_eligibility_context)+(SELECT count(*) FROM offline_recovery_rows) AS TEXT)")
                .map_err(|_| refused())? != vec![vec!["0".to_owned()]] {
                return Err(refused());
            }
        }
        "complete" if !row[2].is_empty() && !row[3].is_empty() => {
            let valid = connection.rows(c"SELECT CAST(count(*) AS TEXT) FROM runtime_eligibility_context AS context JOIN runtime_eligibility_bootstrap AS bootstrap ON bootstrap.singleton=context.singleton AND bootstrap.generation=context.generation AND bootstrap.bootstrap_nonce=context.bootstrap_nonce AND bootstrap.inventory_digest=context.bootstrap_inventory_digest WHERE context.singleton=1 AND context.state='ready' AND NOT EXISTS(SELECT 1 FROM offline_recovery_rows WHERE context_kind!='nonadmin' OR context_generation IS NOT context.generation OR disposition IN ('uncertain_hold','refused_hold'))")
                .map_err(|_| refused())?;
            if valid != vec![vec!["1".to_owned()]] { return Err(refused()); }
        }
        _ => return Err(refused()),
    }
    Ok(())
}

fn refused() -> io::Error {
    io::Error::other("maintenance discovery unresolved")
}
