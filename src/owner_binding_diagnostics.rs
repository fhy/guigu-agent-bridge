use super::Failure;

pub(super) fn sql(stage: &'static str, error: &sqlx::Error) {
    let code = error
        .as_database_error()
        .and_then(|e| e.code())
        .and_then(|value| value.parse::<i32>().ok());
    let kind = match error {
        sqlx::Error::Database(_) => "database",
        sqlx::Error::Io(_) => "io",
        sqlx::Error::WorkerCrashed => "worker-crashed",
        sqlx::Error::RowNotFound => "row-not-found",
        _ => "other",
    };
    tracing::debug!(
        stage,
        kind,
        sqlite_code = code,
        "owner-binding SQL boundary failed"
    );
    #[cfg(test)]
    eprintln!("owner-binding stage={stage} kind={kind} sqlite_code={code:?}");
}

pub(super) fn failure(stage: &'static str, error: Failure) {
    tracing::debug!(
        stage,
        cause = error.cause,
        substage = error.substage,
        "owner-binding boundary failed"
    );
    #[cfg(test)]
    eprintln!(
        "owner-binding stage={stage} cause={} substage={}",
        error.cause, error.substage
    );
}

pub(super) fn predicate(stage: &'static str) {
    tracing::debug!(stage, "owner-binding predicate failed");
    #[cfg(test)]
    eprintln!("owner-binding predicate={stage}");
}
