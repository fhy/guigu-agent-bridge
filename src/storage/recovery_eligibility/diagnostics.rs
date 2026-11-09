//! Bootstrap diagnostics contain static predicates and error classes, never SQL,
//! row identities, driver messages or business/credential data.
pub(crate) fn class(error: &sqlx::Error) -> &'static str {
    match error {
        sqlx::Error::Database(_) => "database",
        sqlx::Error::Protocol(_) => "predicate",
        sqlx::Error::RowNotFound => "row-not-found",
        sqlx::Error::PoolTimedOut => "pool-timeout",
        sqlx::Error::PoolClosed => "pool-closed",
        sqlx::Error::Io(_) => "io",
        _ => "decode-or-driver",
    }
}

pub(super) fn stage(stage: &'static str, error: sqlx::Error) -> sqlx::Error {
    let code = error.as_database_error().and_then(|db| db.code());
    let code = code.as_deref().filter(|code| {
        !code.is_empty() && code.len() <= 10 && code.bytes().all(|b| b.is_ascii_digit())
    });
    tracing::debug!(
        phase = "bootstrap_nonadmin",
        stage,
        error_class = class(&error),
        database_code = code,
        outcome = "refused",
        "bootstrap stage failed"
    );
    error
}

pub(super) fn refusal(stage: &'static str, predicate: &'static str) -> sqlx::Error {
    tracing::debug!(
        phase = "bootstrap_nonadmin",
        stage,
        predicate,
        outcome = "refused",
        "bootstrap predicate failed"
    );
    super::refused()
}
