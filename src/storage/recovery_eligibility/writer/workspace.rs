use super::{RecoveryScope, SqliteConnection, effect_scope, refused, require, text_identity};
use crate::storage::recovery_eligibility::deletion::{self, ReleaseEvidence};

pub(crate) async fn delete_workspace(
    connection: &mut SqliteConnection,
    scope: Option<&RecoveryScope>,
    claim: &str,
    owner: &str,
    task: &str,
    fence: i64,
) -> Result<(), sqlx::Error> {
    let identity = text_identity("workspace_claims", claim)?;
    let routed = match scope {
        Some(scope) => Some(effect_scope::resolve(connection, scope, &identity).await?),
        None => None,
    };
    require(connection, routed.as_ref(), &identity).await?;
    if let Some(scope) = routed.as_ref() {
        deletion::record(
            connection,
            &scope.context(),
            &identity,
            ReleaseEvidence {
                owner,
                task,
                fence,
                authority: scope.fence.as_ref(),
            },
        )
        .await?;
    }
    let deleted = sqlx::query("DELETE FROM workspace_claims WHERE claim_id=? AND runtime_owner=? AND task_id=? AND owner_fence=? AND state='active'")
        .bind(claim).bind(owner).bind(task).bind(fence).execute(connection).await?;
    if deleted.rows_affected() != 1 {
        return Err(refused());
    }
    Ok(())
}
