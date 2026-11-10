//! One immutable batch parent within complete_batch's existing write transaction.
use super::super::{ParentIdentity, ParentRole, RowIdentity, inventory};
use super::*;

#[derive(Default)]
pub(super) struct Parents(Option<(Vec<RowIdentity>, String)>);
impl Parents {
    pub(super) async fn inserted(
        &mut self,
        c: &mut SqliteConnection,
        s: &RecoveryScope,
        id: &RowIdentity,
    ) -> Result<(), sqlx::Error> {
        if id.table != "matrix_recovery_receipts" {
            return Err(refused());
        }
        let snapshot = inventory::snapshot(c, id).await?;
        if let Some((ids, _)) = &self.0 {
            if ids != &snapshot.parents {
                return Err(refused());
            }
        } else {
            let mut parents = Vec::new();
            for id in &snapshot.parents {
                // complete_batch cannot mutate the parent batch until this loop ends.
                if id.table != "matrix_recovery_batches" {
                    return Err(refused());
                }
                super::super::super::require_lineage(c, &s.context(), EffectDomain::Matrix, id)
                    .await?;
                let p = inventory::snapshot(c, id).await?;
                parents.push(ParentIdentity {
                    identity: id.clone(),
                    revision: p.revision,
                    digest: p.digest,
                    role: ParentRole::Effectful,
                });
            }
            self.0 = Some((
                snapshot.parents.clone(),
                serde_json::to_string(&parents).map_err(|_| refused())?,
            ));
        }
        // The complete native inventory is independently checked before commit.
        // Subsequent batch mutation refreshes every child's exact parent reference.
        super::super::insert_classification(
            c,
            s,
            id,
            &snapshot,
            &self.0.as_ref().ok_or_else(refused)?.1,
            true,
        )
        .await
    }
}
