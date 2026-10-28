use sqlx::{Sqlite, SqliteConnection, Transaction};

use super::{
    output_spool::{OutputSpoolStore, OutputStoreError},
    recovery_eligibility::{
        RowIdentity,
        writer::{self, Mutation},
    },
};

pub(super) fn identity(
    table: &str,
    set_id: &str,
    index: Option<i64>,
) -> Result<RowIdentity, sqlx::Error> {
    let encode = |kind: &str, value: &str| {
        format!(
            "{kind}:{}",
            value
                .as_bytes()
                .iter()
                .map(|byte| format!("{byte:02X}"))
                .collect::<String>()
        )
    };
    let mut keys = vec![encode("text", set_id)];
    if let Some(index) = index {
        keys.push(encode("integer", &index.to_string()));
    }
    Ok(RowIdentity {
        table: table.into(),
        key: serde_json::to_string(&keys)
            .map_err(|_| sqlx::Error::Protocol("output-recovery-identity".into()))?,
    })
}

impl OutputSpoolStore {
    pub(crate) async fn authorize_delivery(
        &self,
        set_id: &str,
        index: i64,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.transaction().await?;
        let set = self
            .prepare_row(&mut tx, "acp_output_sets", set_id, None)
            .await?;
        let segment = self
            .prepare_row(&mut tx, "acp_output_segments", set_id, Some(index))
            .await?;
        self.finish_row(&mut tx, segment).await?;
        self.finish_row(&mut tx, set).await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn prepare_reuse(
        &self,
        connection: &mut SqliteConnection,
        set_id: &str,
        index: i64,
    ) -> Result<Option<Mutation>, OutputStoreError> {
        let state: Option<String> = sqlx::query_scalar(
            "SELECT state FROM acp_output_segments WHERE set_id=? AND segment_index=?",
        )
        .bind(set_id)
        .bind(index)
        .fetch_optional(&mut *connection)
        .await?;
        if state.as_deref() != Some("cleaned") {
            return Ok(None);
        }
        self.prepare_row(connection, "acp_output_segments", set_id, Some(index))
            .await
    }
    pub(super) async fn transaction(&self) -> Result<Transaction<'_, Sqlite>, OutputStoreError> {
        Ok(writer::begin(&self.pool, self.recovery.as_ref()).await?)
    }

    pub(super) async fn prepare_row(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        set_id: &str,
        index: Option<i64>,
    ) -> Result<Option<Mutation>, OutputStoreError> {
        Ok(writer::prepare_mutation(
            connection,
            self.recovery.as_ref(),
            &identity(table, set_id, index)?,
        )
        .await?)
    }

    pub(super) async fn finish_row(
        &self,
        connection: &mut SqliteConnection,
        mutation: Option<Mutation>,
    ) -> Result<(), OutputStoreError> {
        Ok(writer::complete_mutation(connection, self.recovery.as_ref(), mutation).await?)
    }

    pub(super) async fn insert_row(
        &self,
        connection: &mut SqliteConnection,
        table: &str,
        set_id: &str,
        index: Option<i64>,
    ) -> Result<(), OutputStoreError> {
        Ok(writer::inserted(
            connection,
            self.recovery.as_ref(),
            &identity(table, set_id, index)?,
        )
        .await?)
    }

    pub(super) async fn prepare_segments(
        &self,
        connection: &mut SqliteConnection,
        set_id: &str,
    ) -> Result<Vec<Option<Mutation>>, OutputStoreError> {
        let indices: Vec<i64> = sqlx::query_scalar("SELECT segment_index FROM acp_output_segments WHERE set_id=? AND state IN ('reserved','landed') ORDER BY segment_index")
            .bind(set_id).fetch_all(&mut *connection).await?;
        let mut changes = Vec::new();
        for index in indices {
            changes.push(
                self.prepare_row(connection, "acp_output_segments", set_id, Some(index))
                    .await?,
            );
        }
        Ok(changes)
    }

    pub(super) async fn finish_segments(
        &self,
        connection: &mut SqliteConnection,
        changes: Vec<Option<Mutation>>,
    ) -> Result<(), OutputStoreError> {
        for change in changes {
            self.finish_row(connection, change).await?;
        }
        Ok(())
    }
}
