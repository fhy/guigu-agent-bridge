use super::output_delivery::PreparedOutput;
#[cfg(test)]
use super::output_spool::OutputSetInput;
use super::output_spool::{OutputSpoolStore, OutputStoreError};
use sqlx::Row;

impl OutputSpoolStore {
    #[cfg(test)]
    pub(crate) async fn create_set(
        &self,
        input: &OutputSetInput<'_>,
    ) -> Result<(), OutputStoreError> {
        let mut tx = self.transaction().await?;
        sqlx::query("INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,thread_root,reply_event_id,render_version,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,'v1','assembling',?,?)")
            .bind(input.set_id).bind(input.task_id).bind(input.attempt).bind(input.turn)
            .bind(input.runtime_fence).bind(input.room_id).bind(input.thread_root)
            .bind(input.reply_event_id).bind(input.now).bind(input.now).execute(&mut *tx).await?;
        self.insert_row(&mut tx, "acp_output_sets", input.set_id, None)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn assembling_set(
        &self,
        task_id: &str,
    ) -> Result<Option<String>, OutputStoreError> {
        let mut tx = self.transaction().await?;
        let set: Option<String> = sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE task_id=? AND state='assembling' ORDER BY turn DESC LIMIT 1")
            .bind(task_id).fetch_optional(&mut *tx).await?;
        if let Some(set) = &set {
            self.prepare_row(&mut tx, "acp_output_sets", set, None)
                .await?;
        }
        tx.commit().await?;
        Ok(set)
    }

    pub(crate) async fn prepare_task(
        &self,
        task_id: &str,
        now: &str,
    ) -> Result<Option<PreparedOutput>, OutputStoreError> {
        let mut tx = self.transaction().await?;
        let row=sqlx::query("SELECT d.attempt,c.completed_turns+1 AS turn,c.runtime_generation,a.reply_room,a.reply_thread_root,a.reply_event_id,COALESCE(a.render_version,'v1') render_version FROM task_continuations c JOIN deliveries d ON d.delivery_id=c.delivery_id JOIN task_admissions a ON a.task_id=c.task_id WHERE c.task_id=? AND c.state='in_flight'")
            .bind(task_id).fetch_optional(&mut *tx).await?;
        let Some(row) = row else { return Ok(None) };
        let Some(room) = row.try_get::<Option<String>, _>("reply_room")? else {
            return Ok(None);
        };
        let attempt: i64 = row.try_get("attempt")?;
        let turn: i64 = row.try_get("turn")?;
        let set_id = uuid::Uuid::new_v5(
            &uuid::Uuid::from_u128(0x64c52b8d_26ef_57a0_a0c1_6d460bb30dca),
            format!("{task_id}\0{attempt}\0{turn}").as_bytes(),
        )
        .to_string();
        sqlx::query("INSERT INTO acp_output_sets(set_id,task_id,attempt,turn,runtime_fence,room_id,thread_root,reply_event_id,render_version,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,'assembling',?,?) ON CONFLICT(task_id,attempt,turn) DO NOTHING")
            .bind(&set_id).bind(task_id).bind(attempt).bind(turn)
            .bind(row.try_get::<String,_>("runtime_generation")?).bind(room)
            .bind(row.try_get::<Option<String>,_>("reply_thread_root")?)
            .bind(row.try_get::<Option<String>,_>("reply_event_id")?)
            .bind(row.try_get::<String,_>("render_version")?).bind(now).bind(now)
            .execute(&mut *tx).await?;
        let winner:Option<String>=sqlx::query_scalar("SELECT set_id FROM acp_output_sets WHERE task_id=? AND attempt=? AND turn=? AND state='assembling'")
            .bind(task_id).bind(attempt).bind(turn).fetch_optional(&mut *tx).await?;
        if winner.as_deref() != Some(&set_id) {
            return Err(OutputStoreError::Conflict);
        }
        self.insert_row(&mut tx, "acp_output_sets", &set_id, None)
            .await?;
        tx.commit().await?;
        Ok(Some(PreparedOutput { set_id }))
    }
}
