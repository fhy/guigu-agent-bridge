use matrix_sdk::sync::SyncResponse;
use sqlx::SqlitePool;

use crate::{
    config::Config,
    matrix::{MatrixClient, MatrixError},
    storage::CutoverStore,
};

pub(super) async fn initialize(
    client: MatrixClient,
    config: &Config,
    pool: &SqlitePool,
) -> Result<(MatrixClient, Option<SyncResponse>, Option<CutoverStore>), MatrixError> {
    let count: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM matrix_cutover_required) + (SELECT count(*) FROM matrix_cutover)")
        .fetch_one(pool)
        .await
        .map_err(|_| MatrixError::Storage)?;
    if count == 0 {
        let response = client.initialize_and_prove().await?;
        return Ok((client, Some(response), None));
    }
    let binding = client.cutover_binding(config).await?;
    let store = CutoverStore::open(pool.clone(), &binding)
        .await?
        .ok_or(MatrixError::CutoverRefused)?;
    let active = client.prepare_cutover(&store).await?;
    Ok((active, None, Some(store)))
}
