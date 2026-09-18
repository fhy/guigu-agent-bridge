use chrono::{DateTime, Utc};
use sqlx::SqlitePool;

use super::{
    A2aError,
    wire::{Artifact, Message, Part, Role},
};

pub(super) async fn store_message(
    pool: &SqlitePool,
    exchange_id: &str,
    message: &Message,
    now: DateTime<Utc>,
) -> Result<i64, A2aError> {
    if message.parts.len() > 64 {
        return Err(A2aError::TooLarge);
    }
    let mut tx = pool.begin().await?;
    let stored_id = uuid::Uuid::now_v7().to_string();
    sqlx::query("INSERT INTO a2a_messages (message_id,exchange_id,external_message_id,role,ordinal,created_at) VALUES (?,?,?,?,0,?)")
        .bind(&stored_id).bind(exchange_id).bind(&message.message_id)
        .bind(match message.role { Role::User => "user", Role::Agent => "agent" })
        .bind(now.to_rfc3339()).execute(&mut *tx).await?;
    let mut total = 0_i64;
    for (ordinal, part) in message.parts.iter().enumerate() {
        let encoded = encode(part)?;
        total = total.checked_add(encoded.bytes).ok_or(A2aError::TooLarge)?;
        if total > 1_048_576 {
            return Err(A2aError::TooLarge);
        }
        let source = encoded
            .text
            .as_deref()
            .or(encoded.json.as_deref())
            .map(str::as_bytes)
            .or(encoded.blob.as_deref())
            .unwrap_or_default();
        let hash = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, source).to_string();
        sqlx::query("INSERT INTO a2a_message_parts (message_id,ordinal,kind,mime_type,text_value,json_value,blob_value,content_hash) VALUES (?,?,?,?,?,?,?,?)")
            .bind(&stored_id).bind(ordinal as i64).bind(encoded.kind).bind(encoded.mime)
            .bind(encoded.text).bind(encoded.json).bind(encoded.blob).bind(hash)
            .execute(&mut *tx).await?;
    }
    sqlx::query("UPDATE a2a_exchanges SET content_bytes=?,updated_at=? WHERE exchange_id=? AND state='reserved'")
        .bind(total).bind(now.to_rfc3339()).bind(exchange_id).execute(&mut *tx).await?;
    tx.commit().await?;
    Ok(total)
}

pub(super) async fn store_artifacts(
    pool: &SqlitePool,
    exchange_id: &str,
    artifacts: &[Artifact],
) -> Result<i64, A2aError> {
    if artifacts.len() > 16 {
        return Err(A2aError::TooLarge);
    }
    let mut tx = pool.begin().await?;
    let mut total = 0_i64;
    for (artifact_ordinal, artifact) in artifacts.iter().enumerate() {
        if artifact.parts.len() > 64 {
            return Err(A2aError::TooLarge);
        }
        let stored_id = uuid::Uuid::now_v7().to_string();
        sqlx::query("INSERT INTO a2a_artifacts (artifact_id,exchange_id,external_artifact_id,ordinal,name,description) VALUES (?,?,?,?,?,?)")
            .bind(&stored_id).bind(exchange_id).bind(&artifact.artifact_id).bind(artifact_ordinal as i64)
            .bind(&artifact.name).bind(&artifact.description).execute(&mut *tx).await?;
        for (ordinal, part) in artifact.parts.iter().enumerate() {
            let encoded = encode(part)?;
            total = total.checked_add(encoded.bytes).ok_or(A2aError::TooLarge)?;
            if total > 1_048_576 {
                return Err(A2aError::TooLarge);
            }
            let source = encoded
                .text
                .as_deref()
                .or(encoded.json.as_deref())
                .map(str::as_bytes)
                .or(encoded.blob.as_deref())
                .unwrap_or_default();
            let hash = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_OID, source).to_string();
            sqlx::query("INSERT INTO a2a_artifact_parts (artifact_id,ordinal,kind,mime_type,text_value,json_value,blob_value,content_hash) VALUES (?,?,?,?,?,?,?,?)")
                .bind(&stored_id).bind(ordinal as i64).bind(encoded.kind).bind(encoded.mime)
                .bind(encoded.text).bind(encoded.json).bind(encoded.blob).bind(hash)
                .execute(&mut *tx).await?;
        }
    }
    sqlx::query(
        "UPDATE a2a_exchanges SET content_bytes=content_bytes+?,updated_at=? WHERE exchange_id=?",
    )
    .bind(total)
    .bind(Utc::now().to_rfc3339())
    .bind(exchange_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(total)
}

struct Encoded {
    kind: &'static str,
    mime: Option<String>,
    text: Option<String>,
    json: Option<String>,
    blob: Option<Vec<u8>>,
    bytes: i64,
}

fn encode(part: &Part) -> Result<Encoded, A2aError> {
    match part {
        Part::Text { text } => Ok(Encoded {
            kind: "text",
            mime: None,
            text: Some(text.clone()),
            json: None,
            blob: None,
            bytes: text.len() as i64,
        }),
        Part::Data { data } => {
            let value =
                serde_json::to_string(data).map_err(|_| A2aError::Protocol("invalid data part"))?;
            Ok(Encoded {
                kind: "data",
                mime: None,
                bytes: value.len() as i64,
                text: None,
                json: Some(value),
                blob: None,
            })
        }
        Part::FileInline { mime_type, data } => {
            if data.len() > 262_144 {
                return Err(A2aError::TooLarge);
            }
            Ok(Encoded {
                kind: "file_inline",
                mime: Some(mime_type.clone()),
                text: None,
                json: None,
                blob: Some(data.as_bytes().to_vec()),
                bytes: data.len() as i64,
            })
        }
        Part::FileUri { .. } => Err(A2aError::Unsupported),
    }
}
