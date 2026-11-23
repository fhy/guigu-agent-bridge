use std::{path::Path, time::Duration};

use matrix_sdk::{
    Client, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    ruma::{
        OwnedRoomId, api::client::room::get_room_event,
        events::room::encrypted::OriginalSyncRoomEncryptedEvent, serde::Raw,
    },
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

#[path = "support/r107_key_transfer.rs"]
mod transfer;

const USER: &str = "@bridge-restart-test-20261105:matrix.0x8.xyz";
const DEVICE: &str = "GAZUUBLMHH";
const ROOM: &str = "!oISwKcAHFRwvYvGLFf:matrix.0x8.xyz";
const EVENT: &str = "$FDauUxHWW-NhS7besZtFc9w9fm3x5LojXLaYmr20b-k";
const DIGEST: &str = "b83f8040ef9d374a212b6c3a3c8c0fa90197869a1d876e4c04a14e471a5d4009";

fn field<'value>(value: &'value Value, name: &str) -> Result<&'value str, &'static str> {
    value[name].as_str().ok_or("input-field")
}

async fn client() -> Result<Client, &'static str> {
    let root = std::env::var_os("R107_KEY_ROOT").ok_or("paired-root-missing")?;
    let root = Path::new(&root);
    let session: Value = serde_json::from_slice(
        &std::fs::read(root.join("config/session.json")).map_err(|_| "session-read")?,
    )
    .map_err(|_| "session-json")?;
    if field(&session, "user_id")? != USER || field(&session, "device_id")? != DEVICE {
        return Err("session-identity-mismatch");
    }
    let client = Client::builder()
        .homeserver_url("https://matrix.0x8.xyz")
        .sqlite_store(root.join("state/crypto"), None)
        .build()
        .await
        .map_err(|_| "store-open")?;
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: USER.parse().map_err(|_| "user-id")?,
                device_id: DEVICE.into(),
            },
            tokens: SessionTokens {
                access_token: field(&session, "access_token")?.into(),
                refresh_token: None,
            },
        })
        .await
        .map_err(|_| "restore")?;
    let identity = client.whoami().await.map_err(|_| "whoami")?;
    if identity.user_id.as_str() != USER
        || identity.device_id.as_deref() != client.device_id()
        || identity.is_guest
    {
        return Err("online-identity-mismatch");
    }
    Ok(client)
}

struct BoundEvent {
    event: Raw<OriginalSyncRoomEncryptedEvent>,
    session_id: String,
    digest: String,
}

fn bind_wire(raw: &str) -> Result<BoundEvent, &'static str> {
    bind_wire_expected(raw, EVENT, DIGEST)
}

fn bind_wire_expected(
    raw: &str,
    expected_event: &str,
    expected_digest: &str,
) -> Result<BoundEvent, &'static str> {
    let value: Value = serde_json::from_str(raw).map_err(|_| "event-json")?;
    if value["event_id"] != expected_event
        || value["type"] != "m.room.encrypted"
        || value.get("room_id").is_some_and(|room| room != ROOM)
    {
        return Err("event-identity-mismatch");
    }
    let mut canonical = value.clone();
    canonical
        .as_object_mut()
        .ok_or("event-object")?
        .remove("unsigned");
    let full_digest = format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()));
    let mut sync_shape = canonical.clone();
    if let Some(object) = sync_shape.as_object_mut() {
        object.remove("age");
        object.remove("user_id");
        object.remove("room_id");
    }
    let sync_digest = format!("{:x}", Sha256::digest(sync_shape.to_string().as_bytes()));
    let mut no_server_fields = sync_shape.clone();
    if let Some(object) = no_server_fields.as_object_mut() {
        object.remove("origin_server_ts");
    }
    let no_server_digest = format!(
        "{:x}",
        Sha256::digest(no_server_fields.to_string().as_bytes())
    );
    let (digest, value) = if full_digest == expected_digest {
        (full_digest.clone(), canonical)
    } else if sync_digest == expected_digest {
        (sync_digest.clone(), sync_shape)
    } else if no_server_digest == expected_digest {
        (no_server_digest.clone(), no_server_fields)
    } else {
        println!(
            "{}",
            json!({"stage":"ciphertext-binding","full_digest":full_digest,"sync_digest":sync_digest,"no_server_digest":no_server_digest,"matched":false,"fields":value.as_object().map(|object| object.keys().collect::<Vec<_>>())})
        );
        return Err("ciphertext-digest-mismatch");
    };
    let session_id = field(&value["content"], "session_id")?.to_owned();
    let event = Raw::<OriginalSyncRoomEncryptedEvent>::from_json_string(value.to_string())
        .map_err(|_| "raw-event")?;
    Ok(BoundEvent {
        event,
        session_id,
        digest,
    })
}

async fn run() -> Result<(), &'static str> {
    let client = client().await?;
    let room_id: OwnedRoomId = ROOM.parse().map_err(|_| "room-id")?;
    let response = client
        .send(get_room_event::v3::Request::new(
            room_id.clone(),
            EVENT.parse().map_err(|_| "event-id")?,
        ))
        .await
        .map_err(|_| "event-fetch")?;
    let BoundEvent {
        event,
        session_id,
        digest,
    } = bind_wire(response.event.json().get())?;
    let room = client.get_room(&room_id).ok_or("retained-room-missing")?;
    let before = room
        .decrypt_event(&event, None)
        .await
        .map_err(|_| "decrypt-before")?
        .encryption_info()
        .is_some();
    println!(
        "{}",
        json!({"stage":"exact-event-before","account":USER,"device":DEVICE,"room":ROOM,"event":EVENT,"digest":digest,"decrypted":before,"application_routing":false})
    );
    if before {
        return Ok(());
    }
    let backup = client
        .encryption()
        .backups()
        .download_room_key(&room_id, &session_id)
        .await;
    println!(
        "{}",
        json!({"stage":"bounded-backup","downloaded":backup.as_ref().copied().unwrap_or(false),"request_failed":backup.is_err()})
    );
    for attempt in 1..=2 {
        let sync = client
            .sync_once(SyncSettings::default().timeout(Duration::from_secs(2)))
            .await;
        let decrypted = room
            .decrypt_event(&event, None)
            .await
            .map_err(|_| "decrypt-after")?
            .encryption_info()
            .is_some();
        println!(
            "{}",
            json!({"stage":"crypto-only-sync","attempt":attempt,"sync_completed":sync.is_ok(),"decrypted":decrypted,"application_routing":false,"bridge_checkpoint_changed":false})
        );
        if decrypted {
            return Ok(());
        }
    }
    println!(
        "{}",
        json!({"stage":"exact-event-result","event":EVENT,"room":ROOM,"decrypted":false,"resource":"matching Megolm room session key","refusal_preserved":true})
    );
    Err("exact-room-key-unavailable")
}

#[tokio::main]
async fn main() {
    if std::env::var("R107_DEVICE_DIAGNOSTIC").as_deref() == Ok("62aaab86") {
        if let Err(category) = transfer::diagnose().await {
            println!("{}", json!({"failure":category}));
            std::process::exit(3);
        }
        return;
    }
    if std::env::var("R107_EXACT_TRANSFER").as_deref() == Ok("62aaab86") {
        if let Err(category) = transfer::run().await {
            println!(
                "{}",
                json!({"failure":category,"transfer_retry_allowed":false})
            );
            std::process::exit(3);
        }
        return;
    }
    let result = tokio::time::timeout(Duration::from_secs(90), run()).await;
    match result {
        Ok(Ok(())) => (),
        Ok(Err(category)) => {
            println!("{}", json!({"failure":category}));
            std::process::exit(3);
        }
        Err(_) => {
            println!("{}", json!({"failure":"bounded-sdk-timeout"}));
            std::process::exit(4);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ciphertext() -> Value {
        json!({"event_id":EVENT,"type":"m.room.encrypted","room_id":ROOM,
            "content":{"session_id":"test-session","ciphertext":"test-ciphertext"}})
    }

    #[test]
    fn exact_ciphertext_and_unsigned_metadata_bind() {
        let mut event = ciphertext();
        let digest = format!("{:x}", Sha256::digest(event.to_string().as_bytes()));
        assert!(bind_wire_expected(&event.to_string(), EVENT, &digest).is_ok());
        event["unsigned"] = json!({"age":123});
        assert!(bind_wire_expected(&event.to_string(), EVENT, &digest).is_ok());
    }

    #[test]
    fn changed_ciphertext_is_refused() {
        let mut event = ciphertext();
        let digest = format!("{:x}", Sha256::digest(event.to_string().as_bytes()));
        event["content"]["ciphertext"] = json!("changed");
        assert!(matches!(
            bind_wire_expected(&event.to_string(), EVENT, &digest),
            Err("ciphertext-digest-mismatch")
        ));
    }

    #[test]
    fn foreign_identity_is_refused() {
        let mut event = ciphertext();
        event["room_id"] = json!("!foreign:matrix.0x8.xyz");
        assert!(matches!(
            bind_wire_expected(&event.to_string(), EVENT, DIGEST),
            Err("event-identity-mismatch")
        ));
        event = ciphertext();
        event["event_id"] = json!("$foreign");
        assert!(matches!(
            bind_wire_expected(&event.to_string(), EVENT, DIGEST),
            Err("event-identity-mismatch")
        ));
    }
}
