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

const USER: &str = "@bridge-restart-test-20261105:matrix.0x8.xyz";
const DEVICE: &str = "CHJBUHHCSD";
const ROOM: &str = "!oISwKcAHFRwvYvGLFf:matrix.0x8.xyz";
const EVENT: &str = "$rE0I7FhHhUWL7sxcKXfZE096ZIb6pGLNFdt5iFt3t6s";
const DIGEST: &str = "505b44d2e9d39f378dafb9b486deb4b87531e672d52ab1bce7f4db500235809e";

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
    let mut value: Value = serde_json::from_str(raw).map_err(|_| "event-json")?;
    if value["event_id"] != EVENT
        || value["type"] != "m.room.encrypted"
        || value.get("room_id").is_some_and(|room| room != ROOM)
    {
        return Err("event-identity-mismatch");
    }
    let object = value.as_object_mut().ok_or("event-object")?;
    object.remove("unsigned");
    let full_digest = format!("{:x}", Sha256::digest(value.to_string().as_bytes()));
    let embedded_room = value
        .as_object_mut()
        .ok_or("event-object")?
        .remove("room_id");
    let sync_digest = format!("{:x}", Sha256::digest(value.to_string().as_bytes()));
    if full_digest == DIGEST
        && let Some(room_id) = embedded_room
    {
        value
            .as_object_mut()
            .ok_or("event-object")?
            .insert("room_id".into(), room_id);
    }
    let digest = format!("{:x}", Sha256::digest(value.to_string().as_bytes()));
    if digest != DIGEST {
        println!(
            "{}",
            json!({"stage":"ciphertext-binding","full_digest":full_digest,"sync_digest":sync_digest,"matched":false,"fields":value.as_object().map(|object| object.keys().collect::<Vec<_>>())})
        );
        return Err("ciphertext-digest-mismatch");
    }
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
