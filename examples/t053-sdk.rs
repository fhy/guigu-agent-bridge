//! Private staging SDK sender/recipient. No account material is printed.
use std::{io::{self, BufRead, Write}, path::PathBuf, time::Duration};

use matrix_sdk::{Client, SessionTokens, authentication::matrix::MatrixSession,
    config::SyncSettings, ruma::{OwnedRoomId, events::room::message::RoomMessageEventContent}};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    action: String,
    room: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    transaction: String,
}

async fn execute(client: &Client, command: Command, since: &mut Option<String>) -> Result<Value, &'static str> {
    let room_id: OwnedRoomId = command.room.parse().map_err(|_| "room-format")?;
    match command.action.as_str() {
        "sync" => {
            let mut settings = SyncSettings::default().timeout(Duration::from_secs(1));
            if let Some(token) = since.as_ref() { settings = settings.token(token.clone()); }
            let response = client.sync_once(settings).await.map_err(|_| "sync-failed")?;
            *since = Some(response.next_batch);
            let mut events = Vec::new();
            if let Some(room) = response.rooms.join.get(&room_id) {
                for event in &room.timeline.events {
                    let value: Value = serde_json::from_str(event.raw().json().get()).map_err(|_| "event-format")?;
                    // Only synthetic application replies from the staging primary in
                    // the caller's assigned room leave the private SDK process.
                    if value["sender"] != "@bridge-staging:matrix.0x8.xyz" || value["type"] != "m.room.message" { continue; }
                    let body = value["content"]["body"].as_str().ok_or("body-format")?;
                    if body.len() > 65536 { return Err("body-bound"); }
                    events.push(json!({"event":value["event_id"],"body":body,
                        "reply":value["content"]["m.relates_to"]["m.in_reply_to"]["event_id"],
                        "encrypted":event.encryption_info().is_some()}));
                }
            }
            Ok(json!({"events":events}))
        }
        "send" => {
            if command.body.len() > 65536 || command.transaction.is_empty() { return Err("send-bound"); }
            let room = client.get_room(&room_id).ok_or("room-not-synced")?;
            if !room.is_encrypted().await.map_err(|_| "encryption-state")? { return Err("plaintext-refused"); }
            let result = room.send(RoomMessageEventContent::text_plain(command.body))
                .with_transaction_id(command.transaction.as_str().into())
                .await.map_err(|_| "send-failed")?;
            Ok(json!({"event":result.event_id}))
        }
        _ => Err("action-refused"),
    }
}

async fn run() -> Result<(), &'static str> {
    let store = PathBuf::from(std::env::var("T053_STORE").map_err(|_| "store-missing")?);
    let user = std::env::var("T053_USER").map_err(|_| "user-missing")?;
    if !["@bridge-staging-failure:matrix.0x8.xyz", "@bridge-staging-reviewer:matrix.0x8.xyz", "@bridge-staging-observer:matrix.0x8.xyz"].contains(&user.as_str()) { return Err("user-refused"); }
    let device = std::env::var("T053_DEVICE").map_err(|_| "device-missing")?;
    if device.starts_with("T046") { return Err("device-refused"); }
    let token = std::env::var("T053_TOKEN").map_err(|_| "token-missing")?;
    let client = Client::builder().homeserver_url("https://matrix.0x8.xyz")
        .sqlite_store(store, None).build().await.map_err(|_| "store-init")?;
    client.restore_session(MatrixSession {
        meta: matrix_sdk::SessionMeta {user_id:user.parse().map_err(|_| "user-format")?, device_id:device.as_str().into()},
        tokens: SessionTokens { access_token:token, refresh_token:None },
    }).await.map_err(|_| "restore-failed")?;
    let identity = client.whoami().await.map_err(|_| "whoami-failed")?;
    if identity.user_id.as_str() != user || identity.device_id.as_ref().map(|id|id.as_str()) != Some(device.as_str()) || identity.is_guest { return Err("identity-mismatch"); }
    println!("{{\"ready\":true}}");
    io::stdout().flush().map_err(|_| "output-failed")?;
    let mut since = None;
    for line in io::stdin().lock().lines() {
        let line = line.map_err(|_| "input-failed")?;
        if line.len() > 131072 { return Err("input-bound"); }
        let command: Command = serde_json::from_str(&line).map_err(|_| "command-format")?;
        let result = match tokio::time::timeout(Duration::from_secs(45), execute(&client, command, &mut since)).await {
            Ok(Ok(result)) => result,
            Ok(Err(category)) => json!({"failure":category}),
            Err(_) => json!({"failure":"sdk-timeout"}),
        };
        println!("{result}");
        io::stdout().flush().map_err(|_| "output-failed")?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(category) = run().await {
        println!("{}", json!({"failure":category}));
        std::process::exit(3);
    }
}
