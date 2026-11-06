use std::{
    fs,
    io::{self, BufRead, Write},
    path::Path,
    time::Duration,
};

use matrix_sdk::{
    Client, SessionMeta, SessionTokens,
    authentication::matrix::MatrixSession,
    config::SyncSettings,
    ruma::{
        OwnedRoomId,
        api::client::{membership::invite_user, room::get_room_event},
        events::room::message::RoomMessageEventContent,
    },
};
use serde::Deserialize;
use serde_json::{Value, json};

const ROOM: &str = "!oISwKcAHFRwvYvGLFf:matrix.0x8.xyz";
const SENDER: &str = "@bridge-staging:matrix.0x8.xyz";
const BRIDGE: &str = "@bridge-restart-test-20261105:matrix.0x8.xyz";
const SERVER: &str = "https://matrix.0x8.xyz";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    action: String,
    #[serde(default)]
    body: String,
    #[serde(default)]
    event: String,
    #[serde(default)]
    transaction: String,
}

fn read_json(path: &Path) -> Result<Value, &'static str> {
    serde_json::from_slice(&fs::read(path).map_err(|_| "input-read")?).map_err(|_| "input-json")
}

fn field<'a>(value: &'a Value, name: &str) -> Result<&'a str, &'static str> {
    value[name].as_str().ok_or("input-field")
}

async fn restore(client: &Client, value: &Value, user: &str) -> Result<(), &'static str> {
    if field(value, "user_id")? != user {
        return Err("identity-refused");
    }
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: user.parse().map_err(|_| "user-format")?,
                device_id: field(value, "device_id")?.into(),
            },
            tokens: SessionTokens {
                access_token: field(value, "access_token")?.to_owned(),
                refresh_token: None,
            },
        })
        .await
        .map_err(|_| "restore")
}

async fn sender() -> Result<Client, &'static str> {
    let root = std::env::var_os("R107_SENDER_ROOT").ok_or("sender-root-missing")?;
    let root = Path::new(&root);
    fs::create_dir_all(root).map_err(|_| "root-create")?;
    let session_path = root.join("session.json");
    let store = root.join("store");
    let existing = session_path.exists();
    if existing != store.exists() {
        return Err("session-store-pair-refused");
    }
    let client = Client::builder()
        .homeserver_url(SERVER)
        .sqlite_store(&store, None)
        .build()
        .await
        .map_err(|_| "store-init")?;
    if existing {
        restore(&client, &read_json(&session_path)?, SENDER).await?;
    } else {
        let password_path =
            std::env::var_os("R107_PASSWORD_FILE").ok_or("password-file-missing")?;
        let password = read_json(Path::new(&password_path))?;
        if field(&password, "user_id")? != SENDER {
            return Err("password-identity-refused");
        }
        client
            .matrix_auth()
            .login_username(SENDER, field(&password, "password")?)
            .initial_device_display_name("R107 isolated encrypted acceptance")
            .send()
            .await
            .map_err(|_| "login")?;
        let session = client.matrix_auth().session().ok_or("login-session")?;
        let value = json!({"user_id":session.meta.user_id,
            "device_id":session.meta.device_id,
            "access_token":session.tokens.access_token,"home_server":SERVER});
        let mut output = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&session_path)
            .map_err(|_| "session-create")?;
        output
            .write_all(value.to_string().as_bytes())
            .and_then(|_| output.sync_all())
            .map_err(|_| "session-write")?;
    }
    let identity = client.whoami().await.map_err(|_| "whoami")?;
    if identity.user_id.as_str() != SENDER
        || identity.device_id.as_deref() != client.device_id()
        || identity.is_guest
    {
        return Err("whoami-mismatch");
    }
    Ok(client)
}

async fn invite(room: OwnedRoomId) -> Result<Value, &'static str> {
    let client = Client::builder()
        .homeserver_url(SERVER)
        .build()
        .await
        .map_err(|_| "invite-client")?;
    let session_path =
        std::env::var_os("R107_BRIDGE_SESSION").ok_or("bridge-session-file-missing")?;
    let session = read_json(Path::new(&session_path))?;
    restore(&client, &session, BRIDGE).await?;
    client
        .send(invite_user::v3::Request::new(
            room,
            invite_user::v3::InvitationRecipient::UserId {
                user_id: SENDER.parse().map_err(|_| "invite-user")?,
            },
        ))
        .await
        .map_err(|_| "invite")?;
    Ok(json!({"invited":true}))
}

async fn execute(
    client: &Client,
    command: Command,
    since: &mut Option<String>,
) -> Result<Value, &'static str> {
    let room_id: OwnedRoomId = ROOM.parse().map_err(|_| "room-format")?;
    match command.action.as_str() {
        "invite" => invite(room_id).await,
        "join" => {
            client.join_room_by_id(&room_id).await.map_err(|_| "join")?;
            Ok(json!({"joined":true}))
        }
        "sync" => {
            let mut settings = SyncSettings::new().timeout(Duration::from_secs(1));
            if let Some(token) = since.as_ref() {
                settings = settings.token(token.clone());
            }
            let response = client.sync_once(settings).await.map_err(|_| "sync")?;
            *since = Some(response.next_batch);
            let mut events = Vec::new();
            if let Some(room) = response.rooms.joined.get(&room_id) {
                for event in &room.timeline.events {
                    let value: Value =
                        serde_json::from_str(event.raw().json().get()).map_err(|_| "event-json")?;
                    if value["sender"] != BRIDGE {
                        continue;
                    }
                    events.push(json!({"event":value["event_id"],
                        "type":value["type"],"body":value["content"]["body"],
                        "reply":value["content"]["m.relates_to"]["m.in_reply_to"]["event_id"],
                        "decrypted":event.encryption_info().is_some()}));
                }
            }
            Ok(json!({"events":events,"joined":client.get_room(&room_id)
                .is_some_and(|room| room.state() == matrix_sdk::RoomState::Joined)}))
        }
        "send" => {
            if command.body.len() > 4096
                || !command.body.contains("R107-")
                || !command.transaction.starts_with("r107-")
            {
                return Err("send-bound");
            }
            refresh_recipient(client).await?;
            let room = client.get_room(&room_id).ok_or("room-not-synced")?;
            if !room
                .latest_encryption_state()
                .await
                .map_err(|_| "encryption-state")?
                .is_encrypted()
            {
                return Err("plaintext-refused");
            }
            let response = room
                .send(RoomMessageEventContent::text_plain(command.body))
                .with_transaction_id(command.transaction.as_str().into())
                .await
                .map_err(|_| "send")?;
            Ok(json!({"event":response.event_id}))
        }
        "wire" => {
            let event = command.event.parse().map_err(|_| "event-id")?;
            let response = client
                .send(get_room_event::v3::Request::new(room_id, event))
                .await
                .map_err(|_| "wire-read")?;
            let value: Value =
                serde_json::from_str(response.event.json().get()).map_err(|_| "wire-json")?;
            Ok(json!({"event":value["event_id"],"type":value["type"],
                "sender":value["sender"]}))
        }
        _ => Err("action-refused"),
    }
}

fn recipient_device(session: &Value) -> Result<&str, &'static str> {
    if field(session, "user_id")? != BRIDGE {
        return Err("recipient-account-refused");
    }
    let device = field(session, "device_id")?;
    if device.is_empty() {
        return Err("recipient-device-missing");
    }
    Ok(device)
}

fn require_visible_recipient(visible: bool) -> Result<(), &'static str> {
    if visible {
        Ok(())
    } else {
        Err("recipient-device-not-published")
    }
}

async fn refresh_recipient(client: &Client) -> Result<(), &'static str> {
    let path = std::env::var_os("R107_BRIDGE_SESSION").ok_or("bridge-session-file-missing")?;
    let session = read_json(Path::new(&path))?;
    let device = recipient_device(&session)?;
    let user: matrix_sdk::ruma::OwnedUserId =
        BRIDGE.parse().map_err(|_| "recipient-user-format")?;
    client
        .encryption()
        .request_user_identity(&user)
        .await
        .map_err(|_| "recipient-fresh-key-query")?;
    let devices = client
        .encryption()
        .get_user_devices(&user)
        .await
        .map_err(|_| "recipient-device-query")?;
    require_visible_recipient(devices.get(device.into()).is_some())
}

async fn run() -> Result<(), &'static str> {
    let client = sender().await?;
    println!(
        "{}",
        json!({"ready":true,"user":client.user_id(),"device":client.device_id()})
    );
    io::stdout().flush().map_err(|_| "output")?;
    let mut since = None;
    for line in io::stdin().lock().lines() {
        let line = line.map_err(|_| "input")?;
        if line.len() > 8192 {
            return Err("input-bound");
        }
        let command = serde_json::from_str(&line).map_err(|_| "command-json")?;
        let result = match tokio::time::timeout(
            Duration::from_secs(45),
            execute(&client, command, &mut since),
        )
        .await
        {
            Ok(Ok(result)) => result,
            Ok(Err(category)) => json!({"failure":category}),
            Err(_) => json!({"failure":"sdk-timeout"}),
        };
        println!("{result}");
        io::stdout().flush().map_err(|_| "output")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn receiving_device_is_bound_to_registered_account() {
        assert_eq!(
            recipient_device(&json!({"user_id":BRIDGE,"device_id":"NEW"})),
            Ok("NEW")
        );
        assert!(recipient_device(&json!({"user_id":SENDER,"device_id":"NEW"})).is_err());
        assert!(recipient_device(&json!({"user_id":BRIDGE,"device_id":""})).is_err());
        assert!(recipient_device(&json!({"user_id":BRIDGE})).is_err());
    }

    #[test]
    fn absent_queried_device_cannot_proceed_to_send() {
        assert!(require_visible_recipient(false).is_err());
        assert!(require_visible_recipient(true).is_ok());
    }
}
