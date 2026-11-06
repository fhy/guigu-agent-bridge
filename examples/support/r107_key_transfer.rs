use super::*;
use std::{
    fs::{self, DirBuilder, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt},
    path::PathBuf,
};

const SENDER: &str = "@bridge-staging:matrix.0x8.xyz";

struct Artifact(PathBuf);

impl Drop for Artifact {
    fn drop(&mut self) {
        let removed = fs::remove_file(&self.0).is_ok();
        println!(
            "{}",
            json!({"stage":"exact-transfer-cleanup","artifact_removed":removed})
        );
    }
}

fn private_directory(path: &Path) -> Result<(), &'static str> {
    let metadata = fs::symlink_metadata(path).map_err(|_| "private-directory-missing")?;
    if !metadata.is_dir() || metadata.permissions().mode() & 0o077 != 0 {
        return Err("private-directory-refused");
    }
    Ok(())
}

fn allocate(root: &Path) -> Result<Artifact, &'static str> {
    private_directory(root)?;
    let directory = root.join("exact-session-transfer-once");
    DirBuilder::new()
        .mode(0o700)
        .create(&directory)
        .map_err(|_| "transfer-already-attempted")?;
    let path = directory.join("session.encrypted");
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)
        .map_err(|_| "artifact-create")?;
    Ok(Artifact(path))
}

fn selected(
    room: &str,
    session: &str,
    sender_key: &str,
    expected_session: &str,
    expected_key: &str,
) -> bool {
    room == ROOM && session == expected_session && sender_key == expected_key
}

async fn source(root: &Path) -> Result<Client, &'static str> {
    private_directory(root)?;
    private_directory(&root.join("store"))?;
    let session: Value = serde_json::from_slice(
        &fs::read(root.join("session.json")).map_err(|_| "source-session-read")?,
    )
    .map_err(|_| "source-session-json")?;
    if field(&session, "user_id")? != SENDER {
        return Err("source-account-refused");
    }
    let device = field(&session, "device_id")?;
    let client = Client::builder()
        .homeserver_url("https://matrix.0x8.xyz")
        .sqlite_store(root.join("store"), None)
        .build()
        .await
        .map_err(|_| "source-store")?;
    client
        .restore_session(MatrixSession {
            meta: SessionMeta {
                user_id: SENDER.parse().map_err(|_| "source-user")?,
                device_id: device.into(),
            },
            tokens: SessionTokens {
                access_token: field(&session, "access_token")?.into(),
                refresh_token: None,
            },
        })
        .await
        .map_err(|_| "source-restore")?;
    let identity = client.whoami().await.map_err(|_| "source-whoami")?;
    if identity.user_id.as_str() != SENDER
        || identity.device_id.as_deref() != client.device_id()
        || identity.is_guest
    {
        return Err("source-online-identity");
    }
    Ok(client)
}

pub(super) async fn run() -> Result<(), &'static str> {
    let root = std::env::var_os("R107_KEY_ROOT").ok_or("paired-root-missing")?;
    let root = Path::new(&root);
    let source_root = std::env::var_os("R107_KEY_SENDER_ROOT").ok_or("source-root-missing")?;
    let receiver = client().await?;
    let sender = source(Path::new(&source_root)).await?;
    let room_id: OwnedRoomId = ROOM.parse().map_err(|_| "room-id")?;
    let response = sender
        .send(get_room_event::v3::Request::new(
            room_id.clone(),
            EVENT.parse().map_err(|_| "event-id")?,
        ))
        .await
        .map_err(|_| "source-event-fetch")?;
    let wire = response.event.json().get();
    let bound = bind_wire(wire)?;
    let value: Value = serde_json::from_str(wire).map_err(|_| "source-event-json")?;
    if value["sender"] != SENDER {
        return Err("event-sender-refused");
    }
    let sender_key = field(&value["content"], "sender_key")?;
    let room = sender.get_room(&room_id).ok_or("source-room-missing")?;
    if room
        .decrypt_event(&bound.event, None)
        .await
        .map_err(|_| "source-decrypt")?
        .encryption_info()
        .is_none()
    {
        return Err("source-key-unavailable");
    }
    let artifact = allocate(root)?;
    let mut entropy = [0_u8; 32];
    File::open("/dev/urandom")
        .and_then(|mut file| file.read_exact(&mut entropy))
        .map_err(|_| "passphrase-random")?;
    let passphrase = entropy
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut count = 0;
    sender
        .encryption()
        .export_room_keys(artifact.0.clone(), &passphrase, |session| {
            let matches = selected(
                session.room_id().as_str(),
                session.session_id(),
                &session.sender_key().to_base64(),
                &bound.session_id,
                sender_key,
            );
            if matches {
                count += 1;
            }
            matches
        })
        .await
        .map_err(|_| "exact-export")?;
    if count != 1 {
        return Err("exact-export-count");
    }
    let imported = receiver
        .encryption()
        .import_room_keys(artifact.0.clone(), &passphrase)
        .await
        .map_err(|_| "exact-import")?;
    if imported.total_count != 1 || imported.keys.len() != 1 {
        return Err("exact-import-count");
    }
    let room = receiver.get_room(&room_id).ok_or("receiver-room-missing")?;
    let decrypted = room
        .decrypt_event(&bound.event, None)
        .await
        .map_err(|_| "receiver-decrypt")?
        .encryption_info()
        .is_some();
    let receipt = json!({"stage":"exact-session-transfer","sender":SENDER,"account":USER,"device":DEVICE,
        "room":ROOM,"event":EVENT,"digest":DIGEST,"exported":count,"imported":imported.imported_count,
        "decrypted":decrypted,"bridge_db_opened":false,"checkpoint_changed":false});
    let receipt_path = artifact
        .0
        .parent()
        .ok_or("artifact-parent")?
        .join("receipt.json");
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(receipt_path)
        .map_err(|_| "receipt-create")?;
    output
        .write_all(receipt.to_string().as_bytes())
        .and_then(|_| output.sync_all())
        .map_err(|_| "receipt-write")?;
    println!("{receipt}");
    if !decrypted {
        return Err("transferred-event-not-decrypted");
    }
    Ok(())
}

pub(super) async fn diagnose() -> Result<(), &'static str> {
    let root = std::env::var_os("R107_KEY_SENDER_ROOT").ok_or("source-root-missing")?;
    let sender = source(Path::new(&root)).await?;
    let user: matrix_sdk::ruma::OwnedUserId = USER.parse().map_err(|_| "recipient-user")?;
    for stage in ["cached-device-list", "fresh-device-query"] {
        if stage == "fresh-device-query" {
            sender
                .encryption()
                .request_user_identity(&user)
                .await
                .map_err(|_| "recipient-key-query")?;
        }
        let devices = sender
            .encryption()
            .get_user_devices(&user)
            .await
            .map_err(|_| "recipient-device-list")?;
        let device = devices.get(DEVICE.into());
        println!(
            "{}",
            json!({"stage":stage,"sender":SENDER,"recipient":USER,"device":DEVICE,
            "present":device.is_some(),"verified":device.as_ref().map(|device|device.is_verified()),
            "cross_signed":device.as_ref().map(|device|device.is_cross_signed_by_owner()),
            "devices_count":devices.devices().count(),"keys_logged":false})
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selector_never_exports_another_room_session_or_sender() {
        assert!(selected(ROOM, "session", "sender", "session", "sender"));
        assert!(!selected(
            "!foreign:example.test",
            "session",
            "sender",
            "session",
            "sender"
        ));
        assert!(!selected(ROOM, "other", "sender", "session", "sender"));
        assert!(!selected(ROOM, "session", "foreign", "session", "sender"));
    }

    #[test]
    fn artifact_is_private_removed_and_cannot_be_retried() {
        let root = tempfile::tempdir().unwrap();
        fs::set_permissions(root.path(), fs::Permissions::from_mode(0o700)).unwrap();
        let artifact = allocate(root.path()).unwrap();
        assert_eq!(
            fs::metadata(&artifact.0).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let path = artifact.0.clone();
        drop(artifact);
        assert!(!path.exists());
        assert!(allocate(root.path()).is_err());
    }
}
