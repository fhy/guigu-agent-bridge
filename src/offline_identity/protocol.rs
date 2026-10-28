use std::io::{Read, Write};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

pub(super) const MAX_FRAME: usize = 65536;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Ready {
    pub nonce: String,
    pub policy: String,
    pub executable: String,
    pub process: u32,
    pub generation: String,
    pub no_new_privs: u8,
    pub seccomp: u8,
    pub denied_probes: [i32; 6],
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Capture {
    pub nonce: String,
    pub homeserver: String,
    pub user_id: String,
    pub device_id: String,
    pub access_token: String,
    pub store: std::path::PathBuf,
    pub passphrase: Option<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Captured {
    pub ed25519: String,
    pub curve25519: String,
    pub no_new_privs: u8,
    pub seccomp: u8,
}

pub(super) fn write_frame(output: &mut impl Write, value: &impl Serialize) -> Result<(), ()> {
    let bytes = serde_json::to_vec(value).map_err(|_| ())?;
    if bytes.len() > MAX_FRAME {
        return Err(());
    }
    output
        .write_all(&(bytes.len() as u32).to_be_bytes())
        .map_err(|_| ())?;
    output.write_all(&bytes).map_err(|_| ())?;
    output.flush().map_err(|_| ())
}

pub(super) fn read_frame<Value: DeserializeOwned>(input: &mut impl Read) -> Result<Value, ()> {
    let mut header = [0; 4];
    input.read_exact(&mut header).map_err(|_| ())?;
    let length = u32::from_be_bytes(header) as usize;
    if length == 0 || length > MAX_FRAME {
        return Err(());
    }
    let mut bytes = vec![0; length];
    input.read_exact(&mut bytes).map_err(|_| ())?;
    let result = serde_json::from_slice(&bytes).map_err(|_| ());
    bytes.fill(0);
    result
}
