use std::{collections::BTreeMap, time::Duration};

use matrix_sdk::{
    crypto::DeviceData,
    ruma::{
        OwnedDeviceId, OwnedUserId,
        api::{
            IncomingResponse, MatrixVersion, OutgoingRequest, SendAccessToken,
            client::{account::whoami, keys::get_keys},
        },
    },
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use super::parent::{CollectorRefused, CollectorRequest, CollectorResult};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerObservation {
    pub identity_digest: String,
    pub whoami_digest: String,
    pub keys_query_digest: String,
    pub signed_device_digest: String,
    pub ed25519: String,
    pub curve25519: String,
}

impl ServerObservation {
    pub fn require_continuity(
        &self,
        after: &Self,
        local: &CollectorResult,
    ) -> Result<(), CollectorRefused> {
        if self.identity_digest != after.identity_digest
            || self.signed_device_digest != after.signed_device_digest
            || self.ed25519 != after.ed25519
            || self.curve25519 != after.curve25519
            || self.ed25519 != local.ed25519
            || self.curve25519 != local.curve25519
        {
            return Err(CollectorRefused);
        }
        Ok(())
    }
}

pub async fn observe_server(
    request: &CollectorRequest,
) -> Result<ServerObservation, CollectorRefused> {
    tokio::time::timeout(Duration::from_secs(10), observe(request))
        .await
        .map_err(|_| CollectorRefused)?
}

async fn observe(request: &CollectorRequest) -> Result<ServerObservation, CollectorRefused> {
    let base = validated_base(&request.homeserver)?;
    let user: OwnedUserId = request.user_id.parse().map_err(|_| CollectorRefused)?;
    let device: OwnedDeviceId = request.device_id.as_str().into();
    if device.as_str().is_empty() {
        return Err(CollectorRefused);
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|_| CollectorRefused)?;
    let whoami_bytes = body(
        &client,
        authenticated(&base, &request.access_token, whoami::v3::Request::new())?,
    )
    .await?;
    let whoami = whoami::v3::Response::try_from_http_response(axum::http::Response::new(
        whoami_bytes.clone(),
    ))
    .map_err(|_| CollectorRefused)?;
    if whoami.user_id != user || whoami.device_id.as_ref() != Some(&device) || whoami.is_guest {
        return Err(CollectorRefused);
    }
    let mut query = get_keys::v3::Request::new();
    query.device_keys = BTreeMap::from([(user.clone(), vec![device.clone()])]);
    let keys_bytes = body(&client, authenticated(&base, &request.access_token, query)?).await?;
    let response = get_keys::v3::Response::try_from_http_response(axum::http::Response::new(
        keys_bytes.clone(),
    ))
    .map_err(|_| CollectorRefused)?;
    let keys = exact_device(response, &user, &device)?;
    let verified = DeviceData::try_from(&keys).map_err(|_| CollectorRefused)?;
    let signed = serde_json::to_vec(&keys).map_err(|_| CollectorRefused)?;
    Ok(ServerObservation {
        identity_digest: hash(
            &serde_json::to_vec(&(base.as_str(), &user, &device)).map_err(|_| CollectorRefused)?,
        ),
        whoami_digest: hash(&whoami_bytes),
        keys_query_digest: hash(&keys_bytes),
        signed_device_digest: hash(&signed),
        ed25519: verified.ed25519_key().ok_or(CollectorRefused)?.to_base64(),
        curve25519: verified
            .curve25519_key()
            .ok_or(CollectorRefused)?
            .to_base64(),
    })
}

fn exact_device(
    response: get_keys::v3::Response,
    user: &matrix_sdk::ruma::UserId,
    device: &matrix_sdk::ruma::DeviceId,
) -> Result<matrix_sdk::crypto::types::DeviceKeys, CollectorRefused> {
    if !response.failures.is_empty() || response.device_keys.len() != 1 {
        return Err(CollectorRefused);
    }
    let devices = response.device_keys.get(user).ok_or(CollectorRefused)?;
    if devices.len() != 1 {
        return Err(CollectorRefused);
    }
    let raw = devices.get(device).ok_or(CollectorRefused)?;
    let keys: matrix_sdk::crypto::types::DeviceKeys =
        serde_json::from_str(raw.json().get()).map_err(|_| CollectorRefused)?;
    if keys.user_id != user || keys.device_id != device {
        return Err(CollectorRefused);
    }
    Ok(keys)
}

fn validated_base(value: &str) -> Result<Url, CollectorRefused> {
    let base = Url::parse(value).map_err(|_| CollectorRefused)?;
    let loopback = base
        .host_str()
        .and_then(|host| host.parse::<std::net::IpAddr>().ok())
        .is_some_and(|address| address.is_loopback());
    if !(base.scheme() == "https" || base.scheme() == "http" && loopback)
        || base.host_str().is_none()
        || !base.username().is_empty()
        || base.password().is_some()
        || base.query().is_some()
        || base.fragment().is_some()
        || base.path() != "/"
    {
        return Err(CollectorRefused);
    }
    Ok(base)
}

fn authenticated(
    base: &Url,
    token: &str,
    request: impl OutgoingRequest,
) -> Result<reqwest::Request, CollectorRefused> {
    let wire = request
        .try_into_http_request::<Vec<u8>>(
            base.as_str(),
            SendAccessToken::Always(token),
            &[MatrixVersion::V1_11],
        )
        .map_err(|_| CollectorRefused)?;
    wire.try_into().map_err(|_| CollectorRefused)
}

async fn body(
    client: &reqwest::Client,
    request: reqwest::Request,
) -> Result<Vec<u8>, CollectorRefused> {
    let mut response = client
        .execute(request)
        .await
        .map_err(|_| CollectorRefused)?;
    if !response.status().is_success() {
        return Err(CollectorRefused);
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| CollectorRefused)? {
        if bytes.len() + chunk.len() > 65536 {
            return Err(CollectorRefused);
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
