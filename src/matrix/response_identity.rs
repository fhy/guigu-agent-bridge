use serde_json::Value;
use sha2::{Digest, Sha256};

use super::MatrixError;

pub(crate) fn canonical(raw: &str) -> Result<Value, MatrixError> {
    let mut value: Value = serde_json::from_str(raw).map_err(|_| MatrixError::Protocol {
        detail: "malformed timeline event",
    })?;
    if let Some(object) = value.as_object_mut() {
        object.remove("unsigned");
    }
    Ok(value)
}

pub(crate) fn digest(
    token: Option<&str>,
    next: &str,
    mut payloads: Vec<(String, Value)>,
) -> Result<String, MatrixError> {
    payloads.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.to_string().cmp(&right.1.to_string()))
    });
    let bytes = serde_json::to_vec(&(token, next, payloads)).map_err(|_| MatrixError::Storage)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}
