//! Structured ACP turn results (INC-003).

use serde::Deserialize;

use crate::acp::bounded;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TurnResult {
    Continue {
        reason: String,
        next_prompt: Option<String>,
    },
    Completed {
        output: String,
    },
    Blocked {
        reason: String,
    },
    Failed {
        reason: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WireTurnResult {
    pub kind: String,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub output: Option<String>,
    #[serde(default)]
    pub next_prompt: Option<String>,
}

impl WireTurnResult {
    pub fn into_result(self, streamed: String) -> Result<TurnResult, String> {
        let reason = bounded(self.reason.as_deref().unwrap_or(""));
        Ok(match self.kind.as_str() {
            "continue" => TurnResult::Continue {
                reason,
                next_prompt: self.next_prompt.map(|v| bounded(&v)),
            },
            "completed" => TurnResult::Completed {
                output: bounded(self.output.as_deref().unwrap_or(&streamed)),
            },
            "blocked" => TurnResult::Blocked { reason },
            "failed" => TurnResult::Failed { reason },
            _ => return Err("unknown structured turn result kind".to_owned()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_explicit_kinds_are_accepted() {
        let parsed: WireTurnResult =
            serde_json::from_str(r#"{"kind":"completed","output":"ok"}"#).unwrap();
        assert_eq!(
            parsed.into_result(String::new()),
            Ok(TurnResult::Completed {
                output: "ok".into()
            })
        );
        let parsed: WireTurnResult = serde_json::from_str(r#"{"kind":"wat"}"#).unwrap();
        assert!(parsed.into_result(String::new()).is_err());
    }
}
