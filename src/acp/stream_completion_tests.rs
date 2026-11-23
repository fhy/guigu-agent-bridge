use super::classify_stream_completion;
use crate::acp::{AcpError, CompletedRequest, TurnResult};
use serde_json::json;

fn missing_result() -> CompletedRequest {
    CompletedRequest {
        result: json!({"stopReason": "end_turn"}),
        streamed_text: "untrusted partial output".into(),
    }
}

#[test]
fn strict_completed_response_has_a_typed_missing_result_failure() {
    assert!(matches!(
        classify_stream_completion(missing_result(), false, false),
        Err(AcpError::MissingTaskResult)
    ));
}

#[test]
fn explicit_legacy_and_nonterminal_policies_remain_distinct() {
    assert_eq!(
        classify_stream_completion(missing_result(), true, false).unwrap(),
        TurnResult::Completed {
            output: "untrusted partial output".into()
        }
    );
    assert!(matches!(
        classify_stream_completion(missing_result(), false, true).unwrap(),
        TurnResult::Continue { .. }
    ));
}

#[test]
fn malformed_completion_does_not_become_a_known_missing_result_failure() {
    let mut completed = missing_result();
    completed.result = json!({"stopReason": false});
    assert!(matches!(
        classify_stream_completion(completed, false, false),
        Err(AcpError::Schema { .. })
    ));
}
