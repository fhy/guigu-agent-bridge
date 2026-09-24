//! A mock ACP backend, used by T014's tests as a **real child process**.
//!
//! It speaks the same codec the adapter does — one compact JSON object per line on
//! stdin and stdout — so the tests exercise the real process, the real pipes and the
//! real framing instead of an in-process double. It is a **test fixture**: nothing
//! in the bridge spawns it outside `tests/`, and it is not part of any assembly.
//!
//! The behaviour is chosen by the first argument, one scenario per invocation:
//!
//! | scenario | what it does |
//! |----------|--------------|
//! | `happy` (default) | initialise, create a session, answer a prompt with three text chunks and `end_turn` |
//! | `resume` | like `happy`, requiring `session/resume` for a known id |
//! | `refuse` | answers the prompt with `stopReason: refusal` |
//! | `max-tokens` | answers the prompt with `stopReason: max_tokens` |
//! | `unknown-stop` | answers the prompt with a stop reason the adapter does not model |
//! | `permission` | asks `session/request_permission` mid-turn and turns an allow into `end_turn`, a refusal into `refusal` |
//! | `coalesced` | writes a notification and the prompt response in a single write |
//! | `blank-lines` | writes blank lines between frames |
//! | `bad-protocol-version` | negotiates a protocol version the adapter does not speak |
//! | `need-auth` | advertises and accepts the API-key authentication method |
//! | `oversize-frame` | answers `session/new` with a line far over a lowered frame limit |
//! | `truncated-eof` | writes half a frame and exits |
//! | `exit-early` | exits before answering `initialize`, with code 7 |
//! | `exit-after-init` | answers `initialize`, then exits with code 3 |
//! | `hang` | never answers (the caller's deadline must fire) |
//! | `hang-prompt` | answers the handshake, then never finishes a prompt turn |
//! | `chunks-then-hang` | streams activity, then keeps the turn open |
//! | `hang-after-session-new` | accepts a session, then ignores stdin until killed |
//! | `noisy-stderr` | floods stderr while answering a prompt |
//! | `continue-once` | returns one structured continuation, then completes |

use std::io::{BufRead, Write};
use std::time::Duration;

use serde_json::{Value, json};

fn main() {
    let scenario = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "happy".to_owned());
    let marker = std::env::args().nth(2);

    if scenario == "exit-early" {
        std::process::exit(7);
    }
    if scenario == "hang" {
        // The caller's deadline is what must end the wait.
        std::thread::sleep(Duration::from_secs(3600));
        std::process::exit(0);
    }

    // One reader owns the stdin lock for the whole process: a second `lock()` in
    // the same thread would deadlock (the permission exchange below reads further
    // lines from this same reader).
    let stdin = std::io::stdin();
    let mut reader = std::io::BufReader::new(stdin.lock());
    let mut line = String::new();
    let mut session_counter = 0_u32;
    let mut prompt_counter = 0_u32;

    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => break,
            Ok(_) => {}
        }
        let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
            continue;
        };
        let method = message.get("method").and_then(Value::as_str).unwrap_or("");
        let id = message.get("id").cloned().unwrap_or(Value::Null);
        let params = message.get("params").cloned().unwrap_or(Value::Null);

        match method {
            "initialize" => {
                let version = if scenario == "bad-protocol-version" {
                    99
                } else {
                    1
                };
                let auth = match scenario.as_str() {
                    "need-auth" | "exit-after-auth" => {
                        json!([{"id": "api-key", "name": "API Key", "description": "Use an API key to authenticate"}])
                    }
                    "need-auth-agent" => {
                        json!([{"type": "agent", "id": "api-key", "name": "API Key"}])
                    }
                    "auth-terminal" => {
                        json!([{"type": "terminal", "id": "api-key", "name": "API Key"}])
                    }
                    "auth-unknown" => {
                        json!([{"type": "other", "id": "api-key", "name": "API Key"}])
                    }
                    "auth-mixed" => {
                        json!([{"id": "api-key", "name": "API Key"}, {"type": "other", "id": "other", "name": "Other"}])
                    }
                    "auth-duplicate" => {
                        json!([{"id": "api-key", "name": "API Key"}, {"type": "agent", "id": "api-key", "name": "API Key duplicate"}])
                    }
                    "auth-malformed-mixed" => {
                        json!([{"id": "api-key", "name": "API Key"}, {"type": 7, "id": "other", "name": "Other"}])
                    }
                    _ => json!([]),
                };
                respond(
                    &id,
                    json!({
                        "protocolVersion": version,
                        "agentInfo": {"name": "acp-mock-backend", "version": "0.1.0"},
                        "authMethods": auth,
                    }),
                );
                if scenario == "exit-after-init" {
                    std::process::exit(3);
                }
            }
            "authenticate" => {
                if matches!(
                    scenario.as_str(),
                    "need-auth" | "need-auth-agent" | "auth-mixed" | "exit-after-auth"
                ) && params.get("methodId").and_then(Value::as_str) == Some("api-key")
                {
                    respond(&id, json!({}));
                    if scenario == "exit-after-auth" {
                        std::process::exit(3);
                    }
                } else {
                    write_raw(&error_response(&id, -32602, "invalid params"));
                }
            }
            "session/new" => {
                if scenario == "oversize-frame" {
                    // Far over the test's lowered frame limit, then stop.
                    let huge = "x".repeat(4096);
                    write_raw(&format!("{{\"id\":{id},\"result\":\"{huge}\"}}\n"));
                    continue;
                }
                if scenario == "truncated-eof" {
                    write_raw("{\"id\":1,\"result\":{\"sessionId\":\"trunc");
                    std::process::exit(0);
                }
                session_counter += 1;
                respond(
                    &id,
                    json!({"sessionId": format!("session-{session_counter}")}),
                );
                if scenario == "exit-after-session" {
                    std::process::exit(3);
                }
                if scenario == "hang-after-session-new" {
                    std::thread::sleep(Duration::from_secs(3600));
                    std::process::exit(0);
                }
            }
            "session/resume" => {
                if scenario == "crash-prompt-once"
                    && let Some(path) = &marker
                {
                    let _ = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .and_then(|mut file| file.write_all(b"resume\n"));
                }
                // Echo the requested id: the adapter requires that.
                let session_id = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or("session-1");
                respond(&id, json!({"sessionId": session_id}));
            }
            "session/prompt" => {
                prompt_counter += 1;
                if scenario == "crash-prompt-once"
                    && let Some(path) = &marker
                {
                    let seen = std::fs::read_to_string(path)
                        .unwrap_or_default()
                        .contains("prompt\n");
                    let _ = std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(path)
                        .and_then(|mut file| file.write_all(b"prompt\n"));
                    if !seen {
                        std::process::exit(9);
                    }
                }
                if scenario == "hang-prompt" {
                    // Claims the session, then never finishes the turn.
                    std::thread::sleep(Duration::from_secs(3600));
                    std::process::exit(0);
                }
                let session_id = params
                    .get("sessionId")
                    .and_then(Value::as_str)
                    .unwrap_or("session-1");

                if scenario == "chunks-then-hang" {
                    let mut buffer = String::new();
                    for chunk in ["one", "two", "three", "four"] {
                        buffer.push_str(&notification(session_id, chunk));
                    }
                    write_raw(&buffer);
                    std::thread::sleep(Duration::from_secs(3600));
                    std::process::exit(0);
                }

                if scenario == "permission" {
                    let allowed = request_permission(&mut reader, session_id);
                    respond(
                        &id,
                        json!({"stopReason": if allowed { "end_turn" } else { "refusal" }, "taskResult": if allowed { json!({"kind":"completed","output":"permission complete"}) } else { json!({"kind":"failed","reason":"agent stopped: refusal"}) }}),
                    );
                    continue;
                }

                if scenario == "noisy-stderr" {
                    let noise = "e".repeat(64 * 1024);
                    for _ in 0..16 {
                        let _ = std::io::stderr().write_all(noise.as_bytes());
                    }
                }

                let chunks: &[&str] = match scenario.as_str() {
                    "refuse" | "max-tokens" | "unknown-stop" => &["partial"],
                    _ => &["Hello", ", ", "world"],
                };
                let stop_reason = match scenario.as_str() {
                    "refuse" => "refusal",
                    "max-tokens" => "max_tokens",
                    "unknown-stop" => "some_future_reason",
                    _ => "end_turn",
                };

                // `coalesced` writes everything in one write, `blank-lines` inserts
                // empty lines: both exercise the codec over a real pipe.
                let mut buffer = String::new();
                for chunk in chunks {
                    if scenario == "blank-lines" {
                        buffer.push('\n');
                    }
                    buffer.push_str(&notification(session_id, chunk));
                }
                let task_kind = match scenario.as_str() {
                    "refuse" | "max-tokens" | "unknown-stop" => "failed",
                    _ => "completed",
                };
                let task_reason = match scenario.as_str() {
                    "refuse" | "permission" => "agent stopped: refusal",
                    "unknown-stop" => "agent stopped: some_future_reason",
                    _ => "",
                };
                let result = if scenario == "continue-once" && prompt_counter == 1 {
                    json!({"stopReason":"end_turn","taskResult":{"kind":"continue","reason":"more work","nextPrompt":"finish"}})
                } else if scenario.starts_with("legacy-end-turn") {
                    json!({"stopReason": "end_turn"})
                } else {
                    json!({"stopReason": stop_reason, "taskResult": {"kind": task_kind, "output": chunks.concat(), "reason": task_reason}})
                };
                buffer.push_str(&response(&id, result));
                write_raw(&buffer);
            }
            _ => {
                // Unknown methods (including `session/cancel`) are answered with a
                // JSON-RPC method-not-found so nothing ever hangs silently.
                write_raw(&error_response(&id, -32601, "method not found"));
            }
        }
    }
}

/// Send a request to the client and report whether it was allowed.
fn request_permission(reader: &mut impl BufRead, session_id: &str) -> bool {
    let request = json!({
        "jsonrpc": "2.0",
        "id": 9001,
        "method": "session/request_permission",
        "params": {
            "sessionId": session_id,
            "toolCall": {"title": "run a command"},
            "options": [
                {"optionId": "allow", "kind": "allow_once", "name": "Allow"},
                {"optionId": "reject", "kind": "reject_once", "name": "Reject"}
            ]
        }
    });
    write_raw(&format!("{request}\n"));

    // Read until the response for 9001 arrives (or stdin ends).
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            Ok(0) | Err(_) => return false,
            Ok(_) => {}
        }
        let Ok(message) = serde_json::from_str::<Value>(line.trim()) else {
            continue;
        };
        if message.get("id") != Some(&json!(9001)) {
            continue;
        }
        let result = message.get("result").cloned().unwrap_or(Value::Null);
        let outcome = result.get("outcome").and_then(Value::as_str).unwrap_or("");
        let option = result.get("optionId").and_then(Value::as_str).unwrap_or("");
        // Allowed only if the client explicitly selected the allow option.
        return outcome == "selected" && option == "allow";
    }
}

fn notification(session_id: &str, text: &str) -> String {
    format!(
        "{{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":\
         {{\"sessionId\":{},\"update\":{{\"sessionUpdate\":\"agent_message_chunk\",\
         \"content\":{{\"type\":\"text\",\"text\":{}}}}}}}}}\n",
        json!(session_id),
        json!(text)
    )
}

fn response(id: &Value, result: Value) -> String {
    format!(
        "{}\n",
        json!({"jsonrpc": "2.0", "id": id, "result": result})
    )
}

fn respond(id: &Value, result: Value) {
    write_raw(&response(id, result));
}

fn error_response(id: &Value, code: i64, message: &str) -> String {
    format!(
        "{}\n",
        json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
    )
}

/// Write bytes and flush, so the parent sees each frame immediately.
fn write_raw(text: &str) {
    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();
    let _ = stdout.write_all(text.as_bytes());
    let _ = stdout.flush();
}
