//! JSONL byte framing: the ACP v1 stdio codec.
//!
//! This is the bottom layer of the ACP adapter: it turns a byte stream into
//! complete JSON values and back, and it knows nothing about JSON-RPC or ACP.
//! Everything above it (message correlation, schema mapping, session logic) stays
//! independent of the byte format, which is what ADR-001 requires: a future
//! backend that needs different framing replaces this layer alone.
//!
//! # The codec
//!
//! JSONL is newline-delimited JSON: one compact JSON value per line, UTF-8
//! encoded, `\n`-terminated. The rules below match the reference codec
//! (`ndJsonStream` in the pinned `@agentclientprotocol/sdk`) and are pinned by
//! tests, because compatibility with real backends depends on them:
//!
//! - a frame ends at the first `\n`; a read may deliver a fraction of a frame or
//!   several frames at once — the remainder stays in the reader,
//! - the line is `trim()`ed before parsing, so `\r\n` line endings and stray
//!   spaces are accepted,
//! - a blank or whitespace-only line is **skipped** (the reference codec does the
//!   same), so a keepalive newline is not an error,
//! - writes are compact JSON plus a single `\n`.
//!
//! # Why the reader is a state machine and not an `async fn`
//!
//! [`FrameReader`] accumulates bytes with [`FrameReader::push`] and yields frames
//! with [`FrameReader::next_frame`]; it is synchronous, and the partial line lives
//! in the reader rather than inside a future. That matters: the transport reads
//! its child's stdout inside a `tokio::select!`, and `select!` **drops** the
//! futures of the branches that did not complete. An `async fn` that had already
//! consumed bytes from the pipe would take them with it and desynchronise the
//! stream. Keeping the buffer in the reader makes a cancelled poll harmless — the
//! bytes are still there for the next one.
//!
//! # Bounds
//!
//! A line is refused as soon as it exceeds the limit ([`MAX_FRAME_BYTES`] by
//! default), before the oversized frame is buffered in full, so a broken or
//! hostile backend cannot grow memory here. [`FrameReader::finish`] distinguishes a
//! clean end of stream from EOF in the middle of a frame.
//!
//! # One deliberate divergence from the reference codec
//!
//! The reference codec logs a malformed line and drops it, then keeps reading.
//! This layer reports it as [`FrameError::Malformed`] and the transport fails:
//! a dropped line is either a lost response (which would surface as a misleading
//! timeout) or lost output, and ADR-001 requires framing failures to be reported
//! as their own class. See the T014 analysis §4.2 and decision Q2 = A.

use serde_json::Value;
use thiserror::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::acp::bounded;

/// The default ceiling on one JSONL frame.
///
/// ACP messages are small; 1 MiB is far above any legitimate frame while still
/// bounding what a broken backend can make us buffer.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Why a frame could not be read or written.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum FrameError {
    /// The line exceeded the limit.
    #[error("frame exceeds {limit} bytes")]
    TooLarge {
        /// The limit that was exceeded.
        limit: usize,
    },
    /// The line was not valid UTF-8.
    #[error("frame is not valid UTF-8")]
    NotUtf8,
    /// The stream ended in the middle of a frame.
    #[error("stream ended inside a frame")]
    Truncated,
    /// The line was not valid JSON.
    #[error("frame is not valid JSON: {detail}")]
    Malformed {
        /// Reason the value could not be parsed, bounded and without the frame.
        detail: String,
    },
    /// The underlying pipe failed.
    #[error("transport i/o failed: {0}")]
    Io(#[source] std::io::Error),
}

/// Accumulates bytes and yields complete JSONL frames.
///
/// See the module docs for why this is a state machine instead of an `async fn`.
#[derive(Debug, Clone)]
pub struct FrameReader {
    partial: Vec<u8>,
    max_frame_bytes: usize,
}

impl FrameReader {
    /// A reader with the given per-frame limit.
    pub fn new(max_frame_bytes: usize) -> Self {
        Self {
            partial: Vec::new(),
            max_frame_bytes,
        }
    }

    /// The limit this reader enforces.
    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// Append bytes as they arrive from the stream.
    ///
    /// A chunk may hold several frames or half of one; neither is an error here.
    pub fn push(&mut self, bytes: &[u8]) {
        self.partial.extend_from_slice(bytes);
    }

    /// Take the next complete frame, skipping blank lines.
    ///
    /// Returns `Ok(None)` while the current line is still incomplete.
    ///
    /// # Errors
    ///
    /// [`FrameError::TooLarge`], [`FrameError::NotUtf8`] or
    /// [`FrameError::Malformed`] for a complete but unusable line.
    pub fn next_frame(&mut self) -> Result<Option<Value>, FrameError> {
        loop {
            let Some(newline) = self.partial.iter().position(|byte| *byte == b'\n') else {
                if self.partial.len() > self.max_frame_bytes {
                    return Err(FrameError::TooLarge {
                        limit: self.max_frame_bytes,
                    });
                }
                return Ok(None);
            };
            if newline > self.max_frame_bytes {
                return Err(FrameError::TooLarge {
                    limit: self.max_frame_bytes,
                });
            }
            let line: Vec<u8> = self.partial.drain(..=newline).collect();
            let text = std::str::from_utf8(&line[..newline]).map_err(|_| FrameError::NotUtf8)?;
            let trimmed = text.trim();
            if trimmed.is_empty() {
                continue;
            }
            return serde_json::from_str(trimmed).map(Some).map_err(|error| {
                FrameError::Malformed {
                    detail: bounded(&error.to_string()),
                }
            });
        }
    }

    /// Check that the stream ended between frames.
    ///
    /// # Errors
    ///
    /// [`FrameError::Truncated`] when a partial frame is still buffered.
    pub fn finish(&self) -> Result<(), FrameError> {
        if self.partial.iter().all(|byte| byte.is_ascii_whitespace()) {
            Ok(())
        } else {
            Err(FrameError::Truncated)
        }
    }
}

/// Write one frame as compact JSON followed by a single newline.
///
/// # Errors
///
/// [`FrameError::Io`] when the pipe fails; serialisation cannot fail for a
/// `serde_json::Value`.
pub async fn write_frame<W>(writer: &mut W, value: &Value) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    let mut bytes = serde_json::to_vec(value).map_err(|error| FrameError::Malformed {
        detail: bounded(&error.to_string()),
    })?;
    bytes.push(b'\n');
    writer.write_all(&bytes).await.map_err(FrameError::Io)?;
    writer.flush().await.map_err(FrameError::Io)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Read every frame the reader can produce from `chunks`, fed in order.
    fn read_all(chunks: &[&[u8]], limit: usize) -> Result<Vec<Value>, FrameError> {
        let mut reader = FrameReader::new(limit);
        let mut frames = Vec::new();
        for chunk in chunks {
            reader.push(chunk);
            while let Some(frame) = reader.next_frame()? {
                frames.push(frame);
            }
        }
        reader.finish()?;
        Ok(frames)
    }

    #[test]
    fn a_frame_split_across_reads_is_reassembled() {
        let frames = read_all(
            &[b"{\"id\":1,", b"\"method\":", b"\"initialize\"}", b"\n"],
            MAX_FRAME_BYTES,
        )
        .expect("frames");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0]["method"], "initialize");
    }

    #[test]
    fn several_frames_in_one_read_are_all_yielded() {
        let frames =
            read_all(&[b"{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n"], MAX_FRAME_BYTES).expect("frames");
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame["id"].clone())
                .collect::<Vec<_>>(),
            vec![json!(1), json!(2), json!(3)]
        );
    }

    #[test]
    fn crlf_whitespace_and_blank_lines_are_tolerated() {
        let frames = read_all(
            &[b" \r\n", b"{\"id\":1}\r\n", b"\n\t\n", b"{\"id\":2}\n"],
            MAX_FRAME_BYTES,
        )
        .expect("frames");
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0]["id"], 1);
        assert_eq!(frames[1]["id"], 2);
    }

    #[test]
    fn an_empty_stream_is_a_clean_end() {
        assert!(read_all(&[], MAX_FRAME_BYTES).expect("clean").is_empty());
        assert!(
            read_all(&[b"\n\n"], MAX_FRAME_BYTES)
                .expect("clean")
                .is_empty()
        );
    }

    #[test]
    fn a_frame_truncated_by_eof_is_reported() {
        assert!(matches!(
            read_all(&[b"{\"id\":1}"], MAX_FRAME_BYTES),
            Err(FrameError::Truncated)
        ));
    }

    #[test]
    fn an_oversized_frame_is_refused_before_it_is_buffered_in_full() {
        let mut reader = FrameReader::new(64);
        // No newline yet: the limit is checked as the buffer grows.
        reader.push(&[b'x'; 65]);
        assert!(matches!(
            reader.next_frame(),
            Err(FrameError::TooLarge { limit: 64 })
        ));

        // A single complete oversized line is refused too.
        let mut reader = FrameReader::new(64);
        let mut line = vec![b'y'; 65];
        line.push(b'\n');
        reader.push(&line);
        assert!(matches!(
            reader.next_frame(),
            Err(FrameError::TooLarge { limit: 64 })
        ));
    }

    #[test]
    fn invalid_utf8_and_malformed_json_are_classified() {
        let mut reader = FrameReader::new(MAX_FRAME_BYTES);
        reader.push(&[0xff, 0xfe, b'\n']);
        assert!(matches!(reader.next_frame(), Err(FrameError::NotUtf8)));

        let mut reader = FrameReader::new(MAX_FRAME_BYTES);
        reader.push(b"not json\n");
        match reader.next_frame() {
            Err(FrameError::Malformed { detail }) => {
                assert!(!detail.contains("not json"), "the frame must not be echoed");
            }
            other => panic!("expected a malformed frame, got {other:?}"),
        }
    }

    #[test]
    fn a_frame_arriving_after_a_cancelled_poll_is_not_lost() {
        // The transport pushes bytes and calls `next_frame` repeatedly; an
        // incomplete line simply yields `None` and stays buffered. This is the
        // cancel-safety property the transport depends on.
        let mut reader = FrameReader::new(MAX_FRAME_BYTES);
        reader.push(b"{\"id\":");
        assert!(reader.next_frame().expect("incomplete").is_none());
        reader.push(b"7}");
        assert!(reader.next_frame().expect("still incomplete").is_none());
        reader.push(b"\n");
        let frame = reader.next_frame().expect("frame").expect("one frame");
        assert_eq!(frame["id"], 7);
    }

    #[tokio::test]
    async fn a_written_frame_is_compact_and_newline_terminated() {
        let mut out: Vec<u8> = Vec::new();
        write_frame(&mut out, &json!({"a": 1, "b": [2]}))
            .await
            .expect("write");
        assert_eq!(
            String::from_utf8(out).expect("utf8"),
            "{\"a\":1,\"b\":[2]}\n"
        );
    }
}
