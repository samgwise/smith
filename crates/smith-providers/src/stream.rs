//! Turn an HTTP response body into a stream of text lines. Works for both
//! newline-delimited JSON (Ollama) and server-sent events (OpenAI).

use std::time::Duration;

use futures::{Stream, StreamExt};

/// How long a model stream may stay silent before the step fails (S-21):
/// a hung model call must not hold the session busy forever. Generous —
/// large local models think between chunks.
pub(crate) const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// Yields one line per item, without the trailing `\n` or `\r\n`. Partial
/// lines across network chunks are buffered until their terminator arrives.
pub fn lines(resp: reqwest::Response) -> impl Stream<Item = anyhow::Result<String>> + Send {
    let body = Box::pin(resp.bytes_stream());
    futures::stream::unfold(
        (body, Vec::<u8>::new(), false),
        |(mut body, mut buf, mut eof)| async move {
            loop {
                if let Some(pos) = buf.iter().position(|&b| b == b'\n') {
                    let raw: Vec<u8> = buf.drain(..=pos).collect();
                    let line = decode(&raw);
                    return Some((Ok(line), (body, buf, eof)));
                }
                if eof {
                    if buf.is_empty() {
                        return None;
                    }
                    let raw = std::mem::take(&mut buf);
                    return Some((Ok(decode(&raw)), (body, buf, eof)));
                }
                match body.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        eof = true;
                        return Some((Err(anyhow::Error::new(e)), (body, buf, eof)));
                    }
                    None => eof = true,
                }
            }
        },
    )
}

fn decode(raw: &[u8]) -> String {
    String::from_utf8_lossy(raw)
        .trim_end_matches(['\r', '\n'])
        .to_string()
}
