//! Turn an HTTP response body into a stream of text lines. Works for both
//! newline-delimited JSON (Ollama) and server-sent events (OpenAI).

use std::time::Duration;

use futures::{Stream, StreamExt};

/// How long a model stream may stay silent before the step fails (S-21):
/// a hung model call must not hold the session busy forever. Generous —
/// large local models think between chunks.
pub(crate) const STREAM_IDLE_TIMEOUT: Duration = Duration::from_secs(120);

/// A buffered line larger than this is a protocol error, not something to
/// keep buffering (S-22): a stream that never terminates a line must not
/// grow memory without limit.
pub(crate) const MAX_LINE_BYTES: usize = 1024 * 1024;

/// Yields one line per item, without the trailing `\n` or `\r\n`. Partial
/// lines across network chunks are buffered until their terminator arrives;
/// a line exceeding [`MAX_LINE_BYTES`] without one fails the stream (S-22).
pub fn lines(resp: reqwest::Response) -> impl Stream<Item = anyhow::Result<String>> + Send {
    let body = Box::pin(resp.bytes_stream());
    futures::stream::unfold(
        (body, Vec::<u8>::new(), false),
        |(mut body, mut buf, mut eof)| async move {
            loop {
                if buf.len() > MAX_LINE_BYTES {
                    return Some((
                        Err(anyhow::anyhow!(
                            "stream line exceeds {} MiB without a newline",
                            MAX_LINE_BYTES / (1024 * 1024)
                        )),
                        (body, buf, eof),
                    ));
                }
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a reqwest::Response over raw bytes (the same shape the
    /// providers hand to `lines`).
    fn response_of(chunks: Vec<Vec<u8>>) -> reqwest::Response {
        let stream = futures::stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<_, std::io::Error>(bytes::Bytes::from(c))),
        );
        reqwest::Response::from(
            http::Response::builder()
                .body(reqwest::Body::wrap_stream(stream))
                .unwrap(),
        )
    }

    /// S-22: a line without a newline fails at the cap instead of buffering
    /// without limit.
    #[tokio::test]
    async fn unterminated_line_fails_at_the_cap() {
        let big = vec![b'x'; 2 * MAX_LINE_BYTES];
        let stream = lines(response_of(vec![big]));
        tokio::pin!(stream);
        let item = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("bounded")
            .expect("an item")
            .expect_err("an unterminated oversized line must fail the stream");
        assert!(item.to_string().contains("without a newline"), "{item}");
    }

    #[tokio::test]
    async fn terminated_lines_flow_normally() {
        let stream = lines(response_of(vec![
            b"line one\n".to_vec(),
            b"line two\n".to_vec(),
        ]));
        tokio::pin!(stream);
        assert_eq!(stream.next().await.unwrap().unwrap(), "line one");
        assert_eq!(stream.next().await.unwrap().unwrap(), "line two");
        assert!(stream.next().await.is_none());
    }
}
