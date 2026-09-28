//! The `/metrics` endpoint.

use crate::Registry;
use std::io;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Answers `GET /metrics` on `listener` with `registry` in the text format, until accepting
/// fails. This is a scrape endpoint and nothing more: one request per connection, no keep alive,
/// and anything else gets a 404.
///
/// # Errors
///
/// When accepting a connection fails.
pub async fn serve(listener: TcpListener, registry: Registry) -> io::Result<()> {
    loop {
        let (mut conn, _) = listener.accept().await?;
        let registry = registry.clone();
        tokio::spawn(async move {
            let _ = tokio::time::timeout(Duration::from_secs(10), async {
                let mut buf = Vec::with_capacity(1024);
                let mut chunk = [0u8; 1024];
                while !buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() < 8192 {
                    let n = conn.read(&mut chunk).await?;
                    if n == 0 {
                        return Ok(());
                    }
                    buf.extend_from_slice(&chunk[..n]);
                }
                let (status, kind, body) = if buf.starts_with(b"GET /metrics ") || buf.starts_with(b"GET /metrics?") {
                    ("200 OK", "text/plain; version=0.0.4; charset=utf-8", registry.render())
                } else {
                    ("404 Not Found", "text/plain; charset=utf-8", "not found\n".to_string())
                };
                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                conn.write_all(head.as_bytes()).await?;
                conn.write_all(body.as_bytes()).await?;
                conn.shutdown().await
            })
            .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(addr: std::net::SocketAddr, path: &str) -> String {
        let mut c = tokio::net::TcpStream::connect(addr).await.unwrap();
        c.write_all(format!("GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").as_bytes()).await.unwrap();
        let mut out = String::new();
        c.read_to_string(&mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn scrapes_get_the_registry_and_nothing_else_is_found() {
        let r = Registry::new();
        r.counter("hive_scrape_test_total", "Test.", &[]).with(&[]).add(5);
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(serve(l, r));
        let ok = get(addr, "/metrics").await;
        assert!(ok.starts_with("HTTP/1.1 200 OK\r\n"));
        assert!(ok.ends_with("hive_scrape_test_total 5\n"));
        assert!(get(addr, "/").await.starts_with("HTTP/1.1 404"));
    }
}
