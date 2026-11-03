//! A small asynchronous RESP2 client, built on the same parser as the server.
//!
//! Used by the integration tests and by the `micro-redis-cli` binary. It
//! talks to real Redis too, which is how its correctness was cross-checked.

use crate::resp::{self, Frame, ProtocolError};
use bytes::{Buf, Bytes, BytesMut};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("connection closed by server")]
    Closed,
    #[error("server error: {0}")]
    Server(String),
    #[error("unexpected reply: {0:?}")]
    Unexpected(Frame),
}

pub type Result<T> = std::result::Result<T, ClientError>;

pub struct Client {
    stream: TcpStream,
    buf: BytesMut,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Client> {
        Client::from_stream(TcpStream::connect(addr).await?)
    }

    /// Wrap a stream that is already connected.
    pub fn from_stream(stream: TcpStream) -> Result<Client> {
        stream.set_nodelay(true)?;
        Ok(Client {
            stream,
            buf: BytesMut::with_capacity(4096),
        })
    }

    /// Send a command and wait for its reply. Error replies are returned as
    /// `Ok(Frame::Error(..))` so callers can inspect them.
    pub async fn cmd<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Result<Frame> {
        self.send(args).await?;
        self.read_reply().await
    }

    /// Send a command without waiting for the reply (used for pipelining).
    pub async fn send<A: AsRef<[u8]>>(&mut self, args: &[A]) -> Result<()> {
        let mut out = BytesMut::new();
        resp::encode_command(args, &mut out);
        self.stream.write_all(&out).await?;
        Ok(())
    }

    /// Write raw bytes (used by protocol tests).
    pub async fn send_raw(&mut self, bytes: &[u8]) -> Result<()> {
        self.stream.write_all(bytes).await?;
        Ok(())
    }

    /// Read exactly one reply frame.
    pub async fn read_reply(&mut self) -> Result<Frame> {
        loop {
            if let Some((frame, n)) = resp::parse_frame(&self.buf)? {
                self.buf.advance(n);
                return Ok(frame);
            }
            if self.stream.read_buf(&mut self.buf).await? == 0 {
                return Err(ClientError::Closed);
            }
        }
    }

    /// Like [`read_reply`](Self::read_reply) but gives up after `d`.
    pub async fn read_reply_timeout(&mut self, d: Duration) -> Result<Option<Frame>> {
        match tokio::time::timeout(d, self.read_reply()).await {
            Ok(r) => r.map(Some),
            Err(_) => Ok(None),
        }
    }

    /// Bytes the server sent that were not yet consumed as frames.
    pub fn pending_bytes(&self) -> usize {
        self.buf.len()
    }

    // ---- typed helpers ---------------------------------------------------

    pub async fn ping(&mut self) -> Result<String> {
        match self.cmd(&["PING"]).await? {
            Frame::Simple(s) => Ok(s),
            other => Err(unexpected(other)),
        }
    }

    pub async fn select(&mut self, db: usize) -> Result<()> {
        expect_ok(self.cmd(&["SELECT".to_string(), db.to_string()]).await?)
    }

    pub async fn set(&mut self, key: impl AsRef<[u8]>, val: impl AsRef<[u8]>) -> Result<()> {
        expect_ok(
            self.cmd(&[b"SET".as_ref(), key.as_ref(), val.as_ref()])
                .await?,
        )
    }

    pub async fn get(&mut self, key: impl AsRef<[u8]>) -> Result<Option<Bytes>> {
        match self.cmd(&[b"GET".as_ref(), key.as_ref()]).await? {
            Frame::Bulk(b) => Ok(Some(b)),
            Frame::Null => Ok(None),
            other => Err(unexpected(other)),
        }
    }

    pub async fn exists<K: AsRef<[u8]>>(&mut self, keys: &[K]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![b"EXISTS"];
        args.extend(keys.iter().map(|k| k.as_ref()));
        expect_int(self.cmd(&args).await?)
    }

    pub async fn lpush<V: AsRef<[u8]>>(
        &mut self,
        key: impl AsRef<[u8]>,
        vals: &[V],
    ) -> Result<i64> {
        self.push("LPUSH", key.as_ref(), vals).await
    }

    pub async fn rpush<V: AsRef<[u8]>>(
        &mut self,
        key: impl AsRef<[u8]>,
        vals: &[V],
    ) -> Result<i64> {
        self.push("RPUSH", key.as_ref(), vals).await
    }

    async fn push<V: AsRef<[u8]>>(&mut self, cmd: &str, key: &[u8], vals: &[V]) -> Result<i64> {
        let mut args: Vec<&[u8]> = vec![cmd.as_bytes(), key];
        args.extend(vals.iter().map(|v| v.as_ref()));
        expect_int(self.cmd(&args).await?)
    }

    /// `BLPOP keys... timeout` → `Some((key, value))`, or `None` on timeout.
    pub async fn blpop<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes)>> {
        self.bpop("BLPOP", keys, timeout).await
    }

    pub async fn brpop<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes)>> {
        self.bpop("BRPOP", keys, timeout).await
    }

    async fn bpop<K: AsRef<[u8]>>(
        &mut self,
        cmd: &str,
        keys: &[K],
        timeout: f64,
    ) -> Result<Option<(Bytes, Bytes)>> {
        let t = timeout.to_string();
        let mut args: Vec<&[u8]> = vec![cmd.as_bytes()];
        args.extend(keys.iter().map(|k| k.as_ref()));
        args.push(t.as_bytes());
        match self.cmd(&args).await? {
            Frame::NullArray => Ok(None),
            Frame::Array(items) => match <[Frame; 2]>::try_from(items) {
                Ok([Frame::Bulk(k), Frame::Bulk(v)]) => Ok(Some((k, v))),
                Ok(pair) => Err(unexpected(Frame::Array(pair.into()))),
                Err(items) => Err(unexpected(Frame::Array(items))),
            },
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(f: Frame) -> ClientError {
    match f {
        Frame::Error(e) => ClientError::Server(e),
        other => ClientError::Unexpected(other),
    }
}

fn expect_ok(f: Frame) -> Result<()> {
    match f {
        Frame::Simple(s) if s == "OK" => Ok(()),
        other => Err(unexpected(other)),
    }
}

fn expect_int(f: Frame) -> Result<i64> {
    match f {
        Frame::Integer(n) => Ok(n),
        other => Err(unexpected(other)),
    }
}
