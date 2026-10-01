//! The Tokens service: makes a token from the API key a call carries, by asking the keeper to
//! sign one. The gate sends the keeper the key's hash, not the key.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, UNIX_EPOCH};

use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use hive_proto::v1;
use hive_proto::v1::tokens_server::Tokens;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

/// The hash of the API key a call carries, which the gate puts on calls to Tokens.
#[derive(Clone, Copy, Debug)]
pub struct KeyHash(pub [u8; 32]);

/// The Tokens service, cheap to clone.
#[derive(Clone, Debug)]
pub struct Api {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    members: Vec<(String, KeeperClient<Channel>)>,
    at: AtomicUsize,
}

impl Api {
    /// The service, asking the keeper at `members`.
    ///
    /// # Errors
    ///
    /// There are no members, or an address does not parse.
    pub fn new(members: &[String]) -> Result<Self, String> {
        let members = crate::keys::clients(members)?;
        Ok(Self { inner: Arc::new(Inner { members, at: AtomicUsize::new(0) }) })
    }
}

#[tonic::async_trait]
impl Tokens for Api {
    async fn mint(
        &self,
        req: Request<v1::MintTokenRequest>,
    ) -> Result<Response<v1::Token>, Status> {
        let KeyHash(hash) = req
            .extensions()
            .get::<KeyHash>()
            .copied()
            .ok_or_else(|| Status::permission_denied("a token is made with an API key"))?;
        let req = req.into_inner();
        let ttl = match req.ttl {
            None => Duration::ZERO,
            Some(d) => Duration::try_from(d)
                .map_err(|_| Status::invalid_argument("the ttl is below zero"))?,
        };
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let ask = pb::MintTokenRequest {
            key_hash: hash.to_vec(),
            ttl_ms,
            cells: req.cell_ids,
            ops: req.ops,
        };
        let members = &self.inner.members;
        let start = self.inner.at.load(Ordering::Relaxed);
        let mut last = None;
        for i in 0..members.len() {
            let at = (start + i) % members.len();
            let mut client = members[at].1.clone();
            match client.mint_token(ask.clone()).await {
                Ok(r) => {
                    self.inner.at.store(at, Ordering::Relaxed);
                    let r = r.into_inner();
                    let at = UNIX_EPOCH + Duration::from_millis(r.expires_ms);
                    return Ok(Response::new(v1::Token {
                        token: r.token,
                        expires_at: Some(at.into()),
                    }));
                }
                Err(e) if !matches!(e.code(), Code::Unavailable | Code::DeadlineExceeded) => {
                    return Err(e);
                }
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| Status::unavailable("no keeper member")))
    }
}
