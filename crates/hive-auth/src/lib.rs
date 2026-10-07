//! Biscuit tokens: the keeper signs them, holders narrow them down offline, and the gate checks
//! them with the keeper's public key without asking anyone.
//!
//! A token's first block names its project and the API key it came from, and says when it runs
//! out. A holder can add blocks that let it do less: only some cells, only some calls, or a
//! sooner end. A block can never let a token do more. The design is in
//! `spec/04_control_plane.md`, section 2, and `spec/10_security.md`.

#![forbid(unsafe_code)]

use std::collections::{BTreeSet, HashMap};
use std::time::{Duration, SystemTime};

use biscuit_auth::builder::{Algorithm, Term};
use biscuit_auth::{AuthorizerLimits, Biscuit, BlockBuilder, KeyPair, PrivateKey, PublicKey};

/// The longest a token from the keeper lasts.
pub const MAX_TTL: Duration = Duration::from_secs(3600);

/// The calls a token can be held to, by the name a check uses. A call on one cell is checked
/// with the cell's id too.
pub const OPS: &[&str] = &[
    "create",
    "get",
    "list",
    "watch",
    "pause",
    "resume",
    "stop",
    "quarantine",
    "extend_ttl",
    "update_policy",
    "expose_port",
    "exec",
    "files",
    "verify",
    "llm",
];

/// Why a token was not taken.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denied(pub String);

impl std::fmt::Display for Denied {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Denied {}

fn denied(e: impl std::fmt::Display) -> Denied {
    Denied(e.to_string())
}

/// What a holder narrows a token down to. Empty lists and `None` leave that part as it was.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Narrow {
    /// Only these cells, by id.
    pub cells: Vec<String>,
    /// Only these calls, from [`OPS`].
    pub ops: Vec<String>,
    /// No later than this.
    pub until: Option<SystemTime>,
}

impl Narrow {
    fn is_empty(&self) -> bool {
        self.cells.is_empty() && self.ops.is_empty() && self.until.is_none()
    }

    fn block(&self) -> Result<BlockBuilder, Denied> {
        if let Some(op) = self.ops.iter().find(|o| !OPS.contains(&o.as_str())) {
            return Err(Denied(format!("{op:?} is not a call a token can be held to")));
        }
        let mut b = BlockBuilder::new();
        if let Some(t) = self.until {
            let params = HashMap::from([("until".to_string(), Term::from(t))]);
            b = b
                .code_with_params("check if time($t), $t <= {until};", params, HashMap::new())
                .map_err(denied)?;
        }
        if !self.ops.is_empty() {
            let set: BTreeSet<Term> = self.ops.iter().map(|o| Term::from(o.as_str())).collect();
            let params = HashMap::from([("ops".to_string(), Term::Set(set))]);
            b = b
                .code_with_params("check if op($o), {ops}.contains($o);", params, HashMap::new())
                .map_err(denied)?;
        }
        if !self.cells.is_empty() {
            let set: BTreeSet<Term> = self.cells.iter().map(|c| Term::from(c.as_str())).collect();
            let params = HashMap::from([("cells".to_string(), Term::Set(set))]);
            b = b
                .code_with_params(
                    "check if cell($c), {cells}.contains($c);",
                    params,
                    HashMap::new(),
                )
                .map_err(denied)?;
        }
        Ok(b)
    }
}

/// The keeper's signing key.
pub struct Issuer {
    root: KeyPair,
}

impl std::fmt::Debug for Issuer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Issuer").field("public", &self.public()).finish_non_exhaustive()
    }
}

impl Issuer {
    /// The issuer for the Ed25519 private key `private`.
    ///
    /// # Errors
    ///
    /// The bytes are not a private key.
    pub fn new(private: &[u8; 32]) -> Result<Self, Denied> {
        let key = PrivateKey::from_bytes(private, Algorithm::Ed25519).map_err(denied)?;
        Ok(Self { root: KeyPair::from(&key) })
    }

    /// The public key the gates check tokens with.
    #[must_use]
    pub fn public(&self) -> Vec<u8> {
        self.root.public().to_bytes()
    }

    /// A token for `project` from the key with hash `key`, good until `until` and narrowed down
    /// by `narrow`, as base64.
    ///
    /// # Errors
    ///
    /// `narrow` names a call that is not in [`OPS`], or signing failed.
    pub fn mint(
        &self,
        project: &str,
        key: &[u8; 32],
        until: SystemTime,
        narrow: &Narrow,
    ) -> Result<String, Denied> {
        let params = HashMap::from([
            ("project".to_string(), Term::from(project)),
            ("key".to_string(), Term::Bytes(key.to_vec())),
            ("until".to_string(), Term::from(until)),
        ]);
        let mut b = Biscuit::builder()
            .code_with_params(
                "project({project}); key({key}); check if time($t), $t <= {until};",
                params,
                HashMap::new(),
            )
            .map_err(denied)?;
        if !narrow.is_empty() {
            b = b.merge(narrow.block()?);
        }
        b.build(&self.root).map_err(denied)?.to_base64().map_err(denied)
    }
}

/// Narrows down `token` with a new block. Needs no key, so any holder can do it.
///
/// # Errors
///
/// `token` is not a token, or `narrow` names a call that is not in [`OPS`].
pub fn narrow(token: &str, narrow: &Narrow) -> Result<String, Denied> {
    let t = biscuit_auth::UnverifiedBiscuit::from_base64(token).map_err(denied)?;
    t.append(narrow.block()?).map_err(denied)?.to_base64().map_err(denied)
}

/// Whether a bearer credential is a token rather than an API key, which starts `hb_`.
#[must_use]
pub fn is_token(bearer: &str) -> bool {
    !bearer.starts_with("hb_")
}

/// Checks tokens with the keeper's public key.
#[derive(Clone)]
pub struct Verifier {
    root: PublicKey,
}

impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier").finish_non_exhaustive()
    }
}

/// A token whose signatures were checked, with what its first block says.
#[derive(Debug)]
pub struct Token {
    biscuit: Biscuit,
    /// The project it opens.
    pub project: String,
    /// The hash of the API key it came from.
    pub key: [u8; 32],
}

impl Verifier {
    /// A verifier for the Ed25519 public key `public`.
    ///
    /// # Errors
    ///
    /// The bytes are not a public key.
    pub fn new(public: &[u8]) -> Result<Self, Denied> {
        Ok(Self { root: PublicKey::from_bytes(public, Algorithm::Ed25519).map_err(denied)? })
    }

    /// Checks the signatures on `token` and reads its project and key.
    ///
    /// # Errors
    ///
    /// The token is not one, was not signed by this key, or its first block has no project.
    pub fn verify(&self, token: &str) -> Result<Token, Denied> {
        let biscuit = Biscuit::from_base64(token, self.root).map_err(denied)?;
        let mut a = biscuit_auth::AuthorizerBuilder::new()
            .set_limits(limits())
            .build(&biscuit)
            .map_err(denied)?;
        // Queries see only the first block and the authorizer, so a block a holder added can
        // not name another project.
        let projects: Vec<(String,)> = a.query("data($p) <- project($p)").map_err(denied)?;
        let keys: Vec<(Vec<u8>,)> = a.query("data($k) <- key($k)").map_err(denied)?;
        let ([(project,)], [(key,)]) = (&projects[..], &keys[..]) else {
            return Err(Denied("the token names no single project and key".into()));
        };
        let key = <[u8; 32]>::try_from(key.as_slice())
            .map_err(|_| Denied("the token's key is not a hash".into()))?;
        Ok(Token { project: project.clone(), key, biscuit })
    }
}

impl Token {
    /// Whether the token allows `op` at `now`, on `cell` for a call about one cell.
    ///
    /// # Errors
    ///
    /// A check in one of its blocks fails, which is the answer for a token held to some cells
    /// when the call is not about one cell.
    pub fn allows(&self, op: &str, cell: Option<&str>, now: SystemTime) -> Result<(), Denied> {
        let mut params = HashMap::from([
            ("now".to_string(), Term::from(now)),
            ("op".to_string(), Term::from(op)),
        ]);
        let mut code = String::from("time({now}); op({op}); allow if project($p);");
        if let Some(c) = cell {
            params.insert("cell".into(), Term::from(c));
            code.push_str(" cell({cell});");
        }
        let mut a = biscuit_auth::AuthorizerBuilder::new()
            .code_with_params(code, params, HashMap::new())
            .map_err(denied)?
            .set_limits(limits())
            .build(&self.biscuit)
            .map_err(denied)?;
        a.authorize().map(drop).map_err(|e| match e {
            biscuit_auth::error::Token::FailedLogic(_) => {
                Denied(format!("the token does not allow {op} here or now"))
            }
            e => denied(e),
        })
    }
}

/// How much work checking a token may take. A token from the keeper takes a few facts and one
/// pass, so the facts and passes bound it. The time is wall time, and biscuit's default of 1 ms
/// was hit on a loaded machine when the thread was put off the CPU, which refused good tokens.
fn limits() -> AuthorizerLimits {
    AuthorizerLimits { max_facts: 1000, max_iterations: 100, max_time: Duration::from_millis(100) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pair() -> (Issuer, Verifier) {
        let issuer = Issuer::new(&[7; 32]).unwrap();
        let verifier = Verifier::new(&issuer.public()).unwrap();
        (issuer, verifier)
    }

    #[test]
    fn a_token_opens_its_project_until_it_runs_out() {
        let (issuer, verifier) = pair();
        let now = SystemTime::now();
        let t = issuer.mint("swe", &[1; 32], now + MAX_TTL, &Narrow::default()).unwrap();
        let tok = verifier.verify(&t).unwrap();
        assert_eq!((tok.project.as_str(), tok.key), ("swe", [1; 32]));
        tok.allows("create", None, now).unwrap();
        tok.allows("exec", Some("c1"), now).unwrap();
        assert!(tok.allows("create", None, now + MAX_TTL * 2).is_err());
    }

    #[test]
    fn a_narrowed_token_does_less_and_never_more() {
        let (issuer, verifier) = pair();
        let now = SystemTime::now();
        let t = issuer.mint("swe", &[1; 32], now + MAX_TTL, &Narrow::default()).unwrap();
        let only = Narrow {
            cells: vec!["c1".into()],
            ops: vec!["exec".into(), "files".into()],
            until: Some(now + Duration::from_secs(1800)),
        };
        let small = verifier.verify(&narrow(&t, &only).unwrap()).unwrap();
        small.allows("exec", Some("c1"), now).unwrap();
        small.allows("files", Some("c1"), now).unwrap();
        assert!(small.allows("exec", Some("c2"), now).is_err());
        assert!(small.allows("stop", Some("c1"), now).is_err());
        assert!(small.allows("create", None, now).is_err());
        assert!(small.allows("exec", Some("c1"), now + Duration::from_secs(2000)).is_err());

        // A block that claims another project changes nothing, since only the first block's
        // facts count.
        let b = BlockBuilder::new().code("project(\"other\");").unwrap();
        let forged = Biscuit::from_base64(&t, verifier.root).unwrap().append(b).unwrap();
        let tok = verifier.verify(&forged.to_base64().unwrap()).unwrap();
        assert_eq!(tok.project, "swe");
    }

    #[test]
    fn tokens_from_another_key_or_garbage_are_refused() {
        let (issuer, _) = pair();
        let other = Verifier::new(&Issuer::new(&[9; 32]).unwrap().public()).unwrap();
        let now = SystemTime::now();
        let t = issuer.mint("swe", &[1; 32], now + MAX_TTL, &Narrow::default()).unwrap();
        assert!(other.verify(&t).is_err());
        assert!(other.verify("not a token").is_err());
        let bad = Narrow { ops: vec!["rm".into()], ..Narrow::default() };
        assert!(narrow(&t, &bad).is_err());
        assert!(is_token(&t) && !is_token("hb_0123"));
    }
}
