//! `Verify.Run` across the cluster. A verify with a subject goes to the comb that owns the
//! subject, which makes the verifier cell on the same node and hands it the subject's changes
//! there. One without a subject, such as a check of a gold patch, goes where waggle would put
//! the verifier cell, and to another node when that one has no room.

use hive_proto::convert;
use hive_proto::v1;
use hive_proto::v1::verify_client::VerifyClient;
use hive_proto::v1::verify_server::Verify;
use hive_types::{CellSpec, Error, Reason};
use tonic::{Code, Request, Response, Status};

use crate::cells::{self, Api};

/// How many more nodes a verify without a subject is tried on after one turned it away.
const RETRIES: usize = 2;

/// The biggest request and answer, the same as a comb takes.
const MAX_MESSAGE: usize = 64 << 20;

#[tonic::async_trait]
impl Verify for Api {
    async fn run(
        &self,
        req: Request<v1::VerifyRequest>,
    ) -> Result<Response<v1::VerifyResult>, Status> {
        let subject = req.get_ref().subject_cell_id.clone();
        let project =
            cells::allowed(&req, "verify", Some(subject.as_str()).filter(|s| !s.is_empty()))?;
        let msg = req.into_inner();
        // The verifier cell is a cell like any other as far as the quota goes.
        self.charge(&project).await?;
        if !subject.is_empty() {
            let node = cells::parse_id(&subject)?.node();
            return self.send(node, &project, msg).await.map(Response::new);
        }
        let wire = msg.verifier.clone().unwrap_or_default();
        let spec = convert::spec_from_v1(wire).map_err(|e| convert::error_to_status(&e))?;
        let mut exclude = Vec::new();
        for _ in 0..=RETRIES {
            let Some(node) = self.place_one(&project, &spec, &exclude) else { break };
            let result = self.send(node, &project, msg.clone()).await?;
            if !turned_away(&result) {
                return Ok(Response::new(result));
            }
            self.refused(node, &spec);
            exclude.push(node);
        }
        Err(nowhere(&spec))
    }
}

impl Api {
    /// Sends the verify to `node` and returns its answer.
    async fn send(
        &self,
        node: u16,
        project: &str,
        msg: v1::VerifyRequest,
    ) -> Result<v1::VerifyResult, Status> {
        let channel = self.channel(node)?;
        let mut client = VerifyClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE)
            .max_encoding_message_size(MAX_MESSAGE);
        match client.run(cells::out(project, msg)).await {
            Ok(r) => Ok(r.into_inner()),
            // The node could not be reached. Anything else is the comb's own answer.
            Err(s) if s.code() == Code::Unavailable => {
                Err(convert::error_to_status(&cells::node_error(node, &s)))
            }
            Err(s) => Err(s),
        }
    }
}

/// Whether the node had no room for the verifier cell, so another node may.
fn turned_away(r: &v1::VerifyResult) -> bool {
    r.error.as_ref().is_some_and(|e| e.reason == Reason::CapacityUnavailable.as_str())
}

fn nowhere(spec: &CellSpec) -> Status {
    let e = Error::new(
        Reason::CapacityUnavailable,
        format!("no node has room for a {:?} verifier cell", spec.backend),
    );
    convert::error_to_status(&e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_node_with_no_room_is_tried_again() {
        let with = |reason: &str| v1::VerifyResult {
            error: Some(v1::Error { reason: reason.into(), ..Default::default() }),
            ..Default::default()
        };
        assert!(turned_away(&with(Reason::CapacityUnavailable.as_str())));
        assert!(!turned_away(&with(Reason::FileError.as_str())));
        assert!(!turned_away(&v1::VerifyResult::default()));
    }
}
