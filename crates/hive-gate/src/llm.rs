//! The `Llm` service across the cluster. Each node's comb runs the LLM gateway for its own cells,
//! so a route and a hold go to every node, and the turns of a rollout are gathered from all of
//! them. Turns for a cell go to the comb that owns it alone, which is the cheap way when the
//! trainer knows the cell. Nodes whose cells have no network have no gateway and are left out.

use std::future::Future;

use hive_proto::convert;
use hive_proto::v1;
use hive_proto::v1::llm_client::LlmClient;
use hive_proto::v1::llm_server::Llm;
use tokio::task::JoinSet;
use tonic::transport::Channel;
use tonic::{Code, Request, Response, Status};

use crate::cells::{self, Api};

/// The biggest answer a comb sends, which is a rollout's turns with their token ids.
const MAX_ANSWER: usize = 64 << 20;

#[tonic::async_trait]
impl Llm for Api {
    async fn set_route(&self, req: Request<v1::LlmRoute>) -> Result<Response<v1::Empty>, Status> {
        let project = cells::allowed(&req, "llm", None)?;
        let msg = req.into_inner();
        let answers =
            self.on_every(&project, msg, |mut c, r| async move { c.set_route(r).await }).await;
        all(answers)?;
        Ok(Response::new(v1::Empty {}))
    }

    async fn hold(
        &self,
        req: Request<v1::LlmHoldRequest>,
    ) -> Result<Response<v1::LlmHoldResult>, Status> {
        let project = cells::allowed(&req, "llm", None)?;
        let msg = req.into_inner();
        let answers = self.on_every(&project, msg, |mut c, r| async move { c.hold(r).await }).await;
        let in_flight = all(answers)?.iter().map(|r| r.in_flight).sum();
        Ok(Response::new(v1::LlmHoldResult { in_flight }))
    }

    async fn turns(
        &self,
        req: Request<v1::LlmTurnsRequest>,
    ) -> Result<Response<v1::LlmTurnsResponse>, Status> {
        let cell = req.get_ref().cell_id.clone();
        let project = cells::allowed(&req, "llm", Some(cell.as_str()).filter(|c| !c.is_empty()))?;
        let msg = req.into_inner();
        if !cell.is_empty() {
            let node = cells::parse_id(&cell)?.node();
            let mut client = self.llm(node)?;
            return match client.turns(cells::out(&project, msg)).await {
                Ok(r) => Ok(r),
                Err(s) => Err(convert::error_to_status(&cells::node_error(node, &s))),
            };
        }
        if msg.rollout_id.is_empty() {
            return Err(cells::invalid("name a rollout id, a cell, or both"));
        }
        let answers =
            self.on_every(&project, msg, |mut c, r| async move { c.turns(r).await }).await;
        Ok(Response::new(merge(answers)?))
    }
}

/// What one node said, or why it said nothing.
type Answer<R> = (u16, Result<R, Status>);

impl Api {
    fn llm(&self, node: u16) -> Result<LlmClient<Channel>, Status> {
        Ok(LlmClient::new(self.channel(node)?).max_decoding_message_size(MAX_ANSWER))
    }

    /// Sends `msg` to every node at once and returns each one's answer.
    async fn on_every<T, R, F, Fut>(
        &self,
        project: &cells::Caller,
        msg: T,
        call: F,
    ) -> Vec<Answer<R>>
    where
        T: Clone + Send + 'static,
        R: Send + 'static,
        F: Fn(LlmClient<Channel>, Request<T>) -> Fut,
        Fut: Future<Output = Result<Response<R>, Status>> + Send + 'static,
    {
        let mut calls = JoinSet::new();
        for node in self.nodes().all() {
            let fut = self.llm(node).map(|c| call(c, cells::out(project, msg.clone())));
            calls.spawn(async move {
                let r = match fut {
                    Ok(f) => f.await.map(Response::into_inner),
                    Err(s) => Err(s),
                };
                (node, r)
            });
        }
        let mut answers = Vec::new();
        while let Some(done) = calls.join_next().await {
            let Ok(answer) = done else { continue };
            answers.push(answer);
        }
        answers.sort_by_key(|(node, _)| *node);
        answers
    }
}

/// Whether `s` says the node has no LLM gateway, so it has no cells that call one.
fn no_gateway(s: &Status) -> bool {
    s.code() == Code::FailedPrecondition
}

/// The answers of the nodes with a gateway, or the first error, saying which node.
fn all<R>(answers: Vec<Answer<R>>) -> Result<Vec<R>, Status> {
    let mut out = Vec::new();
    for (node, r) in answers {
        match r {
            Ok(r) => out.push(r),
            Err(s) if no_gateway(&s) => {}
            Err(s) => return Err(convert::error_to_status(&cells::node_error(node, &s))),
        }
    }
    if out.is_empty() {
        return Err(Status::failed_precondition("no node has an LLM gateway"));
    }
    Ok(out)
}

/// One answer from the turns of every node, in the order they were made. A node that could not
/// be asked is named in `unreached` rather than failing the call, since the others may have
/// taken their turns already.
fn merge(answers: Vec<Answer<v1::LlmTurnsResponse>>) -> Result<v1::LlmTurnsResponse, Status> {
    let mut out = v1::LlmTurnsResponse::default();
    let mut asked = 0;
    for (node, r) in answers {
        match r {
            Ok(r) => {
                asked += 1;
                out.turns.extend(r.turns);
                out.dropped += r.dropped;
            }
            Err(s) if no_gateway(&s) => {}
            Err(_) => out.unreached.push(u32::from(node)),
        }
    }
    if asked == 0 && out.unreached.is_empty() {
        return Err(Status::failed_precondition("no node has an LLM gateway"));
    }
    out.turns.sort_by_key(|t| {
        let at = t.started.unwrap_or_default();
        (at.seconds, at.nanos, t.seq)
    });
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn turn(seconds: i64, seq: u64) -> v1::LlmTurn {
        v1::LlmTurn {
            seq,
            started: Some(prost_types::Timestamp { seconds, nanos: 0 }),
            ..Default::default()
        }
    }

    #[test]
    fn turns_from_every_node_come_back_in_order() {
        let a = v1::LlmTurnsResponse {
            turns: vec![turn(5, 1), turn(9, 2)],
            dropped: 3,
            ..Default::default()
        };
        let b = v1::LlmTurnsResponse { turns: vec![turn(7, 1)], dropped: 1, ..Default::default() };
        let r = merge(vec![
            (1, Ok(a)),
            (2, Err(Status::failed_precondition("no gateway"))),
            (3, Ok(b)),
            (4, Err(Status::unavailable("down"))),
        ])
        .unwrap();
        let order: Vec<(i64, u64)> =
            r.turns.iter().map(|t| (t.started.unwrap().seconds, t.seq)).collect();
        assert_eq!(order, [(5, 1), (7, 1), (9, 2)]);
        assert_eq!(r.dropped, 4);
        assert_eq!(r.unreached, [4]);
        let none = merge(vec![(1, Err(Status::failed_precondition("no gateway")))]);
        assert_eq!(none.unwrap_err().code(), Code::FailedPrecondition);
    }

    #[test]
    fn a_route_or_hold_fails_when_a_node_with_a_gateway_does() {
        let held = |n| Ok(v1::LlmHoldResult { in_flight: n });
        let ok = all(vec![
            (1, held(2)),
            (2, Err(Status::failed_precondition("no gateway"))),
            (3, held(5)),
        ])
        .unwrap();
        assert_eq!(ok.iter().map(|r| r.in_flight).sum::<u32>(), 7);
        let err = all(vec![(1, held(2)), (3, Err(Status::unavailable("down")))]).unwrap_err();
        assert!(err.message().contains("node 3"), "{}", err.message());
        let none = all::<v1::LlmHoldResult>(vec![(1, Err(Status::failed_precondition("x")))]);
        assert_eq!(none.unwrap_err().code(), Code::FailedPrecondition);
    }
}
