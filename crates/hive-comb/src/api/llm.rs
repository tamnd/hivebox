//! The `Llm` service, which the trainer uses to steer the node's LLM gateway for its project: where
//! the calls go, holding them while the engine loads new weights, and the token ids of each call.

use super::{Api, invalid, parse_id, project};
use crate::llm::{Gateway, Route, Turn};
use hive_proto::v1;
use hive_proto::v1::llm_server::Llm;
use std::sync::Arc;
use std::time::Duration;
use tonic::{Request, Response, Status};

/// What a 503 says in Retry-After when the trainer sets nothing.
const RETRY_AFTER: Duration = Duration::from_secs(5);
/// How long a hold lasts when the trainer sets nothing.
const TTL: Duration = Duration::from_secs(600);

impl Api {
    fn gateway(&self) -> Result<&Arc<Gateway>, Status> {
        self.comb.inner.llm.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "this node has no LLM gateway, as its cells have no network",
            )
        })
    }
}

fn duration(
    d: Option<prost_types::Duration>,
    unset: Duration,
    what: &str,
) -> Result<Duration, Status> {
    match d {
        None => Ok(unset),
        Some(d) => {
            Duration::try_from(d).map_err(|_| invalid(format!("{what} must not be negative")))
        }
    }
}

fn turn(t: Turn) -> v1::LlmTurn {
    v1::LlmTurn {
        cell_id: t.cell.map(|c| c.to_string()).unwrap_or_default(),
        rollout_id: t.rollout,
        seq: t.seq,
        path: t.path,
        model: t.model,
        status: u32::from(t.status),
        stream: t.stream,
        started: t.started.map(prost_types::Timestamp::from),
        took: prost_types::Duration::try_from(t.took).ok(),
        prompt_ids: t.prompt_ids,
        choices: t
            .choices
            .into_iter()
            .map(|c| v1::LlmChoice {
                index: c.index,
                output_ids: c.output_ids,
                logprobs: c.logprobs,
                finish_reason: c.finish_reason,
                prompt_ids: c.prompt_ids,
            })
            .collect(),
        prompt_tokens: t.prompt_tokens,
        completion_tokens: t.completion_tokens,
        error: t.error,
    }
}

#[tonic::async_trait]
impl Llm for Api {
    async fn set_route(&self, req: Request<v1::LlmRoute>) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req)?;
        let gw = self.gateway()?;
        let r = req.into_inner();
        let route = match r.upstream.as_str() {
            "" => None,
            up => Some(Route::new(up, Some(&r.api_key)).map_err(invalid)?),
        };
        gw.set_route(&project, route);
        Ok(Response::new(v1::Empty {}))
    }

    async fn hold(
        &self,
        req: Request<v1::LlmHoldRequest>,
    ) -> Result<Response<v1::LlmHoldResult>, Status> {
        let project = project(&req)?;
        let gw = self.gateway()?;
        let r = req.into_inner();
        let in_flight = if r.release {
            gw.release(&project)
        } else {
            let retry_after = duration(r.retry_after, RETRY_AFTER, "retry_after")?;
            let ttl = duration(r.ttl, TTL, "ttl")?;
            let drain = duration(r.drain, Duration::ZERO, "drain")?;
            if ttl.is_zero() {
                return Err(invalid("a hold needs a ttl above 0"));
            }
            gw.hold(&project, retry_after, ttl, drain).await
        };
        Ok(Response::new(v1::LlmHoldResult { in_flight }))
    }

    async fn turns(
        &self,
        req: Request<v1::LlmTurnsRequest>,
    ) -> Result<Response<v1::LlmTurnsResponse>, Status> {
        let project = project(&req)?;
        let gw = self.gateway()?;
        let r = req.into_inner();
        let cell = match r.cell_id.as_str() {
            "" => None,
            id => Some(parse_id(id)?),
        };
        if cell.is_none() && r.rollout_id.is_empty() {
            return Err(invalid("name a rollout id, a cell, or both"));
        }
        let (turns, dropped) = gw.turns(&project, &r.rollout_id, cell, r.take);
        Ok(Response::new(v1::LlmTurnsResponse {
            turns: turns.into_iter().map(turn).collect(),
            dropped,
            unreached: Vec::new(),
        }))
    }
}
