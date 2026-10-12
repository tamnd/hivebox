//! The `Node` service, for the operator: cordon, uncordon and drain. A gate does not pass these
//! on, so only a caller of the comb's own socket or port reaches them.

use super::{Api, Args, Call, failed, status};
use crate::drain::{DrainOptions, Drained, How};
use futures::stream::{self, BoxStream, StreamExt};
use hive_proto::convert;
use hive_proto::internal as pb;
use hive_proto::internal::node_server::Node;
use hive_types::CellState;
use std::time::Duration;
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};

/// How long a drain waits on running cells when the caller does not say.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(600);

impl Api {
    fn node_status(&self) -> pb::NodeStatus {
        let started = self.comb.started();
        let mut s = pb::NodeStatus {
            node: u32::from(self.comb.inner.cfg.node),
            cordoned: self.comb.cordoned(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            up: Some(convert::duration_to_v1(self.comb.up())),
            taken_back: started.taken_back,
            ended_while_down: started.ended,
            lost_on_start: started.lost,
            ..pb::NodeStatus::default()
        };
        for c in self.comb.list() {
            match c.status.state {
                state if state.is_terminal() => continue,
                CellState::Running => s.running += 1,
                CellState::Paused => s.paused += 1,
                _ => {}
            }
            s.live += 1;
        }
        s
    }

    fn set_cordon<T>(&self, req: &Request<T>, on: bool) -> Result<pb::NodeStatus, Status> {
        let call = Call::new(self, req, if on { "node.cordon" } else { "node.uncordon" })?;
        let r = self.comb.cordon(on).map_err(|e| Status::internal(e.to_string()));
        call.done("", &Args::default(), &r);
        r.map(|()| self.node_status())
    }
}

fn event(d: Drained) -> pb::DrainEvent {
    let how = match d.how {
        How::Idle => pb::drained_cell::How::Idle,
        How::Ended => pb::drained_cell::How::Ended,
        How::Stopped => pb::drained_cell::How::Stopped,
        How::Left => pb::drained_cell::How::Left,
    };
    let (snapshot_id, error) = match d.snapshot {
        Some(Ok(id)) => (id.to_string(), String::new()),
        Some(Err(e)) => (String::new(), e.message),
        None => (String::new(), String::new()),
    };
    let cell = pb::DrainedCell {
        cell_id: d.id.to_string(),
        project: d.project,
        how: how.into(),
        snapshot_id,
        error,
    };
    pb::DrainEvent { event: Some(pb::drain_event::Event::Cell(cell)) }
}

#[tonic::async_trait]
impl Node for Api {
    type DrainStream = BoxStream<'static, Result<pb::DrainEvent, Status>>;

    async fn status(
        &self,
        _: Request<pb::NodeRequest>,
    ) -> Result<Response<pb::NodeStatus>, Status> {
        Ok(Response::new(self.node_status()))
    }

    async fn cordon(
        &self,
        req: Request<pb::NodeRequest>,
    ) -> Result<Response<pb::NodeStatus>, Status> {
        self.set_cordon(&req, true).map(Response::new)
    }

    async fn uncordon(
        &self,
        req: Request<pb::NodeRequest>,
    ) -> Result<Response<pb::NodeStatus>, Status> {
        self.set_cordon(&req, false).map(Response::new)
    }

    async fn drain(
        &self,
        req: Request<pb::DrainRequest>,
    ) -> Result<Response<Self::DrainStream>, Status> {
        let call = Call::new(self, &req, "node.drain")?;
        let r = req.into_inner();
        let timeout = convert::duration_from_v1(r.timeout, "timeout").map_err(status);
        let args = Args::default()
            .num(
                timeout
                    .as_ref()
                    .ok()
                    .and_then(|d| *d)
                    .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            )
            .num(r.stop_rest.into())
            .num(r.snapshot.into());
        let timeout = call.check("", &args, timeout)?.unwrap_or(DRAIN_TIMEOUT);
        let opts = DrainOptions { timeout, stop_rest: r.stop_rest, snapshot: r.snapshot };
        let (tx, rx) = mpsc::unbounded_channel();
        let api = self.clone();
        tokio::spawn(async move {
            let sent = tx.clone();
            let drain = api.comb.drain(opts, move |d| {
                let _ = sent.send(Ok(event(d)));
            });
            // A caller that goes away stops the drain, though not the stops under way, and the
            // node stays cordoned.
            let done = tokio::select! {
                done = drain => done,
                () = tx.closed() => {
                    call.record("", &args, "Cancelled".to_string());
                    return;
                }
                () = api.stop.cancelled() => {
                    call.record("", &args, "Unavailable".to_string());
                    let _ = tx.send(Err(Status::unavailable("the node is shutting down")));
                    return;
                }
            };
            let last = match done {
                Ok(t) => {
                    call.record(
                        "",
                        &args,
                        format!(
                            "ok idle={} ended={} stopped={} left={}",
                            t.idle, t.ended, t.stopped, t.left
                        ),
                    );
                    let done = pb::DrainDone {
                        status: Some(api.node_status()),
                        idle: t.idle,
                        ended: t.ended,
                        stopped: t.stopped,
                        left: t.left,
                        took: Some(convert::duration_to_v1(t.took)),
                    };
                    Ok(pb::DrainEvent { event: Some(pb::drain_event::Event::Done(done)) })
                }
                Err(e) => {
                    let e = Status::internal(e.to_string());
                    call.record("", &args, failed(&e));
                    Err(e)
                }
            };
            let _ = tx.send(last);
        });
        let events = stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|e| (e, rx)) });
        Ok(Response::new(events.boxed()))
    }
}
