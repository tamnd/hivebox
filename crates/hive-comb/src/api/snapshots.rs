//! The `Snapshots` service for container cells. A snapshot is the cell's image with one more layer
//! holding what the cell wrote, stored in the node's image store, and its id is that image's id.
//! A restore is a create with the snapshot as the source, and a commit gives a scrubbed snapshot a
//! name in the caller's project.

use super::{Api, Args, Call, invalid, parse_id, status};
use futures::stream::BoxStream;
use hive_nectar::BlobId;
use hive_nectar::upper::Scrub;
use hive_proto::v1;
use hive_proto::v1::snapshots_server::Snapshots;
use tonic::{Request, Response, Status};

fn snapshot_name(r: Option<&v1::SnapshotRef>) -> String {
    r.map(|r| r.id.clone()).unwrap_or_default()
}

fn snapshot_id(r: Option<v1::SnapshotRef>) -> Result<BlobId, Status> {
    let id = r.map(|r| r.id).unwrap_or_default();
    if id.is_empty() {
        return Err(invalid("name the snapshot"));
    }
    id.parse().map_err(|_| invalid(format!("{id:?} is not a snapshot id")))
}

#[tonic::async_trait]
impl Snapshots for Api {
    type RestoreStream = BoxStream<'static, Result<v1::CreateEvent, Status>>;
    type ForkStream = BoxStream<'static, Result<v1::CreateEvent, Status>>;

    async fn snapshot(
        &self,
        req: Request<v1::SnapshotRequest>,
    ) -> Result<Response<v1::SnapshotRef>, Status> {
        let call = Call::new(self, &req, "snapshot.create")?;
        let r = req.into_inner();
        let cell = r.cell_id.clone();
        let args = Args::default()
            .num(u64::try_from(r.kind).unwrap_or(0))
            .map(&r.labels)
            .num(r.scrub.into())
            .strs(&r.allow);
        let snap = async {
            let id = parse_id(&r.cell_id)?;
            self.owned(&call.project, id).map_err(status)?;
            match v1::SnapshotKind::try_from(r.kind) {
                Ok(v1::SnapshotKind::Unspecified | v1::SnapshotKind::Disk) => {}
                _ => return Err(Status::unimplemented("only disk snapshots are in yet")),
            }
            if !r.labels.is_empty() {
                return Err(invalid("snapshot labels are not kept yet, so send none"));
            }
            let scrub = match (r.scrub, r.allow.is_empty()) {
                (true, _) => Some(Scrub { allow: r.allow }),
                (false, true) => None,
                (false, false) => return Err(invalid("allow only means something with scrub on")),
            };
            self.comb.snapshot(id, scrub).await.map_err(status)
        }
        .await;
        let snap = call.check(&cell, &args, snap)?;
        call.record(&cell, &args, format!("ok snapshot={snap}"));
        Ok(Response::new(v1::SnapshotRef { id: snap.to_string() }))
    }

    async fn restore(
        &self,
        req: Request<v1::RestoreRequest>,
    ) -> Result<Response<Self::RestoreStream>, Status> {
        let call = Call::new(self, &req, "snapshot.restore")?;
        let (meta, ext, r) = req.into_parts();
        let args = Args::default().str(&snapshot_name(r.snapshot.as_ref()));
        let id = call.check("", &args, snapshot_id(r.snapshot))?;
        let mut spec =
            r.spec.ok_or_else(|| invalid("a restore needs the spec of the new cells"))?;
        spec.source = Some(v1::cell_spec::Source::Snapshot(v1::SnapshotRef { id: id.to_string() }));
        let create = v1::CreateRequest {
            spec: Some(spec),
            count: r.count,
            idempotency_key: r.idempotency_key,
            placement: None,
        };
        self.make(Request::from_parts(meta, ext, create), "snapshot.restore").await
    }

    async fn fork(
        &self,
        req: Request<v1::ForkRequest>,
    ) -> Result<Response<Self::ForkStream>, Status> {
        let call = Call::new(self, &req, "snapshot.fork")?;
        let r = req.get_ref();
        let args = Args::default().num(r.count.into()).map(&r.labels).str(&r.idempotency_key);
        call.check(&r.cell_id, &args, Err(Status::unimplemented("fork is not in yet")))
    }

    async fn commit(
        &self,
        req: Request<v1::CommitRequest>,
    ) -> Result<Response<v1::ImageRef>, Status> {
        let call = Call::new(self, &req, "snapshot.commit")?;
        let r = req.into_inner();
        let args = Args::default().str(&snapshot_name(r.snapshot.as_ref())).str(&r.name);
        let committed = async {
            let id = snapshot_id(r.snapshot)?;
            self.comb.commit(&call.project, id, &r.name).await.map_err(status)
        }
        .await;
        call.done("", &args, &committed);
        committed.map(|()| Response::new(v1::ImageRef { r#ref: r.name }))
    }

    async fn delete(&self, req: Request<v1::SnapshotRef>) -> Result<Response<v1::Empty>, Status> {
        let call = Call::new(self, &req, "snapshot.delete")?;
        let args = Args::default().str(&req.get_ref().id);
        call.check(
            "",
            &args,
            Err(Status::unimplemented(
                "snapshots are images in the store, which has no garbage collection yet",
            )),
        )
    }
}
