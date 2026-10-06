//! The `Snapshots` service for container cells. A snapshot is the cell's image with one more layer
//! holding what the cell wrote, stored in the node's image store, and its id is that image's id.
//! A restore is a create with the snapshot as the source, and a commit gives a scrubbed snapshot a
//! name in the caller's project.

use super::{Api, invalid, parse_id, project, status};
use futures::stream::BoxStream;
use hive_nectar::BlobId;
use hive_nectar::upper::Scrub;
use hive_proto::v1;
use hive_proto::v1::cells_server::Cells;
use hive_proto::v1::snapshots_server::Snapshots;
use tonic::{Request, Response, Status};

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
        let project = project(&req)?;
        let r = req.into_inner();
        let id = parse_id(&r.cell_id)?;
        self.owned(&project, id).map_err(status)?;
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
        let snap = self.comb.snapshot(id, scrub).await.map_err(status)?;
        Ok(Response::new(v1::SnapshotRef { id: snap.to_string() }))
    }

    async fn restore(
        &self,
        req: Request<v1::RestoreRequest>,
    ) -> Result<Response<Self::RestoreStream>, Status> {
        let (meta, ext, r) = req.into_parts();
        let id = snapshot_id(r.snapshot)?;
        let mut spec =
            r.spec.ok_or_else(|| invalid("a restore needs the spec of the new cells"))?;
        spec.source = Some(v1::cell_spec::Source::Snapshot(v1::SnapshotRef { id: id.to_string() }));
        let create = v1::CreateRequest {
            spec: Some(spec),
            count: r.count,
            idempotency_key: r.idempotency_key,
            placement: None,
        };
        Cells::create(self, Request::from_parts(meta, ext, create)).await
    }

    async fn fork(
        &self,
        _: Request<v1::ForkRequest>,
    ) -> Result<Response<Self::ForkStream>, Status> {
        Err(Status::unimplemented("fork is not in yet"))
    }

    async fn commit(
        &self,
        req: Request<v1::CommitRequest>,
    ) -> Result<Response<v1::ImageRef>, Status> {
        let project = project(&req)?;
        let r = req.into_inner();
        let id = snapshot_id(r.snapshot)?;
        self.comb.commit(&project, id, &r.name).await.map_err(status)?;
        Ok(Response::new(v1::ImageRef { r#ref: r.name }))
    }

    async fn delete(&self, _: Request<v1::SnapshotRef>) -> Result<Response<v1::Empty>, Status> {
        Err(Status::unimplemented(
            "snapshots are images in the store, which has no garbage collection yet",
        ))
    }
}
