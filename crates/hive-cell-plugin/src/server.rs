//! The plugin's side: serving any [`CellDriver`] on a Unix socket.

use crate::API_VERSION;
use crate::wire;
use futures::stream;
use hive_cell::CellDriver;
use hive_proto::convert;
use hive_proto::plugin::v1::driver_server::{Driver, DriverServer};
use hive_proto::plugin::v1::{
    Caps, DescribeRequest, Description, Exit, Handle, Liveness, NodeFit, PauseRequest,
    PrepareRequest, ProbeRequest, ResizeRequest, StopRequest, Trimmed,
};
use hive_proto::v1::Empty;
use hive_types::{Error, Reason};
use std::fmt;
use std::future::Future;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::net::UnixListener;
use tonic::{Request, Response, Status};

/// Serves `driver` on `listener` until `stop` finishes. `name` is what the node agent logs for
/// the plugin, such as `"qemu-plugin 0.3.1"`.
///
/// # Errors
///
/// The server fails.
pub async fn serve(
    driver: Arc<dyn CellDriver>,
    name: impl Into<String>,
    listener: UnixListener,
    stop: impl Future<Output = ()> + Send + 'static,
) -> io::Result<()> {
    let incoming = stream::unfold(listener, |l| async move {
        let conn = l.accept().await.map(|(s, _)| s);
        Some((conn, l))
    });
    let service = DriverServer::new(Service { driver, name: name.into() });
    tonic::transport::Server::builder()
        .serve_with_incoming_shutdown(service, incoming, stop)
        .await
        .map_err(io::Error::other)
}

/// Makes the socket at `path` for [`serve`], replacing one an earlier run left there. Only its
/// owner can connect, since whoever can call a driver can start processes as the plugin.
///
/// # Errors
///
/// The directory cannot be made, or the socket cannot be bound or moved into place.
pub fn bind(path: &Path) -> io::Result<UnixListener> {
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty()).unwrap_or(Path::new("."));
    std::fs::create_dir_all(dir)?;
    let name = path.file_name().ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, format!("{} is not a file", path.display()))
    })?;
    // Bound under another name and moved into place once only its owner can open it, so there is
    // no moment when anyone else could connect.
    let mut tmp = name.to_os_string();
    tmp.push(format!(".{}.tmp", std::process::id()));
    let tmp = dir.join(tmp);
    let _ = std::fs::remove_file(&tmp);
    let listener = UnixListener::bind(&tmp)?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))?;
    std::fs::rename(&tmp, path)?;
    Ok(listener)
}

struct Service {
    driver: Arc<dyn CellDriver>,
    name: String,
}

impl fmt::Debug for Service {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Service").field("name", &self.name).finish_non_exhaustive()
    }
}

fn reply<T>(r: Result<T, Error>) -> Result<Response<T>, Status> {
    r.map(Response::new).map_err(|e| convert::error_to_status(&e))
}

fn handle(h: Option<Handle>) -> Result<hive_cell::CellHandle, Error> {
    wire::handle_from(h.ok_or_else(|| Error::new(Reason::InvalidArgument, "no handle"))?)
}

#[tonic::async_trait]
impl Driver for Service {
    async fn describe(&self, _: Request<DescribeRequest>) -> Result<Response<Description>, Status> {
        // A node that speaks another version sees this one in the reply and goes no further.
        let caps: Caps = wire::caps_to(self.driver.caps());
        Ok(Response::new(Description {
            api_version: API_VERSION,
            backend: convert::backend_to_v1(self.driver.backend()).into(),
            caps: Some(caps),
            name: self.name.clone(),
        }))
    }

    async fn probe(&self, _: Request<ProbeRequest>) -> Result<Response<NodeFit>, Status> {
        reply(self.driver.probe().await.map(wire::fit_to))
    }

    async fn prepare(&self, req: Request<PrepareRequest>) -> Result<Response<Handle>, Status> {
        let r = req.into_inner();
        reply(
            async {
                let id = wire::cell_id(&r.cell_id)?;
                let spec = r
                    .spec
                    .ok_or_else(|| Error::new(Reason::InvalidArgument, "no spec"))
                    .and_then(convert::spec_from_v1)?;
                let rootfs = wire::rootfs_from(r.rootfs.unwrap_or_default());
                let slot = wire::slot_from(r.slot.unwrap_or_default())?;
                let h = self.driver.prepare(id, &spec, &rootfs, &slot).await?;
                wire::handle_to(&h)
            }
            .await,
        )
    }

    async fn start(&self, req: Request<Handle>) -> Result<Response<Handle>, Status> {
        reply(
            async {
                let mut h = wire::handle_from(req.into_inner())?;
                self.driver.start(&mut h).await?;
                wire::handle_to(&h)
            }
            .await,
        )
    }

    async fn pause(&self, req: Request<PauseRequest>) -> Result<Response<Empty>, Status> {
        let r = req.into_inner();
        reply(
            async {
                let mode = wire::mode_from(r.mode())?;
                self.driver.pause(&handle(r.handle)?, mode).await.map(|()| Empty {})
            }
            .await,
        )
    }

    async fn resume(&self, req: Request<Handle>) -> Result<Response<Empty>, Status> {
        reply(
            async {
                let h = wire::handle_from(req.into_inner())?;
                self.driver.resume(&h).await.map(|()| Empty {})
            }
            .await,
        )
    }

    async fn trim(&self, req: Request<Handle>) -> Result<Response<Trimmed>, Status> {
        reply(
            async {
                let h = wire::handle_from(req.into_inner())?;
                self.driver.trim(&h).await.map(|bytes| Trimmed { bytes })
            }
            .await,
        )
    }

    async fn resize(&self, req: Request<ResizeRequest>) -> Result<Response<Empty>, Status> {
        let r = req.into_inner();
        reply(
            async {
                let res = wire::resources_from(r.resources)?;
                self.driver.resize(&handle(r.handle)?, &res).await.map(|()| Empty {})
            }
            .await,
        )
    }

    async fn stop(&self, req: Request<StopRequest>) -> Result<Response<Exit>, Status> {
        let r = req.into_inner();
        reply(
            async {
                let grace = wire::grace_from(r.grace)?;
                self.driver.stop(&handle(r.handle)?, grace).await.map(wire::exit_to)
            }
            .await,
        )
    }

    async fn check(&self, req: Request<Handle>) -> Result<Response<Liveness>, Status> {
        reply(
            async {
                let h = wire::handle_from(req.into_inner())?;
                self.driver.check(&h).await.map(wire::liveness_to)
            }
            .await,
        )
    }
}
