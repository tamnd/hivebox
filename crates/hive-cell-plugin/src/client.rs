//! The node agent's side: a [`CellDriver`] whose every call goes to a plugin process.

use crate::API_VERSION;
use crate::wire::{self, Result};
use futures::future::BoxFuture;
use hive_cell::{
    CellDriver, CellHandle, DriverCaps, ExitInfo, Liveness, NodeFit, PauseMode, RootfsPlan, Slot,
};
use hive_proto::convert;
use hive_proto::plugin::v1::driver_client::DriverClient;
use hive_proto::plugin::v1::{
    DescribeRequest, PauseRequest, PrepareRequest, ProbeRequest, ResizeRequest, StopRequest,
};
use hive_types::{Backend, CellId, CellSpec, Error, Reason, Resources};
use hyper_util::rt::TokioIo;
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

/// A backend served by a plugin on a Unix socket, which the node agent adds to its registry like
/// any built in driver.
///
/// The connection is made again on the next call when the plugin restarts, and cell handles are
/// plain data, so a plugin can be restarted or upgraded under a running node: its cells' next
/// calls wait for nothing but the new process.
#[derive(Clone, Debug)]
pub struct PluginDriver {
    client: DriverClient<Channel>,
    socket: PathBuf,
    backend: Backend,
    caps: DriverCaps,
    name: String,
}

impl PluginDriver {
    /// Connects to the plugin listening at `socket` and asks it what it runs.
    ///
    /// # Errors
    ///
    /// `INTERNAL` when nothing answers there, and `POLICY_DENIED` when the plugin speaks another
    /// version of the API or names no backend a cell can ask for.
    pub async fn connect(socket: impl AsRef<Path>) -> Result<Self> {
        let socket = socket.as_ref().to_path_buf();
        let path = socket.clone();
        // The URI is required and never used, since the connector ignores it.
        let channel = Endpoint::from_static("http://plugin")
            .connect_timeout(Duration::from_secs(5))
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let path = path.clone();
                async move {
                    let s = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, io::Error>(TokioIo::new(s))
                }
            }));
        let mut client = DriverClient::new(channel);
        let d = client
            .describe(DescribeRequest { api_version: API_VERSION })
            .await
            .map_err(|s| failed(&socket, &s))?
            .into_inner();
        let refuse = |why: String| {
            Error::new(Reason::PolicyDenied, format!("the plugin at {}: {why}", socket.display()))
        };
        if d.api_version != API_VERSION {
            return Err(refuse(format!(
                "it speaks version {} of the plugin API and this node speaks {API_VERSION}",
                d.api_version
            )));
        }
        let backend = match d.backend() {
            hive_proto::v1::Backend::Unspecified | hive_proto::v1::Backend::Auto => {
                return Err(refuse("it names no backend".into()));
            }
            b => convert::backend_from_v1(b),
        };
        let caps = d.caps.as_ref().map(wire::caps_from).unwrap_or_default();
        Ok(Self { client, socket, backend, caps, name: d.name })
    }

    /// The name and version the plugin gave.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Where the plugin listens.
    #[must_use]
    pub fn socket(&self) -> &Path {
        &self.socket
    }

    fn err(&self, s: &tonic::Status) -> Error {
        failed(&self.socket, s)
    }
}

/// The error a call came back with. One the plugin did not give as a hivebox error, such as the
/// plugin not running, says which plugin it was.
fn failed(socket: &Path, s: &tonic::Status) -> Error {
    let mut e = convert::error_from_status(s);
    if e.reason == Reason::Internal {
        e.message = format!("the driver plugin at {}: {}", socket.display(), e.message);
    }
    e
}

impl CellDriver for PluginDriver {
    fn backend(&self) -> Backend {
        self.backend
    }

    fn caps(&self) -> DriverCaps {
        self.caps
    }

    fn probe(&self) -> BoxFuture<'_, Result<NodeFit>> {
        Box::pin(async move {
            let r = self.client.clone().probe(ProbeRequest {}).await;
            Ok(wire::fit_from(r.map_err(|s| self.err(&s))?.into_inner()))
        })
    }

    fn prepare<'a>(
        &'a self,
        id: CellId,
        spec: &'a CellSpec,
        rootfs: &'a RootfsPlan,
        slot: &'a Slot,
    ) -> BoxFuture<'a, Result<CellHandle>> {
        Box::pin(async move {
            let req = PrepareRequest {
                cell_id: id.to_string(),
                spec: Some(convert::spec_to_v1(spec)),
                rootfs: Some(wire::rootfs_to(rootfs)?),
                slot: Some(wire::slot_to(slot)?),
            };
            let r = self.client.clone().prepare(req).await;
            wire::handle_from(r.map_err(|s| self.err(&s))?.into_inner())
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let r = self.client.clone().start(wire::handle_to(h)?).await;
            let started = wire::handle_from(r.map_err(|s| self.err(&s))?.into_inner())?;
            if started.id != h.id {
                return Err(Error::new(
                    Reason::Internal,
                    format!("the plugin started {} when asked to start {}", started.id, h.id),
                ));
            }
            *h = started;
            Ok(())
        })
    }

    fn pause<'a>(&'a self, h: &'a CellHandle, mode: PauseMode) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = PauseRequest {
                handle: Some(wire::handle_to(h)?),
                mode: wire::mode_to(mode).into(),
            };
            self.client.clone().pause(req).await.map_err(|s| self.err(&s))?;
            Ok(())
        })
    }

    fn resume<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            self.client.clone().resume(wire::handle_to(h)?).await.map_err(|s| self.err(&s))?;
            Ok(())
        })
    }

    fn trim<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<u64>> {
        Box::pin(async move {
            let r = self.client.clone().trim(wire::handle_to(h)?).await;
            Ok(r.map_err(|s| self.err(&s))?.into_inner().bytes)
        })
    }

    fn resize<'a>(&'a self, h: &'a CellHandle, r: &'a Resources) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let req = ResizeRequest {
                handle: Some(wire::handle_to(h)?),
                resources: Some(wire::resources_to(r)),
            };
            self.client.clone().resize(req).await.map_err(|s| self.err(&s))?;
            Ok(())
        })
    }

    fn stop<'a>(&'a self, h: &'a CellHandle, grace: Duration) -> BoxFuture<'a, Result<ExitInfo>> {
        Box::pin(async move {
            let req = StopRequest {
                handle: Some(wire::handle_to(h)?),
                grace: Some(convert::duration_to_v1(grace)),
            };
            let r = self.client.clone().stop(req).await;
            Ok(wire::exit_from(Some(r.map_err(|s| self.err(&s))?.into_inner())))
        })
    }

    fn check<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, Result<Liveness>> {
        Box::pin(async move {
            let r = self.client.clone().check(wire::handle_to(h)?).await;
            wire::liveness_from(r.map_err(|s| self.err(&s))?.into_inner())
        })
    }
}
