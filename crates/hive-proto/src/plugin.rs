//! The generated `hivebox.plugin.v1` messages, client and server: the API a node agent uses to run
//! a backend in a plugin process. `proto/hivebox/plugin/v1/driver.proto` carries the comments.

pub mod v1 {
    //! Version 1 of the driver plugin API.

    #![allow(missing_docs, missing_debug_implementations, unreachable_pub, unused_qualifications)]
    #![allow(clippy::all, clippy::pedantic, clippy::missing_panics_doc)]

    tonic::include_proto!("hivebox.plugin.v1");
}
