//! The node end of the guest channel, fed whatever a guest might send. It must not panic, and once
//! the guest hangs up it must notice and close, whatever came before. A hang is a bug too, which
//! libFuzzer reports as a timeout.

#![no_main]

use hive_proto::drone::{Channel, Side};
use libfuzzer_sys::fuzz_target;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fuzz_target!(|data: &[u8]| {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
    rt.block_on(async {
        let (node, mut guest) = tokio::io::duplex(1 << 16);
        let (channel, mut incoming) = Channel::start(node, Side::Node);
        // Serve every stream the guest opens by reading it to the end, as the node would.
        let serve = tokio::spawn(async move {
            while let Some(i) = incoming.recv().await {
                let mut stream = i.stream;
                tokio::spawn(async move { while let Ok(Some(_)) = stream.recv().await {} });
            }
        });
        let (mut read, mut write) = tokio::io::split(&mut guest);
        let feed = async {
            // The node may refuse the input and drop the connection partway, so a failed write is
            // fine.
            let _ = write.write_all(data).await;
            let _ = write.shutdown().await;
        };
        // Whatever the node answers is read and thrown away, so it never blocks on a full pipe.
        let drain = async {
            let mut buf = vec![0; 1 << 16];
            while matches!(read.read(&mut buf).await, Ok(n) if n > 0) {}
        };
        let closed = async {
            channel.closed().await;
            assert!(channel.is_closed());
        };
        tokio::join!(feed, closed, async {
            tokio::select! {
                () = drain => {}
                () = channel.closed() => {}
            }
        });
        serve.abort();
    });
});
