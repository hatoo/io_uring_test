use std::os::unix::fs::OpenOptionsExt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tempfile::NamedTempFile;

#[repr(align(4096))]
#[derive(PartialEq, Eq, Debug, Clone)]
pub struct Aligned([u8; 4096]);

#[tokio::main]
async fn main() {
    let path = NamedTempFile::new().unwrap().into_temp_path();
    let heap_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .custom_flags(libc::O_DIRECT)
        .open(&path)
        .unwrap();

    let heap_file = Arc::new(heap_file);
    let counter = Arc::new(AtomicU64::new(0));
    let ring = rio::new().unwrap();

    let fs = (0..4)
        .map(|id| {
            let heap_file = heap_file.clone();
            let counter = counter.clone();
            let ring = ring.clone();

            tokio::spawn(async move {
                for _ in 0..16 {
                    let pos = counter.fetch_add(1, Ordering::Relaxed);
                    let at = pos * 4096;
                    let page = Aligned([0; 4096]);

                    println!("START {:?} id = {}", pos, id);
                    ring.write_at(heap_file.as_ref(), &page.0, at)
                        .await
                        .unwrap();
                    println!("END {:?} id = {}", pos, id);
                }
                println!("TASK END {}", id)
            })
        })
        .collect::<Vec<_>>();

    for (id, f) in fs.into_iter().enumerate() {
        println!("AWAITING {}", id);
        f.await.unwrap();
        println!("DONE {}", id);
    }
}
