// E033 no-downgrade probe writer: writes a deterministic bucket with the compression
// gate ON. The resulting WAL carries the E033 compression format sentinel record, so a
// pre-E020 reader must refuse to open it loudly instead of silently recovering only the
// uncompressed prefix.
//
// Usage: e033_gate_on_writer <dir>
use std::path::PathBuf;

use mrecordlog::{MultiRecordLog, PersistAction, PersistPolicy};

fn payload_for(pos: u64) -> Vec<u8> {
    let size = match pos % 5 {
        0 => 1usize,
        1 => 100,
        2 => 500,
        3 => 9_000,
        _ => 40_000,
    };
    let mut v = Vec::with_capacity(size);
    v.extend_from_slice(format!("{pos:012}:").as_bytes());
    let fill = b'a' + ((pos % 26) as u8);
    while v.len() < size {
        v.push(fill);
    }
    v.truncate(size);
    v
}

fn main() {
    let dir = PathBuf::from(std::env::args().nth(1).unwrap());
    let queue = "q";
    let mut log = MultiRecordLog::open_with_prefs_and_compression(
        &dir,
        PersistPolicy::Always(PersistAction::Flush),
        true,
    )
    .unwrap();
    log.create_queue(queue).unwrap();

    let payloads: Vec<Vec<u8>> = (0..500).map(payload_for).collect();
    for chunk in payloads.chunks(50) {
        log.append_records(queue, None, chunk.iter().map(|p| p.as_slice()))
            .unwrap();
    }
    log.persist(PersistAction::FlushAndFsync).unwrap();
    println!("WROTE {} (gate ON)", payloads.len());
}
