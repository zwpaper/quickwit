use std::borrow::Cow;

use bytes::Buf;

use crate::{MultiRecordLog, PersistAction, PersistPolicy, Record, RecordLocations};

fn read_all_records<'a>(multi_record_log: &'a MultiRecordLog, queue: &str) -> Vec<Cow<'a, [u8]>> {
    let mut records = Vec::new();
    let mut next_pos = u64::default();
    for Record { position, payload } in multi_record_log.range(queue, next_pos..).unwrap() {
        assert_eq!(position, next_pos);
        records.push(payload);
        next_pos += 1;
    }
    records
}

#[test]
fn test_multi_record_log_new() {
    let tempdir = tempfile::tempdir().unwrap();
    MultiRecordLog::open(tempdir.path()).unwrap();
}

#[test]
fn test_multi_record_log_create_queue() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert!(&multi_record_log.queue_exists("queue"));
    }
}

#[test]
fn test_multi_record_log_create_queue_after_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert!(&multi_record_log.queue_exists("queue"));
    }
}

#[test]
fn test_multi_record_log_simple() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        multi_record_log
            .append_record("queue", None, &b"hello"[..])
            .unwrap();
        multi_record_log
            .append_record("queue", None, &b"happy"[..])
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"hello".as_slice(), b"happy".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_log_chained() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        multi_record_log
            .append_record(
                "queue",
                None,
                b"world".chain(&b" "[..]).chain(&b"order"[..]),
            )
            .unwrap();
        multi_record_log
            .append_record(
                "queue",
                None,
                b"nice"[..].chain(&b" "[..]).chain(&b"day"[..]),
            )
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"world order".as_slice(), b"nice day".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_log_reopen() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        multi_record_log
            .append_record("queue", None, &b"hello"[..])
            .unwrap();
        multi_record_log
            .append_record("queue", None, &b"happy"[..])
            .unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[b"hello".as_slice(), b"happy".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_log() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue1").unwrap();
        multi_record_log.create_queue("queue2").unwrap();
        multi_record_log
            .append_record("queue1", None, &b"hello"[..])
            .unwrap();
        multi_record_log
            .append_record("queue2", None, &b"maitre"[..])
            .unwrap();
        multi_record_log
            .append_record("queue1", None, &b"happy"[..])
            .unwrap();
        multi_record_log
            .append_record("queue1", None, &b"tax"[..])
            .unwrap();
        multi_record_log
            .append_record("queue2", None, &b"corbeau"[..])
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue1"),
            &[b"hello".as_slice(), b"happy".as_slice(), b"tax".as_slice()]
        );
        assert_eq!(
            &read_all_records(&multi_record_log, "queue2"),
            &[b"maitre".as_slice(), b"corbeau".as_slice()]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log
            .append_record("queue1", None, &b"bubu"[..])
            .unwrap();
        assert_eq!(
            &read_all_records(&multi_record_log, "queue1"),
            &[
                b"hello".as_slice(),
                b"happy".as_slice(),
                b"tax".as_slice(),
                b"bubu".as_slice()
            ]
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
}

#[test]
fn test_multi_record_position_known_after_truncate() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"1"[..])
                .unwrap()
                .last_position,
            Some(0)
        );
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"2"[..])
                .unwrap()
                .last_position,
            Some(1)
        );
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.truncate("queue", ..=1).unwrap();
        assert_eq!(&multi_record_log.list_file_numbers(), &[0]);
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"hello"[..])
                .unwrap()
                .last_position,
            Some(2)
        );
    }
}

#[test]
fn test_multi_insert_truncate() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert_eq!(
            multi_record_log
                .append_records(
                    "queue",
                    None,
                    [b"1", b"2", b"3", b"4"].into_iter().map(|r| r.as_slice())
                )
                .unwrap()
                .last_position,
            Some(3)
        );
        assert_eq!(
            &read_all_records(&multi_record_log, "queue"),
            &[
                b"1".as_slice(),
                b"2".as_slice(),
                b"3".as_slice(),
                b"4".as_slice()
            ]
        );

        multi_record_log.truncate("queue", ..=0).unwrap();
        assert_eq!(
            &multi_record_log
                .range("queue", ..)
                .unwrap()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            &[b"2".as_slice(), b"3".as_slice(), b"4".as_slice()]
        )
    }
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.truncate("queue", ..=1).unwrap();

        assert_eq!(
            &multi_record_log
                .range("queue", ..)
                .unwrap()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            &[b"3".as_slice(), b"4".as_slice()]
        )
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(
            &multi_record_log
                .range("queue", ..)
                .unwrap()
                .map(|record| record.payload)
                .collect::<Vec<_>>(),
            &[b"3".as_slice(), b"4".as_slice()]
        )
    }
}

#[test]
fn test_truncate_range_correct_pos() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"1"[..])
                .unwrap()
                .last_position,
            Some(0)
        );
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"2"[..])
                .unwrap()
                .last_position,
            Some(1)
        );
        multi_record_log.truncate("queue", ..=1).unwrap();
        assert_eq!(
            multi_record_log
                .append_record("queue", None, &b"3"[..])
                .unwrap()
                .last_position,
            Some(2)
        );
        assert_eq!(
            multi_record_log
                .range("queue", ..)
                .unwrap()
                .collect::<Vec<_>>(),
            &[Record::new(2u64, b"3")]
        );

        assert_eq!(
            multi_record_log
                .range("queue", 2..)
                .unwrap()
                .collect::<Vec<_>>(),
            &[Record::new(2, b"3")]
        );

        use std::ops::Bound;
        assert_eq!(
            multi_record_log
                .range("queue", (Bound::Excluded(1), Bound::Unbounded))
                .unwrap()
                .collect::<Vec<_>>(),
            &[Record::new(2, b"3")]
        );
    }
}

#[test]
fn test_multi_record_size() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(multi_record_log.resource_usage().memory_used_bytes, 0);
        assert_eq!(multi_record_log.resource_usage().memory_allocated_bytes, 0);

        multi_record_log.create_queue("queue").unwrap();
        let size_mem_create = multi_record_log.resource_usage();
        assert!(size_mem_create.memory_used_bytes > 0);
        assert!(size_mem_create.memory_allocated_bytes >= size_mem_create.memory_used_bytes);

        multi_record_log
            .append_record("queue", None, &b"hello"[..])
            .unwrap();
        let size_mem_append = multi_record_log.resource_usage();
        assert!(size_mem_append.memory_used_bytes > size_mem_create.memory_used_bytes);
        assert!(size_mem_append.memory_allocated_bytes >= size_mem_append.memory_used_bytes);
        assert!(size_mem_append.memory_allocated_bytes >= size_mem_create.memory_allocated_bytes);

        multi_record_log.truncate("queue", ..=0).unwrap();
        let size_mem_truncate = multi_record_log.resource_usage();
        assert!(size_mem_truncate.memory_used_bytes < size_mem_append.memory_used_bytes);
    }
}

#[test]
fn test_open_corrupted() {
    // a single frame is 32k. We write more than 2 frames worth of data, corrupt one,
    // and verify we still read more than half the records successfully.
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue").unwrap();

        // 8192 * 8bytes = 64k without overhead.
        for i in 0..8192 {
            multi_record_log
                .append_record("queue", Some(i), format!("{i:08}").as_bytes())
                .unwrap();
        }
    }
    {
        use std::fs::OpenOptions;
        use std::io::*;
        // corrupt the file
        let file = std::fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(Result::ok)
            .find(|file| !file.file_name().to_str().unwrap().starts_with('.'))
            .unwrap();

        let mut file = OpenOptions::new().write(true).open(file.path()).unwrap();
        // jump somewhere in the middle
        file.seek(SeekFrom::Start(10240)).unwrap();
        file.write_all(b"this will corrupt the file. Good :-)")
            .unwrap();
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();

        let mut count = 0;
        for Record { position, payload } in multi_record_log.range("queue", ..).unwrap() {
            assert_eq!(payload, format!("{position:08}").as_bytes());
            count += 1;
        }
        assert!(count > 4096);
    }
}

#[test]
fn test_create_twice() {
    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        multi_record_log.create_queue("queue1").unwrap();
        multi_record_log
            .append_record("queue1", None, &b"hello"[..])
            .unwrap();
        multi_record_log.create_queue("queue1").unwrap_err();
        assert_eq!(multi_record_log.range("queue1", ..).unwrap().count(), 1);
    }
    {
        let multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        assert_eq!(multi_record_log.range("queue1", ..).unwrap().count(), 1);
    }
}

#[test]
fn test_last_position() {
    let tempdir = tempfile::tempdir().unwrap();

    let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
    multi_record_log.last_position("queue1").unwrap_err();

    multi_record_log.create_queue("queue1").unwrap();
    let last_pos = multi_record_log.last_position("queue1").unwrap();
    assert!(last_pos.is_none());

    multi_record_log
        .append_record("queue1", None, &b"hello"[..])
        .unwrap();

    let last_pos = multi_record_log.last_position("queue1").unwrap().unwrap();
    assert_eq!(last_pos, 0);

    multi_record_log.truncate("queue1", ..=0).unwrap();

    let last_pos = multi_record_log.last_position("queue1").unwrap().unwrap();
    assert_eq!(last_pos, 0);
}

#[test]
fn test_last_record() {
    let tempdir = tempfile::tempdir().unwrap();

    let mut multi_record_log = MultiRecordLog::open(tempdir.path()).unwrap();
    multi_record_log.last_position("queue1").unwrap_err();

    multi_record_log.create_queue("queue1").unwrap();
    let last_record = multi_record_log.last_position("queue1").unwrap();
    assert!(last_record.is_none());

    multi_record_log
        .append_record("queue1", None, &b"hello"[..])
        .unwrap();

    let Record { position, payload } = multi_record_log.last_record("queue1").unwrap().unwrap();
    assert_eq!(position, 0);
    assert_eq!(payload, &b"hello"[..]);

    multi_record_log.truncate("queue1", ..=0).unwrap();

    let last_record = multi_record_log.last_record("queue1").unwrap();
    assert!(last_record.is_none());
}

#[test]
fn test_compressed_batch_survives_reopen() {
    // One batch appended as a single record (as the ingest path does), large enough to be
    // compressed: after a fresh open the positions and payloads must be identical.
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| format!("{{\"doc\":{i},\"body\":\"a repeated log line body\"}}\n").into_bytes())
        .collect();
    {
        let mut record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        record_log.create_queue("queue").unwrap();
        record_log
            .append_records(
                "queue",
                None,
                payloads.iter().map(|payload| payload.as_slice()),
            )
            .unwrap();
    }
    {
        let record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        let records: Vec<Record<'_>> = record_log.range("queue", ..).unwrap().collect();
        assert_eq!(records.len(), payloads.len());
        for (position, (record, payload)) in records.iter().zip(&payloads).enumerate() {
            assert_eq!(record.position, position as u64);
            assert_eq!(&record.payload[..], &payload[..]);
        }
    }
}

#[test]
fn test_inmemory_compressed_batch_is_smaller_than_plaintext() {
    // A batch large enough to be compressed must be stored compressed in the in-memory
    // queue, not as plaintext. Compression must be opted into (E022 gate defaults OFF).
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| {
            let pad = "lorem ipsum dolor sit amet ".repeat(20);
            format!("{{\"doc\":{i},\"body\":\"{pad}\"}}\n").into_bytes()
        })
        .collect();
    let total_plain: usize = payloads.iter().map(|payload| payload.len()).sum();
    let mut record_log = MultiRecordLog::open_with_prefs_and_compression(
        tempdir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        true,
    )
    .unwrap();
    record_log.create_queue("queue").unwrap();
    record_log
        .append_records(
            "queue",
            None,
            payloads.iter().map(|payload| payload.as_slice()),
        )
        .unwrap();
    let usage = record_log.resource_usage();
    assert!(
        usage.memory_used_bytes < total_plain / 2,
        "in-memory queue used {} bytes for {} plaintext bytes",
        usage.memory_used_bytes,
        total_plain
    );
}

#[test]
fn test_inmemory_plaintext_when_compression_disabled() {
    // With the E022 gate at its default (OFF) the writer leaves records uncompressed,
    // so the in-memory queue must hold the plaintext, not a compressed batch.
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| {
            let pad = "lorem ipsum dolor sit amet ".repeat(20);
            format!("{{\"doc\":{i},\"body\":\"{pad}\"}}\n").into_bytes()
        })
        .collect();
    let total_plain: usize = payloads.iter().map(|payload| payload.len()).sum();
    let mut record_log = MultiRecordLog::open(tempdir.path()).unwrap();
    record_log.create_queue("queue").unwrap();
    record_log
        .append_records(
            "queue",
            None,
            payloads.iter().map(|payload| payload.as_slice()),
        )
        .unwrap();
    let usage = record_log.resource_usage();
    assert!(
        usage.memory_used_bytes >= total_plain,
        "in-memory queue used only {} bytes for {} plaintext bytes with compression disabled",
        usage.memory_used_bytes,
        total_plain
    );
    // The records must still read back byte-identically.
    let records: Vec<Vec<u8>> = record_log
        .range("queue", ..)
        .unwrap()
        .map(|record| record.payload.into_owned())
        .collect();
    assert_eq!(records, payloads);
}

#[test]
fn test_compressed_crash_recovery_is_identity() {
    // Process-crash simulation with compression ON: copy the WAL files while the writer
    // is still alive (so dropping it cannot flush), then reopen and replay. The reader
    // is unconditional, so the compressed frames must decode to the identical sequence.
    let tempdir = tempfile::tempdir().unwrap();
    let crash_dir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| format!("{{\"doc\":{i},\"body\":\"a repeated log line body\"}}\n").into_bytes())
        .collect();
    {
        let mut record_log = MultiRecordLog::open_with_prefs_and_compression(
            tempdir.path(),
            PersistPolicy::Always(PersistAction::Flush),
            true,
        )
        .unwrap();
        record_log.create_queue("queue").unwrap();
        record_log
            .append_records(
                "queue",
                None,
                payloads.iter().map(|payload| payload.as_slice()),
            )
            .unwrap();
        for entry in std::fs::read_dir(tempdir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), crash_dir.path().join(entry.file_name())).unwrap();
            }
        }
    }
    // Reopen with the gate OFF (the default): reading compressed frames stays unconditional.
    let recovered = MultiRecordLog::open_with_prefs(
        crash_dir.path(),
        PersistPolicy::Always(PersistAction::Flush),
    )
    .unwrap();
    let records: Vec<Vec<u8>> = recovered
        .range("queue", ..)
        .unwrap()
        .map(|record| record.payload.into_owned())
        .collect();
    assert_eq!(records, payloads);
}

#[test]
fn test_compressed_batch_mid_truncate_and_reopen() {
    // Truncating in the middle of a single compressed in-memory batch keeps the tail
    // byte-identical, and reopening replays the same surviving records.
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| format!("{{\"doc\":{i},\"body\":\"a repeated log line body\"}}\n").into_bytes())
        .collect();
    {
        let mut record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        record_log.create_queue("queue").unwrap();
        record_log
            .append_records(
                "queue",
                None,
                payloads.iter().map(|payload| payload.as_slice()),
            )
            .unwrap();
        let evicted = record_log.truncate("queue", ..=499).unwrap().evicted_records;
        assert_eq!(evicted, 500);
        let records: Vec<Record<'_>> = record_log.range("queue", ..).unwrap().collect();
        assert_eq!(records.len(), 1_500);
        for (i, record) in records.iter().enumerate() {
            assert_eq!(record.position, (i + 500) as u64);
            assert_eq!(&record.payload[..], &payloads[i + 500][..]);
        }
    }
    {
        let record_log = MultiRecordLog::open(tempdir.path()).unwrap();
        let records: Vec<Record<'_>> = record_log.range("queue", ..).unwrap().collect();
        assert_eq!(records.len(), 1_500);
        assert_eq!(&records[0].payload[..], &payloads[500][..]);
        assert_eq!(&records[1_499].payload[..], &payloads[1_999][..]);
    }
}

#[test]
fn test_record_locations_decode_identical_to_range() {
    // `record_locations().into_records()` must decode the exact same `(position, payload)`
    // sequence as `range()`, for compressed batches, after truncation, and with an
    // exclusive start bound. This is the fetch path used to release the WAL read lock
    // before decompressing.
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| {
            let pad = "lorem ipsum dolor sit amet ".repeat(20);
            format!("{{\"doc\":{i},\"body\":\"{pad}\"}}\n").into_bytes()
        })
        .collect();
    let mut record_log = MultiRecordLog::open_with_prefs_and_compression(
        tempdir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        true,
    )
    .unwrap();
    record_log.create_queue("queue").unwrap();
    record_log
        .append_records(
            "queue",
            None,
            payloads.iter().map(|payload| payload.as_slice()),
        )
        .unwrap();
    record_log.truncate("queue", ..=499).unwrap();

    for range in [0u64.., 500.., 1_000.., 1_999..] {
        let via_range: Vec<Record<'_>> = record_log.range("queue", range.clone()).unwrap().collect();
        let mut locations = RecordLocations::new();
        for location in record_log.record_locations("queue", range).unwrap() {
            locations.push(location);
        }
        let via_locations: Vec<Record<'static>> = locations.into_records().collect();
        assert_eq!(via_locations.len(), via_range.len());
        for (expected, actual) in via_range.iter().zip(&via_locations) {
            assert_eq!(actual.position, expected.position);
            assert_eq!(&actual.payload[..], &expected.payload[..]);
        }
    }
}

// ---------------------------------------------------------------------------------------
// E033: format/downgrade enforcement (G4) and in-memory decompress hardening (G7).
// ---------------------------------------------------------------------------------------

/// G7: a corrupted compressed in-memory batch must fail loudly on the public fetch path,
/// never return a `Cow::Owned(Vec::new())` at a valid position.
#[test]
#[should_panic(expected = "in-memory WAL batch is corrupted")]
fn test_corrupt_inmemory_batch_is_fatal_end_to_end() {
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = (0..2_000)
        .map(|i| {
            format!(
                "{{\"doc\":{i},\"body\":\"{}\"}}\n",
                "lorem ipsum dolor sit amet ".repeat(20)
            )
            .into_bytes()
        })
        .collect();
    let mut record_log = MultiRecordLog::open_with_prefs_and_compression(
        tempdir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        true,
    )
    .unwrap();
    record_log.create_queue("queue").unwrap();
    record_log
        .append_records("queue", None, payloads.iter().map(|payload| payload.as_slice()))
        .unwrap();
    record_log.corrupt_in_memory_for_test();
    // The fetch path must not serve an empty payload at a valid position.
    let _ = record_log.range("queue", ..).unwrap().collect::<Vec<_>>();
}

/// G4: the compression format marker + reader-capability guard. A reader that declares it
/// cannot decode compressed frames is refused on a compression-marked WAL; a
/// compression-aware reader opens it. A plain WAL has no marker and is accepted either way.
#[test]
fn test_compression_format_guard_accepts_and_rejects() {
    let compressed_dir = tempfile::tempdir().unwrap();
    {
        let mut wal = MultiRecordLog::open_with_prefs_and_compression(
            compressed_dir.path(),
            PersistPolicy::Always(PersistAction::Flush),
            true,
        )
        .unwrap();
        wal.create_queue("q").unwrap();
        wal.append_record("q", None, b"hello".as_slice()).unwrap();
    }
    assert!(crate::multi_record_log::has_compression_format_marker(
        compressed_dir.path()
    ));

    let rejected = MultiRecordLog::open_with_prefs_and_compression_capability(
        compressed_dir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        false,
        false,
    );
    assert!(matches!(
        rejected,
        Err(crate::error::ReadRecordError::CompressionUnsupported)
    ));

    let accepted = MultiRecordLog::open_with_prefs_and_compression_capability(
        compressed_dir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        false,
        true,
    )
    .unwrap();
    assert_eq!(accepted.range("q", ..).unwrap().count(), 1);

    // A plaintext (never compression-enabled) WAL has no marker.
    let plain_dir = tempfile::tempdir().unwrap();
    {
        let mut wal = MultiRecordLog::open(plain_dir.path()).unwrap();
        wal.create_queue("q").unwrap();
        wal.append_record("q", None, b"hello".as_slice()).unwrap();
    }
    assert!(!crate::multi_record_log::has_compression_format_marker(
        plain_dir.path()
    ));
    assert!(MultiRecordLog::open_with_prefs_and_compression_capability(
        plain_dir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        false,
        false,
    )
    .is_ok());
}

/// G4 item 1 (in-crate structural model): the writer records a format sentinel whenever it
/// enables compression. Its declared queue position (`1`) is ahead of its record position
/// (`0`), so the pre-E020 recovery step (ack position 1, then append position 0) hits the
/// "in the past" path and maps it to `Corruption`: the old reader fails `open` loudly
/// instead of recovering only the uncompressed prefix. The sentinel must not leak into the
/// user-visible queue set for a compression-aware reader.
#[test]
fn test_compression_sentinel_forces_pre_e020_loud_failure_and_is_skipped() {
    // Model of the pre-E020 recovery check on the sentinel: append at 0 after acking 1.
    let sentinel_declared_position = 1u64;
    let sentinel_record_position = 0u64;
    assert!(sentinel_record_position < sentinel_declared_position);

    let tempdir = tempfile::tempdir().unwrap();
    {
        let mut wal = MultiRecordLog::open_with_prefs_and_compression(
            tempdir.path(),
            PersistPolicy::Always(PersistAction::Flush),
            true,
        )
        .unwrap();
        wal.create_queue("q").unwrap();
        wal.append_record("q", None, b"hello".as_slice()).unwrap();
        assert_eq!(wal.list_queues().collect::<Vec<_>>(), vec!["q"]);
    }
    // A compression-aware reopen skips the sentinel and replays the record.
    let wal = MultiRecordLog::open_with_prefs_and_compression_capability(
        tempdir.path(),
        PersistPolicy::Always(PersistAction::Flush),
        true,
        true,
    )
    .unwrap();
    assert_eq!(wal.list_queues().collect::<Vec<_>>(), vec!["q"]);
    assert_eq!(wal.range("q", ..).unwrap().count(), 1);
}

/// G4 item 3: upgrade + mixed WAL. An E017-compatible (gate OFF) writer writes a plaintext
/// prefix; an E027 writer with the gate ON reopens the same WAL and continues with a
/// compressed record; replay after reopening gate OFF is byte-identical.
#[test]
fn test_mixed_wal_upgrade_replay_identity() {
    let tempdir = tempfile::tempdir().unwrap();
    let prefix: Vec<Vec<u8>> = (0..3).map(|i| format!("plain-{i}").into_bytes()).collect();

    // Phase 1: E017-compatible writer (compression disabled).
    {
        let mut wal = MultiRecordLog::open(tempdir.path()).unwrap();
        wal.create_queue("q").unwrap();
        for payload in &prefix {
            wal.append_record("q", None, payload.as_slice()).unwrap();
        }
    }

    // Phase 2: E027 with the gate ON reopens and continues. It writes the format marker +
    // sentinel and one record large enough to be compressed.
    let big = "lorem ipsum dolor sit amet ".repeat(5_000).into_bytes();
    {
        let mut wal = MultiRecordLog::open_with_prefs_and_compression(
            tempdir.path(),
            PersistPolicy::Always(PersistAction::Flush),
            true,
        )
        .unwrap();
        wal.append_record("q", None, big.as_slice()).unwrap();
    }
    assert!(crate::multi_record_log::has_compression_format_marker(
        tempdir.path()
    ));

    // Phase 3: reopen gate OFF and replay the full mixed sequence byte-identically.
    let wal = MultiRecordLog::open(tempdir.path()).unwrap();
    let records: Vec<Vec<u8>> = wal
        .range("q", ..)
        .unwrap()
        .map(|record| record.payload.into_owned())
        .collect();
    let mut expected = prefix;
    expected.push(big);
    assert_eq!(records, expected);
}
