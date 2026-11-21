use super::{RecordReader, RecordWriter};
use crate::block_read_write::ArrayReader;
use crate::error::ReadRecordError;
use crate::frame::HEADER_LEN;
use crate::{MultiRecordLog, PersistAction, PersistPolicy, BLOCK_NUM_BYTES};

#[test]
fn test_no_data() {
    let data = vec![0u8; BLOCK_NUM_BYTES * 4];
    let mut reader = RecordReader::open(ArrayReader::from(&data[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_empty_record() {
    let mut writer = RecordWriter::in_memory();
    writer.write_record("").unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(""));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_simple_record() {
    let mut writer = RecordWriter::in_memory();
    let record = "hello";
    writer.write_record(record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert!(matches!(reader.read_record::<&str>(), Ok(Some("hello"))));
    assert!(matches!(reader.read_record::<&str>(), Ok(None)));
}

fn make_long_entry(len: usize) -> String {
    "A".repeat(len)
}

#[test]
fn test_spans_over_more_than_one_block() {
    let long_entry: String = make_long_entry(80_000);
    let mut writer = RecordWriter::in_memory();
    writer.write_record(long_entry.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    let record_payload: &str = reader.read_record().unwrap().unwrap();
    assert_eq!(record_payload, &long_entry);
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_block_requires_padding() {
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_NUM_BYTES - HEADER_LEN - HEADER_LEN - 1 - 8);
    let short_record = "hello";
    let mut writer = RecordWriter::in_memory();
    writer.write_record(long_record.as_str()).unwrap();
    writer.write_record(short_record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buffer: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(short_record));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_first_chunk_empty() {
    // We'll miss 1 byte to be able to fit our next chunk header in the
    // first block.
    let long_record = make_long_entry(BLOCK_NUM_BYTES - HEADER_LEN - HEADER_LEN);
    let short_record = "hello";
    let mut writer = RecordWriter::in_memory();
    writer.write_record(&long_record[..]).unwrap();
    writer.write_record(short_record).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(
        reader.read_record::<&str>().unwrap(),
        Some(long_record.as_str())
    );
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(short_record));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_behavior_upon_corruption() {
    let records: Vec<String> = (0..1_000).map(|i| format!("hello{i}")).collect();
    let mut writer = RecordWriter::in_memory();
    for record in &records {
        writer.write_record(record.as_str()).unwrap();
    }
    writer.persist(PersistAction::Flush).unwrap();
    let mut buffer: Vec<u8> = writer.into_writer().into();
    {
        let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
        for record in &records {
            assert_eq!(reader.read_record::<&str>().unwrap(), Some(record.as_str()));
        }
        assert_eq!(reader.read_record::<&str>().unwrap(), None);
    }
    // Introducing a corruption.
    buffer[1_000] = 3;
    {
        let mut reader = RecordReader::open(ArrayReader::from(&buffer[..]));
        for record in &records[0..72] {
            // bug at i=72
            assert_eq!(reader.read_record::<&str>().unwrap(), Some(record.as_str()));
        }
        assert!(matches!(
            reader.read_record::<&str>(),
            Err(ReadRecordError::Corruption)
        ));
    }
}

#[test]
fn test_compressed_record_roundtrip() {
    // A record above the compression threshold made of highly repetitive bytes: it must
    // be stored compressed and decode back byte-for-byte. `write_record` returns the
    // bytes pushed to the underlying writer (the in-memory writer zero-pads its buffer,
    // so its `Vec` length is not the record size).
    let record = "abcdefgh".repeat(20_000); // 160_000 bytes
    let mut writer = RecordWriter::in_memory();
    let num_bytes_written = writer.write_record(record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    assert!(
        num_bytes_written as usize <= record.len() / 8,
        "expected >=8x compression, got {} -> {}",
        record.len(),
        num_bytes_written
    );
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(record.as_str()));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

#[test]
fn test_compressed_record_spans_multiple_frames() {
    // ~4.4 MB of NDJSON shaped like the ingest payload: it still exceeds one 32 KiB
    // frame once compressed, so the compressed record has to be split across several
    // frames and reassembled.
    let record: String = (0..20_000)
        .map(|i| {
            format!(
                "{{\"timestamp\":{},\"severity_text\":\"INFO\",\"body\":\"request completed successfully\",\"attributes\":{{\"http.status_code\":200,\"http.method\":\"GET\"}},\"resource_attributes\":{{\"service.name\":\"frontend\",\"host.name\":\"host-{}\"}}}}\n",
                1_758_585_600_000u64 + i,
                i % 16
            )
        })
        .collect();
    let mut writer = RecordWriter::in_memory();
    let num_bytes_written = writer.write_record(record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    assert!(
        num_bytes_written as usize > BLOCK_NUM_BYTES,
        "expected a multi-frame compressed record, got {num_bytes_written} bytes"
    );
    assert!(num_bytes_written as usize <= record.len() / 8);
    let buf: Vec<u8> = writer.into_writer().into();
    let mut reader = RecordReader::open(ArrayReader::from(&buf[..]));
    assert_eq!(reader.read_record::<&str>().unwrap(), Some(record.as_str()));
    assert_eq!(reader.read_record::<&str>().unwrap(), None);
}

/// Independent re-implementation of the pre-compression (E017) frame encoding for one full
/// frame, kept as a frozen oracle: 7-byte header `[crc32(type_byte || payload):4 LE]
/// [len:2 LE][type_byte:1]`. Recomputing it here (rather than round-tripping through the
/// production reader) makes the gate-OFF byte-identity check meaningful.
fn e017_full_frame(type_byte: u8, payload: &[u8]) -> Vec<u8> {
    let mut hasher = crc32fast::Hasher::default();
    hasher.update(&[type_byte]);
    hasher.update(payload);
    let checksum = hasher.finalize();
    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    out.push(type_byte);
    out.extend_from_slice(payload);
    out
}

#[test]
fn test_compression_disabled_matches_e017_frame_bytes() {
    // A 10 KiB record is above the 8 KiB compression threshold, so a compression-enabled
    // writer would compress it; with the gate OFF it must be laid out exactly as E017 did.
    let record = "A".repeat(10_000);
    let mut writer = RecordWriter::in_memory();
    writer.set_compression_enabled(false);
    writer.write_record(record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();

    let mut expected = e017_full_frame(1u8, record.as_bytes());
    assert!(expected.len() <= BLOCK_NUM_BYTES);
    expected.resize(BLOCK_NUM_BYTES, 0u8);
    assert_eq!(
        buf, expected,
        "gate-OFF on-disk bytes differ from the E017 frame format"
    );

    let mut enabled_writer = RecordWriter::in_memory();
    enabled_writer.set_compression_enabled(true);
    enabled_writer.write_record(record.as_str()).unwrap();
    enabled_writer.persist(PersistAction::Flush).unwrap();
    let enabled_buf: Vec<u8> = enabled_writer.into_writer().into();
    assert_ne!(
        enabled_buf, expected,
        "compression-enabled writer unexpectedly produced uncompressed bytes"
    );
}

#[test]
fn test_compression_enabled_reopen_preserves_positions_and_payloads() {
    let tempdir = tempfile::tempdir().unwrap();
    let payloads: Vec<Vec<u8>> = vec![
        b"small".to_vec(),
        "abcdefgh".repeat(20_000).into_bytes(),
        b"tail".to_vec(),
    ];

    let expected: Vec<(u64, Vec<u8>)> = {
        let mut wal = MultiRecordLog::open_with_prefs_and_compression(
            tempdir.path(),
            PersistPolicy::Always(PersistAction::Flush),
            true,
        )
        .unwrap();
        wal.create_queue("q").unwrap();
        for payload in &payloads {
            wal.append_record("q", None, payload.as_slice()).unwrap();
        }
        wal.range("q", 0..)
            .unwrap()
            .map(|record| (record.position, record.payload.into_owned()))
            .collect()
    };
    assert_eq!(
        expected
            .iter()
            .map(|(_, payload)| payload.clone())
            .collect::<Vec<_>>(),
        payloads
    );

    // Reopening with compression disabled must still read the compressed WAL (the reader
    // is unconditional) and recover the identical (position, payload) sequence.
    let wal = MultiRecordLog::open_with_prefs(
        tempdir.path(),
        PersistPolicy::Always(PersistAction::Flush),
    )
    .unwrap();
    let reopened: Vec<(u64, Vec<u8>)> = wal
        .range("q", 0..)
        .unwrap()
        .map(|record| (record.position, record.payload.into_owned()))
        .collect();
    assert_eq!(reopened, expected);
}

/// The pre-compression reader's strict `FrameType::from_u8` whitelist, reproduced verbatim.
fn old_reader_frame_type(type_byte: u8) -> Option<u8> {
    match type_byte {
        1 | 2 | 3 | 4 => Some(type_byte),
        _ => None,
    }
}

#[test]
fn test_old_reader_rejects_compressed_frames_without_misdecoding() {
    // ~4.4 MB of NDJSON: it compresses to more than one 32 KiB frame, so the check
    // covers every frame type.
    let record: String = (0..20_000)
        .map(|i| {
            format!(
                "{{\"timestamp\":{},\"severity_text\":\"INFO\",\"body\":\"request completed successfully\",\"attributes\":{{\"http.status_code\":200,\"http.method\":\"GET\"}},\"resource_attributes\":{{\"service.name\":\"frontend\",\"host.name\":\"host-{}\"}}}}\n",
                1_758_585_600_000u64 + i,
                i % 16
            )
        })
        .collect();
    let mut writer = RecordWriter::in_memory();
    writer.set_compression_enabled(true);
    writer.write_record(record.as_str()).unwrap();
    writer.persist(PersistAction::Flush).unwrap();
    let buf: Vec<u8> = writer.into_writer().into();

    let mut offset = 0usize;
    let mut num_frames = 0usize;
    loop {
        let num_bytes_to_end_of_block = BLOCK_NUM_BYTES - (offset % BLOCK_NUM_BYTES);
        if num_bytes_to_end_of_block < HEADER_LEN {
            offset += num_bytes_to_end_of_block;
        }
        if offset + HEADER_LEN > buf.len() {
            break;
        }
        let header = &buf[offset..offset + HEADER_LEN];
        if header == [0u8; HEADER_LEN] {
            break;
        }
        let type_byte = header[HEADER_LEN - 1];
        let payload_len = u16::from_le_bytes([header[4], header[5]]) as usize;
        // An old binary masks nothing and feeds the raw type byte to its whitelist.
        assert_eq!(
            old_reader_frame_type(type_byte),
            None,
            "old reader would have decoded raw type byte {type_byte:#04x} instead of corruption"
        );
        offset += HEADER_LEN + payload_len;
        num_frames += 1;
    }
    assert!(
        num_frames > 1,
        "expected a multi-frame compressed record, found {num_frames} frames"
    );
}
