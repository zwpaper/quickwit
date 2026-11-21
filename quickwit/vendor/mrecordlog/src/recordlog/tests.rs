use super::{RecordReader, RecordWriter};
use crate::block_read_write::ArrayReader;
use crate::error::ReadRecordError;
use crate::frame::HEADER_LEN;
use crate::{PersistAction, BLOCK_NUM_BYTES};

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
