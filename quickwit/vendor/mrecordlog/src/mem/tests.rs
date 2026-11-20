use super::*;
use crate::error::{AlreadyExists, AppendError};
use crate::rolling::FileNumber;
use crate::Record;

#[test]
fn test_mem_queues_already_exists() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(matches!(
        mem_queues.create_queue("droopy"),
        Err(AlreadyExists)
    ));
}

#[test]
fn test_mem_queues() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    mem_queues.create_queue("fable").unwrap();
    {
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 0, b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 1, b"happy")
            .is_ok());
    }

    {
        assert!(mem_queues
            .append_record("fable", &FileNumber::for_test(1), 0, b"maitre")
            .is_ok());
        assert!(mem_queues
            .append_record("fable", &FileNumber::for_test(1), 1, b"corbeau")
            .is_ok());
    }

    {
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 2, b"tax")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &FileNumber::for_test(1), 3, b"payer")
            .is_ok());
        assert_eq!(
            mem_queues.range("droopy", 0..).unwrap().next(),
            Some(Record::new(0, b"hello"))
        );
        let droopy: Vec<Record> = mem_queues.range("droopy", 1..).unwrap().collect();
        assert_eq!(
            &droopy,
            &[
                Record::new(1, b"happy"),
                Record::new(2, b"tax"),
                Record::new(3, b"payer"),
            ],
        );
    }
    let fable: Vec<Record> = mem_queues.range("fable", 1..).unwrap().collect();
    assert_eq!(&fable, &[Record::new(1, b"corbeau")]);
}

#[test]
fn test_mem_queues_truncate() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    {
        assert!(mem_queues
            .append_record("droopy", &1.into(), 0, b"hello")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 1, b"happy")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 2, b"tax")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 3, b"payer")
            .is_ok());
        assert!(mem_queues
            .append_record("droopy", &1.into(), 4, b"!")
            .is_ok());
        mem_queues
            .append_record("droopy", &1.into(), 5, b"payer")
            .unwrap();
    }
    mem_queues.truncate("droopy", ..=3);
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[Record::new(4, b"!"), Record::new(5, b"payer"),]
    );
}

#[test]
fn test_mem_queues_skip_advance() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 2, b"happy")
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 3, b"happy")
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 1, b"happy")
        .is_err());
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[
            Record::new(0, b"hello"),
            Record::new(2, b"happy"),
            Record::new(3, b"happy"),
        ]
    );
    let droopy: Vec<Record> = mem_queues.range("droopy", 1..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[Record::new(2, b"happy"), Record::new(3, b"happy"),]
    );
    let droopy: Vec<Record> = mem_queues.range("droopy", 2..).unwrap().collect();
    assert_eq!(
        &droopy[..],
        &[Record::new(2, b"happy"), Record::new(3, b"happy"),]
    );
    let droopy: Vec<Record> = mem_queues.range("droopy", 3..).unwrap().collect();
    assert_eq!(&droopy[..], &[Record::new(3, b"happy")]);
}

#[test]
fn test_mem_queues_append_in_the_past_yield_error() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .is_ok());
    assert!(mem_queues
        .append_record("droopy", &1.into(), 1, b"happy")
        .is_ok());
    assert!(matches!(
        mem_queues.append_record("droopy", &1.into(), 0, b"happy"),
        Err(AppendError::Past)
    ));
}

#[test]
fn test_mem_queues_append_idempotence() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 0, b"hello")
        .is_ok());
    assert!(matches!(
        mem_queues
            .append_record("droopy", &1.into(), 0, b"different")
            .unwrap_err(),
        AppendError::Past
    ));
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(&droopy, &[Record::new(0, b"hello")]);
}

#[test]
fn test_mem_queues_non_zero_first_el() {
    let mut mem_queues = MemQueues::default();
    mem_queues.create_queue("droopy").unwrap();
    assert!(mem_queues
        .append_record("droopy", &1.into(), 5, b"hello")
        .is_ok());
    let droopy: Vec<Record> = mem_queues.range("droopy", 0..).unwrap().collect();
    assert_eq!(droopy, &[Record::new(5, b"hello")]);
}

#[test]
fn test_corrupt_compressed_inmemory_batch_is_fatal_not_empty() {
    // G7 regression: a corrupted compressed in-memory batch must fail loudly. It must
    // never decode to an empty payload at a valid position (the ack-and-truncate path).
    let plain: Vec<u8> = b"a repeated in-memory log line body ".repeat(400);
    let compressed = zstd::bulk::compress(&plain, 1).unwrap();
    let mut blob = Vec::with_capacity(4 + compressed.len());
    blob.extend_from_slice(&(plain.len() as u32).to_le_bytes());
    blob.extend_from_slice(&compressed);

    let mut queue = MemQueue::default();
    queue
        .append_batch(
            &FileNumber::for_test(1),
            blob,
            true,
            std::iter::once((0u64, 0usize, plain.len())),
        )
        .unwrap();
    // Sanity: the batch decodes before corruption.
    assert_eq!(queue.range(..).next().unwrap().payload.as_ref(), &plain[..]);

    queue.inject_compressed_batch_corruption();

    let range_panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = queue.range(..).collect::<Vec<_>>();
    }));
    assert!(
        range_panicked.is_err(),
        "corrupt in-memory batch must be fatal in range(), not a valid-position empty payload"
    );

    let last_panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = queue.last_record();
    }));
    assert!(
        last_panicked.is_err(),
        "corrupt in-memory batch must be fatal in last_record()"
    );

    let mut locations = RecordLocations::new();
    for location in queue.record_locations(..) {
        locations.push(location);
    }
    let fetch_panicked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = locations.into_records().collect::<Vec<_>>();
    }));
    assert!(
        fetch_panicked.is_err(),
        "corrupt in-memory batch must be fatal on the off-lock fetch path"
    );
}

#[test]
fn test_mem_queues_keep_filenum() {
    let mut mem_queues = MemQueues::default();

    let files = (0..4).map(FileNumber::for_test).collect::<Vec<_>>();

    assert!(files.iter().all(FileNumber::can_be_deleted));

    mem_queues.create_queue("droopy").unwrap();
    mem_queues
        .append_record("droopy", &files[0], 0, b"hello")
        .unwrap();

    assert!(!files[0].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[0], 1, b"hello")
        .unwrap();

    assert!(!files[0].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[0], 2, b"hello")
        .unwrap();

    assert!(!files[0].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[1], 3, b"hello")
        .unwrap();

    assert!(!files[0].can_be_deleted());
    assert!(!files[1].can_be_deleted());

    mem_queues.truncate("droopy", ..=1);

    assert!(!files[0].can_be_deleted());
    assert!(!files[1].can_be_deleted());

    mem_queues
        .append_record("droopy", &files[2], 4, b"hello")
        .unwrap();

    assert!(!files[0].can_be_deleted());
    assert!(!files[1].can_be_deleted());
    assert!(!files[2].can_be_deleted());

    mem_queues.truncate("droopy", ..=3);

    assert!(files[0].can_be_deleted());
    assert!(files[1].can_be_deleted());
    assert!(!files[2].can_be_deleted());

    mem_queues.truncate("droopy", ..=4);

    let empty_queues = mem_queues.empty_queues().collect::<Vec<_>>();
    assert_eq!(empty_queues.len(), 1);
    assert_eq!(empty_queues[0].0, "droopy");

    mem_queues.ack_position("droopy", 5);

    assert!(files[2].can_be_deleted());
}
