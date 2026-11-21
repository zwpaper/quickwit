use super::*;
use crate::{BlockRead, BlockWrite, PersistAction, BLOCK_NUM_BYTES};
use std::sync::mpsc;
use std::time::Duration;

#[test]
fn test_read_write() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut buffer = [0u8; BLOCK_NUM_BYTES];
    {
        let rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
        assert!(&rolling_reader.block().iter().all(|&b| b == 0));
        let mut writer: RollingWriter = rolling_reader.into_writer().unwrap();
        buffer.fill(0u8);
        writer.write(&buffer[..]).unwrap();
        buffer.fill(1u8);
        writer.write(&buffer[..]).unwrap();
        buffer.fill(2u8);
        writer.write(&buffer[..]).unwrap();
        writer.persist(PersistAction::Flush).unwrap();
    }
    let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
    assert!(rolling_reader.block().iter().all(|&b| b == 0));
    assert!(rolling_reader.next_block().unwrap());
    assert!(rolling_reader.block().iter().all(|&b| b == 1));
    assert!(rolling_reader.next_block().unwrap());
    assert!(rolling_reader.block().iter().all(|&b| b == 2));
}

#[test]
fn test_read_write_2nd_block() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut buffer = [0u8; BLOCK_NUM_BYTES];
    {
        let rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
        let mut writer: RollingWriter = rolling_reader.into_writer().unwrap();
        for i in 1..=10 {
            buffer.fill(i);
            writer.write(&buffer[..]).unwrap();
        }
        writer.persist(PersistAction::Flush).unwrap();
    }
    {
        let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
        assert!(rolling_reader.block().iter().all(|&b| b == 1));
        assert!(rolling_reader.next_block().unwrap());
        assert!(rolling_reader.block().iter().all(|&b| b == 2));
        assert!(rolling_reader.next_block().unwrap());
        assert!(rolling_reader.block().iter().all(|&b| b == 3));
        let mut writer: RollingWriter = rolling_reader.into_writer().unwrap();
        for i in 13..=23 {
            buffer.fill(i);
            writer.write(&buffer[..]).unwrap();
        }
        writer.persist(PersistAction::Flush).unwrap();
    }
    {
        let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
        assert!(rolling_reader.block().iter().all(|&b| b == 1));
        assert!(rolling_reader.next_block().unwrap());
        assert!(rolling_reader.block().iter().all(|&b| b == 2));
        for i in 13..=23 {
            assert!(rolling_reader.next_block().unwrap());
            assert!(rolling_reader.block().iter().all(|&b| b == i));
        }
    }
}

#[test]
fn test_read_truncated() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut buffer = [0u8; BLOCK_NUM_BYTES];
    let to_write = NUM_BLOCKS_PER_FILE * 3;
    {
        let rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
        let mut writer: RollingWriter = rolling_reader.into_writer().unwrap();
        for i in 0..to_write {
            buffer.fill(i as u8);
            writer.write(&buffer[..]).unwrap();
        }
        writer.persist(PersistAction::Flush).unwrap();
        let file_ids = writer.list_file_numbers();
        let middle_file = file_ids[1];
        let filepath =
            crate::rolling::directory::filepath(tmp_dir.path(), &FileNumber::for_test(middle_file));

        // voluntarily corrupt data by truncating a wal file.
        std::fs::OpenOptions::new()
            .truncate(true)
            .write(true)
            .open(filepath)
            .unwrap();
    }
    {
        let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();

        for i in 0..to_write {
            // ignore file 1 as it was corrupted
            if i / NUM_BLOCKS_PER_FILE == 1 {
                continue;
            }
            assert!(rolling_reader.block().iter().all(|&b| b == i as u8));
            // check we manage to get the next block, except for the last block: there is nothing
            // after
            assert_eq!(rolling_reader.next_block().unwrap(), i != to_write - 1);
        }
    }
}

#[test]
fn test_directory_single_file() {
    let tmp_dir = tempfile::tempdir().unwrap();
    {
        let directory = Directory::open(tmp_dir.path()).unwrap();
        let first_file = directory.first_file_number();
        assert_eq!(first_file.unroll(&directory.files), &[0]);
    }
    let mut rolling_reader: RollingReader = RollingReader::open(tmp_dir.path()).unwrap();
    for _ in 0..NUM_BLOCKS_PER_FILE - 1 {
        assert!(rolling_reader.next_block().unwrap());
    }
    assert!(!rolling_reader.next_block().unwrap());
}

#[test]
fn test_directory_simple() {
    let tmp_dir = tempfile::tempdir().unwrap();
    {
        let mut writer: RollingWriter = RollingReader::open(tmp_dir.path())
            .unwrap()
            .into_writer()
            .unwrap();
        let buf = vec![1u8; FRAME_NUM_BYTES];
        for _ in 0..(NUM_BLOCKS_PER_FILE + 1) {
            writer.write(&buf).unwrap();
        }
    }
    {
        let directory = Directory::open(tmp_dir.path()).unwrap();
        let first_file: &FileNumber = directory.first_file_number();
        assert_eq!(first_file.unroll(&directory.files), &[0, 1]);
    }
}

#[test]
fn test_directory_truncate() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let file_0: FileNumber;
    let file_1: FileNumber;
    let file_2: FileNumber;
    let file_3: FileNumber;
    {
        let reader = RollingReader::open(tmp_dir.path()).unwrap();
        file_0 = reader.current_file().clone();
        assert!(!file_0.can_be_deleted());
        let mut writer: RollingWriter = reader.into_writer().unwrap();
        let buf = vec![1u8; FRAME_NUM_BYTES];
        assert_eq!(&writer.current_file().unroll(&writer.directory.files), &[0]);
        for _ in 0..NUM_BLOCKS_PER_FILE + 1 {
            writer.write(&buf).unwrap();
        }
        assert_eq!(&writer.list_file_numbers(), &[0, 1]);
        file_1 = writer.current_file().clone();
        assert_eq!(file_1.file_number(), 1);
        for _ in 0..NUM_BLOCKS_PER_FILE {
            writer.write(&buf).unwrap();
        }
        assert_eq!(&writer.list_file_numbers(), &[0, 1, 2]);
        file_2 = writer.current_file().clone();
        assert_eq!(file_2.file_number(), 2);
        for _ in 0..NUM_BLOCKS_PER_FILE {
            writer.write(&buf).unwrap();
        }
        file_3 = writer.current_file().clone();
        assert_eq!(&writer.list_file_numbers(), &[0, 1, 2, 3]);
        assert!(!file_0.can_be_deleted());
        drop(file_1);
        writer.directory.gc().unwrap();
        assert_eq!(&writer.list_file_numbers(), &[0, 1, 2, 3]);
        drop(file_0);
        writer.directory.gc().unwrap();
        assert_eq!(&writer.list_file_numbers(), &[2, 3]);
        drop(file_2);
        writer.directory.gc().unwrap();
        assert_eq!(&writer.list_file_numbers(), &[3]);
        drop(file_3);
        writer.directory.gc().unwrap();
        assert_eq!(&writer.list_file_numbers(), &[3]);
    }
}

/// A roll must not fsync inline: the completed file's fsync is deferred to the background thread,
/// and exactly one deferred fsync runs per roll.
#[test]
fn test_roll_fsync_is_deferred() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut writer: RollingWriter = RollingReader::open(tmp_dir.path())
        .unwrap()
        .into_writer()
        .unwrap();
    // Gate the background thread so the write cannot have been fsynced when it returns.
    writer.close_roll_fsync_gate();
    let buf = vec![1u8; FRAME_NUM_BYTES];
    // NUM_BLOCKS_PER_FILE blocks fill a file exactly; the next write rolls it.
    for _ in 0..=NUM_BLOCKS_PER_FILE {
        writer.write(&buf).unwrap();
    }
    assert_eq!(writer.list_file_numbers(), &[0, 1]);
    // The roll write returned without fsyncing inline: the background thread is still gated and
    // has run zero fsyncs.
    assert_eq!(writer.deferred_roll_fsync_count(), 0);
    writer.open_roll_fsync_gate();
    writer.drain_roll_fsync().unwrap();
    // Exactly one deferred fsync for the single roll.
    assert_eq!(writer.deferred_roll_fsync_count(), 1);
}

/// GC must join the pending roll fsync *before* it deletes the completed file. While the
/// background fsync is gated, a `gc` run on another thread must block in `drain` and leave the
/// file on disk; once the gate opens the fsync runs and only then is the file removed.
#[test]
fn test_gc_drains_pending_roll_fsync_before_delete() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut writer: RollingWriter = RollingReader::open(tmp_dir.path())
        .unwrap()
        .into_writer()
        .unwrap();
    let gate = writer.roll_fsync_gate();
    gate.close();
    let buf = vec![1u8; FRAME_NUM_BYTES];
    for _ in 0..=NUM_BLOCKS_PER_FILE {
        writer.write(&buf).unwrap();
    }
    assert_eq!(writer.list_file_numbers(), &[0, 1]);
    let first_path = crate::rolling::directory::filepath(
        tmp_dir.path(),
        &FileNumber::for_test(0),
    );
    assert!(first_path.exists());
    assert_eq!(writer.deferred_roll_fsync_count(), 0);

    let (done_tx, done_rx) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let result = writer.gc();
        let _ = done_tx.send(result.is_ok());
    });
    // The background fsync is still gated, so `gc` is blocked in `drain` and must not have
    // deleted the completed file yet.
    assert!(
        done_rx.recv_timeout(Duration::from_millis(300)).is_err(),
        "gc returned before the pending roll fsync drained"
    );
    assert!(
        first_path.exists(),
        "completed wal file was deleted while its fsync was still pending"
    );

    gate.open();
    assert_eq!(done_rx.recv_timeout(Duration::from_secs(10)).unwrap(), true);
    handle.join().unwrap();
    assert!(
        !first_path.exists(),
        "gc did not delete the completed wal file after draining its fsync"
    );
}

/// A background roll-fsync failure must surface on the next write, matching "a failed roll fsync
/// fails the append" pre-E017 behaviour. The directory is removed while the fsync is gated so the
/// directory sync fails deterministically.
#[test]
fn test_background_roll_fsync_error_surfaces_on_next_write() {
    let tmp_dir = tempfile::tempdir().unwrap();
    let mut writer: RollingWriter = RollingReader::open(tmp_dir.path())
        .unwrap()
        .into_writer()
        .unwrap();
    let gate = writer.roll_fsync_gate();
    gate.close();
    let buf = vec![1u8; FRAME_NUM_BYTES];
    for _ in 0..=NUM_BLOCKS_PER_FILE {
        writer.write(&buf).unwrap();
    }
    assert_eq!(writer.list_file_numbers(), &[0, 1]);
    assert_eq!(writer.deferred_roll_fsync_count(), 0);

    // Removing the directory makes the background `sync_directory` fail with ENOENT. The file
    // handle itself stays valid, so the data fsync is not the failure source.
    std::fs::remove_dir_all(tmp_dir.path()).unwrap();
    gate.open();
    for _ in 0..1_000 {
        if writer.deferred_roll_fsync_count() == 1 {
            break;
        }
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(writer.deferred_roll_fsync_count(), 1);

    let err = writer.write(&buf).unwrap_err();
    assert!(
        err.to_string().contains("background roll fsync failed"),
        "unexpected error: {err}"
    );
    // The first error is surfaced once and then cleared, so a later write is not failed by the
    // stale background error.
    writer.write(&buf).unwrap();
    writer.drain_roll_fsync().unwrap();
}

