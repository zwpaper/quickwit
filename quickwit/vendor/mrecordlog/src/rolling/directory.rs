use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;

#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::Condvar;

use tracing::{info, warn};

use super::{FileNumber, FileTracker};
use crate::rolling::{FILE_NUM_BYTES, FRAME_NUM_BYTES};
use crate::{BlockRead, BlockWrite, PersistAction, BLOCK_NUM_BYTES};

/// Fsync a directory so that the creation/removal of the files it contains is durable.
fn sync_directory(dir: &Path) -> io::Result<()> {
    let mut open_opts = OpenOptions::new();
    // Linux needs read to be set, otherwise returns EINVAL
    // write must not be set, or it fails with EISDIR
    open_opts.read(true);
    let fd = open_opts.open(dir)?;
    fd.sync_data()?;
    Ok(())
}

/// Bounded roll-fsync queue depth.
///
/// The completed files sitting in the queue are the power-loss exposure window; a full queue
/// blocks the writer (backpressure), which keeps that window bounded and keeps the WAL from
/// growing without limit.
const ROLL_FSYNC_QUEUE_DEPTH: usize = 4;

/// A message for the background roll-fsync thread.
enum RollSyncMsg {
    /// Fsync the completed file's data and its directory entry before the file may be deleted.
    Sync { dir: PathBuf, file: File },
    /// Barrier: ack once every previously queued `Sync` has completed. The thread stays alive.
    Drain(mpsc::Sender<()>),
    /// Stop the thread.
    Shutdown,
}

/// Test-only gate and counter, used to observe that a roll schedules its fsync instead of running
/// it inline on the write path.
#[cfg(test)]
struct SyncGate {
    open: Mutex<bool>,
    cv: Condvar,
    sync_count: AtomicUsize,
}

#[cfg(test)]
impl SyncGate {
    fn new() -> Self {
        SyncGate {
            open: Mutex::new(true),
            cv: Condvar::new(),
            sync_count: AtomicUsize::new(0),
        }
    }

    fn wait_open(&self) {
        let mut open = self.open.lock().unwrap();
        while !*open {
            open = self.cv.wait(open).unwrap();
        }
    }

    fn close(&self) {
        *self.open.lock().unwrap() = false;
    }

    fn open(&self) {
        *self.open.lock().unwrap() = true;
        self.cv.notify_all();
    }

    fn count(&self) -> usize {
        self.sync_count.load(Ordering::SeqCst)
    }
}

/// Runs the file-roll `sync_data` + `sync_directory` off the ingester's write path.
///
/// A roll used to fsync the completed 128 MiB file inline while the ingester held the WAL write
/// lock, so the client ack waited on device fsync latency. The syncer runs those fsyncs on a
/// dedicated thread; its bounded queue preserves backpressure, and `drain` joins every pending
/// fsync before a rolled file is deleted (`Directory::gc`) or the writer is closed.
struct RollSyncer {
    tx: mpsc::SyncSender<RollSyncMsg>,
    /// The first background fsync error, surfaced on the next write/drain.
    err: Arc<Mutex<Option<io::Error>>>,
    handle: Option<JoinHandle<()>>,
    #[cfg(test)]
    gate: Arc<SyncGate>,
}

impl RollSyncer {
    fn new() -> io::Result<Self> {
        let (tx, rx) = mpsc::sync_channel::<RollSyncMsg>(ROLL_FSYNC_QUEUE_DEPTH);
        let err: Arc<Mutex<Option<io::Error>>> = Arc::new(Mutex::new(None));
        let err_slot = Arc::clone(&err);
        #[cfg(test)]
        let gate = Arc::new(SyncGate::new());
        #[cfg(test)]
        let gate_thread = Arc::clone(&gate);
        let handle = std::thread::Builder::new()
            .name("mrecordlog-roll-fsync".to_string())
            .spawn(move || {
                while let Ok(msg) = rx.recv() {
                    match msg {
                        RollSyncMsg::Sync { dir, file } => {
                            #[cfg(test)]
                            gate_thread.wait_open();
                            let result = file.sync_data().and_then(|()| sync_directory(&dir));
                            #[cfg(test)]
                            gate_thread.sync_count.fetch_add(1, Ordering::SeqCst);
                            if let Err(sync_err) = result {
                                let mut slot = err_slot.lock().unwrap();
                                if slot.is_none() {
                                    *slot = Some(sync_err);
                                }
                            }
                        }
                        RollSyncMsg::Drain(ack) => {
                            let _ = ack.send(());
                        }
                        RollSyncMsg::Shutdown => break,
                    }
                }
            })?;
        Ok(RollSyncer {
            tx,
            err,
            handle: Some(handle),
            #[cfg(test)]
            gate,
        })
    }

    /// Queue a completed file for a background `sync_data` + `sync_directory`.
    ///
    /// Blocks while the queue is full: that is the backpressure that keeps the WAL from growing
    /// without limit (an unbounded queue is the DIAG's ENOSPC failure mode).
    fn schedule(&self, dir: PathBuf, file: File) -> io::Result<()> {
        self.tx.send(RollSyncMsg::Sync { dir, file }).map_err(|_| {
            io::Error::new(
                io::ErrorKind::BrokenPipe,
                "background roll fsync thread stopped",
            )
        })
    }

    /// Surface (once) the first background fsync error on the write path.
    fn check(&mut self) -> io::Result<()> {
        match self.err.lock().unwrap().take() {
            Some(err) => Err(io::Error::new(
                err.kind(),
                format!("background roll fsync failed: {err}"),
            )),
            None => Ok(()),
        }
    }

    /// Wait until every fsync scheduled so far has completed, then surface any error. The thread
    /// stays alive for later rolls.
    fn drain(&mut self) -> io::Result<()> {
        let (ack_tx, ack_rx) = mpsc::channel();
        if self.tx.send(RollSyncMsg::Drain(ack_tx)).is_ok() {
            let _ = ack_rx.recv();
        }
        self.check()
    }

    /// Stop the background thread and wait for it. Idempotent.
    fn shutdown(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.tx.send(RollSyncMsg::Shutdown);
            let _ = handle.join();
        }
    }

    #[cfg(test)]
    fn close_gate(&self) {
        self.gate.close();
    }

    #[cfg(test)]
    fn open_gate(&self) {
        self.gate.open();
    }

    #[cfg(test)]
    fn sync_count(&self) -> usize {
        self.gate.count()
    }
}

pub struct Directory {
    dir: PathBuf,
    pub(crate) files: FileTracker,
}

fn filename_to_position(file_name: &str) -> Option<u64> {
    if file_name.len() != 24 {
        return None;
    }
    if !file_name.starts_with("wal-") {
        return None;
    }
    let seq_number_str = &file_name[4..];
    if !seq_number_str.as_bytes().iter().all(u8::is_ascii_digit) {
        return None;
    }
    file_name[4..].parse::<u64>().ok()
}

pub(crate) fn filepath(dir: &Path, file_number: &FileNumber) -> PathBuf {
    dir.join(file_number.filename())
}

fn create_file(dir_path: &Path, file_number: &FileNumber) -> io::Result<File> {
    let new_filepath = filepath(dir_path, file_number);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(new_filepath)?;
    file.set_len(FILE_NUM_BYTES as u64)?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

impl Directory {
    /// Open a `Directory`, or create a new, empty, one. `dir_path` must exist and be a directory.
    pub fn open(dir_path: &Path) -> io::Result<Directory> {
        let mut file_numbers: Vec<u64> = Default::default();
        for dir_entry_res in std::fs::read_dir(dir_path)? {
            let dir_entry = dir_entry_res?;
            if !dir_entry.file_type()?.is_file() {
                continue;
            }
            let file_name = if let Some(file_name) = dir_entry.file_name().to_str() {
                file_name.to_string()
            } else {
                continue;
            };
            if let Some(seq_number) = filename_to_position(&file_name) {
                file_numbers.push(seq_number);
            }
        }
        let files = if let Some(files) = FileTracker::from_file_numbers(file_numbers) {
            files
        } else {
            let files = FileTracker::new();
            let file_number = files.first();
            create_file(dir_path, file_number)?;
            files
        };
        Ok(Directory {
            dir: dir_path.to_path_buf(),
            files,
        })
    }

    /// Get the first still used FileNumber.
    pub fn first_file_number(&self) -> &FileNumber {
        self.files.first()
    }

    /// Returns true if some file could be GCed.
    pub fn has_files_that_can_be_deleted(&self) -> bool {
        self.files.count() >= 2 && self.files.first().can_be_deleted()
    }

    /// Delete FileNumbers and the associated wal files no longer used.
    ///
    /// We never delete the last file.
    pub(crate) fn gc(&mut self) -> io::Result<()> {
        while let Some(file) = self.files.take_first_unused() {
            let filepath = filepath(&self.dir, &file);
            info!(file=%filepath.display(), "gc remove file");
            std::fs::remove_file(&filepath)?;
        }
        Ok(())
    }

    /// Open the wal file with the provided FileNumber.
    pub fn open_file(&self, file_number: &FileNumber) -> io::Result<File> {
        let filepath = filepath(&self.dir, file_number);
        let mut file = OpenOptions::new().read(true).write(true).open(filepath)?;
        file.seek(SeekFrom::Start(0u64))?;
        Ok(file)
    }

    fn sync_directory(&self) -> io::Result<()> {
        sync_directory(&self.dir)
    }
}

pub struct RollingReader {
    file: File,
    directory: Directory,
    file_number: FileNumber,
    block_id: usize,
    block: Box<[u8; BLOCK_NUM_BYTES]>,
}

impl RollingReader {
    /// Open a directory for reading.
    pub fn open(dir_path: &Path) -> io::Result<Self> {
        let directory = Directory::open(dir_path)?;
        let first_file = directory.first_file_number().clone();
        let mut file = directory.open_file(&first_file)?;
        let mut block = Box::new([0u8; BLOCK_NUM_BYTES]);
        file.read_exact(&mut *block)?;
        Ok(RollingReader {
            file,
            directory,
            file_number: first_file.clone(),
            block_id: 0,
            block,
        })
    }

    pub fn current_file(&self) -> &FileNumber {
        &self.file_number
    }

    /// Creates a write positioned at the beginning of the last read block.
    ///
    /// If no block was read, positions itself at the beginning.
    pub fn into_writer(mut self) -> io::Result<RollingWriter> {
        let offset = self.block_id * crate::BLOCK_NUM_BYTES;
        self.file.seek(SeekFrom::Start(offset as u64))?;
        Ok(RollingWriter {
            file: BufWriter::with_capacity(FRAME_NUM_BYTES, self.file),
            offset,
            file_number: self.file_number.clone(),
            directory: self.directory,
            syncer: RollSyncer::new()?,
        })
    }
}

fn read_block(file: &mut File, block: &mut [u8; BLOCK_NUM_BYTES]) -> io::Result<bool> {
    match file.read_exact(block) {
        Ok(()) => Ok(true),
        Err(io_err) if io_err.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(io_err) => Err(io_err),
    }
}

impl BlockRead for RollingReader {
    fn next_block(&mut self) -> io::Result<bool> {
        let success = read_block(&mut self.file, &mut self.block)?;
        if success {
            self.block_id += 1;
            return Ok(true);
        }

        let mut next_file_number =
            if let Some(next_file_number) = self.directory.files.next(&self.file_number) {
                next_file_number
            } else {
                return Ok(false);
            };

        loop {
            let mut next_file: File = self.directory.open_file(&next_file_number)?;
            let success = read_block(&mut next_file, &mut self.block)?;
            if success {
                self.block_id = 0;
                self.file = next_file;
                self.file_number = next_file_number;
                return Ok(true);
            }

            next_file_number =
                if let Some(next_file_number) = self.directory.files.next(&next_file_number) {
                    next_file_number
                } else {
                    return Ok(false);
                };
        }
    }

    fn block(&self) -> &[u8; BLOCK_NUM_BYTES] {
        &self.block
    }
}

pub struct RollingWriter {
    file: BufWriter<File>,
    offset: usize,
    file_number: FileNumber,
    pub(crate) directory: Directory,
    syncer: RollSyncer,
}

impl Drop for RollingWriter {
    fn drop(&mut self) {
        if let Err(err) = self.syncer.drain() {
            warn!(error = %err, "failed to fsync a rolled wal file while closing");
        }
        self.syncer.shutdown();
    }
}

impl RollingWriter {
    /// Move forward of `num_bytes` without actually writing anything.
    pub fn forward(&mut self, num_bytes: usize) -> io::Result<()> {
        self.file.seek(SeekFrom::Current(num_bytes as i64))?;
        self.offset += num_bytes;
        Ok(())
    }

    /// Delete WAL files that are no longer used.
    ///
    /// A rolled file must never be deleted while its background fsync is still pending, so every
    /// queued fsync is joined first.
    pub(crate) fn gc(&mut self) -> io::Result<()> {
        self.syncer.drain()?;
        self.directory.gc()
    }

    #[cfg(test)]
    pub(crate) fn close_roll_fsync_gate(&self) {
        self.syncer.close_gate();
    }

    #[cfg(test)]
    pub(crate) fn open_roll_fsync_gate(&self) {
        self.syncer.open_gate();
    }

    #[cfg(test)]
    pub(crate) fn deferred_roll_fsync_count(&self) -> usize {
        self.syncer.sync_count()
    }

    #[cfg(test)]
    pub(crate) fn drain_roll_fsync(&mut self) -> io::Result<()> {
        self.syncer.drain()
    }

    pub fn current_file(&self) -> &FileNumber {
        &self.file_number
    }

    pub fn size(&self) -> usize {
        self.directory.files.count() * FILE_NUM_BYTES
    }

    #[cfg(test)]
    pub fn list_file_numbers(&self) -> Vec<u64> {
        self.directory
            .first_file_number()
            .unroll(&self.directory.files)
    }
}

impl BlockWrite for RollingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.syncer.check()?;
        assert!(buf.len() <= self.num_bytes_remaining_in_block());
        if self.offset + buf.len() > FILE_NUM_BYTES {
            // Flush the completed file into the OS page cache, then hand it to the background
            // fsync thread. The flush is cheap (a memory copy); the device fsync is what the
            // client ack used to wait on, and it now runs off this path.
            self.file.flush()?;

            let (file_number, file) =
                if let Some(next_file_number) = self.directory.files.next(&self.file_number) {
                    let file = self.directory.open_file(&next_file_number)?;
                    (next_file_number, file)
                } else {
                    let next_file_number = self.directory.files.inc(&self.file_number);
                    let file = create_file(&self.directory.dir, &next_file_number)?;
                    (next_file_number, file)
                };

            let completed = std::mem::replace(
                &mut self.file,
                BufWriter::with_capacity(FRAME_NUM_BYTES, file),
            );
            let completed = completed.into_inner().map_err(|err| err.into_error())?;
            self.file_number = file_number;
            self.offset = 0;
            self.syncer.schedule(self.directory.dir.clone(), completed)?;
        }
        self.offset += buf.len();
        self.file.write_all(buf)?;
        Ok(())
    }

    fn persist(&mut self, persist_action: PersistAction) -> io::Result<()> {
        match persist_action {
            PersistAction::FlushAndFsync => {
                self.file.flush()?;
                self.file.get_ref().sync_data()?;
                self.directory.sync_directory()
            }
            PersistAction::Flush => {
                // This will flush the buffer of the BufWriter to the underlying OS.
                self.file.flush()
            }
        }
    }

    fn num_bytes_remaining_in_block(&self) -> usize {
        BLOCK_NUM_BYTES - (self.offset % BLOCK_NUM_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use super::filename_to_position;

    #[test]
    fn test_filename_to_seq_number_invalid_prefix_rejected() {
        assert_eq!(filename_to_position("fil-00000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_padding_rejected() {
        assert_eq!(filename_to_position("wal-0000000000000000001"), None);
    }

    #[test]
    fn test_filename_to_seq_number_invalid_len_rejected() {
        assert_eq!(filename_to_position("wal-000000000000000000011"), None);
    }

    #[test]
    fn test_filename_to_seq_number_simple() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1));
    }

    #[test]
    fn test_filename_to_seq_number() {
        assert_eq!(filename_to_position("wal-00000000000000000001"), Some(1));
    }

    #[test]
    fn test_filename_to_seq_number_33b() {
        // 2**32, overflow a u32
        assert_eq!(
            filename_to_position("wal-00000000004294967296"),
            Some(4294967296)
        );
    }

    #[test]
    fn test_filename_to_seq_number_64b() {
        // 2**64-1, max supported value
        assert_eq!(
            filename_to_position(&format!("wal-{}", u64::MAX)),
            Some(u64::MAX)
        );
    }
}
