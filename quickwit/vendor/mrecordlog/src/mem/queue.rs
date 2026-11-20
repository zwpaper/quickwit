use std::borrow::Cow;
use std::collections::VecDeque;
use std::ops::{Bound, RangeBounds, RangeToInclusive};
use std::sync::Arc;

use crate::error::AppendError;
use crate::mem::QueueSummary;
use crate::record::{MULTIPLEXED_RECORD_HEADER_LEN, MULTIRECORD_ENTRY_HEADER_LEN};
use crate::rolling::FileNumber;
use crate::Record;

/// Number of bytes at the start of a compressed batch holding the decrypted length
/// as a little-endian `u32` (the same prefix the record writer uses on disk).
const UNCOMPRESSED_LEN_NUM_BYTES: usize = 4;

#[derive(Clone)]
struct RecordMeta {
    // in a vec of RecordMeta, this field should be set only on the last record
    // which relate to that File.
    file_number: Option<FileNumber>,
    position: u64,
    /// Index of the batch this record lives in.
    batch_idx: usize,
    /// Byte offset of the record payload in the batch's *plaintext* buffer.
    offset: usize,
    /// Length in bytes of the record payload.
    len: usize,
}

/// A batch of consecutive records. When `is_compressed`, `bytes` holds
/// `[u32 plain_len][zstd(plaintext)]` exactly as written to disk (E020's record
/// compression); otherwise `bytes` is the plaintext itself.
struct Batch {
    bytes: Vec<u8>,
    is_compressed: bool,
}

/// A cheaply cloneable handle to one append batch's bytes (compressed or plaintext).
///
/// Cloning a `RecordBatch` is an `Arc` refcount bump: a fetch path can collect the
/// batches it needs while holding the WAL read lock and decode them after the lock has
/// been released.
#[derive(Clone)]
pub struct RecordBatch {
    inner: Arc<Batch>,
}

/// The location of one record inside an append batch, collected under the WAL read lock.
///
/// The batch bytes are kept alive by the [`RecordBatch`] handle, so the payload can be
/// decoded safely after the read lock is released.
pub struct RecordLocation {
    position: u64,
    offset: usize,
    len: usize,
    batch: RecordBatch,
}

impl RecordLocation {
    /// Position of the record in the queue.
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Length in bytes of the record payload.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// A set of [`RecordLocation`]s collected under the WAL read lock.
///
/// Calling [`RecordLocations::into_records`] after the lock has been released decodes the
/// (possibly compressed) batches into owned [`Record`] payloads without holding any WAL lock.
#[derive(Default)]
pub struct RecordLocations {
    locations: Vec<RecordLocation>,
}

impl RecordLocations {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            locations: Vec::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, location: RecordLocation) {
        self.locations.push(location);
    }

    pub fn len(&self) -> usize {
        self.locations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// Decodes the collected batches into owned records, decompressing each batch at most
    /// once. The decoded `(position, payload)` sequence is identical to
    /// [`MemQueue::range`].
    pub fn into_records(self) -> impl Iterator<Item = Record<'static>> {
        let mut decompressor = zstd::bulk::Decompressor::new().ok();
        let mut plain_buffer: Vec<u8> = Vec::new();
        let mut current_batch: Option<RecordBatch> = None;
        self.locations.into_iter().map(move |location| {
            let is_new_batch = current_batch
                .as_ref()
                .map_or(true, |current| !Arc::ptr_eq(&current.inner, &location.batch.inner));
            if is_new_batch {
                location
                    .batch
                    .inner
                    .write_plain_into(decompressor.as_mut(), &mut plain_buffer);
                current_batch = Some(location.batch.clone());
            }
            let end = location.offset + location.len;
            let payload = if end <= plain_buffer.len() {
                Cow::Owned(plain_buffer[location.offset..end].to_vec())
            } else {
                Cow::Owned(Vec::new())
            };
            Record {
                position: location.position,
                payload,
            }
        })
    }
}

impl Batch {
    /// Copies the batch's plaintext into `out` (reusing `out`'s allocation).
    ///
    /// A decompression failure leaves `out` empty; the payload accessors then yield
    /// an empty payload rather than panicking. In-memory corruption is not expected
    /// (the bytes are produced and owned by this process) and the on-disk record
    /// remains authoritative until truncation.
    fn write_plain_into(
        &self,
        mut decompressor: Option<&mut zstd::bulk::Decompressor<'static>>,
        out: &mut Vec<u8>,
    ) {
        out.clear();
        if self.is_compressed {
            let Some(decompressor) = decompressor.as_mut() else {
                return;
            };
            if self.bytes.len() < UNCOMPRESSED_LEN_NUM_BYTES {
                return;
            }
            let plain_len =
                u32::from_le_bytes(self.bytes[..UNCOMPRESSED_LEN_NUM_BYTES].try_into().unwrap())
                    as usize;
            out.resize(plain_len, 0);
            if decompressor
                .decompress_to_buffer(&self.bytes[UNCOMPRESSED_LEN_NUM_BYTES..], out)
                .is_err()
            {
                tracing::error!("failed to decompress in-memory record batch");
                out.clear();
            }
        } else {
            out.extend_from_slice(&self.bytes);
        }
    }
}

#[derive(Default)]
pub(crate) struct MemQueue {
    // Consecutive records, grouped by append batch and stored compressed when the
    // writer compressed them for disk. The `Arc` makes a batch handle cheap to clone so
    // the fetch path can release the read lock before decompressing.
    batches: VecDeque<Arc<Batch>>,
    start_position: u64,
    record_metas: Vec<RecordMeta>,
}

impl MemQueue {
    pub fn with_next_position(next_position: u64) -> Self {
        MemQueue {
            batches: VecDeque::new(),
            start_position: next_position,
            record_metas: Vec::new(),
        }
    }

    pub fn summary(&self) -> QueueSummary {
        QueueSummary {
            start: self.start_position(),
            end: self.last_position(),
            file_number: self.first_file_number(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.record_metas.is_empty()
    }

    pub(crate) fn first_file_number(&self) -> Option<u64> {
        let file_number: &FileNumber = self
            .record_metas
            .iter()
            .filter_map(|record_meta| record_meta.file_number.as_ref())
            .next()?;
        Some(file_number.file_number())
    }

    pub(crate) fn start_position(&self) -> u64 {
        self.start_position
    }

    /// Returns the position of the last record appended to the queue.
    pub fn last_position(&self) -> Option<u64> {
        self.next_position().checked_sub(1)
    }

    /// Returns the last record stored in the queue.
    pub fn last_record(&self) -> Option<Record<'_>> {
        let record_meta = self.record_metas.last()?;
        let mut decompressor = zstd::bulk::Decompressor::new().ok();
        let mut buffer = Vec::new();
        self.batches[record_meta.batch_idx].write_plain_into(decompressor.as_mut(), &mut buffer);
        Some(self.record_from_plain(record_meta, &buffer))
    }

    /// Returns what the next position should be.
    pub fn next_position(&self) -> u64 {
        self.record_metas
            .last()
            .map(|record| record.position + 1)
            .unwrap_or(self.start_position)
    }

    fn record_from_plain(&self, record_meta: &RecordMeta, plain: &[u8]) -> Record<'_> {
        let end = record_meta.offset + record_meta.len;
        let payload = if end <= plain.len() {
            Cow::Owned(plain[record_meta.offset..end].to_vec())
        } else {
            Cow::Owned(Vec::new())
        };
        Record {
            position: record_meta.position,
            payload,
        }
    }

    /// Appends a batch of records whose payloads have already been serialized (and,
    /// when `is_compressed`, compressed by the record writer). `records` yields
    /// `(position, offset, len)` where `offset` is the byte offset of the payload in
    /// the batch's plaintext buffer.
    ///
    /// Returns an error if any record is strangely in the past.
    pub fn append_batch(
        &mut self,
        file_number: &FileNumber,
        blob: Vec<u8>,
        is_compressed: bool,
        records: impl Iterator<Item = (u64, usize, usize)>,
    ) -> Result<(), AppendError> {
        let records: Vec<(u64, usize, usize)> = records.collect();
        if records.is_empty() {
            return Ok(());
        }

        let mut next_position = self.next_position();
        for (position, _, _) in &records {
            if *position < next_position {
                return Err(AppendError::Past);
            }
            next_position = position + 1;
        }

        if self.start_position == 0u64 && self.record_metas.is_empty() {
            self.start_position = records[0].0;
        }

        // Move the file number from the previous last record if it belongs to the
        // same file, so it ends up on the last record of this batch (the existing
        // `file_number`-on-last-record invariant).
        if let Some(last_record_meta) = self.record_metas.last_mut() {
            if last_record_meta.file_number.as_ref() == Some(file_number) {
                last_record_meta.file_number = None;
            }
        }

        let batch_idx = self.batches.len();
        let num_records = records.len();
        for (idx, (position, offset, len)) in records.into_iter().enumerate() {
            self.record_metas.push(RecordMeta {
                position,
                file_number: if idx + 1 == num_records {
                    Some(file_number.clone())
                } else {
                    None
                },
                batch_idx,
                offset,
                len,
            });
        }
        self.batches.push_back(Arc::new(Batch {
            bytes: blob,
            is_compressed,
        }));
        Ok(())
    }

    /// Appends a new record at a given position.
    /// Returns an error if the record was not added.
    ///
    /// AppendError if the record is strangely in the past or is too much in the future.
    pub fn append_record(
        &mut self,
        file_number: &FileNumber,
        target_position: u64,
        payload: &[u8],
    ) -> Result<(), AppendError> {
        self.append_batch(
            file_number,
            payload.to_vec(),
            false,
            std::iter::once((target_position, 0, payload.len())),
        )
    }

    /// Get the position of the record.
    ///
    /// Returns Ok(_) if the record was found, or Err(idx) with idx being the index just after
    /// where that element would have been if it existed.
    fn position_to_idx(&self, position: u64) -> Result<usize, usize> {
        self.record_metas
            .binary_search_by_key(&position, |record| record.position)
    }

    /// Returns the index of the first record at or after the start bound of `range`.
    fn range_start_idx<R>(&self, range: &R) -> usize
    where
        R: RangeBounds<u64>,
    {
        match range.start_bound() {
            Bound::Included(&start_from) => {
                // if pos is included, we can use position_to_idx result directly
                self.position_to_idx(start_from)
                    .unwrap_or_else(std::convert::identity)
            }
            Bound::Excluded(&start_from) => {
                // if pos is excluded, an Err can be used directly, but an Ok must be incremented
                // by one to skip the element matching exactly.
                self.position_to_idx(start_from)
                    .map(|idx| idx + 1)
                    .unwrap_or_else(std::convert::identity)
            }
            Bound::Unbounded => 0,
        }
    }

    /// Returns the locations of the records in `range`, without decompressing any batch.
    ///
    /// Each item keeps its batch alive through a [`RecordBatch`] handle, so the caller may
    /// release the WAL read lock and decode the payloads afterwards with
    /// [`RecordLocations::into_records`]. The decoded `(position, payload)` sequence is
    /// identical to [`MemQueue::range`].
    pub fn record_locations<R>(
        &self,
        range: R,
    ) -> impl Iterator<Item = RecordLocation> + '_
    where
        R: RangeBounds<u64> + 'static,
    {
        let start_idx = self.range_start_idx(&range);
        (start_idx..self.record_metas.len())
            .take_while(move |idx| range.contains(&self.record_metas[*idx].position))
            .map(move |idx| {
                let record_meta = &self.record_metas[idx];
                RecordLocation {
                    position: record_meta.position,
                    offset: record_meta.offset,
                    len: record_meta.len,
                    batch: RecordBatch {
                        inner: self.batches[record_meta.batch_idx].clone(),
                    },
                }
            })
    }

    pub fn range<R>(&self, range: R) -> impl Iterator<Item = Record<'_>> + '_
    where R: RangeBounds<u64> + 'static {
        let start_idx: usize = self.range_start_idx(&range);
        let mut decompressor = zstd::bulk::Decompressor::new().ok();
        let mut plain_buffer: Vec<u8> = Vec::new();
        let mut current_batch_idx: Option<usize> = None;
        (start_idx..self.record_metas.len())
            .take_while(move |idx| range.contains(&self.record_metas[*idx].position))
            .map(move |idx| {
                let record_meta = &self.record_metas[idx];
                if current_batch_idx != Some(record_meta.batch_idx) {
                    current_batch_idx = Some(record_meta.batch_idx);
                    self.batches[record_meta.batch_idx]
                        .write_plain_into(decompressor.as_mut(), &mut plain_buffer);
                }
                self.record_from_plain(record_meta, &plain_buffer)
            })
    }

    /// Removes all records coming before position, and including the record at "position".
    ///
    /// If truncating to a future position, make the queue go forward to that position.
    /// Return the number of record removed.
    pub fn truncate_head(&mut self, truncate_range: RangeToInclusive<u64>) -> usize {
        let truncate_up_to_pos = truncate_range.end;
        if self.start_position > truncate_up_to_pos {
            return 0;
        }
        if truncate_up_to_pos + 1 >= self.next_position() {
            self.start_position = truncate_up_to_pos + 1;
            self.batches.clear();
            let record_count = self.record_metas.len();
            self.record_metas.clear();
            return record_count;
        }
        let first_record_to_keep = self
            .position_to_idx(truncate_up_to_pos + 1)
            .unwrap_or_else(std::convert::identity);

        // Drop batches that are entirely before the first kept record. A partially
        // evicted batch (the first kept record is mid-batch) is kept whole: its
        // records are contiguous in the plaintext buffer and dropping the head would
        // require re-compressing.
        let first_batch_to_keep = self.record_metas[first_record_to_keep].batch_idx;
        if first_batch_to_keep > 0 {
            self.batches.drain(..first_batch_to_keep);
            for record_meta in &mut self.record_metas[first_record_to_keep..] {
                record_meta.batch_idx -= first_batch_to_keep;
            }
        }
        self.record_metas.drain(..first_record_to_keep);
        self.start_position = truncate_up_to_pos + 1;
        first_record_to_keep
    }

    pub fn size(&self) -> usize {
        self.batches.iter().map(|batch| batch.bytes.len()).sum::<usize>()
            + self.record_metas.len() * std::mem::size_of::<RecordMeta>()
    }

    pub fn capacity(&self) -> usize {
        self.batches
            .iter()
            .map(|batch| batch.bytes.capacity())
            .sum::<usize>()
            + self.batches.capacity() * std::mem::size_of::<Arc<Batch>>()
            + self.record_metas.capacity() * std::mem::size_of::<RecordMeta>()
    }
}

impl MemQueue {
    /// Helper used by the recovery path: appends a contiguous plaintext batch,
    /// computing per-record offsets from the payloads.
    pub fn append_plain_records<'a>(
        &mut self,
        file_number: &FileNumber,
        payloads: impl Iterator<Item = (u64, &'a [u8])>,
    ) -> Result<(), AppendError> {
        let mut blob = Vec::new();
        let mut metas = Vec::new();
        for (position, payload) in payloads {
            let offset = blob.len();
            blob.extend_from_slice(payload);
            metas.push((position, offset, payload.len()));
        }
        self.append_batch(file_number, blob, false, metas.into_iter())
    }
}
