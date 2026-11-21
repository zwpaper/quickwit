use std::io;

use crate::block_read_write::VecBlockWriter;
use crate::frame::{FrameType, FrameWriter};
use crate::rolling::{Directory, FileNumber, RollingWriter};
use crate::{BlockWrite, PersistAction, Serializable};

/// Minimum serialized record size worth compressing. Smaller records (queue
/// bookkeeping, single small appends) keep the uncompressed framing so the codec
/// overhead never makes a record bigger.
const COMPRESSION_MIN_BYTES: usize = 8 * 1024;

/// zstd level used for record compression. Level 1 keeps the CPU added inside the
/// ingester write lock well below the device time it removes.
const COMPRESSION_LEVEL: i32 = 1;

/// Number of bytes prefixed to a compressed record holding its uncompressed length
/// as a little-endian `u32`, so the reader knows how much to reserve.
const UNCOMPRESSED_LEN_NUM_BYTES: usize = 4;

pub struct RecordWriter<W> {
    frame_writer: FrameWriter<W>,
    buffer: Vec<u8>,
    compression_buffer: Vec<u8>,
    compressor: Option<zstd::bulk::Compressor<'static>>,
    // Whether the record currently held in `buffer` is compressed. Set by the last
    // `write_record` call so callers (the in-memory queue) can reuse the exact blob
    // written to disk without recompressing.
    last_record_is_compressed: bool,
}

fn frame_type(is_first_frame: bool, is_last_frame: bool) -> FrameType {
    match (is_first_frame, is_last_frame) {
        (true, true) => FrameType::Full,
        (true, false) => FrameType::First,
        (false, true) => FrameType::Last,
        (false, false) => FrameType::Middle,
    }
}

impl<W: BlockWrite + Unpin> From<FrameWriter<W>> for RecordWriter<W> {
    fn from(frame_writer: FrameWriter<W>) -> Self {
        RecordWriter {
            frame_writer,
            buffer: Vec::with_capacity(10_000),
            compression_buffer: Vec::new(),
            // If the context cannot be created, records are written uncompressed.
            compressor: zstd::bulk::Compressor::new(COMPRESSION_LEVEL).ok(),
            last_record_is_compressed: false,
        }
    }
}

impl<W: BlockWrite + Unpin> RecordWriter<W> {
    #[cfg(test)]
    pub fn into_writer(self) -> W {
        self.frame_writer.into_writer()
    }
}

impl<W: BlockWrite + Unpin> RecordWriter<W> {
    /// Writes a record. Returns the total number of bytes pushed to the underlying writer
    /// for this record, including frame headers and any block padding.
    ///
    /// Even if this call returns `Ok(_)`, at this point the data
    /// is likely to be not durably stored on disk.
    ///
    /// For instance, the data could be stale in a library level buffer,
    /// by a writer level buffer, or an application buffer,
    /// or could not be flushed to disk yet by the OS.
    pub fn write_record<'a>(&mut self, record: impl Serializable<'a>) -> io::Result<u64> {
        let mut is_first_frame = true;
        let mut num_bytes_written: u64 = 0;

        self.buffer.clear();
        record.serialize(&mut self.buffer);
        let compressed = self.compress_buffer_if_worthwhile();
        self.last_record_is_compressed = compressed;
        let mut payload = &self.buffer[..];

        loop {
            let frame_payload_len = self
                .frame_writer
                .max_writable_frame_length()
                .min(payload.len());
            let frame_payload = &payload[..frame_payload_len];
            payload = &payload[frame_payload_len..];
            let is_last_frame = payload.is_empty();
            let frame_type = frame_type(is_first_frame, is_last_frame);
            num_bytes_written +=
                self.frame_writer
                    .write_frame(frame_type, compressed, frame_payload)? as u64;
            is_first_frame = false;

            if is_last_frame {
                break;
            }
        }
        Ok(num_bytes_written)
    }

    /// Replaces `self.buffer` with `[u32 uncompressed_len][zstd payload]` when the
    /// record is large enough and compression actually shrinks it. Returns whether the
    /// buffer now holds a compressed record.
    fn compress_buffer_if_worthwhile(&mut self) -> bool {
        let uncompressed_len = self.buffer.len();
        if uncompressed_len < COMPRESSION_MIN_BYTES || uncompressed_len > u32::MAX as usize {
            return false;
        }
        let Some(compressor) = self.compressor.as_mut() else {
            return false;
        };
        let mut compressed = std::mem::take(&mut self.compression_buffer);
        compressed.clear();
        compressed.reserve(zstd::compress_bound(uncompressed_len));
        let compress_result = compressor.compress_to_buffer(&self.buffer, &mut compressed);
        if compress_result.is_err()
            || compressed.len() + UNCOMPRESSED_LEN_NUM_BYTES >= uncompressed_len
        {
            // Compressing failed or did not help: keep the record uncompressed.
            self.compression_buffer = compressed;
            return false;
        }
        self.buffer.clear();
        self.buffer
            .extend_from_slice(&(uncompressed_len as u32).to_le_bytes());
        self.buffer.extend_from_slice(&compressed);
        self.compression_buffer = compressed;
        true
    }

    /// Returns the bytes of the record produced by the last [`Self::write_record`] call
    /// (exactly what was framed to disk), together with whether they are compressed.
    /// The returned slice is valid until the next write. This lets the in-memory queue
    /// store the already-computed compressed blob instead of recompressing.
    pub fn last_written_blob(&self) -> (&[u8], bool) {
        (&self.buffer, self.last_record_is_compressed)
    }

    /// Persist the data to disk, according to the persist_action.
    pub fn persist(&mut self, persist_action: PersistAction) -> io::Result<()> {
        self.frame_writer.persist(persist_action)
    }

    pub fn get_underlying_wrt(&self) -> &W {
        self.frame_writer.get_underlying_wrt()
    }
}

impl RecordWriter<RollingWriter> {
    pub fn directory(&mut self) -> &mut Directory {
        self.frame_writer.directory()
    }

    /// Delete WAL files that are no longer used, after joining their background fsyncs.
    pub fn gc(&mut self) -> io::Result<()> {
        self.frame_writer.gc()
    }

    pub fn current_file(&mut self) -> &FileNumber {
        self.get_underlying_wrt().current_file()
    }

    pub fn size(&self) -> usize {
        self.get_underlying_wrt().size()
    }
}

impl RecordWriter<VecBlockWriter> {
    #[cfg(test)]
    pub fn in_memory() -> Self {
        FrameWriter::create(VecBlockWriter::default()).into()
    }
}
