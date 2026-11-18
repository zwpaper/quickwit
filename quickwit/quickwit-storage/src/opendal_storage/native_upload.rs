// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::future::Future;
use std::io;
use std::time::Instant;

use bytes::{Bytes, BytesMut};
use tracing::warn;

use crate::{PutPayload, StorageResult};

/// No detached work: consume one frame only after all preceding writes are awaited. Close is a
/// durability gate; errors abort where supported and never replace the original upload failure.
/// Future cancellation drops owned state but does not promise an async abort has completed.
pub(super) async fn write_owned_payload<W: OwnedFrameWriter>(
    writer: &mut W,
    payload: &dyn PutPayload,
    feed_start: Instant,
) -> StorageResult<(f64, f64)> {
    let result: StorageResult<(f64, f64)> = async {
        let mut input = payload.byte_stream().await?;
        let mut coalescer = FrameCoalescer::default();
        while let Some(frame) = input.next().await {
            for bytes in coalescer
                .push(frame.map_err(io::Error::other)?)
                .into_iter()
                .flatten()
            {
                writer.write_frame(bytes).await?;
            }
        }
        if let Some(bytes) = coalescer.finish() {
            writer.write_frame(bytes).await?;
        }
        let feed_secs = feed_start.elapsed().as_secs_f64();
        let close_start = Instant::now();
        writer.close_upload().await?;
        Ok((feed_secs, close_start.elapsed().as_secs_f64()))
    }
    .await;
    if result.is_err() {
        if let Err(abort_error) = writer.abort_upload().await {
            warn!(
                ?abort_error,
                "e053-upload-abort-failed; preserving original error"
            );
        }
    }
    result
}

// Statically dispatched adapter: inject faults without global env mutation or boxed futures.
pub(super) trait OwnedFrameWriter: Send {
    fn write_frame(&mut self, bytes: Bytes) -> impl Future<Output = opendal::Result<()>> + Send;
    fn close_upload(&mut self) -> impl Future<Output = opendal::Result<()>> + Send;
    fn abort_upload(&mut self) -> impl Future<Output = opendal::Result<()>> + Send;
}

impl OwnedFrameWriter for opendal::Writer {
    async fn write_frame(&mut self, bytes: Bytes) -> opendal::Result<()> {
        self.write(bytes).await.map(|_| ())
    }
    async fn close_upload(&mut self) -> opendal::Result<()> {
        self.close().await.map(|_| ())
    }
    async fn abort_upload(&mut self) -> opendal::Result<()> {
        self.abort().await.map(|_| ())
    }
}

// Match the existing OpenDAL AsyncWrite adapter's aggregation scale. The caller awaits every
// returned frame before reading another input: no unbounded fragment list, tasks or work queue.
pub(super) const SMALL_FRAME_LIMIT: usize = 256 * 1024;

pub(super) struct FrameCoalescer {
    pending: BytesMut,
}

impl Default for FrameCoalescer {
    fn default() -> Self {
        Self {
            pending: BytesMut::with_capacity(SMALL_FRAME_LIMIT),
        }
    }
}

impl FrameCoalescer {
    pub(super) fn push(&mut self, bytes: Bytes) -> [Option<Bytes>; 2] {
        if bytes.is_empty() {
            return [None, None];
        }
        if bytes.len() >= SMALL_FRAME_LIMIT {
            // Preserve the original owned large frame, rather than copying through an adapter.
            return [self.finish(), Some(bytes)];
        }
        let available = SMALL_FRAME_LIMIT - self.pending.len();
        let prefix = available.min(bytes.len());
        self.pending.extend_from_slice(&bytes[..prefix]);
        if self.pending.len() < SMALL_FRAME_LIMIT {
            return [None, None];
        }
        let ready = self.finish().expect("full coalescer contains bytes");
        self.pending.extend_from_slice(&bytes[prefix..]);
        [Some(ready), None]
    }

    pub(super) fn finish(&mut self) -> Option<Bytes> {
        if self.pending.is_empty() {
            return None;
        }
        Some(
            std::mem::replace(
                &mut self.pending,
                BytesMut::with_capacity(SMALL_FRAME_LIMIT),
            )
            .freeze(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect(chunks: Vec<Bytes>) -> (Vec<u8>, Vec<usize>) {
        let mut coalescer = FrameCoalescer::default();
        let mut data = Vec::new();
        let mut sizes = Vec::new();
        for chunk in chunks {
            for frame in coalescer.push(chunk).into_iter().flatten() {
                sizes.push(frame.len());
                data.extend_from_slice(&frame);
            }
            assert!(coalescer.pending.len() < SMALL_FRAME_LIMIT);
        }
        if let Some(frame) = coalescer.finish() {
            sizes.push(frame.len());
            data.extend_from_slice(&frame);
        }
        assert!(coalescer.finish().is_none());
        (data, sizes)
    }

    #[tokio::test]
    async fn test_e053_native_writer_awaits_close_and_preserves_remote_bytes() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        for size in [
            0,
            3,
            SMALL_FRAME_LIMIT - 1,
            SMALL_FRAME_LIMIT,
            SMALL_FRAME_LIMIT * 4 + 3,
        ] {
            let expected: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
            let mut writer = operator.writer("object").await.unwrap();
            write_owned_payload(&mut writer, &expected, Instant::now())
                .await
                .unwrap();
            assert_eq!(operator.read("object").await.unwrap().to_vec(), expected);
        }
    }

    #[derive(Clone)]
    struct ReadErrorPayload;

    #[async_trait::async_trait]
    impl PutPayload for ReadErrorPayload {
        fn len(&self) -> u64 {
            1
        }
        async fn range_byte_stream(
            &self,
            _range: std::ops::Range<u64>,
        ) -> io::Result<aws_sdk_s3::primitives::ByteStream> {
            Err(io::Error::other("injected-payload-read-error"))
        }
    }

    #[tokio::test]
    async fn test_e053_native_writer_preserves_read_error_and_does_not_publish() {
        let operator = opendal::Operator::new(opendal::services::Memory::default()).unwrap();
        let mut writer = operator.writer("incomplete").await.unwrap();
        let error = write_owned_payload(&mut writer, &ReadErrorPayload, Instant::now())
            .await
            .unwrap_err();
        assert!(format!("{error:?}").contains("injected-payload-read-error"));
        assert!(!operator.exists("incomplete").await.unwrap());
    }

    struct FaultWriter {
        stage: &'static str,
        close_calls: usize,
        abort_calls: usize,
    }

    impl OwnedFrameWriter for FaultWriter {
        async fn write_frame(&mut self, _bytes: Bytes) -> opendal::Result<()> {
            if self.stage == "write" {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "injected-write",
                ));
            }
            Ok(())
        }
        async fn close_upload(&mut self) -> opendal::Result<()> {
            self.close_calls += 1;
            Err(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "injected-close",
            ))
        }
        async fn abort_upload(&mut self) -> opendal::Result<()> {
            self.abort_calls += 1;
            if self.stage == "close-and-abort" {
                return Err(opendal::Error::new(
                    opendal::ErrorKind::Unexpected,
                    "injected-abort",
                ));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_e053_write_close_abort_failures_preserve_original_error() {
        for stage in ["write", "close", "close-and-abort"] {
            let mut writer = FaultWriter {
                stage,
                close_calls: 0,
                abort_calls: 0,
            };
            let error = write_owned_payload(&mut writer, &vec![5u8; 3], Instant::now())
                .await
                .unwrap_err();
            let expected = if stage == "write" {
                "injected-write"
            } else {
                "injected-close"
            };
            assert!(format!("{error:?}").contains(expected));
            assert_eq!(writer.abort_calls, 1);
            assert_eq!(writer.close_calls, usize::from(stage != "write"));
        }
    }

    #[test]
    fn test_e053_empty_and_tiny_frames() {
        assert_eq!(collect(vec![Bytes::new()]), (Vec::new(), Vec::new()));
        let (data, sizes) = collect(vec![Bytes::from_static(b"a"); 1000]);
        assert_eq!(data, vec![b'a'; 1000]);
        assert_eq!(sizes, vec![1000]);
    }

    #[test]
    fn test_e053_large_frame_retains_owned_allocation() {
        let bytes = Bytes::from(vec![7; SMALL_FRAME_LIMIT * 4]);
        let pointer = bytes.as_ptr();
        let [small, large] = FrameCoalescer::default().push(bytes);
        assert!(small.is_none());
        assert_eq!(large.unwrap().as_ptr(), pointer);
    }

    #[test]
    fn test_e053_small_large_small_preserves_order() {
        let chunks = vec![
            Bytes::from_static(b"abc"),
            Bytes::from(vec![8; SMALL_FRAME_LIMIT]),
            Bytes::new(),
            Bytes::from_static(b"xyz"),
        ];
        let expected: Vec<u8> = chunks.iter().flat_map(|b| b.iter().copied()).collect();
        let (data, sizes) = collect(chunks);
        assert_eq!(data, expected);
        assert_eq!(sizes, vec![3, SMALL_FRAME_LIMIT, 3]);
    }

    #[test]
    fn test_e053_ragged_threshold_crossings() {
        for size in [
            1,
            3,
            8191,
            16384,
            SMALL_FRAME_LIMIT - 1,
            SMALL_FRAME_LIMIT,
            SMALL_FRAME_LIMIT + 1,
        ] {
            let chunks = (0..13)
                .map(|i| Bytes::from(vec![i; size]))
                .collect::<Vec<_>>();
            let expected: Vec<u8> = chunks.iter().flat_map(|b| b.iter().copied()).collect();
            let (actual, sizes) = collect(chunks);
            assert_eq!(actual, expected);
            assert_eq!(sizes.iter().sum::<usize>(), actual.len());
        }
    }
}
