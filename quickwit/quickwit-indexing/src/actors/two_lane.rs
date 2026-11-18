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

// Experimental E050 execution primitive. Opt-in only; no change to split format or publication.
use std::sync::Arc;

use anyhow::{bail, ensure};
use quickwit_config::SourceInputFormat;
use quickwit_doc_mapper::DocMapper;
use rayon::prelude::*;

// Historical internal name retained to keep the generalization delta small; supports 2 or 4 lanes.
pub(super) struct TwoLane {
    pool: rayon::ThreadPool,
    count: usize,
    serial: bool,
    byte_balanced_parse: bool,
}

impl TwoLane {
    pub(super) fn new(count: usize, serial: bool) -> anyhow::Result<Arc<Self>> {
        Self::new_with_parse_balance(count, serial, false)
    }

    pub(super) fn new_with_parse_balance(
        count: usize,
        serial: bool,
        byte_balanced_parse: bool,
    ) -> anyhow::Result<Arc<Self>> {
        ensure!(matches!(count, 2 | 4), "E050 supports exactly 2 or 4 lanes");
        ensure!(
            !byte_balanced_parse || count == 2,
            "E055 requires exactly two ingest lanes"
        );
        Ok(Arc::new(Self {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(2 * count)
                .thread_name(|i| format!("ingest-lane-{i}"))
                .build()?,
            count,
            serial,
            byte_balanced_parse,
        }))
    }

    pub(super) fn from_env(
        mapper: &DocMapper,
        format: SourceInputFormat,
        has_transform: bool,
        has_fingerprint: bool,
        cooperative: bool,
    ) -> anyhow::Result<Option<Arc<Self>>> {
        let lanes = std::env::var("QW_EXPERIMENTAL_INGEST_LANES").unwrap_or_else(|_| "1".into());
        let balanced = std::env::var("QW_E055_BYTE_BALANCED_PARSE").is_ok_and(|v| v == "1");
        ensure!(
            !balanced || lanes == "2",
            "E055 requires exactly two ingest lanes"
        );
        match lanes.as_str() {
            "1" => Ok(None),
            "2" | "4" => {
                Self::validate(mapper, format, has_transform, has_fingerprint, cooperative)?;
                let count = lanes.parse()?;
                let serial = std::env::var("QW_E050_SERIAL_STAGES").is_ok_and(|v| v == "1");
                tracing::warn!(
                    count,
                    serial,
                    "E050 experimental multi-lane batch joins enabled; not production-ready"
                );
                Ok(Some(if balanced {
                    Self::new_with_parse_balance(count, serial, true)?
                } else {
                    Self::new(count, serial)?
                }))
            }
            _ => bail!("QW_EXPERIMENTAL_INGEST_LANES must be 1, 2 or 4"),
        }
    }

    pub(super) fn validate(
        mapper: &DocMapper,
        format: SourceInputFormat,
        has_transform: bool,
        has_fingerprint: bool,
        cooperative: bool,
    ) -> anyhow::Result<()> {
        ensure!(
            mapper.supports_fast_json()
                && matches!(format, SourceInputFormat::Json)
                && !has_transform
                && !has_fingerprint
                && !cooperative,
            "E050 requires fast-json-compatible stateless JSON, no \
             routing/transform/fingerprint/cooperative indexing"
        );
        Ok(())
    }

    pub(super) fn chunks<T>(&self, docs: Vec<T>) -> Vec<Vec<T>> {
        if docs.is_empty() {
            return Vec::new();
        }
        let chunk_size = docs.len().div_ceil(self.count);
        let mut iter = docs.into_iter();
        (0..self.count)
            .map(|_| iter.by_ref().take(chunk_size).collect::<Vec<T>>())
            .take_while(|chunk| !chunk.is_empty())
            .collect()
    }

    pub(super) fn byte_balanced_parse_enabled(&self) -> bool {
        self.byte_balanced_parse
    }

    pub(super) fn parse_chunks<T>(
        &self,
        docs: Vec<T>,
        weight: impl Fn(&T) -> usize,
    ) -> Vec<Vec<T>> {
        if !self.byte_balanced_parse || docs.len() < 2 {
            return self.chunks(docs);
        }
        let total: u128 = docs.iter().map(|doc| weight(doc) as u128).sum();
        if total == 0 {
            return self.chunks(docs);
        }
        let mut prefix = 0u128;
        let mut best = u128::MAX;
        let mut cut = 1;
        for (i, doc) in docs.iter().take(docs.len() - 1).enumerate() {
            prefix += weight(doc) as u128;
            let distance = prefix.abs_diff(total - prefix);
            if distance < best {
                best = distance;
                cut = i + 1;
            }
        }
        let len = docs.len();
        let mut iter = docs.into_iter();
        // Same descriptor-moving collection pattern as count cuts: no source payload clone and
        // no oversized original vector retained by split_off. Only parser scheduling changes.
        vec![
            iter.by_ref().take(cut).collect(),
            iter.by_ref().take(len - cut).collect(),
        ]
    }

    // Collect a Vec<R>, never a Result<Vec<_>>: every job must finish before an error
    // can be propagated. Indexed parallel collection also preserves source chunk order.
    // No spawned task retains writer state beyond this synchronous call.
    pub(super) fn run<T: Send, R: Send>(
        &self,
        inputs: Vec<T>,
        action: impl Fn(T) -> R + Send + Sync,
    ) -> Vec<R> {
        if self.serial {
            inputs.into_iter().map(action).collect()
        } else {
            self.pool
                .install(|| inputs.into_par_iter().map(action).collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn test_run_drains_all_lanes_on_error() -> anyhow::Result<()> {
        for count in [2, 4] {
            for serial in [false, true] {
                let done = AtomicUsize::new(0);
                let pool = TwoLane::new(count, serial)?;
                let results: Vec<anyhow::Result<usize>> = pool.run((0..count).collect(), |i| {
                    std::thread::sleep(std::time::Duration::from_millis(2));
                    done.fetch_add(1, Ordering::SeqCst);
                    ensure!(i != 0, "injected lane failure");
                    Ok(i)
                });
                assert_eq!(done.load(Ordering::SeqCst), count);
                assert!(results[0].is_err());
                assert!(results[1..].iter().all(Result::is_ok));
            }
        }
        Ok(())
    }

    #[test]
    fn test_e055_contiguous_weighted_cut_oracle_order_and_extreme_weights() -> anyhow::Result<()> {
        let pool = TwoLane::new_with_parse_balance(2, false, true)?;
        assert!(TwoLane::new_with_parse_balance(4, false, true).is_err());
        let mut cases = vec![
            vec![],
            vec![0],
            vec![1],
            vec![0; 7],
            vec![1, 1, 100, 100],
            vec![100, 1, 1, 1],
            vec![usize::MAX, 0, usize::MAX, 1],
        ];
        for len in 2..98 {
            cases.push(
                (0..len)
                    .map(|i| ((i * 991 + len * 13) % 257) as usize)
                    .collect(),
            );
        }
        for weights in cases {
            let input: Vec<_> = weights.iter().copied().enumerate().collect();
            let chunks = pool.parse_chunks(input.clone(), |item| item.1);
            assert!(chunks.len() <= 2 && chunks.iter().all(|c| !c.is_empty()));
            assert_eq!(chunks.iter().flatten().copied().collect::<Vec<_>>(), input);
            assert_eq!(chunks, pool.parse_chunks(input.clone(), |item| item.1));
            let total: u128 = weights.iter().map(|&w| w as u128).sum();
            if input.len() >= 2 && total != 0 {
                let cut = chunks[0].len();
                let prefix: u128 = weights[..cut].iter().map(|&w| w as u128).sum();
                let actual = prefix.abs_diff(total - prefix);
                for candidate in 1..input.len() {
                    let other: u128 = weights[..candidate].iter().map(|&w| w as u128).sum();
                    assert!(actual <= other.abs_diff(total - other));
                    if candidate < cut {
                        assert!(actual < other.abs_diff(total - other));
                    }
                }
            }
        }
        let count_pool = TwoLane::new(2, false)?;
        for len in 0..33 {
            let docs: Vec<_> = (0..len).collect();
            assert_eq!(
                count_pool.parse_chunks(docs.clone(), |_| 1),
                count_pool.chunks(docs)
            );
        }
        Ok(())
    }

    #[test]
    fn test_chunks_are_bounded_ordered_and_cover_small_and_ragged_batches() -> anyhow::Result<()> {
        for count in [2, 4] {
            let pool = TwoLane::new(count, false)?;
            for size in 0..33 {
                let input: Vec<_> = (0..size).collect();
                let chunks = pool.chunks(input.clone());
                assert!(chunks.len() <= count);
                assert!(chunks.iter().all(|c| !c.is_empty()));
                let output: Vec<_> = pool
                    .run(chunks, |chunk| chunk)
                    .into_iter()
                    .flatten()
                    .collect();
                assert_eq!(output, input);
            }
        }
        assert!(TwoLane::new(3, false).is_err());
        Ok(())
    }
}
