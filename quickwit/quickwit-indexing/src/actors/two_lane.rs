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
}

impl TwoLane {
    pub(super) fn new(count: usize, serial: bool) -> anyhow::Result<Arc<Self>> {
        ensure!(matches!(count, 2 | 4), "E050 supports exactly 2 or 4 lanes");
        Ok(Arc::new(Self {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(2 * count)
                .thread_name(|i| format!("ingest-lane-{i}"))
                .build()?,
            count,
            serial,
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
                Ok(Some(Self::new(count, serial)?))
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
