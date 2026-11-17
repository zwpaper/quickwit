// Experimental E050 execution primitive. Opt-in only; no change to split format or publication.
use std::sync::Arc;

use anyhow::{bail, ensure};
use quickwit_config::SourceInputFormat;
use quickwit_doc_mapper::DocMapper;

pub(super) struct TwoLane {
    pool: rayon::ThreadPool,
    serial: bool,
}

impl TwoLane {
    pub(super) fn new(serial: bool) -> anyhow::Result<Arc<Self>> {
        Ok(Arc::new(Self {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(4)
                .thread_name(|i| format!("ingest-lane-{i}"))
                .build()?,
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
            "2" => {
                Self::validate(mapper, format, has_transform, has_fingerprint, cooperative)?;
                let serial = std::env::var("QW_E050_SERIAL_STAGES").is_ok_and(|v| v == "1");
                tracing::warn!(
                    serial,
                    "E050 experimental two-lane batch joins enabled; not production-ready"
                );
                Ok(Some(Self::new(serial)?))
            }
            _ => bail!("QW_EXPERIMENTAL_INGEST_LANES must be 1 or 2"),
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

    // Both tasks finish (including errors) before the actor can process a commit/lock/EOF.
    // No spawned task retains writer state beyond this call.
    pub(super) fn join<A: Send, B: Send>(
        &self,
        left: impl FnOnce() -> A + Send,
        right: impl FnOnce() -> B + Send,
    ) -> (A, B) {
        if self.serial {
            (left(), right())
        } else {
            self.pool.install(|| rayon::join(left, right))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    #[test]
    fn test_join_drains_other_lane_on_error() {
        let done = AtomicBool::new(false);
        let pool = TwoLane::new(false).unwrap();
        let (left, right) = pool.join(
            || anyhow::bail!("injected lane failure"),
            || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                done.store(true, Ordering::SeqCst);
                7
            },
        );
        let left: anyhow::Result<()> = left;
        assert!(left.is_err());
        assert_eq!(right, 7);
        assert!(done.load(Ordering::SeqCst));
    }
}
