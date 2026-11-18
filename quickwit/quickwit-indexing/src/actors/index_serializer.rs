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

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::io::{IoControls, Limiter};
use quickwit_common::runtimes::RuntimeType;
use tokio::runtime::Handle;
use tracing::{info, instrument};

use super::two_lane::TwoLane;
use crate::actors::Packager;
use crate::models::{EmptySplit, IndexedSplit, IndexedSplitBatch, IndexedSplitBatchBuilder};

/// The index serializer takes a non-serialized split,
/// and serializes it before passing it to the packager.
///
/// This is usually a CPU heavy operation.
///
/// Depending on the data
/// (terms cardinality) and the index settings (sorted or not)
/// it can range from medium IO to IO heavy.
pub struct IndexSerializer {
    packager_mailbox: Mailbox<Packager>,
    indexing_io_throughput_limiter_opt: Option<Limiter>,
    finalization_pool_opt: Option<Arc<TwoLane>>,
}

impl IndexSerializer {
    pub fn new(
        packager_mailbox: Mailbox<Packager>,
        indexing_io_throughput_limiter_opt: Option<Limiter>,
    ) -> Self {
        Self {
            packager_mailbox,
            indexing_io_throughput_limiter_opt,
            finalization_pool_opt: None,
        }
    }

    pub(super) fn with_finalization_pool(mut self, pool: Option<Arc<TwoLane>>) -> Self {
        self.finalization_pool_opt = pool;
        self
    }
}

// In the parallel path collect every result before inspecting errors. No owned writer (including
// its compressor) may outlive the batch. The pinned compressor Drop joins early-error cleanup.
fn finalize_all<T: Send, R: Send>(
    inputs: Vec<T>,
    pool: Option<&TwoLane>,
    finalize: impl Fn(T) -> anyhow::Result<R> + Send + Sync,
) -> anyhow::Result<Vec<R>> {
    if let Some(pool) = pool {
        let results = pool.run(inputs, finalize);
        results.into_iter().collect()
    } else {
        inputs.into_iter().map(finalize).collect()
    }
}

#[async_trait]
impl Actor for IndexSerializer {
    type ObservableState = ();

    fn observable_state(&self) -> Self::ObservableState {}

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }
}

#[async_trait]
impl Handler<IndexedSplitBatchBuilder> for IndexSerializer {
    type Reply = ();

    #[instrument(
        name="serialize_split_batch"
        parent=batch_builder.batch_parent_span.id(),
        skip_all,
    )]
    async fn handle(
        &mut self,
        batch_builder: IndexedSplitBatchBuilder,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let split_count = batch_builder.splits.len();
        if self.finalization_pool_opt.is_some() && split_count > 2 {
            return Err(anyhow::anyhow!("E052 requires at most two split builders").into());
        }
        let started = Instant::now();
        let protect_guard = ctx.protect_zone();
        for split_builder in &batch_builder.splits {
            let io_controls = IoControls::default()
                .set_throughput_limiter_opt(self.indexing_io_throughput_limiter_opt.clone())
                .set_progress(ctx.progress().clone())
                .set_kill_switch(ctx.kill_switch().clone())
                .set_component("index_serializer");
            split_builder
                .controlled_directory
                .set_io_controls(io_controls);
        }
        let splits: Vec<IndexedSplit> = finalize_all(
            batch_builder.splits,
            self.finalization_pool_opt.as_deref(),
            |split_builder| split_builder.finalize(),
        )?;
        drop(protect_guard);
        info!(
            split_count,
            parallel = self.finalization_pool_opt.is_some(),
            elapsed_secs = started.elapsed().as_secs_f64(),
            "e052-finalization-batch-complete"
        );
        let indexed_split_batch = IndexedSplitBatch {
            splits,
            checkpoint_delta_opt: batch_builder.checkpoint_delta_opt,
            publish_lock: batch_builder.publish_lock,
            merge_task_opt: None,
            batch_parent_span: batch_builder.batch_parent_span,
        };
        ctx.send_message(&self.packager_mailbox, indexed_split_batch)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[test]
    fn test_e052_finalize_all_order_empty_singleton_and_pair() -> anyhow::Result<()> {
        let pool = TwoLane::new(2, false)?;
        for inputs in [vec![], vec![11], vec![11, 22]] {
            let serial = finalize_all(inputs.clone(), None, Ok)?;
            let parallel = finalize_all(inputs.clone(), Some(&pool), Ok)?;
            assert_eq!(serial, inputs);
            assert_eq!(parallel, inputs);
        }
        Ok(())
    }

    #[test]
    fn test_e052_finalize_all_drains_failure_and_drops_owned_inputs() -> anyhow::Result<()> {
        let pool = TwoLane::new(2, false)?;
        let completed = AtomicUsize::new(0);
        let dropped = Arc::new(AtomicUsize::new(0));
        struct Owned(usize, Arc<AtomicUsize>);
        impl Drop for Owned {
            fn drop(&mut self) {
                self.1.fetch_add(1, Ordering::SeqCst);
            }
        }
        let inputs = vec![Owned(0, dropped.clone()), Owned(1, dropped.clone())];
        let result = finalize_all(inputs, Some(&pool), |input| {
            completed.fetch_add(1, Ordering::SeqCst);
            anyhow::ensure!(input.0 != 0, "injected finalization failure");
            Ok(input.0)
        });
        assert!(result.is_err());
        assert_eq!(completed.load(Ordering::SeqCst), 2);
        assert_eq!(dropped.load(Ordering::SeqCst), 2);
        Ok(())
    }
}

#[async_trait]
impl Handler<EmptySplit> for IndexSerializer {
    type Reply = ();

    #[instrument(
        name="serialize_empty_split"
        parent=empty_split.batch_parent_span.id(),
        skip_all,
    )]
    async fn handle(
        &mut self,
        empty_split: EmptySplit,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        ctx.send_message(&self.packager_mailbox, empty_split)
            .await?;
        Ok(())
    }
}
