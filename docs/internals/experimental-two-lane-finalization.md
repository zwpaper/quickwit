# Experimental paired finalization (E052)

Default behavior is unchanged. This is a default-off experiment stacked on the restricted E050
stateless JSON two-lane MVP, not a production configuration or a complete recovery proof.

Set both `QW_EXPERIMENTAL_INGEST_LANES=2` and `QW_E052_PARALLEL_FINALIZATION=1` to finalize the
at-most-two builders in each sealed batch on the existing shared four-worker pool. Enabling the
finalization flag without exactly two ingest lanes is rejected. Leaving it unset (or not `1`)
provides serial finalization for same-binary controls.

Both writers remain exclusively owned. All parallel results are collected before error propagation;
only an all-success ordered batch reaches packaging. Existing checkpoints, publish lock, source/
mapping identity, delete snapshot and single-segment split format remain unchanged. The same I/O
limiter, kill switch and progress handle apply to both finalizers. No extra workers or queues are
created. Simultaneous serialization scratch must still be included in process-memory validation;
the global writer-memory trigger is not a whole-process RSS cap.

The pinned Tantivy compressor disconnects its sender and joins on abandoned-writer Drop as well as
explicit close, so early errors/unwind do not leave an owned compressor using a directory after its
finalization job has returned. Explicit close continues to report worker errors. This lifecycle
change is shared by both serial and parallel benchmark arms.

`e052-finalization-batch-complete` logs report whole-batch finalization wall (including shared-pool
scheduling waits), not pure CPU service or guaranteed exposed ingest wall. Complete-ingest comparisons
must include all merge, upload, publication and shutdown work with identical resource/policy controls.
