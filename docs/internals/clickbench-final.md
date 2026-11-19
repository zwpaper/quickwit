# Final ClickBench integration stack

The `clickbench-final` branches combine the retained ingest stack with the requested non-negative
query optimization stack from `wsxiaoys/tantivy`, without modifying any PR. Existing retained-stack
ancestry already contains the accepted ingest PR commits; recreating their merges or cherry-picking
them again would duplicate history. Tantivy merges the query stack on top. Quickwit adds only this
integration note and a committed dependency override/lockfile, so the combined stack is selected
without an external build-time patch.

## Included retained ingest stack

| Repository | Experiment / fork PR | Change |
|---|---|---|
| Quickwit | E028 / zwpaper/quickwit#1 | Schema-guided flat JSON decoder |
| Quickwit | E042 / zwpaper/quickwit#4 | 1 MiB split-upload read buffer |
| Tantivy | E002 / zwpaper/tantivy#1 | Arithmetic stacked postings doc-id remap |
| Tantivy | E006 / zwpaper/tantivy#2 | Cached fast-field column address |
| Tantivy | E028 / zwpaper/tantivy#4 | Truthful grouped-document sidecar |
| Tantivy | E033 / zwpaper/tantivy#5 | Single-walk u64 column serialization |
| Tantivy | E032 / zwpaper/tantivy#6 | Keep term streamer out of merger heap |
| Tantivy | E035 / zwpaper/tantivy#8 | Reuse postings cursors during merges |
| Tantivy | E039 / zwpaper/tantivy#12 | Bulk Basic-postings doc-id block writes |
| Tantivy | E048 / zwpaper/tantivy#13 | Overlap independent built-in plugin merges |

Quickwit retained ingest tip: `6424698c36c9147e72f537c5d1abf01b41ab7abf`.
Tantivy retained ingest parent: `eb2d494b5f4e26064a3ff30b7b4d41cd68dc71f4`.

## Additional query stack from wsxiaoys/tantivy

| PR | Change |
|---|---|
| wsxiaoys/tantivy#1 | Partition terms top-k instead of fully sorting all final buckets |
| wsxiaoys/tantivy#3 | Inline the first entries of nested term maps |
| wsxiaoys/tantivy#4 | Gate inline maps to nested collectors, retaining ordinary top-level maps |
| wsxiaoys/tantivy#5 | Reuse the zstd decompressor across sstable blocks |
| wsxiaoys/tantivy#6 | Bulk blockwise-linear column get_range decoding |
| wsxiaoys/tantivy#7 | Paged storage for stats and nested term-bucket values |
| wsxiaoys/tantivy#8 | Select top-k raw entries before final bucket materialization |

These seven PR heads form one chain ending at
`b3ca88261b7b7988fc2ba65b843406c32ab8ce63`. The final state includes #4's top-level gating fix;
#3's ungated intermediate state is not used as the final implementation. The separately branched
negative pre-sizing experiment wsxiaoys/tantivy#2 is not an ancestor and is excluded.

Combined Tantivy merge and immutable dependency pin:
`f16464cee1f7f818f04c8d239c203a1a55dcf3ee`.

The query PRs are integrated at the operator's request, not presented as a newly benchmarked win.
Combined correctness/build checks do not establish full-corpus performance or memory improvements.

## Excluded

- Two/four-lane MVPs and E052–E055: zwpaper/quickwit#5–#10, zwpaper/tantivy#14–#15.
- Other reverted/abandoned proposals: zwpaper/quickwit#2–#3; zwpaper/tantivy#3,#7,#9–#11.
- Explicitly negative, open record: wsxiaoys/tantivy#2 (top-level term-map pre-sizing).
- No rejected change is enabled, silently included or re-scoped as part of this integration.

All PR states, heads and bases are left unchanged. Only the separate final branch refs are pushed.
The experiment decisions and historical 100M results are recorded in
https://github.com/zwpaper/apol/tree/feat/index-arch/result/quickwit-clickbench-index-10m/final-100m .
That historical benchmark is evidence for the retained ingest combination, not a measurement of
this combined ingest/query integration. This operation does not run a new benchmark.

## Build

From `quickwit/`, use the committed lockfile:

```sh
cargo build --locked --release -p quickwit-cli --features release-feature-set --bin quickwit
```

Cargo resolves the pinned Tantivy fork commit; it does not depend on a sibling checkout or on the
moving tip of `clickbench-final`. Neither lane environment switch is present in this branch.
