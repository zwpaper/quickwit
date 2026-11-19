# Final retained ClickBench stack

The `clickbench-final` branches integrate only retained experiment code, without modifying any PR.
Existing retained-stack ancestry already contains the accepted PR commits; recreating their merges
or cherry-picking them again would duplicate history. Tantivy's final branch points directly to the
retained tip. Quickwit adds only this integration note and a committed dependency override/lockfile,
so the tested Tantivy stack is selected without an external build-time patch.

## Included

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

Quickwit retained tip: `6424698c36c9147e72f537c5d1abf01b41ab7abf`.
Tantivy retained tip and immutable dependency pin: `eb2d494b5f4e26064a3ff30b7b4d41cd68dc71f4`.

## Excluded

- Two/four-lane MVPs and their E052–E055 follow-ups: Quickwit#5–#10, Tantivy#14–#15.
- Other reverted/abandoned proposals: Quickwit#2–#3; Tantivy#3,#7,#9–#11.
- No rejected change is enabled, silently included or re-scoped as part of this integration.

All PR states, heads and bases are left unchanged. Only the separate final branch refs are pushed.
The experiment decisions and historical 100M results are recorded in
https://github.com/zwpaper/apol/tree/feat/index-arch/result/quickwit-clickbench-index-10m/final-100m .
That historical benchmark is evidence for the retained engine combination, not a new measurement
of this integration commit. This operation does not run a new benchmark or expand experiment scope.

## Build

From `quickwit/`, use the committed lockfile:

```sh
cargo build --locked --release -p quickwit-cli --features release-feature-set --bin quickwit
```

Cargo resolves the pinned Tantivy fork commit; it does not depend on a sibling checkout or on the
moving tip of `clickbench-final`. Neither lane environment switch is present in this branch.
