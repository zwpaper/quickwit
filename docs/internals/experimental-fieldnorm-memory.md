# Experimental exact fieldnorm capacity cache (E054)

`TANTIVY_E054_CACHED_FIELDNORM_MEMORY=1` enables a default-off Tantivy fieldnorm accounting experiment.
The flag is cached once per process and stored per writer at construction. Other values keep full
capacity recomputation. Each constructor logs the selected mode for experiment verification.

The cache tracks the exact sum of actual fieldnorm buffer capacities after every mutation, not
lengths, samples or estimated per-document costs. Both existing Quickwit per-document observations,
commit fences and the global writer budget remain unchanged. No segment or metastore format changes.
Debug builds and explicit release tests compare against full recomputation; tests also compare
serialized bytes and commit-threshold observations across capacity growth and sparse/wide schemas.

This branch is independent of E052 paired finalization and E053 native upload; neither is active or
included. Compare fresh same-binary controls/candidates and complete all merge/upload/publication/
shutdown work before making an ingest claim. The earlier profile attributed little CPU to accounting;
a stable whole-ingest gain is not established by a faster cached accessor alone.
