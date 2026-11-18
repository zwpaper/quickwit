# Experimental byte-balanced parse cuts (E055)

`QW_E055_BYTE_BALANCED_PARSE=1` enables default-off contiguous source-byte-weighted parse bisection
and requires exactly two experimental ingest lanes. The normal path keeps count-based cuts.

The boundary minimizes distance from half the total raw source bytes, with stable lower-prefix ties,
nonempty chunks and count-based fallback for empty/singleton/zero-weight batches. It moves descriptors,
not source payloads. The same bounded shared pool runs both jobs and preserves source-order joins.
Writer-side count cuts and assignment are unchanged, as are memory observations, checkpoints, global
commit fences and the single-segment format. This is a parser scheduling experiment, not routing.

Both modes record source document/byte counts, inside-job wall and ordered join wall. Job wall
includes OS descheduling but excludes pool-queue wait; do not treat it as CPU or additive exposed
full-ingest wall. An external pre-check screens for meaningful source-byte/service imbalance before
any10M weighted-candidate comparison. Tests prove deterministic contiguous coverage and parsing
answer/order equivalence, not a universal throughput improvement. No E052/E053/E054 path is included.
