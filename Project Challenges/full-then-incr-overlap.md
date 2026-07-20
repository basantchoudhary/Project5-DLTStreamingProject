# Full → incr overlap & duplicates

**Problem.** You onboard a table with a `__full` snapshot to seed history, but
Qlik is *already* writing change records to `__ct`. Both flows land the same
rows for the overlap window. Won't the target get duplicates?

**Cause.** Two independent sources (full seed + ongoing ct) can carry the same
primary key for the same or adjacent points in time.

**Fix — `apply_changes` de-dups by key + sequence.** The writer merges on
`primary_keys` ordered by `sequence_by` (`_seq`). For a given key it keeps the
row with the highest sequence and discards the rest, so:

- a full-load row and a ct row for the same key **collapse to one** target row;
- the *newest* wins, which is exactly what you want.

**Lifecycle.** Onboard with `load_type=full` (full seed flow + ct flow both on).
Once the target is seeded and caught up, flip `dataset_config.load_type` to
`incr` — the controller drops the full `append_flow`, keeps the streaming table
and its state, and the pipeline runs ct-only. No reprocessing, no duplicates.

**Watch out.** This safety depends on a **correct `sequence_by`**. If the full
snapshot has no sequence column, the framework falls back to ingest time — fine
for a one-time seed, but make sure ct rows always carry the real Qlik change
sequence so ordering across the two sources is meaningful.
