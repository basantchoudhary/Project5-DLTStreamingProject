# Idempotency via primary key

**Idempotent** = running the same thing again produces the **same result**, not
duplicates. In a streaming CDC pipeline you *will* reprocess (restarts,
retries, full+ct overlap, backfills), so idempotency isn't optional.

## The two guarantees that make it work

1. **Exactly-once ingest (checkpoint).** Auto Loader records which files it
   processed. A restart or duplicate notification doesn't re-ingest a file
   (see [Auto Loader](05-auto-loader.md), [streaming](04-spark-structured-streaming.md)).

2. **Key-based merge (apply_changes).** Even if the *same change* arrives twice,
   `apply_changes` merges by **primary key** and keeps the latest by
   `sequence_by`. Two copies of `UPDATE id=101 seq=5002` collapse to one row —
   the second is a no-op.

```
   process file → apply I(101) U(101)          → row 101 = latest
   REPLAY same file → apply I(101) U(101) again → row 101 = same latest (no dup)
```

## Why the primary key is the linchpin

- `apply_changes.keys = primary_keys` defines **row identity**. Without a correct
  key, the merge can't tell "same row again" from "new row" → duplicates or
  wrong overwrites.
- `sequence_by` breaks ties so the newest wins deterministically.
- Together: **(key, sequence)** makes reprocessing safe and repeatable.

## In this project

- `primary_keys` lives in **`dataset_config`** (key/value), per table — e.g.
  `('ORA_SALES.ORDERS','primary_keys','order_id')`.
- `derived_config` parses it into a list; `writer.py` passes it as
  `apply_changes(keys=...)`.
- Result: re-running a pipeline, replaying a batch, or overlapping full+ct all
  converge to the same target — **no duplicate rows**.

## Watch-outs

- **No PK** → no row identity → can't dedup (see
  [Qlik no-PK challenge](../Project%20Challenges/qlik-no-primary-key.md)).
- **Changing PK** → identity moves → looks like delete+insert (see
  [changing-PK challenge](../Project%20Challenges/changing-primary-key.md)).

**Related:** [apply_changes](07-dlt-apply-changes.md),
[full → incr overlap](../Project%20Challenges/full-then-incr-overlap.md).
