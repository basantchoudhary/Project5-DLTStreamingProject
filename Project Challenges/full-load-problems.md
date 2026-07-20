# Full-load problems (rollback / undo, ORA-01555)

**Problem.** The initial **full load** of a big Oracle table (Qlik snapshot) runs
for hours and fails partway with a **rollback-segment / undo** error — classically
`ORA-01555: snapshot too old`.

**Cause.** A long-running consistent read needs Oracle to reconstruct the table
*as of the query's start* using **undo** (rollback) data. If the table is busy
and undo is recycled before the read finishes, Oracle can no longer rebuild the
old image → the read dies. Long full loads on hot tables are exactly this risk.

**Fixes / levers.**
- **Chunk the full load** — extract by key ranges / partitions so no single read
  runs long enough to outlive undo (Qlik parallel full-load / segmented load).
- **Bigger / longer-retained undo** — increase `UNDO_RETENTION` and undo tablespace
  so consistent reads survive (a DBA lever, often temporary for the load window).
- **Run during a quieter window** — less concurrent DML = less undo churn.
- **Prefer resumable, restartable loads** — so a failure resumes a chunk rather
  than restarting the whole table.
- **Seed from a copy** — snapshot/standby/export as the full source if the primary
  is too hot.

## How we actually solved it — segmentation (parallel full load)

For the high-volume tables we used **Qlik Replicate segmented full load**. Instead
of one giant consistent read of the whole table, Qlik splits the table into
**segments** on a **segmentation column** and reads each segment independently
using its **from / to** boundary values, in **parallel**.

```
   segmentation column (e.g. numeric/date key), N = 14 segments:

   seg 1 :  WHERE col >= from_1  AND col < to_1     ┐
   seg 2 :  WHERE col >= from_2  AND col < to_2     │  14 readers run
   ...                                              │  concurrently
   seg 14:  WHERE col >= from_14 AND col < to_14    ┘
        └────────► parquet to ADLS <table>__full (many files, filled faster)
```

Why this fixes the rollback/undo problem:
- **Each segment read is short** — it scans a slice, not the whole table, so no
  single read lives long enough to outlast undo → **no more ORA-01555**.
- **Total wall-clock drops** — 14 slices ingest concurrently instead of one serial
  scan, so the big table finishes in a fraction of the time.

**Sizing the segments to cores.** The Qlik VM had **16 cores**; we used **14
segments** (14 parallel readers), deliberately **leaving 2 cores** as headroom for
the OS, the Qlik control processes, and the CDC (`__ct`) task that keeps running.
Rule of thumb: `segments ≈ cores − 2` — maxing out all 16 starves the box and
actually slows the load (and can stall ongoing CDC).

**Choosing the segmentation column.** Pick a column with an **even distribution**
(a numeric surrogate key, an evenly-spread date) so segments are balanced — a
skewed column makes one segment do most of the work and kills the parallelism
benefit. Compute the from/to boundaries from min/max (or NTILE-style buckets).

**Downstream note.** Segmentation produces **more, smaller `__full` files** landing
concurrently — fine for Auto Loader, and `apply_changes` de-dups by key+seq as
usual (see [full → incr overlap](full-then-incr-overlap.md)). If file counts get
extreme, see [small files from CDC](small-files-from-cdc.md).

**In the framework.** The Databricks side is unaffected — it just ingests whatever
`__full` lands. This is a **source-extraction** problem; the fix is in how Qlik/
Oracle run the snapshot. Once `__full` is complete, `load_type=full → incr` seeds
and switches as usual.

**Related:** [reload strategies](../ComponentWiseConceptToLearn/17-reload-strategies.md),
[redo-log flood](redo-log-flood-from-maintenance.md).
