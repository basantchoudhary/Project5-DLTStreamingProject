# Testing framework (mock data → DLT → assert)

**Goal.** Prove the pipeline is correct *before* real data — especially the
tricky part, **SCD Type 2**. The framework generates mock CDC, runs the pipeline,
and asserts expected vs actual.

## The loop

```
   1. GENERATE mock data → parquet in ADLS (__full / __ct), per test scenario
   2. RUN the DLT pipeline over that landing data
   3. ASSERT: expected count == actual count; SCD2 shape is correct
   4. TEARDOWN / next scenario
```

- **Count check (mock):** you *know* how many rows you generated, so
  `expected_count == spark.table(target).count()` catches drops, duplicates, and
  merge bugs immediately.
- **Real test suite:** the same rules run against a curated real sample, not just
  synthetic data — catches things mock data doesn't imagine.

## SCD2 correctness rules

SCD2 is where merges go wrong, so assert its **invariants** explicitly. The
**sequence / date column is central** — it drives versioning, so most rules key
off it:

| Rule | What it checks |
|---|---|
| **One current per key** | exactly one row with `__current = true` (or open `__end`) per primary key. |
| **No overlapping windows** | for a key, version windows `[__start, __end)` don't overlap. |
| **Contiguous history** | each version's `__end` = next version's `__start` (no gaps). |
| **Ordering by sequence** | versions are ordered by the **sequence/date** column; a lower sequence never appears after a higher one. |
| **Latest = source** | the current version's attributes match the latest source change. |
| **Delete closes, not drops** | a delete closes the current window (hard) or sets `is_deleted` (soft), not a silent vanish. |
| **Count conservation** | `#versions == #distinct change events applied` for the key. |

## Why the sequence/date column is the linchpin

SCD2 effective-dating is computed **from** `sequence_by`. If it's wrong,
non-monotonic, or null, versions get mis-ordered, windows overlap, and "current"
becomes ambiguous — so the tests deliberately hammer scenarios with
out-of-order, duplicate, and same-timestamp sequences.

## In this project

A `testing/` harness generates scenario parquet, invokes the pipeline (or the
processor+writer path), and runs these assertions — mock-first for fast feedback,
plus a real-sample suite. Counts and rule results can log to `ops.*` like the rest
of the platform.

**Related:** [SCD1/2](03-scd1-vs-scd2.md), [apply_changes](07-dlt-apply-changes.md),
[late arrival](13-late-arrival-handling.md), [record drop / invalid](14-record-drop-and-invalid-records.md).
