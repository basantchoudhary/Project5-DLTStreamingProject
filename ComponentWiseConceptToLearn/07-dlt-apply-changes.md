# apply_changes (DLT CDC merge)

**What it is.** `dlt.apply_changes` (aka APPLY CHANGES INTO) is DLT's built-in
operator that turns a **stream of CDC events** into a correctly-merged table —
handling inserts, updates, deletes, ordering, and SCD Type 1 or 2 — **without a
hand-written MERGE**.

## How it works

You give it:

```python
dlt.create_streaming_table("bronze_scd1.orders")
dlt.apply_changes(
    target       = "bronze_scd1.orders",
    source       = "orders_processed",       # the CDC stream (view)
    keys         = ["order_id"],             # identity of a row
    sequence_by  = F.col("_seq"),            # ordering (Qlik change seq / SCN)
    apply_as_deletes   = F.expr("_op = 'D'"),# which events are deletes
    except_column_list = ["_op","_seq", ...],# don't store framework/header cols
    stored_as_scd_type = 1,                  # 1 = latest state; 2 = history
)
```

Then, per micro-batch, for each **key** it:

1. Orders the incoming events by `sequence_by`.
2. Keeps the **latest** event per key (so out-of-order arrival is safe).
3. Applies it: insert/update the row (SCD1) or close-old + open-new version (SCD2).
4. If it matches `apply_as_deletes`, deletes (SCD1) or closes the version (SCD2).

```
   events for order_id=101:  I(seq5001) U(seq5002) D(seq5003)
   → SCD1 target: row 101 ends deleted
   → SCD2 target: two closed versions, no current row
```

## Why it's a big deal

- **No manual MERGE** for every table — one declarative call, driven by metadata.
- **Ordering built in** via `sequence_by` (see [ordering challenge](../Project%20Challenges/apply-changes-ordering.md)).
- **SCD1 and SCD2** from the same source stream (`stored_as_scd_type`).
- **Idempotent** — replays/duplicates collapse by key+seq (see [11](11-idempotency-primary-key.md)).
- **Stateful & checkpointed** — it remembers per-key state across batches.

**In this project.** `writer.py` calls `apply_changes` once per SCD target;
`keys` = `primary_keys` from `dataset_config`, `sequence_by` = `_seq` from the
processor. Deletes are driven by `_op='D'` (or soft-deleted — see [10](10-delete-handling-soft-delete.md)).

**Related:** [CDC](02-change-data-capture.md), [SCD1/2](03-scd1-vs-scd2.md),
[append_flow](08-append-flow.md).
