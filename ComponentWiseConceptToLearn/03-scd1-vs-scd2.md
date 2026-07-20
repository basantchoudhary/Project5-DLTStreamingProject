# SCD Type 1 vs Type 2

**SCD = Slowly Changing Dimension** — how you handle a dimension attribute that
changes over time (a customer's city, a product's category). The two common
strategies:

## SCD Type 1 — overwrite (latest state only)

Keep **one row per key**. When it changes, overwrite. No history.

```
customer_id=7  name=Asha  city=Mumbai        -- previous "Pune" is gone
```

- **Pros:** small, simple, fast to query "what is true now".
- **Cons:** you lose the past — can't ask "what city was customer 7 in last March?"

## SCD Type 2 — versioned history

Keep **every version** of a key, each with an effective window. A change closes
the old row and opens a new one.

```
customer_id=7  name=Asha  city=Pune    __START=t1  __END=t2   __current=false
customer_id=7  name=Asha  city=Mumbai  __START=t2  __END=null __current=true
```

- **Pros:** full history; point-in-time queries; audit.
- **Cons:** more rows, more storage, queries must pick the right version.

## Side by side

| | SCD1 | SCD2 |
|---|---|---|
| Rows per key | 1 | 1 per version |
| History | no | yes |
| Delete | row removed (or flagged) | version closed |
| Use when | "current value" is enough | you need the timeline |

**In this project.** `dataset_config.scd_type = 1 / 2 / both` decides which
targets a table gets. The **same** processed CDC stream feeds:
- `bronze_scd1.<table>` via `apply_changes(stored_as_scd_type=1)`
- `bronze_scd2.<table>` via `apply_changes(stored_as_scd_type=2)`

DLT maintains the SCD2 effective-dating and `__current` flag for you — no
hand-rolled MERGE. Ordering comes from `sequence_by` (`_seq`).

**Related:** [apply_changes](07-dlt-apply-changes.md), [CDC](02-change-data-capture.md),
[delete handling](10-delete-handling-soft-delete.md).
