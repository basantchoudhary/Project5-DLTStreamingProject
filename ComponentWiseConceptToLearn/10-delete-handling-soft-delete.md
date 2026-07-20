# Delete handling (hard vs soft delete)

A CDC stream carries **deletes** (`_op = 'D'`). You choose what a delete *does*
to the target.

## Hard delete (physical)

The row is physically removed (SCD1) or its version closed (SCD2).

```python
apply_changes(..., apply_as_deletes = F.expr("_op = 'D'"), stored_as_scd_type=1)
```

- Target matches the source exactly — deleted in Oracle ⇒ gone in UC.
- **Downside:** downstream consumers can't tell "deleted" from "never existed",
  and you lose the record for audit.

## Soft delete (is_deleted flag) — this project's option

Keep the row, but **mark it deleted**. Convert the **delete event into an
update** that sets `is_deleted = true` (and keeps the last known values).

```
   incoming:  _op=D  id=101
   becomes:   _op=U  id=101  is_deleted=true   (upsert, not remove)
```

- The row stays queryable; consumers filter `WHERE NOT is_deleted` for "active".
- **Audit-friendly** — you can see *when* it was deleted (via `_seq`/timestamps).
- SCD2 keeps the delete as just another version in the timeline.

## How the framework does it

Driven by `dataset_config.delete_mode` (`hard` default, `soft`):

- **processor** (`processor.py`): in soft mode, add `is_deleted = (_op == 'D')`
  and **remap** `_op` `D → U` so `apply_changes` upserts instead of deleting.
- **writer** (`writer.py`): in soft mode, **omit** `apply_as_deletes` and keep the
  `is_deleted` column in the target (don't add it to `except_column_list`).

```
   hard:  D  → apply_as_deletes → row removed / version closed
   soft:  D  → U + is_deleted=true → row retained, flagged
```

**When to pick which.** Soft delete when the business needs to *see* deletions
(compliance, reversals, analytics on churn). Hard delete when the target must be
a faithful mirror and storage/PII rules favour removal.

**Related:** [apply_changes](07-dlt-apply-changes.md), [SCD1/2](03-scd1-vs-scd2.md).
