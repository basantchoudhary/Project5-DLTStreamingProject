# Oracle Redo Log

**What it is.** Oracle's **redo log** is a set of files where the database
records *every* change made to its data — inserts, updates, deletes, DDL — in
commit order, *before* (and as) the change is applied. It exists so Oracle can
recover to a consistent state after a crash: replay the redo log and no committed
transaction is lost.

**Why we care (CDC source).** Because the redo log is a complete, ordered history
of change, it's the ideal place to *capture* change without touching the
application tables. A tool that reads the redo log sees the same insert/update/
delete stream the database applied — this is **log-based CDC**.

```
   App commits ──► Oracle writes redo entries ──► (recovery + replication read here)
   INSERT order_id=101 ...        SCN 5001
   UPDATE order_id=101 status=... SCN 5002
   DELETE order_id=101            SCN 5003
```

`SCN` (System Change Number) is Oracle's monotonically increasing change
sequence — this is what makes ordering reliable downstream (it becomes our
`sequence_by`).

**In this project.** Qlik Replicate reads the redo log on the Oracle VM, turns
each change into a row, and lands it as parquet in ADLS `__ct` folders. We never
query Oracle tables directly — **near-zero load on the source**.

**Why log-based beats query-based CDC.**
- No `WHERE updated_at > :last` polling (which misses deletes and hard-deletes).
- Captures **deletes** (a query-based approach can't see a row that's gone).
- Minimal source impact — it reads logs the DB already writes.

**Related:** [CDC](02-change-data-capture.md), [apply_changes](07-dlt-apply-changes.md).
