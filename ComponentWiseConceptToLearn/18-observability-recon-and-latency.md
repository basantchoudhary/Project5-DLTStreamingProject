# Observability: reconciliation & record-level latency

Two custom reports built on top of the framework prove the pipeline is **correct**
(recon) and **fast enough** (latency). Both loop the datasets from `meta_loader`.

## 1 · Reconciliation (recon) — is UC complete vs Oracle?

**Goal.** Catch missing/extra rows: does the UC target match the live source?

**How.** Loop every dataset, get a **live count from Oracle** (JDBC) and a **count
from the UC target**, diff them, write a report row.

```
   for each dataset:
       src = SELECT count(*) FROM <oracle schema.table>        (JDBC, live)
       tgt = spark.table("bronze_scd1.<table>").count()        (active rows)
       report(dataset, src, tgt, diff = src - tgt, status = OK if 0 else MISMATCH)
```

- Compare against the **active** view of SCD1 (or SCD2 `__current = true`), and
  account for soft-deletes (`WHERE NOT is_deleted`).
- Counts are a *point-in-time* check — a streaming target may lag by seconds;
  a persistent non-zero diff is the real signal.
- Framework module: `framework/recon.py` → writes `ops.recon_report`.

## 2 · Record-level end-to-end latency (time-travel report)

**Goal.** For a record, know **when it committed in Oracle** and **when it
committed in UC** — the true end-to-end latency, per record.

**How — two commit timestamps:**
- **Oracle commit time** comes from the CDC header (Qlik stamps the change
  timestamp / SCN time) — carried through as a column.
- **UC commit time** comes from **Change Data Feed** on the target table
  (`delta.enableChangeDataFeed = true`): reading the table's CDF exposes
  `_commit_timestamp` (and `_commit_version`) per row change.

```
   latency(record) = uc_commit_ts − oracle_commit_ts

   SELECT key, _change_type, _commit_version, _commit_timestamp AS uc_commit_ts
   FROM table_changes('bronze_scd1.orders', <from_version>)
   -- join to the record's oracle commit ts (from the CDC header column)
```

- **Change Data Feed / time travel** is what makes UC commit time observable —
  you're reading *when Delta recorded the change*, not guessing.
- Produces a per-record latency you can percentile (p50/p95) and alert on against
  the [< 1 min SLA](../Project%20Challenges/end-to-end-latency.md).
- Framework module: `framework/latency_report.py` → writes `ops.latency_report`.

## Why these matter

- **Trust:** recon turns "looks fine" into a number the business can audit.
- **SLA proof:** record-level latency shows you actually hit "< 1 min", and
  *where* the time went when you don't.

**Related:** [CDC](02-change-data-capture.md), [idempotency](11-idempotency-primary-key.md),
[end-to-end latency](../Project%20Challenges/end-to-end-latency.md).
